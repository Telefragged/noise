use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use iroh_base::SecretKey;

use crate::ids::{RoomId, RoomSecret, Timestamp, TopicId};
use crate::message::{Body, Invalid, MAX_SIGNED_SIZE, Message, SignedMessage, Verified, sign};

/// The bytes that sealing adds to a signed message: the nonce and the
/// authentication tag.
const SEAL_OVERHEAD: usize = 24 + 16;

/// The largest sealed message.
pub const MAX_SEALED_SIZE: usize = MAX_SIGNED_SIZE + SEAL_OVERHEAD;

/// A room as a session has it: a label and the room secret.
///
/// A message travels sealed: signed by its author, then encrypted with
/// XChaCha20-Poly1305 under a key that derives from the secret. Only a
/// member, which has the secret, can read or write it, and the signature
/// keeps a member from writing as another one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Room {
    label: String,
    secret: RoomSecret,
}

impl Room {
    #[must_use]
    pub fn new(label: String, secret: RoomSecret) -> Self {
        Self { label, secret }
    }

    #[must_use]
    pub fn id(&self) -> RoomId {
        self.secret.id()
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub fn secret(&self) -> &RoomSecret {
        &self.secret
    }

    #[must_use]
    pub fn topic(&self) -> TopicId {
        self.secret.topic()
    }

    /// Signs a message of `key` with `body` and seals it. Returns the
    /// message and the sealed bytes to broadcast.
    ///
    /// The nonce is a keyed hash of the signed message, so sealing needs
    /// no randomness and the same message always seals the same way.
    ///
    /// # Errors
    ///
    /// When the message is larger than [`MAX_SIGNED_SIZE`].
    pub fn seal(
        &self,
        key: &SecretKey,
        body: Body,
        now: Timestamp,
    ) -> Result<(Verified, Vec<u8>), Invalid> {
        let message = sign(
            Message {
                room: self.id(),
                author: key.public(),
                time: now,
                body,
            },
            key,
        )?;
        let plain = message.signed().to_concat();
        let hash = blake3::keyed_hash(&self.secret.derive("noise nonce key v1"), &plain);
        let nonce = XNonce::from_slice(&hash.as_bytes()[..24]);
        let sealed = self
            .cipher()
            .encrypt(nonce, plain.as_slice())
            .map_err(|_| Invalid::NotSealed)?;
        Ok((message, [nonce.as_slice(), &sealed].concat()))
    }

    /// Decrypts and checks `sealed`, which [`Room::seal`] wrote.
    ///
    /// # Errors
    ///
    /// When another key sealed the bytes, or the message in them is
    /// invalid or belongs to another room.
    pub fn open(&self, sealed: &[u8]) -> Result<Verified, Invalid> {
        if sealed.len() > MAX_SEALED_SIZE || sealed.len() < SEAL_OVERHEAD {
            return Err(Invalid::NotSealed);
        }
        let (nonce, ciphertext) = sealed.split_at(24);
        let plain = self
            .cipher()
            .decrypt(XNonce::from_slice(nonce), ciphertext)
            .map_err(|_| Invalid::NotSealed)?;
        let message = SignedMessage::from_concat(&plain)
            .ok_or(Invalid::NotSealed)?
            .verify()?;
        if message.message().room != self.id() {
            return Err(Invalid::OtherRoom);
        }
        Ok(message)
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(&self.secret.derive("noise message key v1").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u8) -> SecretKey {
        SecretKey::from_bytes(&[n; 32])
    }

    fn room(n: u8) -> Room {
        Room::new(format!("room {n}"), RoomSecret::from_bytes([n; 32]))
    }

    fn text(text: &str) -> Body {
        Body::Text {
            text: text.into(),
            name: "vetle".into(),
            status: String::new(),
        }
    }

    #[test]
    fn a_sealed_message_opens_in_its_room() {
        let (sent, sealed) = room(1)
            .seal(&key(1), text("hi"), Timestamp(5))
            .expect("seal");
        let opened = room(1).open(&sealed).expect("open");
        assert_eq!(opened.id(), sent.id());
        assert_eq!(opened.message().body, text("hi"));
        assert_eq!(opened.message().author, key(1).public());
    }

    #[test]
    fn sealing_is_deterministic_and_hides_the_text() {
        let (_, one) = room(1)
            .seal(&key(1), text("secret"), Timestamp(5))
            .expect("seal");
        let (_, two) = room(1)
            .seal(&key(1), text("secret"), Timestamp(5))
            .expect("seal");
        assert_eq!(one, two);
        assert!(!one.windows(6).any(|window| window == b"secret"));
    }

    #[test]
    fn another_rooms_key_does_not_open_it() {
        let (_, sealed) = room(1)
            .seal(&key(1), text("hi"), Timestamp(5))
            .expect("seal");
        assert!(matches!(room(2).open(&sealed), Err(Invalid::NotSealed)));
    }

    #[test]
    fn a_changed_byte_does_not_open() {
        let (_, mut sealed) = room(1)
            .seal(&key(1), text("hi"), Timestamp(5))
            .expect("seal");
        let last = sealed.len() - 1;
        sealed[last] ^= 1;
        assert!(matches!(room(1).open(&sealed), Err(Invalid::NotSealed)));
        assert!(matches!(
            room(1).open(&sealed[..10]),
            Err(Invalid::NotSealed)
        ));
    }

    #[test]
    fn a_message_of_one_room_sealed_for_another_does_not_open() {
        // A member of both rooms seals room 1's signed message with room
        // 2's key.
        let (sent, _) = room(1)
            .seal(&key(1), text("hi"), Timestamp(5))
            .expect("seal");
        let plain = sent.signed().to_concat();
        let nonce = XNonce::from_slice(&[0; 24]);
        let sealed = room(2)
            .cipher()
            .encrypt(nonce, plain.as_slice())
            .expect("encrypt");
        let moved = [nonce.as_slice(), &sealed].concat();
        assert!(matches!(room(2).open(&moved), Err(Invalid::OtherRoom)));
    }

    #[test]
    fn the_largest_message_seals_within_the_limit() {
        let base = room(1)
            .seal(&key(1), text(""), Timestamp(5))
            .expect("seal")
            .0;
        let fill = MAX_SIGNED_SIZE - base.signed().size();
        let (_, sealed) = room(1)
            .seal(&key(1), text(&"x".repeat(fill)), Timestamp(5))
            .expect("seal");
        assert_eq!(sealed.len(), MAX_SEALED_SIZE);
        assert!(room(1).open(&sealed).is_ok());
        assert!(matches!(
            room(1).seal(&key(1), text(&"x".repeat(fill + 1)), Timestamp(5)),
            Err(Invalid::TooLarge { .. })
        ));
    }
}
