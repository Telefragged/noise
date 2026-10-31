use iroh_base::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::ids::{MessageId, RoomId, Timestamp};

/// The largest message, its bytes and its signature together.
pub const MAX_SIGNED_SIZE: usize = 64 * 1024;

/// Precedes a message's bytes in what its author signs, so that the
/// signature cannot pass for one over other data that the same key signs,
/// such as iroh's address records.
const SIGNING_CONTEXT: &[u8] = b"noise/msg/v1\0";

/// A message of a room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// The room. A member of two rooms cannot pass a message of one off
    /// as a message of the other.
    pub room: RoomId,
    /// The key of the session that wrote and signed the message.
    pub author: PublicKey,
    pub time: Timestamp,
    pub body: Body,
}

/// A member of a room, as the session's list of members has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub key: PublicKey,
    /// The name and status from the member's newest message, empty until
    /// one arrives.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Body {
    /// A chat message, with its author's name and status when it was sent.
    Text {
        text: String,
        name: String,
        status: String,
    },
    /// The current name and status of the message's author.
    Profile { name: String, status: String },
    /// The message's author admitted `key` to the room with a ticket.
    Admit { key: PublicKey },
    /// The message's author left the room.
    Leave,
    /// A body that a later version of noise added, which this version
    /// ignores.
    #[serde(other)]
    Unknown,
}

/// A message's exact bytes and its author's signature over them.
///
/// noise stores and sends these bytes as they are and never serializes a
/// received message again, so its id and signature stay valid in every
/// version.
#[derive(Debug, Clone)]
pub struct SignedMessage {
    bytes: Vec<u8>,
    signature: Signature,
}

/// Why a [`SignedMessage`] is not a valid message.
#[derive(Debug, thiserror::Error)]
pub enum Invalid {
    #[error("the message is {size} bytes, more than {MAX_SIGNED_SIZE}")]
    TooLarge { size: usize },
    #[error("the message is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("the signature does not match the author")]
    BadSignature,
    #[error("the payload is not sealed with the room's key")]
    NotSealed,
    #[error("the message belongs to another room")]
    OtherRoom,
}

impl SignedMessage {
    #[must_use]
    pub fn new(bytes: Vec<u8>, signature: Signature) -> Self {
        Self { bytes, signature }
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The size that [`MAX_SIGNED_SIZE`] limits.
    #[must_use]
    pub fn size(&self) -> usize {
        self.bytes.len() + Signature::LENGTH
    }

    /// The bytes followed by the signature, the form in which a file or a
    /// chunk carries a message.
    #[must_use]
    pub fn to_concat(&self) -> Vec<u8> {
        [&self.bytes[..], &self.signature.to_bytes()].concat()
    }

    /// The message in `data`, which [`SignedMessage::to_concat`] wrote, or
    /// `None` when `data` is shorter than a signature.
    #[must_use]
    pub fn from_concat(data: &[u8]) -> Option<Self> {
        let split = data.len().checked_sub(Signature::LENGTH)?;
        let signature = Signature::try_from(&data[split..]).ok()?;
        Some(Self::new(data[..split].to_vec(), signature))
    }

    /// Checks the size, the format and the author's signature.
    ///
    /// # Errors
    ///
    /// When the message is too large, malformed, or not signed by its
    /// author.
    pub fn verify(self) -> Result<Verified, Invalid> {
        let message: Message = serde_json::from_slice(&self.bytes)?;
        message
            .author
            .verify(&signed_data(&self.bytes), &self.signature)
            .map_err(|_| Invalid::BadSignature)?;
        Verified::new(message, self)
    }
}

/// A message whose size, format and signature [`SignedMessage::verify`]
/// checked. Only a `Verified` can enter a [`Room`](crate::Room).
#[derive(Debug, Clone)]
pub struct Verified {
    id: MessageId,
    message: Message,
    signed: SignedMessage,
}

impl Verified {
    /// Checks the size and computes the id of `signed`, the signed form of
    /// `message`.
    fn new(message: Message, signed: SignedMessage) -> Result<Self, Invalid> {
        let size = signed.size();
        if size > MAX_SIGNED_SIZE {
            return Err(Invalid::TooLarge { size });
        }
        Ok(Self {
            id: MessageId(*blake3::hash(&signed.bytes).as_bytes()),
            message,
            signed,
        })
    }

    #[must_use]
    pub fn id(&self) -> MessageId {
        self.id
    }

    #[must_use]
    pub fn message(&self) -> &Message {
        &self.message
    }

    #[must_use]
    pub fn signed(&self) -> &SignedMessage {
        &self.signed
    }
}

/// Signs `message` with `key`, which must be the message's author.
///
/// # Errors
///
/// When the signed message is larger than [`MAX_SIGNED_SIZE`].
pub(crate) fn sign(message: Message, key: &SecretKey) -> Result<Verified, Invalid> {
    debug_assert_eq!(message.author, key.public());
    let bytes = serde_json::to_vec(&message)?;
    let signature = key.sign(&signed_data(&bytes));
    Verified::new(message, SignedMessage { bytes, signature })
}

fn signed_data(bytes: &[u8]) -> Vec<u8> {
    [SIGNING_CONTEXT, bytes].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u8) -> SecretKey {
        SecretKey::from_bytes(&[n; 32])
    }

    fn text(author: &SecretKey, text: &str) -> Message {
        Message {
            room: RoomId([9; 32]),
            author: author.public(),
            time: Timestamp(1),
            body: Body::Text {
                text: text.into(),
                name: "vetle".into(),
                status: String::new(),
            },
        }
    }

    #[test]
    fn a_message_round_trips_through_its_concatenated_form() {
        let signed = sign(text(&key(1), "hi"), &key(1)).expect("sign");
        let back =
            SignedMessage::from_concat(&signed.signed().to_concat()).expect("concatenated form");
        assert_eq!(back.verify().expect("verify").id(), signed.id());
        assert!(SignedMessage::from_concat(&[0; 10]).is_none());
    }

    #[test]
    fn a_signed_message_verifies() {
        let signed = sign(text(&key(1), "hi"), &key(1)).expect("sign");
        let verified = signed.signed().clone().verify().expect("verify");
        assert_eq!(verified.id(), signed.id());
        assert_eq!(verified.message(), signed.message());
    }

    #[test]
    fn changed_bytes_fail_the_signature() {
        let signed = sign(text(&key(1), "hi"), &key(1)).expect("sign");
        let mut bytes = signed.signed().bytes().to_vec();
        let at = bytes
            .windows(2)
            .position(|w| w == b"hi")
            .expect("text in bytes");
        bytes[at] = b'H';
        let forged = SignedMessage::new(bytes, *signed.signed().signature());
        assert!(matches!(forged.verify(), Err(Invalid::BadSignature)));
    }

    #[test]
    fn another_key_fails_the_signature() {
        let message = text(&key(1), "hi");
        let bytes = serde_json::to_vec(&message).expect("serialize");
        let signature = key(2).sign(&signed_data(&bytes));
        let forged = SignedMessage::new(bytes, signature);
        assert!(matches!(forged.verify(), Err(Invalid::BadSignature)));
    }

    #[test]
    fn a_signature_without_the_context_fails() {
        let bytes = serde_json::to_vec(&text(&key(1), "hi")).expect("serialize");
        let signature = key(1).sign(&bytes);
        let forged = SignedMessage::new(bytes, signature);
        assert!(matches!(forged.verify(), Err(Invalid::BadSignature)));
    }

    #[test]
    fn a_message_over_the_limit_is_rejected() {
        let large = "x".repeat(MAX_SIGNED_SIZE);
        assert!(matches!(
            sign(text(&key(1), &large), &key(1)),
            Err(Invalid::TooLarge { .. })
        ));
        let bytes = serde_json::to_vec(&text(&key(1), &large)).expect("serialize");
        let signature = key(1).sign(&signed_data(&bytes));
        assert!(matches!(
            SignedMessage::new(bytes, signature).verify(),
            Err(Invalid::TooLarge { .. })
        ));
    }

    #[test]
    fn an_unknown_body_parses_as_unknown() {
        let author = key(1).public();
        let json = format!(
            r#"{{"room":"{}","author":"{author}","time":5,"body":{{"type":"poll","options":["a","b"]}}}}"#,
            RoomId([9; 32])
        );
        let bytes = json.into_bytes();
        let signature = key(1).sign(&signed_data(&bytes));
        let verified = SignedMessage::new(bytes, signature)
            .verify()
            .expect("verify");
        assert_eq!(verified.message().body, Body::Unknown);
    }
}
