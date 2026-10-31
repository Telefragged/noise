//! The handling of the messages of a room's gossip topic, without I/O:
//! the node passes each gossip message in and receives the room message
//! that it completes.

use noise_room::{PublicKey, Room, Verified};

use crate::chunk::{self, Reassembly};

/// The gossip state of one room: the chunks of incomplete messages.
#[derive(Debug)]
pub(crate) struct Inbound {
    reassembly: Reassembly,
}

impl Inbound {
    pub(crate) fn new(max_message_size: usize) -> Self {
        Self {
            reassembly: Reassembly::new(max_message_size),
        }
    }

    /// A gossip message of `room` arrived from the neighbor `from`.
    /// Returns the room message that it completes. A message that does not
    /// open with the room's key is dropped.
    pub(crate) fn received(
        &mut self,
        payload: &[u8],
        from: PublicKey,
        room: &Room,
    ) -> Option<Verified> {
        let chunk = chunk::parse(payload)?;
        let sealed = self.reassembly.add(chunk, from)?;
        room.open(&sealed).ok()
    }
}

#[cfg(test)]
mod tests {
    use noise_room::{Body, RoomSecret, SecretKey, Timestamp};

    use super::*;

    const MAX: usize = 4096;

    fn room(n: u8) -> Room {
        Room::new("ci".into(), RoomSecret::from_bytes([n; 32]))
    }

    /// A text of `size` bytes by key 1, sealed for room 1.
    fn sealed(size: usize) -> (Verified, Vec<u8>) {
        let body = Body::Text {
            text: "x".repeat(size),
            name: String::new(),
            status: String::new(),
        };
        room(1)
            .seal(&SecretKey::from_bytes(&[1; 32]), body, Timestamp(0))
            .expect("seal")
    }

    fn from() -> PublicKey {
        SecretKey::from_bytes(&[3; 32]).public()
    }

    #[test]
    fn a_message_in_many_chunks_is_returned_once_complete() {
        let (text, sealed) = sealed(30_000);
        let mut inbound = Inbound::new(MAX);
        let payloads = chunk::split(&sealed, MAX);
        assert!(payloads.len() > 1);
        let (last, rest) = payloads.split_last().expect("chunks");
        for payload in rest {
            assert!(inbound.received(payload, from(), &room(1)).is_none());
        }
        let message = inbound.received(last, from(), &room(1)).expect("message");
        assert_eq!(message.id(), text.id());
    }

    #[test]
    fn another_rooms_message_is_dropped() {
        let payload = chunk::split(&sealed(10).1, MAX).remove(0);
        assert!(
            Inbound::new(MAX)
                .received(&payload, from(), &room(2))
                .is_none()
        );
    }

    #[test]
    fn a_corrupt_message_is_dropped() {
        let mut payload = chunk::split(&sealed(10).1, MAX).remove(0);
        let last = payload.len() - 1;
        payload[last] ^= 1;
        assert!(
            Inbound::new(MAX)
                .received(&payload, from(), &room(1))
                .is_none()
        );
    }
}
