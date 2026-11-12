//! The handling of the messages of a room's gossip topic, without I/O:
//! the node passes each gossip message in with the time, and receives
//! the room message that it completes.

use std::time::Duration;

use noise_room::{PublicKey, RecentIds, Rejected, Room, Timestamp, Verified};

use crate::chunk::{self, Reassembly};

/// The gossip state of one room: the chunks of incomplete messages, and
/// the ids of the recent complete ones.
///
/// The task that handles the room's topic owns it, so it is in memory
/// only: a `noise watch` process that starts again has no ids. The task
/// sleeps for [`Inbound::wait`] and then calls [`Inbound::expire`], which
/// drops each id once its message is too old to arrive again.
#[derive(Debug)]
pub(crate) struct Inbound {
    reassembly: Reassembly,
    recent: RecentIds,
}

impl Inbound {
    pub(crate) fn new(max_message_size: usize) -> Self {
        Self {
            reassembly: Reassembly::new(max_message_size),
            recent: RecentIds::default(),
        }
    }

    /// A gossip message of `room` arrived from the neighbor `from` when
    /// the session's clock read `now`. Returns the room message that it
    /// completes, or `None` when it completes none. A message that does
    /// not open with the room's key is dropped.
    ///
    /// # Errors
    ///
    /// When the room message arrived before, or its time is too far from
    /// `now`: the message, and why [`RecentIds::accept`] rejects it.
    pub(crate) fn received(
        &mut self,
        payload: &[u8],
        from: PublicKey,
        room: &Room,
        now: Timestamp,
    ) -> Option<Result<Verified, (Verified, Rejected)>> {
        let chunk = chunk::parse(payload)?;
        let sealed = self.reassembly.add(chunk, from)?;
        let message = room.open(&sealed).ok()?;
        Some(match self.recent.accept(&message, now) {
            Ok(()) => Ok(message),
            Err(why) => Err((message, why)),
        })
    }

    /// How long after `now` the oldest id is due for
    /// [`Inbound::expire`]. `None` when there is no id.
    pub(crate) fn wait(&self, now: Timestamp) -> Option<Duration> {
        let deadline = self.recent.deadline()?;
        Some(Duration::from_millis(deadline.0.saturating_sub(now.0)))
    }

    /// Drops the ids of the messages that are too old at `now`.
    pub(crate) fn expire(&mut self, now: Timestamp) {
        self.recent.expire(now);
    }
}

#[cfg(test)]
mod tests {
    use noise_room::{Body, MAX_TIME_DIFFERENCE, RoomSecret, SecretKey};

    use super::*;

    const MAX: usize = 4096;

    fn room(n: u8) -> Room {
        Room::new("ci".into(), RoomSecret::from_bytes([n; 32]))
    }

    /// The time of the tests' messages, and of the clock when they arrive.
    const NOW: Timestamp = Timestamp(0);

    /// A text of `size` bytes by key 1 at [`NOW`], sealed for room 1.
    fn sealed(size: usize) -> (Verified, Vec<u8>) {
        let body = Body::Text {
            text: "x".repeat(size),
            name: String::new(),
            status: String::new(),
        };
        room(1)
            .seal(&SecretKey::from_bytes(&[1; 32]), body, NOW)
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
            assert!(inbound.received(payload, from(), &room(1), NOW).is_none());
        }
        let message = inbound
            .received(last, from(), &room(1), NOW)
            .expect("complete")
            .expect("message");
        assert_eq!(message.id(), text.id());
    }

    #[test]
    fn a_message_that_arrives_again_is_rejected() {
        let (text, sealed) = sealed(10);
        let payload = chunk::split(&sealed, MAX).remove(0);
        let mut inbound = Inbound::new(MAX);
        let first = inbound.received(&payload, from(), &room(1), NOW);
        assert!(matches!(first, Some(Ok(_))));
        // Another neighbor sends the same bytes later.
        let other = SecretKey::from_bytes(&[4; 32]).public();
        let again = inbound.received(&payload, other, &room(1), Timestamp(NOW.0 + 1));
        let (message, why) = again.expect("complete").expect_err("rejected");
        assert_eq!((message.id(), why), (text.id(), Rejected::Repeated));
    }

    #[test]
    fn the_wait_ends_when_the_oldest_message_is_too_old() {
        let payload = chunk::split(&sealed(10).1, MAX).remove(0);
        let mut inbound = Inbound::new(MAX);
        assert_eq!(inbound.wait(NOW), None);
        assert!(inbound.received(&payload, from(), &room(1), NOW).is_some());
        let wait = MAX_TIME_DIFFERENCE + Duration::from_millis(1);
        assert_eq!(inbound.wait(NOW), Some(wait));
        let later = Timestamp(NOW.0 + 1);
        assert_eq!(inbound.wait(later), Some(MAX_TIME_DIFFERENCE));

        let due = Timestamp(u64::try_from(wait.as_millis()).expect("milliseconds"));
        assert_eq!(inbound.wait(due), Some(Duration::ZERO));
        inbound.expire(due);
        assert_eq!(inbound.wait(due), None);
    }

    #[test]
    fn a_message_with_a_time_far_from_the_clock_is_rejected() {
        let payload = chunk::split(&sealed(10).1, MAX).remove(0);
        let late = MAX_TIME_DIFFERENCE + Duration::from_millis(1);
        let now = Timestamp(u64::try_from(late.as_millis()).expect("milliseconds"));
        let rejected = Inbound::new(MAX).received(&payload, from(), &room(1), now);
        let (_, why) = rejected.expect("complete").expect_err("rejected");
        assert_eq!(why, Rejected::Past { by: late });
    }

    #[test]
    fn another_rooms_message_is_dropped() {
        let payload = chunk::split(&sealed(10).1, MAX).remove(0);
        assert!(
            Inbound::new(MAX)
                .received(&payload, from(), &room(2), NOW)
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
                .received(&payload, from(), &room(1), NOW)
                .is_none()
        );
    }
}
