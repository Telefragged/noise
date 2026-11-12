//! The ids of a room's recent messages. With them a session accepts each
//! message once, so that the bytes of a message that are sent again later
//! do not pass for a new message.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::ids::{MessageId, Timestamp};
use crate::message::Verified;

/// [`MAX_TIME_DIFFERENCE`] in milliseconds, the unit of a [`Timestamp`].
const MAX_DIFFERENCE_MS: u64 = 300_000;

/// The most that a message's time may differ from the clock of the
/// session that receives it.
///
/// iroh-gossip drops a gossip message that arrives again only for 90
/// seconds after its first arrival.
pub const MAX_TIME_DIFFERENCE: Duration = Duration::from_millis(MAX_DIFFERENCE_MS);

/// The times and ids of the messages that a session accepted in a room,
/// for as long as the session's clock is within [`MAX_TIME_DIFFERENCE`] of
/// each message's time.
#[derive(Debug, Default)]
pub struct RecentIds {
    ids: BTreeSet<(Timestamp, MessageId)>,
}

/// Why [`RecentIds::accept`] rejects a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Rejected {
    #[error("the message's time is {} s before the session's clock", .by.as_secs())]
    Past { by: Duration },
    #[error("the message's time is {} s after the session's clock", .by.as_secs())]
    Future { by: Duration },
    #[error("the message arrived before")]
    Repeated,
}

impl RecentIds {
    /// Records `message`, which arrives when the session's clock reads
    /// `now`.
    ///
    /// # Errors
    ///
    /// When the message's time differs from `now` by more than
    /// [`MAX_TIME_DIFFERENCE`], or an earlier call recorded the message.
    pub fn accept(&mut self, message: &Verified, now: Timestamp) -> Result<(), Rejected> {
        let time = message.message().time;
        let by = Duration::from_millis(time.0.abs_diff(now.0));
        if by > MAX_TIME_DIFFERENCE {
            return Err(if time < now {
                Rejected::Past { by }
            } else {
                Rejected::Future { by }
            });
        }
        self.expire(now);
        if self.ids.insert((time, message.id())) {
            Ok(())
        } else {
            Err(Rejected::Repeated)
        }
    }

    /// The first time of the session's clock at which the oldest recorded
    /// message is too old for [`RecentIds::accept`], so that its id is not
    /// needed again. `None` when no message is recorded.
    #[must_use]
    pub fn deadline(&self) -> Option<Timestamp> {
        let (oldest, _) = self.ids.first()?;
        Some(Timestamp(oldest.0.saturating_add(MAX_DIFFERENCE_MS + 1)))
    }

    /// Drops the ids of the messages that are too old at `now`.
    pub fn expire(&mut self, now: Timestamp) {
        while self.deadline().is_some_and(|deadline| deadline <= now) {
            self.ids.pop_first();
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh_base::SecretKey;

    use super::*;
    use crate::ids::RoomId;
    use crate::message::{Body, Message, sign};

    /// The time of the test's messages, far enough from zero for a time
    /// before it.
    const TIME: u64 = 1_000_000;

    /// A message with `text` and the time `time`.
    fn message(text: &str, time: u64) -> Verified {
        let key = SecretKey::from_bytes(&[1; 32]);
        let message = Message {
            room: RoomId([9; 32]),
            author: key.public(),
            time: Timestamp(time),
            body: Body::Text {
                text: text.into(),
                name: String::new(),
                status: String::new(),
            },
        };
        sign(message, &key).expect("sign")
    }

    #[test]
    fn a_message_is_accepted_once() {
        let mut recent = RecentIds::default();
        let (one, two) = (message("one", TIME), message("two", TIME));
        assert_eq!(recent.accept(&one, Timestamp(TIME)), Ok(()));
        assert_eq!(recent.accept(&two, Timestamp(TIME)), Ok(()));
        assert_eq!(
            recent.accept(&one, Timestamp(TIME + 1)),
            Err(Rejected::Repeated)
        );
        assert_eq!(
            recent.accept(&one, Timestamp(TIME + MAX_DIFFERENCE_MS)),
            Err(Rejected::Repeated)
        );
    }

    #[test]
    fn a_time_at_the_limit_is_accepted_and_one_past_it_is_not() {
        let mut recent = RecentIds::default();
        let now = Timestamp(TIME);
        let limit = MAX_DIFFERENCE_MS;
        assert_eq!(recent.accept(&message("a", TIME - limit), now), Ok(()));
        assert_eq!(recent.accept(&message("b", TIME + limit), now), Ok(()));
        let by = Duration::from_millis(limit + 1);
        assert_eq!(
            recent.accept(&message("c", TIME - limit - 1), now),
            Err(Rejected::Past { by })
        );
        assert_eq!(
            recent.accept(&message("d", TIME + limit + 1), now),
            Err(Rejected::Future { by })
        );
    }

    #[test]
    fn a_message_that_arrives_again_after_the_limit_is_too_old() {
        let mut recent = RecentIds::default();
        let old = message("old", TIME);
        assert_eq!(recent.accept(&old, Timestamp(TIME)), Ok(()));
        let later = TIME + MAX_DIFFERENCE_MS + 1;
        assert_eq!(
            recent.accept(&old, Timestamp(later)),
            Err(Rejected::Past {
                by: Duration::from_millis(MAX_DIFFERENCE_MS + 1)
            })
        );
        // The next accepted message drops the old one's id.
        assert_eq!(
            recent.accept(&message("new", later), Timestamp(later)),
            Ok(())
        );
        assert_eq!(recent.ids.len(), 1);
    }

    #[test]
    fn each_id_expires_when_its_message_is_too_old() {
        let mut recent = RecentIds::default();
        assert_eq!(recent.deadline(), None);
        let (one, two) = (message("one", TIME), message("two", TIME + 10));
        assert_eq!(recent.accept(&one, Timestamp(TIME)), Ok(()));
        assert_eq!(recent.accept(&two, Timestamp(TIME + 10)), Ok(()));
        let deadline = TIME + MAX_DIFFERENCE_MS + 1;
        assert_eq!(recent.deadline(), Some(Timestamp(deadline)));

        recent.expire(Timestamp(deadline - 1));
        assert_eq!(recent.ids.len(), 2);
        recent.expire(Timestamp(deadline));
        assert_eq!(recent.ids.len(), 1);
        assert_eq!(recent.deadline(), Some(Timestamp(deadline + 10)));
        recent.expire(Timestamp(deadline + 10));
        assert_eq!(recent.deadline(), None);
    }
}
