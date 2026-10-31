//! What the `noise watch` process posts to the session's inbox socket:
//! the new messages and member changes of the session's rooms, in
//! batches.
//!
//! The code here reads no clock: the caller passes the time in and sleeps
//! until [`Delivery::deadline`].

use std::time::{Duration, Instant};

use noise_room::{Body, MessageId, PublicKey, RoomId, Timestamp, Verified};
use serde::Serialize;

/// How long the process waits for another message before it posts.
pub const QUIET: Duration = Duration::from_millis(200);

/// The longest the process delays a post while messages keep arriving.
pub const CEILING: Duration = Duration::from_secs(1);

/// The first line of every post.
const LEAD: &str = "noise: new messages from remote Claude sessions in your noise rooms. \
Each line below is one JSON object. Reply in a room with `noise send <room> <text>`.";

/// A room as a post names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRef {
    pub id: RoomId,
    pub label: String,
}

/// Something that happened in one of the session's rooms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arrival {
    /// A chat message.
    Text { room: RoomRef, message: Shown },
    /// The member `by` admitted `key` to the room with a ticket.
    Joined {
        room: RoomRef,
        key: PublicKey,
        by: String,
    },
    /// The member `key`, with the display name `name`, left the room.
    Left {
        room: RoomRef,
        key: PublicKey,
        name: String,
    },
}

/// A chat message as a post shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shown {
    pub id: MessageId,
    pub author: PublicKey,
    pub name: String,
    pub status: String,
    pub time: Timestamp,
    pub text: String,
}

impl Shown {
    /// The chat message in `message`, or `None` when it is not a `text`.
    #[must_use]
    pub fn of(message: &Verified) -> Option<Self> {
        let Body::Text { text, name, status } = &message.message().body else {
            return None;
        };
        Some(Self {
            id: message.id(),
            author: message.message().author,
            name: name.clone(),
            status: status.clone(),
            time: message.message().time,
            text: text.clone(),
        })
    }
}

/// The arrivals that wait for a post.
#[derive(Debug, Default)]
pub struct Delivery {
    pending: Vec<Arrival>,
    first: Option<Instant>,
    last: Option<Instant>,
}

impl Delivery {
    pub fn add(&mut self, arrival: Arrival, now: Instant) {
        self.pending.push(arrival);
        self.first.get_or_insert(now);
        self.last = Some(now);
    }

    /// When the pending arrivals are due: [`QUIET`] after the last one, or
    /// [`CEILING`] after the first, whichever is earlier. `None` when
    /// nothing is pending.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        Some((self.last? + QUIET).min(self.first? + CEILING))
    }

    /// The post for the pending arrivals when they are due at `now`, or
    /// `None` when they are not due.
    pub fn take(&mut self, now: Instant) -> Option<String> {
        if self.deadline()? > now {
            return None;
        }
        self.first = None;
        self.last = None;
        let lines = self.pending.drain(..).map(|arrival| arrival_line(&arrival));
        Some(
            std::iter::once(LEAD.to_owned())
                .chain(lines)
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }
}

#[derive(Serialize)]
struct MessageLine<'a> {
    room: &'a str,
    room_id: RoomId,
    id: MessageId,
    from: String,
    key: String,
    time: Timestamp,
    text: &'a str,
}

#[derive(Serialize)]
struct JoinedLine<'a> {
    room: &'a str,
    room_id: RoomId,
    joined: String,
    admitted_by: &'a str,
}

#[derive(Serialize)]
struct LeftLine<'a> {
    room: &'a str,
    room_id: RoomId,
    left: String,
    from: &'a str,
}

fn arrival_line(arrival: &Arrival) -> String {
    match arrival {
        Arrival::Text { room, message } => line(&MessageLine {
            room: &room.label,
            room_id: room.id,
            id: message.id,
            from: display_name(&message.name, &message.status),
            key: short_key(&message.author),
            time: message.time,
            text: &message.text,
        }),
        Arrival::Joined { room, key, by } => line(&JoinedLine {
            room: &room.label,
            room_id: room.id,
            joined: short_key(key),
            admitted_by: by,
        }),
        Arrival::Left { room, key, name } => line(&LeftLine {
            room: &room.label,
            room_id: room.id,
            left: short_key(key),
            from: name,
        }),
    }
}

/// `name - status`, or the part that is not empty.
#[must_use]
pub fn display_name(name: &str, status: &str) -> String {
    match (name.is_empty(), status.is_empty()) {
        (false, false) => format!("{name} - {status}"),
        (false, true) => name.to_owned(),
        (true, false) => status.to_owned(),
        (true, true) => String::new(),
    }
}

/// The first 16 hex characters of a key, which distinguish sessions with
/// the same name.
#[must_use]
pub fn short_key(key: &PublicKey) -> String {
    key.to_string()[..16].to_owned()
}

/// One JSON object. JSON escapes every newline and quote in a peer's
/// fields, so no field can end the line or imitate another one.
fn line(value: &impl Serialize) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use noise_room::{Room, RoomSecret, SecretKey};

    use super::*;

    fn room_ref(n: u8) -> RoomRef {
        let room = Room::new(format!("room {n}"), RoomSecret::from_bytes([n; 32]));
        RoomRef {
            id: room.id(),
            label: room.label().to_owned(),
        }
    }

    /// A text by key 9, sealed in a room of its own.
    fn shown(text: &str) -> Shown {
        let room = Room::new("texts".into(), RoomSecret::from_bytes([9; 32]));
        let body = Body::Text {
            text: text.into(),
            name: "vetle".into(),
            status: "CI optimization".into(),
        };
        let (message, _) = room
            .seal(&SecretKey::from_bytes(&[9; 32]), body, Timestamp(5))
            .expect("seal");
        Shown::of(&message).expect("a text")
    }

    fn text(room: &RoomRef, text: &str) -> Arrival {
        Arrival::Text {
            room: room.clone(),
            message: shown(text),
        }
    }

    fn lines(post: &str) -> Vec<serde_json::Value> {
        post.lines()
            .skip(1)
            .map(|line| serde_json::from_str(line).expect("json"))
            .collect()
    }

    #[test]
    fn a_post_is_due_after_the_quiet_time() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        assert_eq!(delivery.deadline(), None);
        delivery.add(text(&room_ref(1), "one"), start);
        assert_eq!(delivery.deadline(), Some(start + QUIET));
        assert_eq!(delivery.take(start + QUIET / 2), None);
        delivery.add(text(&room_ref(1), "two"), start + QUIET / 2);
        assert_eq!(delivery.deadline(), Some(start + QUIET / 2 + QUIET));
        let post = delivery
            .take(start + QUIET / 2 + QUIET)
            .expect("a due post");
        assert_eq!(lines(&post).len(), 2);
        assert_eq!(delivery.deadline(), None);
    }

    #[test]
    fn steady_messages_post_by_the_ceiling() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        let step = QUIET / 2;
        let mut now = start;
        while now < start + CEILING {
            delivery.add(text(&room_ref(1), &format!("{now:?}")), now);
            now += step;
        }
        assert_eq!(delivery.deadline(), Some(start + CEILING));
    }

    #[test]
    fn a_line_shows_the_message_and_its_author() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        delivery.add(text(&room_ref(1), "line one\nline two \"quoted\""), start);
        let post = delivery.take(start + QUIET).expect("a due post");
        let mut post_lines = post.lines();
        assert_eq!(post_lines.next(), Some(LEAD));
        assert_eq!(post_lines.count(), 1);
        let line = &lines(&post)[0];
        assert_eq!(line["room"], "room 1");
        assert_eq!(line["from"], "vetle - CI optimization");
        assert_eq!(line["text"], "line one\nline two \"quoted\"");
        assert_eq!(line["key"].as_str().expect("string").len(), 16);
    }

    #[test]
    fn a_join_and_a_leave_post_as_lines() {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        let (room, key) = (room_ref(1), SecretKey::from_bytes(&[4; 32]).public());
        let joined = Arrival::Joined {
            room: room.clone(),
            key,
            by: "vetle".into(),
        };
        let left = Arrival::Left {
            room,
            key,
            name: "colleague".into(),
        };
        delivery.add(joined, start);
        delivery.add(left, start);
        let post = lines(&delivery.take(start + QUIET).expect("a due post"));
        assert_eq!(post[0]["joined"], short_key(&key));
        assert_eq!(post[0]["admitted_by"], "vetle");
        assert_eq!(post[1]["left"], short_key(&key));
        assert_eq!(post[1]["from"], "colleague");
    }

    #[test]
    fn display_names_join_name_and_status() {
        assert_eq!(display_name("vetle", "CI"), "vetle - CI");
        assert_eq!(display_name("vetle", ""), "vetle");
        assert_eq!(display_name("", "CI"), "CI");
    }
}
