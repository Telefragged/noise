//! What the `noise watch` process posts to the session's inbox socket:
//! the new messages and member changes of the session's rooms, in
//! batches.
//!
//! The code here reads no clock: the caller passes the time in and sleeps
//! until [`Delivery::deadline`].

use std::time::{Duration, Instant};

use noise_room::{PublicKey, RoomId};

/// How long the process waits for another message before it posts.
pub const QUIET: Duration = Duration::from_millis(200);

/// The longest the process delays a post while messages keep arriving.
pub const CEILING: Duration = Duration::from_secs(1);

/// The first line of every post.
const LEAD: &str =
    "noise: new messages from peer Claude sessions. To reply, follow the noise:noise skill.";

/// What each line end of a text becomes: the line end and an indent. A
/// line of a post that starts with the indent is text from a peer, and no
/// other line is.
const TEXT_LINE_END: &str = "\n    ";

/// A room as a post names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRef {
    pub id: RoomId,
    pub label: String,
}

/// Something that happened in a room of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    pub room: RoomRef,
    pub event: Event,
}

/// What happened in the room. Each name is a member's display name, or
/// its short key when it has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The member `from` sent the chat message `text`.
    Text { from: String, text: String },
    /// The member `by` admitted a session to the room with a ticket.
    Joined { by: String },
    /// A member changed its name from `from` to `to`.
    Renamed { from: String, to: String },
    /// The member `name` left the room.
    Left { name: String },
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
        let mut post = LEAD.to_owned();
        let mut current = None;
        for Arrival { room, event } in self.pending.drain(..) {
            if current != Some(room.id) {
                current = Some(room.id);
                post.push_str("\nNoise - ");
                clean_into(&mut post, &room.label, " ");
            }
            post.push('\n');
            push_event(&mut post, &event);
        }
        Some(post)
    }
}

/// Adds the line of `event` to `post`.
fn push_event(post: &mut String, event: &Event) {
    let one_line = |post: &mut String, text: &str| clean_into(post, text, " ");
    match event {
        Event::Text { from, text } => {
            one_line(post, from);
            post.push_str(": ");
            clean_into(post, text, TEXT_LINE_END);
        }
        Event::Joined { by } => {
            post.push_str("a session joined, admitted by ");
            one_line(post, by);
        }
        Event::Renamed { from, to } => {
            one_line(post, from);
            post.push_str(" changed its name to ");
            one_line(post, to);
        }
        Event::Left { name } => {
            one_line(post, name);
            post.push_str(" left");
        }
    }
}

/// Adds `text` to `post`, with `line_end` for each line end and a space
/// for every other control character, so that the text ends a line only
/// where `line_end` does.
fn clean_into(post: &mut String, text: &str, line_end: &str) {
    post.reserve(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' | '\u{2028}' | '\u{2029}' => post.push_str(line_end),
            '\t' => post.push(c),
            c if c.is_control() => post.push(' '),
            c => post.push(c),
        }
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

#[cfg(test)]
mod tests {
    use noise_room::{Room, RoomSecret};

    use super::*;

    fn room_ref(n: u8) -> RoomRef {
        let room = Room::new(format!("room {n}"), RoomSecret::from_bytes([n; 32]));
        RoomRef {
            id: room.id(),
            label: room.label().to_owned(),
        }
    }

    fn arrival(room: &RoomRef, event: Event) -> Arrival {
        Arrival {
            room: room.clone(),
            event,
        }
    }

    fn text(room: &RoomRef, text: &str) -> Arrival {
        let event = Event::Text {
            from: "vetle - CI optimization".into(),
            text: text.into(),
        };
        arrival(room, event)
    }

    /// The post of `arrivals`, without its first line.
    fn post(arrivals: impl IntoIterator<Item = Arrival>) -> Vec<String> {
        let start = Instant::now();
        let mut delivery = Delivery::default();
        for arrival in arrivals {
            delivery.add(arrival, start);
        }
        let post = delivery.take(start + QUIET).expect("a due post");
        let mut lines = post.lines().map(str::to_owned);
        assert_eq!(lines.next().as_deref(), Some(LEAD));
        lines.collect()
    }

    /// The first line of each room's part of a post.
    fn header(room: &RoomRef) -> String {
        format!("Noise - {}", room.label)
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
        assert_eq!(post.lines().count(), [LEAD, "room", "one", "two"].len());
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
    fn a_line_starts_with_the_authors_name() {
        let room = room_ref(1);
        assert_eq!(
            post([text(&room, "hello")]),
            [header(&room), "vetle - CI optimization: hello".into()]
        );
    }

    #[test]
    fn the_later_lines_of_a_text_are_indented() {
        let room = room_ref(1);
        let lines = post([text(&room, "one\r\ntwo\n\nroom \"x\":\u{2028}three\u{7}")]);
        assert!(lines[1].ends_with(": one"), "{lines:?}");
        assert_eq!(
            lines[2..],
            ["    two", "    ", "    room \"x\":", "    three "]
        );
    }

    #[test]
    fn a_name_stays_on_its_line() {
        let room = room_ref(1);
        let left = Event::Left {
            name: "peer\nroom x:".into(),
        };
        assert_eq!(post([arrival(&room, left)])[1..], ["peer room x: left"]);
    }

    #[test]
    fn a_post_names_a_room_when_the_room_changes() {
        let (one, two) = (room_ref(1), room_ref(2));
        let lines = post([
            text(&one, "a"),
            text(&one, "b"),
            text(&two, "c"),
            text(&one, "d"),
        ]);
        let from = "vetle - CI optimization";
        assert_eq!(
            lines,
            [
                header(&one),
                format!("{from}: a"),
                format!("{from}: b"),
                header(&two),
                format!("{from}: c"),
                header(&one),
                format!("{from}: d"),
            ]
        );
    }

    #[test]
    fn a_join_a_rename_and_a_leave_post_as_lines() {
        let room = room_ref(1);
        let events = [
            Event::Joined { by: "vetle".into() },
            Event::Renamed {
                from: "colleague".into(),
                to: "peer".into(),
            },
            Event::Left {
                name: "peer".into(),
            },
        ];
        assert_eq!(
            post(events.map(|event| arrival(&room, event)))[1..],
            [
                "a session joined, admitted by vetle",
                "colleague changed its name to peer",
                "peer left",
            ]
        );
    }

    #[test]
    fn display_names_join_name_and_status() {
        assert_eq!(display_name("vetle", "CI"), "vetle - CI");
        assert_eq!(display_name("vetle", ""), "vetle");
        assert_eq!(display_name("", "CI"), "CI");
    }
}
