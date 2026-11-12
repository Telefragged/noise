//! The session's rooms and their members, as the node and the CLI use
//! them.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use noise_net::{Admission, Redeemed, Session, Token};
use noise_room::{
    Body, Member, PublicKey, Rejected, Room, RoomId, RoomSecret, SecretKey, Timestamp, Verified,
};
use noise_store::{RoomRecord, SessionDir};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::delivery::{Arrival, RoomRef, Shown, display_name, short_key};

/// How long a ticket stays valid after the session issued it.
pub const TICKET_LIFETIME: Duration = Duration::from_hours(24);

/// The rooms of one session and their members. Each change to a room or
/// its members reaches the session's rooms file before the call returns.
pub struct SessionState {
    key: SecretKey,
    dir: SessionDir,
    config: PathBuf,
    rooms: Mutex<BTreeMap<RoomId, RoomMembers>>,
    tickets: Mutex<HashMap<Token, Issued>>,
    arrivals: mpsc::UnboundedSender<Arrival>,
}

/// A room and its members, by key.
struct RoomMembers {
    room: Room,
    members: BTreeMap<PublicKey, Member>,
}

impl RoomMembers {
    /// Records `name` and `status` for `key`, which wrote a message.
    /// Returns whether that changed the members.
    fn update(&mut self, key: PublicKey, name: &str, status: &str) -> bool {
        let member = Member {
            key,
            name: name.to_owned(),
            status: status.to_owned(),
        };
        self.members.insert(key, member.clone()) != Some(member)
    }

    /// Adds `key` without a name, unless it is a member already. Returns
    /// whether that changed the members.
    fn add(&mut self, key: PublicKey) -> bool {
        let new = !self.members.contains_key(&key);
        self.members.entry(key).or_insert_with(|| Member {
            key,
            name: String::new(),
            status: String::new(),
        });
        new
    }

    fn name_of(&self, key: &PublicKey) -> String {
        self.members
            .get(key)
            .map(|member| display_name(&member.name, &member.status))
            .unwrap_or_default()
    }

    fn room_ref(&self) -> RoomRef {
        RoomRef {
            id: self.room.id(),
            label: self.room.label().to_owned(),
        }
    }
}

/// The room of a ticket that the session issued, and when the ticket
/// expires.
struct Issued {
    room: RoomId,
    expires: Instant,
}

/// A room, as `noise room list` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomInfo {
    pub id: RoomId,
    pub label: String,
    pub members: Vec<Member>,
}

impl SessionState {
    /// Opens the session's key and rooms. `config` is the path of the
    /// config with the session's name. The state sends what happens in its
    /// rooms to `arrivals`.
    ///
    /// # Errors
    ///
    /// When the key or the rooms file cannot be read.
    pub fn open(
        dir: &SessionDir,
        config: PathBuf,
        arrivals: mpsc::UnboundedSender<Arrival>,
    ) -> Result<Self> {
        let key = dir.secret_key().context("read the session's key")?;
        let rooms = dir
            .rooms()
            .context("read the session's rooms")?
            .into_iter()
            .map(|record| {
                let room = Room::new(record.label, record.secret);
                let members = record
                    .members
                    .into_iter()
                    .map(|member| (member.key, member))
                    .collect();
                (room.id(), RoomMembers { room, members })
            })
            .collect();
        Ok(Self {
            key,
            dir: dir.clone(),
            config,
            rooms: Mutex::new(rooms),
            tickets: Mutex::new(HashMap::new()),
            arrivals,
        })
    }

    #[must_use]
    pub fn key(&self) -> &SecretKey {
        &self.key
    }

    /// The global config.
    ///
    /// # Errors
    ///
    /// When the config cannot be read.
    pub fn config(&self) -> Result<Config> {
        Config::read(&self.config)
    }

    /// This session's name, from the config, and its status.
    ///
    /// # Errors
    ///
    /// When the config or the status file cannot be read.
    pub fn name_and_status(&self) -> Result<(String, String)> {
        Ok((self.config()?.name, self.dir.status()?))
    }

    /// Creates a room with `label` and this session as its only member.
    ///
    /// # Errors
    ///
    /// When the config or the rooms file fails.
    pub fn create(&self, label: String) -> Result<Room> {
        let room = Room::new(label, RoomSecret::from_bytes(rand::random()));
        let me = self.me()?;
        self.insert(room.clone(), vec![me])?;
        Ok(room)
    }

    /// Adds the room of `admission`, which this session joined. Returns the
    /// room and its other members, the peers to join its topic through.
    ///
    /// # Errors
    ///
    /// When the config or the rooms file fails.
    pub fn join(&self, admission: Admission) -> Result<(Room, Vec<PublicKey>)> {
        let room = Room::new(admission.label, admission.secret);
        let me = self.me()?;
        let peers = admission
            .members
            .iter()
            .map(|member| member.key)
            .filter(|key| *key != me.key)
            .collect();
        let mut members = admission.members;
        members.push(me);
        self.insert(room.clone(), members)?;
        Ok((room, peers))
    }

    /// Removes `room`, which this session left.
    ///
    /// # Errors
    ///
    /// When the rooms file cannot be written.
    pub fn remove(&self, room: &RoomId) -> Result<()> {
        let mut rooms = self.lock();
        rooms.remove(room);
        self.save(&rooms)
    }

    /// The room that `room` names: a room id, or a label that exactly one
    /// of the session's rooms has.
    ///
    /// # Errors
    ///
    /// When no room, or more than one, matches.
    pub fn resolve(&self, room: &str) -> Result<RoomId> {
        let rooms = self.lock();
        if let Ok(id) = room.parse::<RoomId>()
            && rooms.contains_key(&id)
        {
            return Ok(id);
        }
        let mut matching = rooms
            .values()
            .map(|entry| &entry.room)
            .filter(|stored| stored.label() == room);
        match (matching.next(), matching.next()) {
            (Some(found), None) => Ok(found.id()),
            (None, _) => bail!("the session has no room {room:?}"),
            (Some(_), Some(_)) => bail!("more than one room has the label {room:?}; use its id"),
        }
    }

    /// Signs a message of this session with `body` and seals it for
    /// `room`. Returns the message and the sealed bytes to send.
    ///
    /// # Errors
    ///
    /// When the session has no such room, or the message is too large.
    pub fn seal(&self, room: &RoomId, body: Body) -> Result<(Verified, Vec<u8>)> {
        let rooms = self.lock();
        let entry = rooms
            .get(room)
            .ok_or_else(|| anyhow!("the session has no room {room}"))?;
        Ok(entry.room.seal(&self.key, body, now())?)
    }

    /// Replaces the session's status, and returns the session's rooms, in
    /// which to announce it.
    ///
    /// # Errors
    ///
    /// When the status file or the rooms file cannot be written.
    pub fn set_status(&self, status: &str) -> Result<Vec<RoomId>> {
        self.dir.set_status(status)?;
        let me = self.me()?;
        let mut rooms = self.lock();
        for entry in rooms.values_mut() {
            entry.update(me.key, &me.name, &me.status);
        }
        self.save(&rooms)?;
        Ok(rooms.keys().copied().collect())
    }

    /// The session's rooms, each with its other members, the peers to join
    /// its topic through.
    #[must_use]
    pub fn rooms(&self) -> Vec<(Room, Vec<PublicKey>)> {
        let me = self.key.public();
        self.lock()
            .values()
            .map(|entry| {
                let peers = entry.members.keys().copied().filter(|key| *key != me);
                (entry.room.clone(), peers.collect())
            })
            .collect()
    }

    /// The single-use token of a new ticket to `room`, which stays valid
    /// for [`TICKET_LIFETIME`] or until this process ends.
    ///
    /// # Errors
    ///
    /// When the session has no such room.
    pub fn issue_ticket(&self, room: &RoomId) -> Result<Token> {
        if !self.lock().contains_key(room) {
            bail!("the session has no room {room}");
        }
        let token = rand::random();
        let issued = Issued {
            room: *room,
            expires: Instant::now() + TICKET_LIFETIME,
        };
        lock(&self.tickets).insert(token, issued);
        Ok(token)
    }

    /// `peer`'s display name in the rooms that it shares with the session,
    /// and the labels of those rooms.
    #[must_use]
    pub fn describe(&self, peer: &PublicKey) -> (String, Vec<String>) {
        let rooms = self.lock();
        let shared: Vec<&RoomMembers> = rooms
            .values()
            .filter(|entry| entry.members.contains_key(peer))
            .collect();
        let name = shared
            .iter()
            .map(|entry| entry.name_of(peer))
            .find(|name| !name.is_empty())
            .unwrap_or_default();
        let labels = shared
            .iter()
            .map(|entry| entry.room.label().to_owned())
            .collect();
        (name, labels)
    }

    /// The session's rooms and their members.
    #[must_use]
    pub fn list(&self) -> Vec<RoomInfo> {
        self.lock()
            .values()
            .map(|entry| RoomInfo {
                id: entry.room.id(),
                label: entry.room.label().to_owned(),
                members: entry.members.values().cloned().collect(),
            })
            .collect()
    }

    fn me(&self) -> Result<Member> {
        let (name, status) = self.name_and_status()?;
        Ok(Member {
            key: self.key.public(),
            name,
            status,
        })
    }

    fn insert(&self, room: Room, members: Vec<Member>) -> Result<()> {
        let members = members
            .into_iter()
            .map(|member| (member.key, member))
            .collect();
        let mut rooms = self.lock();
        rooms.insert(room.id(), RoomMembers { room, members });
        self.save(&rooms)
    }

    fn save(&self, rooms: &BTreeMap<RoomId, RoomMembers>) -> Result<()> {
        let records: Vec<RoomRecord> = rooms
            .values()
            .map(|entry| RoomRecord {
                label: entry.room.label().to_owned(),
                secret: entry.room.secret().clone(),
                members: entry.members.values().cloned().collect(),
            })
            .collect();
        self.dir
            .set_rooms(&records)
            .context("write the session's rooms")
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<RoomId, RoomMembers>> {
        lock(&self.rooms)
    }
}

impl Session for SessionState {
    fn admits(&self, peer: &PublicKey) -> bool {
        self.lock()
            .values()
            .any(|entry| entry.members.contains_key(peer))
    }

    fn now(&self) -> Timestamp {
        now()
    }

    fn rejected(&self, _room: &RoomId, message: &Verified, why: Rejected) {
        // iroh-gossip delivers a message to a neighbor again when the
        // session sends the same bytes twice, as with two profiles of one
        // millisecond.
        if why != Rejected::Repeated {
            // The clocks of the two machines differ, or a peer sent the
            // bytes of an old message again.
            let author = short_key(&message.message().author);
            eprintln!("noise: dropped a message by {author}: {why}");
        }
    }

    fn receive(&self, room: &RoomId, message: Verified) {
        let me = self.key.public();
        let author = message.message().author;
        let mut rooms = self.lock();
        let Some(entry) = rooms.get_mut(room) else {
            return;
        };
        // Only a member has the room's key, so the author of a message
        // that opened is a member.
        let (changed, arrival) = match &message.message().body {
            Body::Text { name, status, .. } => {
                let changed = entry.update(author, name, status);
                let arrival = Shown::of(&message).map(|message| Arrival::Text {
                    room: entry.room_ref(),
                    message,
                });
                (changed, arrival)
            }
            Body::Profile { name, status } => (entry.update(author, name, status), None),
            Body::Admit { key } => {
                let known = entry.add(author);
                let new = entry.add(*key);
                let arrival = (new && *key != me).then(|| Arrival::Joined {
                    room: entry.room_ref(),
                    key: *key,
                    by: entry.name_of(&author),
                });
                (known || new, arrival)
            }
            Body::Leave => {
                let name = entry.name_of(&author);
                let left = entry.members.remove(&author).is_some();
                let arrival = left.then(|| Arrival::Left {
                    room: entry.room_ref(),
                    key: author,
                    name,
                });
                (left, arrival)
            }
            Body::Unknown => (entry.add(author), None),
        };
        if changed && let Err(err) = self.save(&rooms) {
            eprintln!("noise: {err:#}");
        }
        drop(rooms);
        if author != me
            && let Some(arrival) = arrival
        {
            // A closed channel means the process is ending.
            let _ = self.arrivals.send(arrival);
        }
    }

    fn profile(&self, room: &RoomId) -> Option<Vec<u8>> {
        let (name, status) = self.name_and_status().ok()?;
        let (_, sealed) = self.seal(room, Body::Profile { name, status }).ok()?;
        Some(sealed)
    }

    fn redeem(&self, token: &Token, peer: PublicKey) -> Result<Redeemed, String> {
        let issued = lock(&self.tickets)
            .remove(token)
            .ok_or("the ticket was used already, or this session did not issue it")?;
        if issued.expires < Instant::now() {
            return Err("the ticket expired".into());
        }
        let mut rooms = self.lock();
        let entry = rooms
            .get_mut(&issued.room)
            .ok_or("the inviter left the room")?;
        entry.add(peer);
        let admission = Admission {
            label: entry.room.label().to_owned(),
            secret: entry.room.secret().clone(),
            members: entry.members.values().cloned().collect(),
        };
        let (_, announcement) = entry
            .room
            .seal(&self.key, Body::Admit { key: peer }, now())
            .map_err(|err| err.to_string())?;
        self.save(&rooms).map_err(|err| format!("{err:#}"))?;
        Ok(Redeemed {
            room: issued.room,
            admission,
            announcement,
        })
    }
}

/// The current time as a message timestamp.
pub fn now() -> Timestamp {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp(u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX))
}

/// Locks `mutex`. A thread that panicked while it held the lock leaves
/// data that every user here can still read, so the poison is ignored.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _dir: tempfile::TempDir,
        state: SessionState,
        arrivals: mpsc::UnboundedReceiver<Arrival>,
        room: Room,
    }

    /// A session named "me" in one room, "ci", whose only member it is.
    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = dir.path().join("config.toml");
        std::fs::write(&config, "name = \"me\"\n").expect("write");
        let session = SessionDir::new(dir.path(), "s1");
        let room = Room::new("ci".into(), RoomSecret::from_bytes([1; 32]));
        let me = Member {
            key: session.secret_key().expect("key").public(),
            name: "me".into(),
            status: String::new(),
        };
        let record = RoomRecord {
            label: "ci".into(),
            secret: room.secret().clone(),
            members: vec![me],
        };
        session.set_rooms(&[record]).expect("write rooms");
        let (sender, arrivals) = mpsc::unbounded_channel();
        let state = SessionState::open(&session, config, sender).expect("session state");
        Fixture {
            _dir: dir,
            state,
            arrivals,
            room,
        }
    }

    fn key(n: u8) -> SecretKey {
        SecretKey::from_bytes(&[n; 32])
    }

    fn text(text: &str, name: &str) -> Body {
        Body::Text {
            text: text.into(),
            name: name.into(),
            status: String::new(),
        }
    }

    /// `body` by `author`, sealed for `room` and opened again.
    fn message(room: &Room, author: &SecretKey, body: Body) -> Verified {
        let (_, sealed) = room.seal(author, body, Timestamp(5)).expect("seal");
        room.open(&sealed).expect("open")
    }

    /// The display name of `key` in `room`, or `None` when it is no member.
    fn member(state: &SessionState, room: &Room, key: &PublicKey) -> Option<String> {
        let rooms = state.lock();
        let entry = &rooms[&room.id()];
        entry.members.contains_key(key).then(|| entry.name_of(key))
    }

    #[test]
    fn a_room_and_its_members_survive_a_reopen() {
        let fixture = fixture();
        let other = key(7);
        let id = fixture.room.id();
        fixture
            .state
            .receive(&id, message(&fixture.room, &other, text("hi", "peer")));
        let (sender, _) = mpsc::unbounded_channel();
        let reopened = SessionState::open(&fixture.state.dir, fixture.state.config.clone(), sender)
            .expect("session state");
        assert_eq!(reopened.rooms()[0].1, vec![other.public()]);
        assert_eq!(
            member(&reopened, &fixture.room, &other.public()),
            Some("peer".into())
        );
    }

    #[test]
    fn a_text_posts_and_names_its_author() {
        let mut fixture = fixture();
        let other = key(7);
        let id = fixture.room.id();
        let sent = message(&fixture.room, &other, text("hi", "peer"));
        fixture.state.receive(&id, sent.clone());
        match fixture.arrivals.try_recv().expect("an arrival") {
            Arrival::Text { message, room } => {
                assert_eq!(message.id, sent.id());
                assert_eq!(room.label, "ci");
            }
            other => panic!("expected a text, got {other:?}"),
        }
        assert_eq!(
            member(&fixture.state, &fixture.room, &other.public()),
            Some("peer".into())
        );
    }

    #[test]
    fn an_admission_adds_the_key_and_posts_who_admitted_it() {
        let mut fixture = fixture();
        let (other, newcomer) = (key(7), key(8));
        let id = fixture.room.id();
        let profile = Body::Profile {
            name: "peer".into(),
            status: String::new(),
        };
        fixture
            .state
            .receive(&id, message(&fixture.room, &other, profile));
        let admit = Body::Admit {
            key: newcomer.public(),
        };
        fixture
            .state
            .receive(&id, message(&fixture.room, &other, admit));
        match fixture.arrivals.try_recv().expect("an arrival") {
            Arrival::Joined { key, by, .. } => {
                assert_eq!((key, by.as_str()), (newcomer.public(), "peer"));
            }
            other => panic!("expected a join, got {other:?}"),
        }
        assert!(fixture.state.admits(&newcomer.public()));
    }

    #[test]
    fn a_leave_removes_the_member() {
        let mut fixture = fixture();
        let other = key(7);
        let id = fixture.room.id();
        fixture
            .state
            .receive(&id, message(&fixture.room, &other, text("bye", "peer")));
        fixture.arrivals.try_recv().expect("an arrival");
        fixture
            .state
            .receive(&id, message(&fixture.room, &other, Body::Leave));
        assert!(matches!(
            fixture.arrivals.try_recv().expect("an arrival"),
            Arrival::Left { name, .. } if name == "peer"
        ));
        assert!(!fixture.state.admits(&other.public()));
    }

    #[test]
    fn the_sessions_own_messages_are_not_posted() {
        let mut fixture = fixture();
        let id = fixture.room.id();
        let (own, _) = fixture.state.seal(&id, text("mine", "me")).expect("seal");
        fixture.state.receive(&id, own);
        assert!(fixture.arrivals.try_recv().is_err());
    }

    #[test]
    fn a_ticket_redeems_once() {
        let fixture = fixture();
        let (id, peer) = (fixture.room.id(), key(9).public());
        let token = fixture.state.issue_ticket(&id).expect("ticket");
        let redeemed = fixture.state.redeem(&token, peer).expect("redeem");
        assert_eq!(redeemed.room, id);
        assert_eq!(redeemed.admission.secret, *fixture.room.secret());
        let announcement = fixture.room.open(&redeemed.announcement).expect("open");
        assert_eq!(announcement.message().body, Body::Admit { key: peer });
        assert!(fixture.state.admits(&peer));
        assert!(fixture.state.redeem(&token, key(10).public()).is_err());
        assert!(!fixture.state.admits(&key(10).public()));
    }

    #[test]
    fn an_admission_joins_its_room() {
        let fixture = fixture();
        let dir = tempfile::tempdir().expect("tempdir");
        let (sender, _) = mpsc::unbounded_channel();
        let config = dir.path().join("config.toml");
        let joiner = SessionState::open(&SessionDir::new(dir.path(), "s2"), config, sender)
            .expect("session state");
        let token = fixture
            .state
            .issue_ticket(&fixture.room.id())
            .expect("ticket");
        let redeemed = fixture
            .state
            .redeem(&token, joiner.key().public())
            .expect("redeem");
        let (room, peers) = joiner.join(redeemed.admission).expect("join");
        assert_eq!(room, fixture.room);
        assert_eq!(peers, vec![fixture.state.key().public()]);
        assert_eq!(joiner.list()[0].members.len(), 2);
    }
}
