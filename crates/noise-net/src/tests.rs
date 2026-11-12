//! Tests over real iroh endpoints on the loopback interface. Each test
//! awaits the one event that produces the state it checks: the return of
//! a join, a neighbor event, or a message that a session received.

use std::collections::{BTreeSet, HashMap};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use iroh::Endpoint;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::{ConnectionError, presets};
use iroh_gossip::Gossip;
use noise_room::{
    Body, Member, PublicKey, Rejected, Room, RoomId, RoomSecret, SecretKey, Timestamp, Verified,
};
use tokio::sync::{broadcast, mpsc};

use super::*;
use crate::status::{Connections, Paths, by_peer};

/// A session's rooms and their members, in memory.
struct TestSession {
    key: SecretKey,
    rooms: Mutex<HashMap<RoomId, (Room, BTreeSet<PublicKey>)>>,
    tokens: Mutex<HashMap<Token, RoomId>>,
    received: mpsc::UnboundedSender<(RoomId, Verified)>,
}

type Received = mpsc::UnboundedReceiver<(RoomId, Verified)>;

impl TestSession {
    /// A session of `key` in `rooms`, each with its members.
    fn new(key: &SecretKey, rooms: &[(&Room, &[&SecretKey])]) -> (Arc<Self>, Received) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let rooms = rooms
            .iter()
            .map(|(room, members)| {
                let members = members.iter().map(|member| member.public()).collect();
                (room.id(), ((*room).clone(), members))
            })
            .collect();
        let session = Self {
            key: key.clone(),
            rooms: Mutex::new(rooms),
            tokens: Mutex::new(HashMap::new()),
            received: sender,
        };
        (Arc::new(session), receiver)
    }

    fn room(&self, id: &RoomId) -> Room {
        self.rooms.lock().expect("lock")[id].0.clone()
    }

    fn has_member(&self, id: &RoomId, key: &PublicKey) -> bool {
        self.rooms.lock().expect("lock")[id].1.contains(key)
    }

    fn seal(&self, id: &RoomId, body: Body) -> (Verified, Vec<u8>) {
        self.room(id)
            .seal(&self.key, body, Timestamp(0))
            .expect("seal")
    }
}

impl Session for TestSession {
    fn admits(&self, peer: &PublicKey) -> bool {
        let rooms = self.rooms.lock().expect("lock");
        rooms.values().any(|(_, members)| members.contains(peer))
    }

    /// The time of every message that the tests seal.
    fn now(&self) -> Timestamp {
        Timestamp(0)
    }

    fn rejected(&self, _room: &RoomId, _message: &Verified, _why: Rejected) {}

    fn receive(&self, room: &RoomId, message: Verified) {
        if let Some((_, members)) = self.rooms.lock().expect("lock").get_mut(room) {
            members.insert(message.message().author);
        }
        // The test may have stopped reading; nothing depends on it.
        let _ = self.received.send((*room, message));
    }

    fn profile(&self, room: &RoomId) -> Option<Vec<u8>> {
        let body = Body::Profile {
            name: "test".into(),
            status: String::new(),
        };
        Some(self.seal(room, body).1)
    }

    fn redeem(&self, token: &Token, peer: PublicKey) -> Result<Redeemed, String> {
        let id = self
            .tokens
            .lock()
            .expect("lock")
            .remove(token)
            .ok_or("the ticket was used")?;
        let mut rooms = self.rooms.lock().expect("lock");
        let (room, members) = rooms.get_mut(&id).expect("room");
        members.insert(peer);
        let admission = Admission {
            label: room.label().into(),
            secret: room.secret().clone(),
            members: members
                .iter()
                .map(|key| Member {
                    key: *key,
                    name: String::new(),
                    status: String::new(),
                })
                .collect(),
        };
        let (_, announcement) = room
            .seal(&self.key, Body::Admit { key: peer }, Timestamp(0))
            .expect("seal");
        Ok(Redeemed {
            room: id,
            admission,
            announcement,
        })
    }
}

fn key(n: u8) -> SecretKey {
    SecretKey::from_bytes(&[n; 32])
}

fn room() -> Room {
    Room::new("ci".into(), RoomSecret::from_bytes([7; 32]))
}

/// A node on the loopback interface that `lookup` can find.
async fn node(
    lookup: &MemoryLookup,
    key: &SecretKey,
    session: Arc<TestSession>,
) -> Node<TestSession> {
    node_with(lookup, key, session, Connections::default()).await
}

/// A node that records its connections in `connections`.
async fn node_with(
    lookup: &MemoryLookup,
    key: &SecretKey,
    session: Arc<TestSession>,
    connections: Connections,
) -> Node<TestSession> {
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(key.clone())
        .hooks(connections)
        .address_lookup(lookup.clone())
        .clear_ip_transports()
        .bind_addr((Ipv4Addr::LOCALHOST, 0))
        .expect("bind address")
        .bind()
        .await
        .expect("bind");
    lookup.add_endpoint_info(endpoint.addr());
    Node::spawn(endpoint, Gossip::builder(), session)
}

/// Returns at the event that reports `peer` as a neighbor.
async fn neighbor_up(events: &mut broadcast::Receiver<NetEvent>, peer: PublicKey) {
    loop {
        if let NetEvent::NeighborUp { peer: up, .. } = events.recv().await.expect("receive")
            && up == peer
        {
            return;
        }
    }
}

/// Returns the first message by `author` with a body that `matches`
/// accepts.
async fn received_from(
    received: &mut Received,
    author: PublicKey,
    matches: impl Fn(&Body) -> bool,
) -> Verified {
    loop {
        let (_, message) = received.recv().await.expect("receive");
        if message.message().author == author && matches(&message.message().body) {
            return message;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_broadcast_reaches_a_neighbor() {
    let (a, b) = (key(1), key(2));
    let (room, members) = (room(), [&a, &b]);
    let lookup = MemoryLookup::new();
    let (at_a, _) = TestSession::new(&a, &[(&room, &members)]);
    let (at_b, mut received_b) = TestSession::new(&b, &[(&room, &members)]);
    let node_a = node(&lookup, &a, Arc::clone(&at_a)).await;
    let node_b = node(&lookup, &b, at_b).await;
    let mut events_a = node_a.events();
    drop(
        node_b
            .open_room(room.clone(), vec![a.public()])
            .await
            .expect("open room"),
    );
    drop(
        node_a
            .open_room(room.clone(), Vec::new())
            .await
            .expect("open room"),
    );
    // From this event on, b is in a's active view, so a's gossip sends
    // each broadcast to b over their connection.
    neighbor_up(&mut events_a, b.public()).await;

    let text = Body::Text {
        text: "x".repeat(30_000),
        name: String::new(),
        status: String::new(),
    };
    let (sent, sealed) = at_a.seal(&room.id(), text.clone());
    node_a.send(&room.id(), &sealed).await.expect("send");
    let message = received_from(&mut received_b, a.public(), |body| *body == text).await;
    assert_eq!(message.id(), sent.id());
    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_neighbor_receives_the_profile() {
    let (a, b) = (key(1), key(2));
    let (room, members) = (room(), [&a, &b]);
    let lookup = MemoryLookup::new();
    let (at_a, _) = TestSession::new(&a, &[(&room, &members)]);
    let (at_b, mut received_b) = TestSession::new(&b, &[(&room, &members)]);
    let node_a = node(&lookup, &a, at_a).await;
    let node_b = node(&lookup, &b, at_b).await;
    drop(
        node_a
            .open_room(room.clone(), Vec::new())
            .await
            .expect("open room"),
    );
    drop(
        node_b
            .open_room(room.clone(), vec![a.public()])
            .await
            .expect("open room"),
    );
    received_from(
        &mut received_b,
        a.public(),
        |body| matches!(body, Body::Profile { name, .. } if name == "test"),
    )
    .await;
    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_member_cannot_open_a_gossip_connection() {
    let (a, e) = (key(1), key(5));
    let lookup = MemoryLookup::new();
    let node_a = node(&lookup, &a, TestSession::new(&a, &[(&room(), &[&a])]).0).await;
    let node_e = node(&lookup, &e, TestSession::new(&e, &[]).0).await;
    let connection = node_e
        .endpoint()
        .connect(a.public(), iroh_gossip::ALPN)
        .await
        .expect("connect");
    match connection.closed().await {
        ConnectionError::ApplicationClosed(close) => {
            assert_eq!(close.error_code, NOT_A_MEMBER);
        }
        other => panic!("unexpected close: {other:?}"),
    }
    node_a.shutdown().await;
    node_e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ticket_admits_one_session_once() {
    let (a, c, d) = (key(1), key(3), key(4));
    let room = room();
    let lookup = MemoryLookup::new();
    let (at_a, _) = TestSession::new(&a, &[(&room, &[&a])]);
    at_a.tokens.lock().expect("lock").insert([9; 16], room.id());
    let node_a = node(&lookup, &a, Arc::clone(&at_a)).await;
    let node_c = node(&lookup, &c, TestSession::new(&c, &[]).0).await;
    let node_d = node(&lookup, &d, TestSession::new(&d, &[]).0).await;

    let admission = node_c
        .join(node_a.endpoint().addr(), [9; 16])
        .await
        .expect("join");
    assert_eq!(admission.secret, *room.secret());
    assert_eq!(admission.label, "ci");
    assert!(
        admission
            .members
            .iter()
            .any(|member| member.key == a.public())
    );
    assert!(at_a.has_member(&room.id(), &c.public()));

    let again = node_d.join(node_a.endpoint().addr(), [9; 16]).await;
    assert!(
        again
            .expect_err("an error")
            .to_string()
            .contains("the ticket was used")
    );
    assert!(!at_a.has_member(&room.id(), &d.public()));
    for node in [node_a, node_c, node_d] {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_joined_session_announces_itself_to_the_room() {
    let (a, c) = (key(1), key(3));
    let room = room();
    let lookup = MemoryLookup::new();
    let (at_a, mut received_a) = TestSession::new(&a, &[(&room, &[&a])]);
    at_a.tokens.lock().expect("lock").insert([9; 16], room.id());
    let node_a = node(&lookup, &a, Arc::clone(&at_a)).await;
    drop(
        node_a
            .open_room(room.clone(), Vec::new())
            .await
            .expect("open room"),
    );
    let (at_c, _) = TestSession::new(&c, &[]);
    let node_c = node(&lookup, &c, Arc::clone(&at_c)).await;

    let admission = node_c
        .join(node_a.endpoint().addr(), [9; 16])
        .await
        .expect("join");
    let joined = Room::new(admission.label, admission.secret);
    at_c.rooms.lock().expect("lock").insert(
        joined.id(),
        (
            joined.clone(),
            admission.members.iter().map(|member| member.key).collect(),
        ),
    );
    let bootstrap = admission.members.iter().map(|member| member.key).collect();
    let first_neighbor = node_c
        .open_room(joined, bootstrap)
        .await
        .expect("open room");
    first_neighbor.await.expect("first neighbor");
    received_from(&mut received_a, c.public(), |body| {
        matches!(body, Body::Profile { .. })
    })
    .await;
    node_a.shutdown().await;
    node_c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn both_sides_report_a_gossip_connection_with_its_path_and_bytes() {
    let (a, b) = (key(1), key(2));
    let (room, members) = (room(), [&a, &b]);
    let lookup = MemoryLookup::new();
    let (at_a, at_b) = (Connections::default(), Connections::default());
    let session_a = TestSession::new(&a, &[(&room, &members)]).0;
    let session_b = TestSession::new(&b, &[(&room, &members)]).0;
    let node_a = node_with(&lookup, &a, session_a, at_a.clone()).await;
    let node_b = node_with(&lookup, &b, session_b, at_b.clone()).await;
    let (mut events_a, mut events_b) = (node_a.events(), node_b.events());
    drop(
        node_a
            .open_room(room.clone(), Vec::new())
            .await
            .expect("open room"),
    );
    drop(
        node_b
            .open_room(room.clone(), vec![a.public()])
            .await
            .expect("open room"),
    );
    // The hook runs in the handshake, before either side's gossip.
    neighbor_up(&mut events_a, b.public()).await;
    neighbor_up(&mut events_b, a.public()).await;

    for (connections, peer) in [(&at_b, a.public()), (&at_a, b.public())] {
        let peers = by_peer(connections.snapshot());
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer, peer);
        assert_eq!(peers[0].paths, Paths::Direct);
        assert!(peers[0].sent > 0 && peers[0].received > 0);
        assert_eq!(peers[0].connections[0].alpn, iroh_gossip::ALPN);
    }
    node_a.shutdown().await;
    node_b.shutdown().await;
}
