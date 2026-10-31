//! Tests over real iroh endpoints on the loopback interface. Each test
//! awaits the one event that produces the state it checks: a neighbor
//! event, or a message that a session received.

use std::collections::{BTreeSet, HashMap};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use iroh::Endpoint;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::{ConnectionError, presets};
use iroh_gossip::Gossip;
use noise_room::{Body, PublicKey, Room, RoomId, RoomSecret, SecretKey, Timestamp, Verified};
use tokio::sync::{broadcast, mpsc};

use super::*;

/// A session's rooms and their members, in memory.
struct TestSession {
    key: SecretKey,
    rooms: Mutex<HashMap<RoomId, (Room, BTreeSet<PublicKey>)>>,
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
            received: sender,
        };
        (Arc::new(session), receiver)
    }

    fn room(&self, id: &RoomId) -> Room {
        self.rooms.lock().expect("lock")[id].0.clone()
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
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(key.clone())
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
