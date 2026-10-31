use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use iroh::Endpoint;
use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::{Gossip, TopicId as GossipTopicId};
use n0_future::StreamExt;
use noise_room::{PublicKey, Room, RoomId, Verified};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinSet;

use crate::chunk;
use crate::inbound::Inbound;
use crate::lock;

/// The session state that the network uses.
pub trait Session: Send + Sync + 'static {
    /// Whether `peer` is a member of at least one of the session's rooms.
    fn admits(&self, peer: &PublicKey) -> bool;

    /// Handles `message`, which arrived through `room`'s gossip topic.
    fn receive(&self, room: &RoomId, message: Verified);

    /// This session's profile message for `room`, sealed. The node sends
    /// it when the room gets a neighbor, so that the room's members learn
    /// the session's name and status.
    fn profile(&self, room: &RoomId) -> Option<Vec<u8>>;
}

/// What the network did, for observers such as tests.
#[derive(Debug, Clone)]
pub enum NetEvent {
    /// `peer` became a neighbor in the room's gossip topic: a member that
    /// this session keeps a direct gossip connection to for the room.
    /// iroh-gossip keeps a few neighbors per topic, and a broadcast
    /// reaches the rest of the room through them.
    NeighborUp { room: RoomId, peer: PublicKey },
}

/// The session's iroh endpoint with the rooms' gossip topics.
pub struct Node<S: Session> {
    inner: Arc<Inner<S>>,
}

impl<S: Session> Clone for Node<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<S: Session> {
    router: Router,
    gossip: Gossip,
    session: Arc<S>,
    /// The sender of each open room's topic.
    senders: Mutex<HashMap<RoomId, GossipSender>>,
    events: broadcast::Sender<NetEvent>,
    tasks: Mutex<JoinSet<()>>,
}

impl<S: Session> Node<S> {
    /// Starts gossip on `endpoint`.
    pub fn spawn(endpoint: Endpoint, gossip: iroh_gossip::net::Builder, session: Arc<S>) -> Self {
        let gossip = gossip.spawn(endpoint.clone());
        let router = Router::builder(endpoint)
            .accept(
                iroh_gossip::ALPN,
                GossipGate {
                    gossip: gossip.clone(),
                    session: Arc::clone(&session),
                },
            )
            .spawn();
        let (events, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(Inner {
                router,
                gossip,
                session,
                senders: Mutex::new(HashMap::new()),
                events,
                tasks: Mutex::new(JoinSet::new()),
            }),
        }
    }

    #[must_use]
    pub fn id(&self) -> PublicKey {
        self.inner.router.endpoint().id()
    }

    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        self.inner.router.endpoint()
    }

    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<NetEvent> {
        self.inner.events.subscribe()
    }

    /// Subscribes to `room`'s gossip topic, with `bootstrap` as the first
    /// peers, and handles its events from now on. The returned receiver
    /// fires when the room gets its first neighbor.
    ///
    /// # Errors
    ///
    /// When gossip fails.
    pub async fn open_room(
        &self,
        room: Room,
        bootstrap: Vec<PublicKey>,
    ) -> Result<oneshot::Receiver<()>> {
        let topic = GossipTopicId::from_bytes(*room.topic().as_bytes());
        let me = self.id();
        let bootstrap = bootstrap.into_iter().filter(|peer| *peer != me).collect();
        let (sender, receiver) = self.inner.gossip.subscribe(topic, bootstrap).await?.split();
        let (first, first_neighbor) = oneshot::channel();
        lock(&self.inner.senders).insert(room.id(), sender);
        let node = self.clone();
        lock(&self.inner.tasks).spawn(async move {
            node.handle_room(room, receiver, first).await;
        });
        Ok(first_neighbor)
    }

    /// Broadcasts `sealed`, a message sealed for `room`, as chunks on the
    /// room's topic.
    ///
    /// # Errors
    ///
    /// When the room is not open, or gossip fails.
    pub async fn send(&self, room: &RoomId, sealed: &[u8]) -> Result<()> {
        let sender = self.sender(room)?;
        for chunk in chunk::split(sealed, self.inner.gossip.max_message_size()) {
            sender.broadcast(Bytes::from(chunk)).await?;
        }
        Ok(())
    }

    /// Stops the endpoint and every task of the node.
    pub async fn shutdown(&self) {
        lock(&self.inner.tasks).abort_all();
        // A failed shutdown leaves nothing to clean up.
        let _ = self.inner.router.shutdown().await;
    }

    fn sender(&self, room: &RoomId) -> Result<GossipSender> {
        lock(&self.inner.senders)
            .get(room)
            .cloned()
            .ok_or_else(|| anyhow!("room {room} is not open"))
    }

    async fn handle_room(
        &self,
        room: Room,
        mut receiver: GossipReceiver,
        first: oneshot::Sender<()>,
    ) {
        let id = room.id();
        let session = &self.inner.session;
        let mut inbound = Inbound::new(self.inner.gossip.max_message_size());
        let mut first = Some(first);
        loop {
            match receiver.next().await {
                Some(Ok(Event::Received(message))) => {
                    if let Some(message) =
                        inbound.received(&message.content, message.delivered_from, &room)
                    {
                        session.receive(&id, message);
                    }
                }
                Some(Ok(Event::NeighborUp(peer))) => {
                    // The first neighbor forwards the profile to the whole
                    // room; a later one receives it on its own.
                    let everyone = first.take().is_some_and(|first| first.send(()).is_ok());
                    if let Some(profile) = session.profile(&id) {
                        // A lost profile only leaves a name unknown until
                        // the session's next message.
                        let _ = self.announce(&id, &profile, everyone).await;
                    }
                    self.emit(NetEvent::NeighborUp { room: id, peer });
                }
                Some(Ok(Event::NeighborDown(_) | Event::Lagged)) => {}
                Some(Err(_)) | None => return,
            }
        }
    }

    /// Sends `sealed` to the whole room, or only to the room's current
    /// neighbors.
    async fn announce(&self, room: &RoomId, sealed: &[u8], everyone: bool) -> Result<()> {
        if everyone {
            return self.send(room, sealed).await;
        }
        let sender = self.sender(room)?;
        for chunk in chunk::split(sealed, self.inner.gossip.max_message_size()) {
            sender.broadcast_neighbors(Bytes::from(chunk)).await?;
        }
        Ok(())
    }

    fn emit(&self, event: NetEvent) {
        // Without observers there is nobody to tell.
        let _ = self.inner.events.send(event);
    }
}

/// Accepts gossip connections only from members of the session's rooms.
/// iroh-gossip has no admission control of its own.
struct GossipGate<S> {
    gossip: Gossip,
    session: Arc<S>,
}

impl<S> fmt::Debug for GossipGate<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GossipGate")
    }
}

/// The error code with which the gate closes a connection.
pub const NOT_A_MEMBER: VarInt = VarInt::from_u32(1);

impl<S: Session> ProtocolHandler for GossipGate<S> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self.session.admits(&connection.remote_id()) {
            connection.close(NOT_A_MEMBER, b"not a member");
            return Ok(());
        }
        self.gossip
            .handle_connection(connection)
            .await
            .map_err(AcceptError::from_err)
    }
}
