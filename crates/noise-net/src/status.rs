//! The endpoint's connections, as the status interface reports them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks, WeakConnectionHandle};
use n0_future::StreamExt;
use noise_room::PublicKey;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::lock;

/// The paths of a connection, or of all connections to a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Paths {
    /// No path is open.
    None,
    /// Only direct paths, over IP.
    Direct,
    /// Only paths through a relay.
    Relay,
    /// Direct paths and relay paths.
    Both,
}

impl Paths {
    fn of(direct: bool, relay: bool) -> Self {
        match (direct, relay) {
            (false, false) => Self::None,
            (true, false) => Self::Direct,
            (false, true) => Self::Relay,
            (true, true) => Self::Both,
        }
    }

    fn join(self, other: Self) -> Self {
        let direct =
            matches!(self, Self::Direct | Self::Both) || matches!(other, Self::Direct | Self::Both);
        let relay =
            matches!(self, Self::Relay | Self::Both) || matches!(other, Self::Relay | Self::Both);
        Self::of(direct, relay)
    }
}

/// One connection, as a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStatus {
    pub peer: PublicKey,
    pub alpn: Vec<u8>,
    pub paths: Paths,
    /// Whether the path that carries the data is a relay path. `None`
    /// when no path is selected.
    pub selected_relay: Option<bool>,
    pub sent: u64,
    pub received: u64,
}

impl ConnectionStatus {
    fn of(connection: &Connection) -> Self {
        let paths = connection.paths();
        let (mut direct, mut relay, mut selected_relay) = (false, false, None);
        for path in &paths {
            direct |= path.is_ip();
            relay |= path.is_relay();
            if path.is_selected() {
                selected_relay = Some(path.is_relay());
            }
        }
        let stats = connection.stats();
        Self {
            peer: connection.remote_id(),
            alpn: connection.alpn().to_vec(),
            paths: Paths::of(direct, relay),
            selected_relay,
            sent: stats.udp_tx.bytes,
            received: stats.udp_rx.bytes,
        }
    }
}

/// The connections to one peer, as a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerStatus {
    pub peer: PublicKey,
    pub paths: Paths,
    pub sent: u64,
    pub received: u64,
    pub connections: Vec<ConnectionStatus>,
}

/// Groups `connections` by peer, in the order of the peers' keys.
#[must_use]
pub fn by_peer(connections: Vec<ConnectionStatus>) -> Vec<PeerStatus> {
    let mut peers: BTreeMap<PublicKey, PeerStatus> = BTreeMap::new();
    for connection in connections {
        let peer = peers.entry(connection.peer).or_insert_with(|| PeerStatus {
            peer: connection.peer,
            paths: Paths::None,
            sent: 0,
            received: 0,
            connections: Vec::new(),
        });
        peer.paths = peer.paths.join(connection.paths);
        peer.sent += connection.sent;
        peer.received += connection.received;
        peer.connections.push(connection);
    }
    peers.into_values().collect()
}

/// Records every connection of the endpoint that it is installed on, and
/// signals each change: a new connection, a path event, or a close.
#[derive(Debug, Clone)]
pub struct Connections {
    handles: Arc<Mutex<Vec<WeakConnectionHandle>>>,
    changed: watch::Sender<()>,
}

impl Default for Connections {
    fn default() -> Self {
        Self {
            handles: Arc::default(),
            changed: watch::channel(()).0,
        }
    }
}

impl Connections {
    /// The open connections. A weak handle does not keep a connection
    /// open, so the closed ones drop out here.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ConnectionStatus> {
        let mut handles = lock(&self.handles);
        let mut open = Vec::new();
        handles.retain(|handle| match handle.upgrade() {
            Some(connection) => {
                open.push(ConnectionStatus::of(&connection));
                true
            }
            None => false,
        });
        open
    }

    /// A receiver of each change after this call.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }
}

impl EndpointHooks for Connections {
    fn after_handshake<'a>(
        &'a self,
        connection: &'a Connection,
    ) -> impl Future<Output = AfterHandshakeOutcome> + Send + 'a {
        let handle = connection.weak_handle();
        lock(&self.handles).push(handle.clone());
        self.changed.send_replace(());
        let mut events = connection.path_events();
        let changed = self.changed.clone();
        tokio::spawn(async move {
            while events.next().await.is_some() {
                changed.send_replace(());
            }
            handle.closed().await;
            changed.send_replace(());
        });
        std::future::ready(AfterHandshakeOutcome::accept())
    }
}

#[cfg(test)]
mod tests {
    use noise_room::SecretKey;

    use super::*;

    fn connection(peer: u8, alpn: &[u8], paths: Paths, sent: u64) -> ConnectionStatus {
        ConnectionStatus {
            peer: SecretKey::from_bytes(&[peer; 32]).public(),
            alpn: alpn.to_vec(),
            paths,
            selected_relay: Some(paths == Paths::Relay),
            sent,
            received: sent * 2,
        }
    }

    #[test]
    fn connections_group_by_peer_with_summed_bytes_and_joined_paths() {
        let peers = by_peer(vec![
            connection(1, b"gossip", Paths::Direct, 10),
            connection(2, b"gossip", Paths::Relay, 5),
            connection(1, b"noise/0", Paths::Relay, 7),
        ]);
        assert_eq!(peers.len(), 2);
        let one = peers
            .iter()
            .find(|peer| peer.peer == SecretKey::from_bytes(&[1; 32]).public())
            .expect("peer");
        assert_eq!(one.paths, Paths::Both);
        assert_eq!((one.sent, one.received), (17, 34));
        assert_eq!(one.connections.len(), 2);
        let two = peers
            .iter()
            .find(|peer| peer.peer == SecretKey::from_bytes(&[2; 32]).public())
            .expect("peer");
        assert_eq!(two.paths, Paths::Relay);
    }

    #[test]
    fn paths_join_into_both_only_when_both_kinds_exist() {
        assert_eq!(Paths::None.join(Paths::Direct), Paths::Direct);
        assert_eq!(Paths::Direct.join(Paths::Direct), Paths::Direct);
        assert_eq!(Paths::Direct.join(Paths::Relay), Paths::Both);
        assert_eq!(Paths::Relay.join(Paths::None), Paths::Relay);
    }
}
