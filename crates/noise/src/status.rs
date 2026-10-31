//! The status interface: the session's peers and their connections, as
//! `noise status` prints them.

use std::time::Duration;

use noise_net::JOIN_ALPN;
use noise_net::status::{ConnectionStatus, Paths, PeerStatus};
use noise_room::PublicKey;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

/// How often a followed status reports the byte counters, which have no
/// change event of their own.
pub const TICK: Duration = Duration::from_secs(1);

/// One peer of the session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub key: PublicKey,
    /// The peer's display name, from its newest message in a shared room.
    pub name: String,
    /// The labels of the rooms that the session shares with the peer.
    pub rooms: Vec<String>,
    pub paths: Paths,
    pub sent: u64,
    pub received: u64,
    pub connections: Vec<ConnectionInfo>,
}

/// One connection to a peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// `join` or `gossip`.
    pub protocol: String,
    pub paths: Paths,
    /// The kind of the path that carries the data: `direct` or `relay`.
    pub selected: Option<String>,
    pub sent: u64,
    pub received: u64,
}

/// The peer `status`, with the peer's `name` and the labels of the
/// `rooms` that the session shares with the peer.
#[must_use]
pub fn peer_info(status: &PeerStatus, name: String, rooms: Vec<String>) -> PeerInfo {
    PeerInfo {
        key: status.peer,
        name,
        rooms,
        paths: status.paths,
        sent: status.sent,
        received: status.received,
        connections: status.connections.iter().map(connection_info).collect(),
    }
}

fn connection_info(connection: &ConnectionStatus) -> ConnectionInfo {
    let protocol = if connection.alpn == JOIN_ALPN {
        "join"
    } else {
        "gossip"
    };
    ConnectionInfo {
        protocol: protocol.into(),
        paths: connection.paths,
        selected: connection
            .selected_relay
            .map(|relay| if relay { "relay" } else { "direct" }.into()),
        sent: connection.sent,
        received: connection.received,
    }
}

/// Sends a status from `snapshot` now, after each change, and every
/// [`TICK`], until the receiver of `sender` is gone.
pub async fn follow<T>(
    snapshot: impl Fn() -> T,
    mut changes: watch::Receiver<()>,
    sender: mpsc::Sender<T>,
) {
    let mut tick = tokio::time::interval(TICK);
    tick.tick().await;
    loop {
        if sender.send(snapshot()).await.is_err() {
            return;
        }
        tokio::select! {
            result = changes.changed() => if result.is_err() {
                return;
            },
            _ = tick.tick() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use noise_room::SecretKey;

    use super::*;

    #[test]
    fn a_peer_names_its_connections_by_protocol_and_path() {
        let peer = SecretKey::from_bytes(&[1; 32]).public();
        let status = PeerStatus {
            peer,
            paths: Paths::Both,
            sent: 3,
            received: 4,
            connections: vec![
                ConnectionStatus {
                    peer,
                    alpn: JOIN_ALPN.to_vec(),
                    paths: Paths::Direct,
                    selected_relay: Some(false),
                    sent: 1,
                    received: 2,
                },
                ConnectionStatus {
                    peer,
                    alpn: b"/iroh-gossip/1".to_vec(),
                    paths: Paths::Relay,
                    selected_relay: Some(true),
                    sent: 2,
                    received: 2,
                },
            ],
        };
        let info = peer_info(&status, "vetle".into(), vec!["ci".into()]);
        assert_eq!(info.paths, Paths::Both);
        let kinds: Vec<(&str, Option<&str>)> = info
            .connections
            .iter()
            .map(|c| (c.protocol.as_str(), c.selected.as_deref()))
            .collect();
        assert_eq!(
            kinds,
            vec![("join", Some("direct")), ("gossip", Some("relay"))]
        );
    }

    /// The clock is paused: time moves only when every task waits, so the
    /// order of the reports is fixed.
    #[tokio::test(start_paused = true)]
    async fn a_followed_status_reports_at_once_on_each_change_and_each_tick() {
        let count = Arc::new(AtomicU32::new(0));
        let (signal, changes) = watch::channel(());
        let (sender, mut reports) = mpsc::channel(1);
        let counter = Arc::clone(&count);
        let started = tokio::time::Instant::now();
        tokio::spawn(follow(
            move || counter.fetch_add(1, Ordering::SeqCst),
            changes,
            sender,
        ));
        assert_eq!(reports.recv().await, Some(0));
        signal.send_replace(());
        assert_eq!(reports.recv().await, Some(1));
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(reports.recv().await, Some(2));
        assert_eq!(started.elapsed(), TICK);
        drop(reports);
        drop(signal);
    }
}
