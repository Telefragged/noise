//! `noise watch`: the process that connects one Claude Code session to its
//! rooms.
//!
//! The plugin's `SessionStart` hook starts it as a child of the session,
//! which lets Claude Code deliver its posts to the session's inbox socket
//! without a permission prompt. It ends when the Claude Code process
//! exits.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use iroh::Endpoint;
use iroh::endpoint::{Builder, presets};
use iroh_gossip::Gossip;
use noise_net::Node;
use noise_room::Body;
use noise_store::SessionDir;
use tokio::sync::{OnceCell, mpsc};

use crate::config;
use crate::delivery::{Arrival, Delivery};
use crate::inbox::Inbox;
use crate::parent::exit_with_parent;
use crate::state::SessionState;
use crate::ticket::Ticket;
use crate::watch_socket::{self, Request, Response, SOCKET_FILE};

/// The lock file that keeps one `noise watch` process per session.
/// `SessionStart` hooks run again after compaction, with the same id.
const LOCK_FILE: &str = "watch.lock";

/// How long `noise room join` waits for the room's first neighbor, so
/// that a message sent right after the join reaches the room.
const JOIN_WAIT: Duration = Duration::from_secs(10);

/// The builder of the session's endpoint, before the session's key.
type Bind = fn() -> Result<Builder>;

/// Runs the `noise watch` process of the session in `dir`. Returns at once
/// when another process runs for the session.
///
/// # Errors
///
/// When the process does not run in a Claude Code session, or the state
/// directory, the endpoint, or the socket fails.
pub fn run(dir: &SessionDir) -> Result<()> {
    std::fs::create_dir_all(dir.path())?;
    // The socket's path is relative to the state directory.
    std::env::set_current_dir(dir.path())?;
    let Some(_lock) = lock(Path::new(LOCK_FILE))? else {
        println!("another noise watch runs for this session");
        return Ok(());
    };
    exit_with_parent()?;
    let inbox = Inbox::from_env()?;
    let config = config::path()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(dir, inbox, config))
}

/// Sends `request` to the `noise watch` process of the session in `dir`.
/// `None` when no process runs.
///
/// # Errors
///
/// When the socket breaks the protocol.
pub fn call(dir: &SessionDir, request: &Request) -> Result<Option<Response>> {
    let Some(runtime) = enter(dir)? else {
        return Ok(None);
    };
    runtime.block_on(watch_socket::call(Path::new(SOCKET_FILE), request))
}

/// Enters the session's state directory, where the socket's relative path
/// points, and builds a runtime for one request. `None` when the directory
/// does not exist: no process ran for the session.
fn enter(dir: &SessionDir) -> Result<Option<tokio::runtime::Runtime>> {
    match std::env::set_current_dir(dir.path()) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("enter {}", dir.path().display())),
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    Ok(Some(runtime))
}

/// The running process's shared parts.
struct Watch {
    state: Arc<SessionState>,
    node: OnceCell<Node<SessionState>>,
    bind: Bind,
}

impl Watch {
    /// The session's node. The endpoint binds on the first call, so a
    /// session without rooms has none.
    async fn node(&self) -> Result<&Node<SessionState>> {
        self.node
            .get_or_try_init(|| async {
                let endpoint = (self.bind)()?
                    .secret_key(self.state.key().clone())
                    .bind()
                    .await?;
                anyhow::Ok(Node::spawn(
                    endpoint,
                    Gossip::builder(),
                    Arc::clone(&self.state),
                ))
            })
            .await
    }

    async fn handle(&self, request: Request) -> Response {
        match self.answer(request).await {
            Ok(response) => response,
            Err(err) => Response::Error {
                message: format!("{err:#}"),
            },
        }
    }

    async fn answer(&self, request: Request) -> Result<Response> {
        let state = &self.state;
        Ok(match request {
            // `serve` returns after this response, and the process ends.
            Request::Stop => Response::Ok,
            Request::Create { label } => {
                let room = state.create(label)?;
                let id = room.id();
                // A new room has no other member to wait for.
                drop(self.node().await?.open_room(room, Vec::new()).await?);
                Response::Created { room: id }
            }
            Request::List => Response::Rooms {
                rooms: state.list(),
            },
            Request::Send { room, text } => {
                let room = state.resolve(&room)?;
                let (name, status) = state.name_and_status()?;
                let body = Body::Text { text, name, status };
                let (message, sealed) = state.seal(&room, body)?;
                self.node().await?.send(&room, &sealed).await?;
                Response::Sent { id: message.id() }
            }
            Request::SetStatus { status } => {
                let name = state.name_and_status()?.0;
                for room in state.set_status(&status)? {
                    let body = Body::Profile {
                        name: name.clone(),
                        status: status.clone(),
                    };
                    let (_, sealed) = state.seal(&room, body)?;
                    self.node().await?.send(&room, &sealed).await?;
                }
                Response::Ok
            }
            Request::Invite { room } => {
                let token = state.issue_ticket(&state.resolve(&room)?)?;
                let ticket = Ticket {
                    inviter: self.node().await?.endpoint().addr(),
                    token,
                };
                Response::Ticket {
                    ticket: ticket.encode()?,
                }
            }
            Request::Join { ticket } => {
                let ticket = Ticket::decode(&ticket)?;
                let node = self.node().await?;
                let admission = node.join(ticket.inviter, ticket.token).await?;
                let (room, peers) = state.join(admission)?;
                let id = room.id();
                let first_neighbor = node.open_room(room, peers).await?;
                // The room works without a neighbor too; the wait only
                // lets a message sent right after the join reach it.
                let _ = tokio::time::timeout(JOIN_WAIT, first_neighbor).await;
                Response::Joined { room: id }
            }
        })
    }
}

/// An endpoint with n0's relays and address lookup.
#[expect(
    clippy::unnecessary_wraps,
    reason = "a `Bind`, which a test's loopback endpoint also fits"
)]
fn bind_n0() -> Result<Builder> {
    Ok(Endpoint::builder(presets::N0))
}

async fn serve(dir: &SessionDir, inbox: Inbox, config: PathBuf) -> Result<()> {
    let (arrivals_in, arrivals) = mpsc::unbounded_channel();
    let watch = Arc::new(start(dir, bind_n0, config, arrivals_in).await?);
    tokio::spawn(deliver(inbox, arrivals));
    let listener = watch_socket::bind(Path::new(SOCKET_FILE))?;
    watch_socket::serve(listener, move |request| {
        let watch = Arc::clone(&watch);
        async move { watch.handle(request).await }
    })
    .await;
    Ok(())
}

/// Opens the session's state and, when the session is in a room, binds
/// its endpoint and opens every room.
async fn start(
    dir: &SessionDir,
    bind: Bind,
    config: PathBuf,
    arrivals: mpsc::UnboundedSender<Arrival>,
) -> Result<Watch> {
    let state = Arc::new(SessionState::open(dir, config, arrivals)?);
    let watch = Watch {
        state: Arc::clone(&state),
        node: OnceCell::new(),
        bind,
    };
    let rooms = state.rooms();
    if !rooms.is_empty() {
        let node = watch.node().await?;
        for (room, peers) in rooms {
            drop(node.open_room(room, peers).await?);
        }
    }
    Ok(watch)
}

/// Posts the arrivals to the inbox socket at the deadlines of
/// [`Delivery`].
async fn deliver(inbox: Inbox, mut arrivals: mpsc::UnboundedReceiver<Arrival>) {
    let mut delivery = Delivery::default();
    loop {
        let deadline = delivery.deadline();
        tokio::select! {
            arrival = arrivals.recv() => match arrival {
                Some(arrival) => delivery.add(arrival, Instant::now()),
                None => return,
            },
            () = sleep_until(deadline), if deadline.is_some() => {
                if let Some(post) = delivery.take(Instant::now()) {
                    let inbox = inbox.clone();
                    let posted = tokio::task::spawn_blocking(move || inbox.post(&post)).await;
                    if let Ok(Err(err)) = posted {
                        eprintln!("noise: posting to the inbox failed: {err:#}");
                    }
                }
            }
        }
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline.into()).await;
    }
}

/// Takes the lock at `path`. `None` when another process holds it.
fn lock(path: &Path) -> Result<Option<File>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(err)) => {
            Err(err).with_context(|| format!("lock {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use noise_net::{NetEvent, Session};
    use noise_room::{PublicKey, RoomId};
    use tokio::sync::broadcast;

    use super::*;

    #[test]
    fn a_second_lock_of_one_session_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(LOCK_FILE);
        let held = lock(&path).expect("lock");
        assert!(held.is_some());
        assert!(lock(&path).expect("lock").is_none());
        drop(held);
        assert!(lock(&path).expect("lock").is_some());
    }

    /// An endpoint on the loopback interface, without relays or address
    /// lookup.
    fn bind_loopback() -> Result<Builder> {
        Ok(Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr((Ipv4Addr::LOCALHOST, 0))?)
    }

    /// A `Watch` for a new session, with `name` in its config, and the
    /// receiver of what happens in its rooms.
    async fn watch(root: &Path, name: &str) -> (Arc<Watch>, mpsc::UnboundedReceiver<Arrival>) {
        let config = root.join("config.toml");
        std::fs::write(&config, format!("name = {name:?}\n")).expect("write");
        let dir = SessionDir::new(root, "s1");
        let (arrivals, received) = mpsc::unbounded_channel();
        let watch = start(&dir, bind_loopback, config, arrivals)
            .await
            .expect("start");
        (Arc::new(watch), received)
    }

    async fn reply(watch: &Arc<Watch>, request: Request) -> Response {
        watch.handle(request).await
    }

    /// Shuts the watch's node down before the test's runtime ends.
    async fn stop(watch: &Arc<Watch>) {
        if let Some(node) = watch.node.get() {
            node.shutdown().await;
        }
    }

    async fn create(watch: &Arc<Watch>, label: &str) -> RoomId {
        let created = reply(
            watch,
            Request::Create {
                label: label.into(),
            },
        )
        .await;
        let Response::Created { room } = created else {
            panic!("expected a created room, got {created:?}");
        };
        room
    }

    async fn invite(watch: &Arc<Watch>, room: &str) -> String {
        let invited = reply(watch, Request::Invite { room: room.into() }).await;
        let Response::Ticket { ticket } = invited else {
            panic!("expected a ticket, got {invited:?}");
        };
        ticket
    }

    async fn events(watch: &Arc<Watch>) -> broadcast::Receiver<NetEvent> {
        watch.node().await.expect("node").events()
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

    /// A room that `a` created and `b` joined, once each is the other's
    /// gossip neighbor, and the ticket that `b` used.
    async fn joined(a: &Arc<Watch>, b: &Arc<Watch>) -> (RoomId, String) {
        let room = create(a, "ci").await;
        let (mut events_a, mut events_b) = (events(a).await, events(b).await);
        let ticket = invite(a, "ci").await;
        let joined = reply(
            b,
            Request::Join {
                ticket: ticket.clone(),
            },
        )
        .await;
        assert_eq!(joined, Response::Joined { room });
        neighbor_up(&mut events_a, b.state.key().public()).await;
        neighbor_up(&mut events_b, a.state.key().public()).await;
        (room, ticket)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_created_room_takes_messages_by_label_or_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (watch, _) = watch(dir.path(), "vetle").await;
        let room = create(&watch, "ci").await;
        let Response::Rooms { rooms } = reply(&watch, Request::List).await else {
            panic!("expected rooms");
        };
        assert_eq!(rooms.len(), 1);
        assert_eq!((rooms[0].id, rooms[0].label.as_str()), (room, "ci"));
        assert_eq!(rooms[0].members.len(), 1);
        assert_eq!(rooms[0].members[0].name, "vetle");

        for name in ["ci".to_owned(), room.to_string()] {
            let sent = reply(
                &watch,
                Request::Send {
                    room: name,
                    text: "hello".into(),
                },
            )
            .await;
            assert!(matches!(sent, Response::Sent { .. }), "{sent:?}");
        }
        stop(&watch).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_status_shows_in_the_member_list() {
        let root = tempfile::tempdir().expect("tempdir");
        let (watch, _) = watch(root.path(), "vetle").await;
        create(&watch, "ci").await;
        let set = reply(
            &watch,
            Request::SetStatus {
                status: "CI optimization".into(),
            },
        )
        .await;
        assert_eq!(set, Response::Ok);
        let Response::Rooms { rooms } = reply(&watch, Request::List).await else {
            panic!("expected rooms");
        };
        assert_eq!(rooms[0].members[0].status, "CI optimization");
        stop(&watch).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_ticket_joins_one_session_and_its_messages_arrive() {
        let roots: Vec<_> = (0..3)
            .map(|_| tempfile::tempdir().expect("tempdir"))
            .collect();
        let (a, mut arrivals_a) = watch(roots[0].path(), "a").await;
        let (b, _) = watch(roots[1].path(), "b").await;
        let (c, _) = watch(roots[2].path(), "c").await;
        let (room, ticket) = joined(&a, &b).await;
        for watch in [&a, &b] {
            let Response::Rooms { rooms } = reply(watch, Request::List).await else {
                panic!("expected rooms");
            };
            assert_eq!((rooms[0].id, rooms[0].members.len()), (room, 2));
        }

        let sent = reply(
            &b,
            Request::Send {
                room: "ci".into(),
                text: "hello from b".into(),
            },
        )
        .await;
        assert!(matches!(sent, Response::Sent { .. }), "{sent:?}");
        match arrivals_a.recv().await.expect("receive") {
            Arrival::Text { message, .. } => {
                assert_eq!(
                    (message.text.as_str(), message.name.as_str()),
                    ("hello from b", "b")
                );
            }
            other => panic!("expected a text, got {other:?}"),
        }

        let again = reply(&c, Request::Join { ticket }).await;
        assert!(matches!(again, Response::Error { .. }), "{again:?}");
        assert!(!a.state.admits(&c.state.key().public()));
        for watch in [&a, &b, &c] {
            stop(watch).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_room_is_an_error() {
        let root = tempfile::tempdir().expect("tempdir");
        let (watch, _) = watch(root.path(), "vetle").await;
        let response = reply(
            &watch,
            Request::Send {
                room: "nowhere".into(),
                text: "hi".into(),
            },
        )
        .await;
        assert!(matches!(response, Response::Error { .. }), "{response:?}");
        stop(&watch).await;
    }
}
