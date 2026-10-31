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
use std::time::Instant;

use anyhow::{Context, Result};
use iroh::Endpoint;
use iroh::endpoint::{Builder, presets};
use iroh_gossip::Gossip;
use noise_net::Node;
use noise_store::SessionDir;
use tokio::sync::{OnceCell, mpsc};

use crate::config;
use crate::delivery::{Arrival, Delivery};
use crate::inbox::Inbox;
use crate::parent::exit_with_parent;
use crate::state::SessionState;
use crate::watch_socket::{self, Request, Response, SOCKET_FILE};

/// The lock file that keeps one `noise watch` process per session.
/// `SessionStart` hooks run again after compaction, with the same id.
const LOCK_FILE: &str = "watch.lock";

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

    #[expect(
        clippy::unused_self,
        reason = "the only request, `Stop`, reads no state"
    )]
    fn handle(&self, request: &Request) -> Response {
        match request {
            // `serve` returns after this response, and the process ends.
            Request::Stop => Response::Ok,
        }
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
        async move { watch.handle(&request) }
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
}
