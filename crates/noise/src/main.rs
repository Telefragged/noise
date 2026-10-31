mod config;
mod delivery;
mod dirs;
mod inbox;
mod parent;
mod state;
mod watch;
mod watch_socket;

use std::path::PathBuf;

use anyhow::{Result, anyhow, bail};
use clap::{Parser, Subcommand};
use noise_store::SessionDir;

use crate::watch_socket::{Request, Response};

/// The variable that contains the session id, which Claude Code exports
/// to every command.
const SESSION_ID_VAR: &str = "CLAUDE_CODE_SESSION_ID";

/// Group chat between Claude Code sessions.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a session's `EndpointId`, the key that identifies it in
    /// rooms.
    ///
    /// Without a session id, it prints the current session's key and
    /// creates the key when the session has none.
    Key {
        /// The session id (default: `$CLAUDE_CODE_SESSION_ID`).
        session: Option<String>,
    },
    /// Run the process that connects the current session to its rooms.
    ///
    /// The plugin's `SessionStart` hook starts it. It returns at once when
    /// another `noise watch` process runs for the session.
    Watch {
        /// Stop the session's `noise watch` process instead.
        #[arg(long)]
        stop: bool,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Key { session } => key(session),
        Command::Watch { stop: false } => watch::run(&current_session_dir()?),
        Command::Watch { stop: true } => request(&Request::Stop).map(|_| ()),
    }
}

/// Sends `request` to the current session's `noise watch` process.
/// Returns `false` when no process runs.
fn request(request: &Request) -> Result<bool> {
    match watch::call(&current_session_dir()?, request)? {
        None => Ok(false),
        Some(Response::Ok) => Ok(true),
        Some(Response::Error { message }) => bail!("{message}"),
    }
}

fn current_session_dir() -> Result<SessionDir> {
    let id =
        current_session().ok_or_else(|| anyhow!("no session: run in a Claude Code session"))?;
    session_dir(&id)
}

fn key(session: Option<String>) -> Result<()> {
    let current = current_session();
    let key = match session {
        Some(id) if current.as_deref() != Some(id.as_str()) => session_dir(&id)?
            .public_key()?
            .ok_or_else(|| anyhow!("session {id} has no key"))?,
        Some(id) => session_dir(&id)?.secret_key()?.public(),
        None => {
            let id = current.ok_or_else(|| {
                anyhow!("no session: pass a session id, or run in a Claude Code session")
            })?;
            session_dir(&id)?.secret_key()?.public()
        }
    };
    println!("{key}");
    Ok(())
}

/// The current Claude Code session, or `None` outside one.
fn current_session() -> Option<String> {
    dirs::env_var(SESSION_ID_VAR)?.into_string().ok()
}

fn session_dir(id: &str) -> Result<SessionDir> {
    Ok(SessionDir::new(&state_root()?, id))
}

/// `$XDG_STATE_HOME/noise`, else `~/.local/state/noise`.
fn state_root() -> Result<PathBuf> {
    Ok(dirs::xdg("XDG_STATE_HOME", &[".local", "state"])?.join("noise"))
}
