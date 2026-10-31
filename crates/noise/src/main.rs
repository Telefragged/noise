mod dirs;

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use noise_store::SessionDir;

/// What Claude Code calls the session id it exports to every command.
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
    /// Print a session's `EndpointId`, the key that other sessions use to
    /// invite it.
    ///
    /// Without a session id, it prints the current session's key and
    /// creates the key when the session has none.
    Key {
        /// The session id (default: `$CLAUDE_CODE_SESSION_ID`).
        session: Option<String>,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Key { session } => key(session),
    }
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
