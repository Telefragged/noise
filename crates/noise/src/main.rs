mod config;
mod delivery;
mod dirs;
mod inbox;
mod parent;
mod state;
mod ticket;
mod watch;
mod watch_socket;

use std::io::Read;
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
    /// Create, list, invite to, and join the current session's rooms.
    #[command(subcommand)]
    Room(RoomCommand),
    /// Send a chat message to a room.
    Send {
        /// The room: its id, or a label that one room has.
        room: String,
        /// The message (default: standard input).
        text: Option<String>,
    },
    /// Set the current session's status, the second part of its display
    /// name, and announce it in every room.
    SetStatus { status: String },
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

#[derive(Subcommand)]
enum RoomCommand {
    /// Create a room and print its id.
    Create { label: String },
    /// Print the session's rooms and their members, one JSON object per
    /// line.
    List,
    /// Print a single-use ticket that lets one session join a room. The
    /// ticket works while this session's `noise watch` process runs, for
    /// at most 24 hours.
    Invite {
        /// The room: its id, or a label that one room has.
        room: String,
    },
    /// Join a room with a ticket from one of its members.
    Join { ticket: String },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Key { session } => key(session),
        Command::Room(RoomCommand::Create { label }) => print(&Request::Create { label }),
        Command::Room(RoomCommand::List) => print(&Request::List),
        Command::Room(RoomCommand::Invite { room }) => print(&Request::Invite { room }),
        Command::Room(RoomCommand::Join { ticket }) => print(&Request::Join { ticket }),
        Command::Send { room, text } => {
            let text = if let Some(text) = text {
                text
            } else {
                let mut text = String::new();
                std::io::stdin().read_to_string(&mut text)?;
                text
            };
            print(&Request::Send { room, text })
        }
        Command::SetStatus { status } => print(&Request::SetStatus { status }),
        Command::Watch { stop: false } => watch::run(&current_session_dir()?),
        // A session without a running process has nothing to stop.
        Command::Watch { stop: true } => request(&Request::Stop).map(|_| ()),
    }
}

/// Sends `request` to the current session's `noise watch` process.
/// `None` when no process runs.
fn request(request: &Request) -> Result<Option<Response>> {
    match watch::call(&current_session_dir()?, request)? {
        Some(Response::Error { message }) => bail!("{message}"),
        response => Ok(response),
    }
}

/// Sends `request` to the current session's `noise watch` process and
/// prints the result.
fn print(request: &Request) -> Result<()> {
    let response = self::request(request)?
        .ok_or_else(|| anyhow!("no noise watch process runs for this session"))?;
    match response {
        Response::Ok | Response::Error { .. } => {}
        Response::Created { room } | Response::Joined { room } => println!("{room}"),
        Response::Ticket { ticket } => println!("{ticket}"),
        Response::Sent { id } => println!("{id}"),
        Response::Rooms { rooms } => {
            for room in rooms {
                println!("{}", serde_json::to_string(&room)?);
            }
        }
    }
    Ok(())
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
