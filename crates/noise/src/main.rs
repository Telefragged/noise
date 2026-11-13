mod config;
mod delivery;
mod dirs;
mod inbox;
mod parent;
mod plugin;
mod state;
mod status;
mod ticket;
mod watch;
mod watch_socket;

use std::io::Read;
use std::path::PathBuf;

use anyhow::{Result, anyhow, bail};
use clap::{Parser, Subcommand};
use noise_room::PublicKey;
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
    /// Print a session's `EndpointId`, the key that identifies it in rooms
    /// and in `noise status`.
    ///
    /// Without a session id, it prints the current session's key and
    /// creates the key when the session has none.
    Key {
        /// The session id (default: `$CLAUDE_CODE_SESSION_ID`).
        session: Option<String>,
    },
    /// Create, list, invite to, join and leave the current session's
    /// rooms.
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
    /// Print a session's peers and their connections: the path of each
    /// connection, direct or through a relay, and the bytes sent and
    /// received. One JSON array of peers per line.
    Status {
        /// The session id (default: `$CLAUDE_CODE_SESSION_ID`).
        session: Option<String>,
        /// Print a new line at each change, and every second, until
        /// interrupted.
        #[arg(long)]
        follow: bool,
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
    /// Install the Claude Code plugin that this build contains.
    InstallPlugin,
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
        /// The key of the session to invite, which `noise key` prints in
        /// that session. Only that session can use the ticket.
        #[arg(long)]
        key: Option<PublicKey>,
    },
    /// Join a room with a ticket from one of its members.
    Join { ticket: String },
    /// Leave a room. Joining it again takes a new ticket.
    Leave {
        /// The room: its id, or a label that one room has.
        room: String,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Key { session } => key(session),
        Command::Room(RoomCommand::Create { label }) => print(&Request::Create { label }),
        Command::Room(RoomCommand::List) => print(&Request::List),
        Command::Room(RoomCommand::Invite { room, key }) => print(&Request::Invite { room, key }),
        Command::Room(RoomCommand::Join { ticket }) => print(&Request::Join { ticket }),
        Command::Room(RoomCommand::Leave { room }) => print(&Request::Leave { room }),
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
        Command::Status { session, follow } => status(session, follow),
        Command::Watch { stop: false } => watch::run(&current_session_dir()?),
        // A session without a running process has nothing to stop.
        Command::Watch { stop: true } => request(&Request::Stop).map(|_| ()),
        Command::InstallPlugin => plugin::install_plugin(),
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
    let response = self::request(request)?.ok_or_else(|| {
        anyhow!("no noise watch process runs for this session; is the noise plugin installed?")
    })?;
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
        Response::Status { peers } => println!("{}", serde_json::to_string(&peers)?),
    }
    Ok(())
}

/// Prints the status of the session `session`, or of the current one.
fn status(session: Option<String>, follow: bool) -> Result<()> {
    let dir = match session {
        Some(id) => session_dir(&id)?,
        None => current_session_dir()?,
    };
    let mut failed = None;
    let answered = watch::stream(&dir, &Request::Status { follow }, |response| {
        match response {
            Response::Status { peers } => match serde_json::to_string(&peers) {
                Ok(line) => println!("{line}"),
                Err(err) => failed = Some(err.into()),
            },
            Response::Error { message } => failed = Some(anyhow!("{message}")),
            other => failed = Some(anyhow!("unexpected response: {other:?}")),
        }
        failed.is_none() && follow
    })?;
    if let Some(err) = failed {
        return Err(err);
    }
    if !answered {
        bail!("no noise watch process runs for that session");
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
