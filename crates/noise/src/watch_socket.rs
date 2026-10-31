//! The `noise watch` socket: the CLI and the plugin's hooks send their
//! requests to the session's `noise watch` process through it. Each
//! connection carries one request line and one response line, both JSON.
//!
//! The socket is `watch.sock` in the session's state directory. The CLI
//! and the process both change into that directory and use the relative
//! path, so a long home directory cannot exceed the 104 or 108 bytes of a
//! socket path.

use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use noise_room::{MessageId, RoomId};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::state::RoomInfo;

/// The socket's file name in the session's state directory.
pub const SOCKET_FILE: &str = "watch.sock";

/// A request from the CLI to the `noise watch` process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "snake_case")]
pub enum Request {
    /// End the process. [`serve`] returns once the `Ok` response is
    /// written.
    Stop,
    /// Create a room.
    Create { label: String },
    /// List the session's rooms.
    List,
    /// Send a chat message to a room. `room` is a room id or a label.
    Send { room: String, text: String },
    /// Set the session's status and announce it in every room.
    SetStatus { status: String },
}

/// The process's response to a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    Ok,
    Error { message: String },
    Created { room: RoomId },
    Rooms { rooms: Vec<RoomInfo> },
    Sent { id: MessageId },
}

/// Sends `request` to the `noise watch` process at `socket` and returns
/// the response, or `None` when no process listens.
///
/// # Errors
///
/// When the socket breaks the protocol.
pub async fn call(socket: &Path, request: &Request) -> Result<Option<Response>> {
    let stream = match UnixStream::connect(socket).await {
        Ok(stream) => stream,
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::NotFound | ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(err) => return Err(err).context("connect to the noise watch socket"),
    };
    let (read, mut write) = stream.into_split();
    let mut line = serde_json::to_vec(request)?;
    line.push(b'\n');
    write.write_all(&line).await?;
    let mut answer = String::new();
    BufReader::new(read).read_line(&mut answer).await?;
    if answer.is_empty() {
        return Err(anyhow!("the noise watch process closed the socket"));
    }
    Ok(Some(serde_json::from_str(&answer)?))
}

/// Binds `socket`, replacing a file that an ended process left.
///
/// # Errors
///
/// When the socket cannot be bound.
pub fn bind(socket: &Path) -> Result<UnixListener> {
    match std::fs::remove_file(socket) {
        Err(err) if err.kind() != ErrorKind::NotFound => return Err(err.into()),
        _ => {}
    }
    UnixListener::bind(socket).context("bind the noise watch socket")
}

/// Answers every connection on `listener` with `handle`, until `handle`
/// answers a [`Request::Stop`] with [`Response::Ok`].
pub async fn serve<H, F>(listener: UnixListener, handle: H)
where
    H: Fn(Request) -> F + Clone + Send + 'static,
    F: Future<Output = Response> + Send,
{
    let (stopped, mut stop) = mpsc::channel(1);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    return;
                };
                let (handle, stopped) = (handle.clone(), stopped.clone());
                tokio::spawn(async move {
                    // A client that breaks the protocol gets no answer.
                    let _ = answer(stream, handle, stopped).await;
                });
            }
            Some(()) = stop.recv() => return,
        }
    }
}

/// Answers the request on `stream`. After it wrote the `Ok` response to a
/// `Stop`, it sends to `stopped`.
async fn answer<H, F>(stream: UnixStream, handle: H, stopped: mpsc::Sender<()>) -> Result<()>
where
    H: Fn(Request) -> F,
    F: Future<Output = Response>,
{
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    let (stop, response) = match serde_json::from_str(&line) {
        Ok(request) => (request == Request::Stop, handle(request).await),
        Err(err) => (
            false,
            Response::Error {
                message: format!("malformed request: {err}"),
            },
        ),
    };
    let mut line = serde_json::to_vec(&response)?;
    line.push(b'\n');
    write.write_all(&line).await?;
    if stop && response == Response::Ok {
        // The serving loop is the only receiver, and it waits for this.
        let _ = stopped.send(()).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_request_gets_the_handlers_response_and_a_stop_ends_the_loop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join(SOCKET_FILE);
        assert_eq!(call(&socket, &Request::List).await.expect("call"), None);

        let listener = bind(&socket).expect("bind");
        let serving = tokio::spawn(serve(listener, |request| async move {
            match request {
                Request::Stop => Response::Ok,
                _ => Response::Error {
                    message: "not now".into(),
                },
            }
        }));
        assert_eq!(
            call(&socket, &Request::List).await.expect("call"),
            Some(Response::Error {
                message: "not now".into()
            })
        );
        assert_eq!(
            call(&socket, &Request::Stop).await.expect("call"),
            Some(Response::Ok)
        );
        serving.await.expect("serve");
    }
}
