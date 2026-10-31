//! Posts to the session's inbox socket, which Claude Code exports to every
//! command of the session. A post starts a turn in an idle session.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use serde_json::json;

use crate::dirs::env_var;

/// What Claude Code calls the inbox socket it exports to every command.
const SOCKET_VAR: &str = "CLAUDE_CODE_MESSAGING_SOCKET";
/// The token that proves a post comes from the session's own child.
const TOKEN_VAR: &str = "CLAUDE_CODE_MESSAGING_TOKEN";

/// The session's inbox socket.
#[derive(Debug, Clone)]
pub struct Inbox {
    path: PathBuf,
    /// The first line of every connection, when the session has a token.
    auth: Option<String>,
}

impl Inbox {
    /// The inbox socket that Claude Code exported to this process.
    ///
    /// # Errors
    ///
    /// When the process does not run in a Claude Code session.
    pub fn from_env() -> Result<Self> {
        let exported = env_var(SOCKET_VAR)
            .and_then(|value| value.into_string().ok())
            .ok_or_else(|| anyhow!("no inbox: ${SOCKET_VAR} is not set"))?;
        // `/status` spells the same address with this prefix.
        let path = PathBuf::from(exported.strip_prefix("uds:").unwrap_or(&exported));
        let token = env_var(TOKEN_VAR).and_then(|value| value.into_string().ok());
        Ok(Self::new(path, token.as_deref()))
    }

    /// The inbox socket at `path`, with the session's token when it has
    /// one.
    #[must_use]
    pub fn new(path: PathBuf, token: Option<&str>) -> Self {
        let auth = token.map(|token| json!({ "type": "auth", "token": token }).to_string());
        Self { path, auth }
    }

    /// Posts `text` as one message. The connection opens only now, because
    /// Claude Code drops a connection that sends no complete line within
    /// 30 seconds.
    ///
    /// # Errors
    ///
    /// When the socket cannot be reached or written.
    pub fn post(&self, text: &str) -> Result<()> {
        let mut socket = UnixStream::connect(&self.path)
            .with_context(|| format!("connect to {}", self.path.display()))?;
        if let Some(auth) = &self.auth {
            writeln!(socket, "{auth}")?;
        }
        let post = json!({ "type": "user", "message": { "content": text } });
        writeln!(socket, "{post}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixListener;

    use super::*;

    #[test]
    fn a_post_is_the_auth_line_then_the_message() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("inbox.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let inbox = Inbox::new(path, Some("secret"));
        inbox.post("first line\nsecond line").expect("post");

        let (socket, _) = listener.accept().expect("accept");
        let lines: Vec<serde_json::Value> = BufReader::new(socket)
            .lines()
            .map(|line| serde_json::from_str(&line.expect("line")).expect("json"))
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["type"], "auth");
        assert_eq!(lines[0]["token"], "secret");
        assert_eq!(lines[1]["type"], "user");
        assert_eq!(lines[1]["message"]["content"], "first line\nsecond line");
    }
}
