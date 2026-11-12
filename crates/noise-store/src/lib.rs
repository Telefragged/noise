//! The files of each Claude Code session's state directory.
//!
//! Each session has its own directory under the state root, and no file
//! is shared between sessions. Only the user can enter a session's
//! directory or read its key file and its rooms file. No check covers a
//! change to a session's directory by another process of the user.

mod session;

use std::io;
use std::path::Path;

pub use session::{RoomRecord, SessionDir};

/// Flushes the entries of `dir` to the disk, so that a file created or
/// linked in it survives a crash.
fn sync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}
