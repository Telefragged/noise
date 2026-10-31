//! The files that noise keeps for each Claude Code session.
//!
//! Each session has its own directory under the state root, and no file
//! is shared between sessions. noise assumes that no other local process
//! modifies a session's directory.

mod session;

use std::io;
use std::path::Path;

pub use session::SessionDir;

/// Flushes the entries of `dir` to the disk, so that a file created or
/// linked in it survives a crash.
fn sync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}
