use std::fs::{self, DirBuilder, OpenOptions, Permissions};
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use iroh_base::{PublicKey, SecretKey};
use noise_room::{Member, RoomSecret};
use serde::{Deserialize, Serialize};

/// The name of the file that contains a session's secret key.
const KEY_FILE: &str = "key";

/// The name of the file that contains a session's status.
const STATUS_FILE: &str = "status";

/// The name of the file that contains a session's rooms.
const ROOMS_FILE: &str = "rooms.json";

/// The mode of a session's directory: only the user can enter it.
const DIR_MODE: u32 = 0o700;

/// The mode of the key file and the rooms file, which contain the
/// session's secret key and the room secrets: only the user can read them.
const FILE_MODE: u32 = 0o600;

/// A room of the session, as its rooms file has it: what a restarted
/// `noise watch` process needs to rejoin the room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomRecord {
    pub label: String,
    pub secret: RoomSecret,
    pub members: Vec<Member>,
}

/// The state directory of one Claude Code session:
/// `<root>/sessions/<session-id>/`.
#[derive(Debug, Clone)]
pub struct SessionDir {
    path: PathBuf,
}

impl SessionDir {
    /// The directory of the session `id` under the state `root`. The
    /// directory need not exist yet.
    #[must_use]
    pub fn new(root: &Path, id: &str) -> Self {
        Self {
            path: root.join("sessions").join(id),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Creates the session's directory with the mode 0700, so that only
    /// the user can enter it. A directory that exists with another mode,
    /// which an earlier version of noise created, gets this mode too.
    ///
    /// # Errors
    ///
    /// When the directory cannot be created or its mode cannot be set.
    pub fn create(&self) -> io::Result<()> {
        DirBuilder::new()
            .recursive(true)
            .mode(DIR_MODE)
            .create(&self.path)?;
        fs::set_permissions(&self.path, Permissions::from_mode(DIR_MODE))
    }

    /// The session's rooms, none when the session has no rooms file.
    ///
    /// # Errors
    ///
    /// When the rooms file cannot be read or parsed.
    pub fn rooms(&self) -> io::Result<Vec<RoomRecord>> {
        match fs::read(self.path.join(ROOMS_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::from),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    /// Replaces the session's rooms. The new file reaches the disk under
    /// another name first and then replaces the old one, so a crash leaves
    /// one of the two whole.
    ///
    /// # Errors
    ///
    /// When the rooms file cannot be written.
    pub fn set_rooms(&self, rooms: &[RoomRecord]) -> io::Result<()> {
        self.create()?;
        let next = self.path.join(format!("{ROOMS_FILE}.next"));
        let mut file = private().create(true).truncate(true).open(&next)?;
        // A file that a crash left here keeps the mode it has.
        file.set_permissions(Permissions::from_mode(FILE_MODE))?;
        file.write_all(&serde_json::to_vec_pretty(rooms)?)?;
        file.sync_all()?;
        fs::rename(&next, self.path.join(ROOMS_FILE))?;
        crate::sync_dir(&self.path)
    }

    /// The session's status, empty when the session has set none.
    ///
    /// # Errors
    ///
    /// When the status file cannot be read.
    pub fn status(&self) -> io::Result<String> {
        match fs::read_to_string(self.path.join(STATUS_FILE)) {
            Ok(status) => Ok(status),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(String::new()),
            Err(err) => Err(err),
        }
    }

    /// Replaces the session's status.
    ///
    /// # Errors
    ///
    /// When the status file cannot be written.
    pub fn set_status(&self, status: &str) -> io::Result<()> {
        self.create()?;
        fs::write(self.path.join(STATUS_FILE), status)
    }

    /// The session's secret key. The first call creates it.
    ///
    /// Two processes that create the key at the same time get the same
    /// key: each writes a candidate to a file of its own and links it to
    /// the key file, and a link fails when the key file exists.
    ///
    /// # Errors
    ///
    /// When the directory or the key file cannot be read or written, or
    /// when the key file does not contain a key.
    pub fn secret_key(&self) -> io::Result<SecretKey> {
        if let Some(key) = self.read_key()? {
            return Ok(key);
        }
        self.create()?;
        let candidate = self
            .path
            .join(format!("{KEY_FILE}.{:016x}", rand::random::<u64>()));
        let mut file = private().create_new(true).open(&candidate)?;
        file.write_all(&SecretKey::generate().to_bytes())?;
        file.sync_all()?;
        let linked = fs::hard_link(&candidate, self.path.join(KEY_FILE));
        fs::remove_file(&candidate)?;
        match linked {
            Ok(()) => crate::sync_dir(&self.path)?,
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
        self.read_key()?
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "the key file vanished"))
    }

    /// The session's public key, or `None` when the session has no key.
    ///
    /// # Errors
    ///
    /// When the key file cannot be read, or does not contain a key.
    pub fn public_key(&self) -> io::Result<Option<PublicKey>> {
        Ok(self.read_key()?.map(|key| key.public()))
    }

    fn read_key(&self) -> io::Result<Option<SecretKey>> {
        let bytes = match fs::read(self.path.join(KEY_FILE)) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "the key file is not 32 bytes"))?;
        Ok(Some(SecretKey::from_bytes(&bytes)))
    }
}

/// The options that open a file for writing and create it with
/// [`FILE_MODE`].
fn private() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).mode(FILE_MODE);
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The permission bits of `path` for the group and for other users.
    fn group_and_others(path: &Path) -> u32 {
        let metadata = fs::metadata(path).expect("metadata");
        metadata.permissions().mode() & 0o077
    }

    #[test]
    fn only_the_user_can_read_the_key_and_the_rooms() {
        let root = tempfile::tempdir().expect("tempdir");
        let session = SessionDir::new(root.path(), "s1");
        session.secret_key().expect("key");
        session.set_rooms(&[]).expect("write rooms");
        for path in [
            session.path().to_owned(),
            session.path().join(KEY_FILE),
            session.path().join(ROOMS_FILE),
        ] {
            assert_eq!(group_and_others(&path), 0, "{}", path.display());
        }
    }

    #[test]
    fn a_directory_and_a_rooms_file_that_others_can_read_lose_that_mode() {
        let root = tempfile::tempdir().expect("tempdir");
        let session = SessionDir::new(root.path(), "s1");
        fs::create_dir_all(session.path()).expect("create dir");
        fs::set_permissions(session.path(), Permissions::from_mode(0o755)).expect("set mode");
        let next = session.path().join(format!("{ROOMS_FILE}.next"));
        fs::write(&next, "").expect("write");
        fs::set_permissions(&next, Permissions::from_mode(0o644)).expect("set mode");
        session.set_rooms(&[]).expect("write rooms");
        assert_eq!(group_and_others(session.path()), 0);
        assert_eq!(group_and_others(&session.path().join(ROOMS_FILE)), 0);
    }

    #[test]
    fn a_session_without_a_key_has_no_public_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let session = SessionDir::new(root.path(), "s1");
        assert_eq!(session.public_key().expect("key"), None);
    }

    #[test]
    fn the_key_stays_the_same_across_calls_and_handles() {
        let root = tempfile::tempdir().expect("tempdir");
        let first = SessionDir::new(root.path(), "s1")
            .secret_key()
            .expect("key");
        let again = SessionDir::new(root.path(), "s1")
            .secret_key()
            .expect("key");
        assert_eq!(first.public(), again.public());
        assert_eq!(
            SessionDir::new(root.path(), "s1")
                .public_key()
                .expect("key"),
            Some(first.public())
        );
    }

    #[test]
    fn sessions_have_different_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let one = SessionDir::new(root.path(), "s1")
            .secret_key()
            .expect("key");
        let two = SessionDir::new(root.path(), "s2")
            .secret_key()
            .expect("key");
        assert_ne!(one.public(), two.public());
    }

    #[test]
    fn concurrent_creation_yields_one_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let keys: Vec<PublicKey> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        SessionDir::new(root.path(), "s1")
                            .secret_key()
                            .expect("key")
                            .public()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("join"))
                .collect()
        });
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn the_status_is_empty_until_set() {
        let root = tempfile::tempdir().expect("tempdir");
        let session = SessionDir::new(root.path(), "s1");
        assert_eq!(session.status().expect("status"), "");
        session.set_status("CI optimization").expect("set status");
        assert_eq!(session.status().expect("status"), "CI optimization");
    }

    #[test]
    fn the_rooms_round_trip_through_their_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let session = SessionDir::new(root.path(), "s1");
        assert_eq!(session.rooms().expect("read rooms"), Vec::new());
        let rooms = vec![RoomRecord {
            label: "ci".into(),
            secret: RoomSecret::from_bytes([3; 32]),
            members: vec![Member {
                key: SecretKey::from_bytes(&[1; 32]).public(),
                name: "vetle".into(),
                status: "CI".into(),
            }],
        }];
        session.set_rooms(&rooms).expect("write rooms");
        assert_eq!(session.rooms().expect("read rooms"), rooms);
        session.set_rooms(&[]).expect("write rooms");
        assert_eq!(session.rooms().expect("read rooms"), Vec::new());
    }

    #[test]
    fn a_truncated_key_file_is_an_error() {
        let root = tempfile::tempdir().expect("tempdir");
        let session = SessionDir::new(root.path(), "s1");
        fs::create_dir_all(session.path()).expect("create dir");
        fs::write(session.path().join(KEY_FILE), [0u8; 5]).expect("write");
        assert_eq!(
            session.secret_key().expect_err("an error").kind(),
            ErrorKind::InvalidData
        );
    }
}
