use std::fs;
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};

use iroh_base::{PublicKey, SecretKey};

/// The name of the file that contains a session's secret key.
const KEY_FILE: &str = "key";

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
        fs::create_dir_all(&self.path)?;
        let candidate = self
            .path
            .join(format!("{KEY_FILE}.{:016x}", rand::random::<u64>()));
        let mut file = fs::File::create_new(&candidate)?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
