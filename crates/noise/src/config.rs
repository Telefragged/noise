//! The global config: `$XDG_CONFIG_HOME/noise/config.toml`, else
//! `~/.config/noise/config.toml`.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// The settings that every session of the user shares.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The fixed part of each session's display name, such as `vetle`.
    #[serde(default)]
    pub name: String,
}

impl Config {
    /// Reads the config at `path`. A missing file is the default config.
    ///
    /// # Errors
    ///
    /// When the file cannot be read or is not a valid config.
    pub fn read(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text).with_context(|| format!("read {}", path.display())),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(Self::default()),
            Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
}

/// `$XDG_CONFIG_HOME/noise/config.toml`, else
/// `~/.config/noise/config.toml`.
///
/// # Errors
///
/// When neither `XDG_CONFIG_HOME` nor `HOME` is set.
pub fn path() -> Result<PathBuf> {
    Ok(crate::dirs::xdg("XDG_CONFIG_HOME", &[".config"])?
        .join("noise")
        .join("config.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_file_is_the_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = Config::read(&dir.path().join("config.toml")).expect("config");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn the_name_comes_from_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "name = \"vetle\"\n").expect("write");
        assert_eq!(Config::read(&path).expect("config").name, "vetle");
    }

    #[test]
    fn an_unknown_setting_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "nmae = \"vetle\"\n").expect("write");
        assert!(Config::read(&path).is_err());
    }
}
