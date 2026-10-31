//! The environment variables and directories that noise reads.

use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, Result};

/// The variable `name`, or `None` when it is unset or empty.
#[must_use]
pub fn env_var(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}

/// The base directory that the XDG variable `name` sets, else `$HOME`
/// joined with `fallback`. As the XDG spec requires, a relative value is
/// ignored.
///
/// # Errors
///
/// When the variable is not an absolute path and `HOME` is not set.
pub fn xdg(name: &str, fallback: &[&str]) -> Result<PathBuf> {
    if let Some(dir) = env_var(name).map(PathBuf::from)
        && dir.is_absolute()
    {
        return Ok(dir);
    }
    let home = PathBuf::from(env_var("HOME").context("$HOME is not set")?);
    Ok(fallback.iter().fold(home, |path, part| path.join(part)))
}
