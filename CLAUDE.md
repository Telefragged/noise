# noise

Peer-to-peer chat for Claude agents. The crate is a hello-world binary
for now.

## Development

- Run every tool in the nix devShell: `nix develop -c <cmd>`.
- `Cargo.nix` is generated. After any `Cargo.lock` change, run
  `nix develop -c crate2nix generate` and commit the result in the same
  commit.
- Format before each commit with `nix develop -c treefmt`. The `treefmt`
  flake check fails on an unformatted file.
- `nix develop -c cargo check` is the fast check. `nix flake check` is the
  full one: it builds every crate under clippy with warnings denied, and
  it runs the tests and rustdoc, which `nix build` skips.
- `scripts/check-commits.sh master` runs `nix flake check` on every commit
  in `master..HEAD`. Each commit must pass on its own.
- Keep commits small, with one concern each. Hard-wrap the message at 72
  columns: a one-line subject, a blank line, then the body.

## Releases

Raise `workspace.package.version` in `Cargo.toml`, then run
`nix develop -c cargo check` and `nix develop -c crate2nix generate`. A
push to master whose version has no release tag yet makes the release
workflow publish it. `nix run .#release-check` prints whether the
workflow will publish a release.

The release job is off until the first release, with `if: false` in
`.github/workflows/release.yml`. Removing that line is the first release:
no release tag exists, so the push that adds the change to master
publishes the version in `Cargo.toml`.

## Wording

These rules cover everything written for noise: code, comments, docs,
chat output, commit messages, PR titles, descriptions and comments, nit
comments, and release notes.

- Do not anthropomorphize. No part of noise speaks, says, tells, gives,
  holds, asks, knows or wants. Use the simplest common technical verb: a
  process sends, receives, reads, writes, returns or prints, and a
  workspace contains a crate. A verb that is the standard technical term
  is fine, such as a socket that listens.
- Do not invent names for parts. Name a part by what it is. A socket is
  "the X socket", where X is what it connects to, not "the listener". The
  other end of a conversation is "the peer Claude session", not "the
  speaker".

## Layout

- `crates/noise/` — the `noise` binary
- `scripts/` — the per-commit check, the PR wait script, and the git-mito
  hook
- `release/` — the release check that the release workflow runs
