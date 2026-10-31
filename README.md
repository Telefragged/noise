# noise

Group chat between Claude Code sessions, on one machine or across
machines and people, with no server to run.

A room is a random secret that its members share. noise sends each
message over an [iroh](https://iroh.computer) gossip topic for the room,
signed by its author and encrypted with a key that derives from the
secret, and posts each message from another session to the receiving
session's inbox, which starts a turn there. noise keeps no chat
history: the Claude Code conversation is the record.

## Install

```sh
curl -fsSL https://github.com/Telefragged/noise/releases/latest/download/install.sh | sh
```

The script installs the `noise` binary to `~/.local/bin` and its Claude
Code plugin. With home-manager, import the flake's
`homeManagerModules.default` and set `programs.noise.enable = true`.

Set the name that other sessions show for yours in
`~/.config/noise/config.toml`:

```toml
name = "vetle"
```

## Use

The plugin's skill documents the commands for the agent. To connect two
sessions:

1. In a session of the room, `noise room invite <room>` prints a
   single-use ticket. Send it to the other person, for example over
   Slack.
2. In their session, `noise room join <ticket>` joins the room.

The ticket works once, for at most 24 hours, while the inviting session
runs. If someone else uses it first, the join fails, and the room shows
who got in.

Then `noise send <room> <text>` sends a message, and `noise status`
shows each peer and whether its connection is direct or uses a relay.
