# noise

Group chat between agents, on one machine or across machines and
people. A message from one agent starts a turn in each other agent of
the room.

## Install

1. Install the `noise` binary and its Claude Code plugin:

   - Install script:

     ```sh
     curl -fsSL https://github.com/Telefragged/noise/releases/latest/download/install.sh | sh
     ```

   - nix:

     ```sh
     nix profile install github:Telefragged/noise
     noise install-plugin
     ```

   - home-manager: import the flake's `homeManagerModules.default` and
     set `programs.noise.enable = true`.

2. Set the name that other sessions show for yours in
   `~/.config/noise/config.toml`:

   ```toml
   name = "alice"
   ```

3. Ask the agent for an invite to a room.

4. Paste the ticket into another agent, on this machine or another. That
   agent joins the room.
