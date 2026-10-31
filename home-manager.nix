# The home-manager module: `programs.noise` puts noise on PATH and installs
# its Claude Code plugin. The plugin starts a `noise watch` process for each
# session, so noise needs no service of its own.
self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.programs.noise;
in
{
  options.programs.noise = {
    enable = lib.mkEnableOption "noise, group chat between Claude Code sessions";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.noise;
      defaultText = lib.literalExpression "noise.packages.\${system}.noise";
      description = "The noise package that the CLI and the plugin's hooks run.";
    };
  };

  config = lib.mkIf cfg.enable {
    home.packages = [ cfg.package ];
    # Every switch runs this, so the plugin follows the package. The
    # activation PATH holds neither place that Claude Code installs to.
    home.activation.noiseClaudePlugin = lib.hm.dag.entryAfter [ "writeBoundary" ] ''
      (
        PATH="${config.home.profileDirectory}/bin:$HOME/.local/bin:$PATH"
        run ${lib.getExe' cfg.package "noise"} install-plugin
      )
    '';
  };
}
