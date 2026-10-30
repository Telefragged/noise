{
  description = "noise — peer-to-peer chat for Claude agents";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            }
          )
        );
      # Pin rustc from rust-toolchain.toml so nix and rustup builds match.
      rustToolchainFor = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

      # Rustdoc, run inside the crate's own build. `runTests` covers every
      # test kind except the doc examples, and no build runs rustdoc:
      # buildRustCrate runs rustc directly, so nothing extracts the examples
      # from doc-comments or resolves the intra-doc links between them.
      # rustdoc needs the same flags as the lib build, and build-crate.nix
      # sets them in the shell that runs `postBuild`, so this hook runs
      # rustdoc with those flags. It runs only when `buildTests` is set, in
      # the test build, whose dependencies include the dev-dependencies that
      # cargo also passes to doc examples. `docTests` is false for a cdylib,
      # which has no rlib for an example to link against. cargo skips its
      # doc tests for the same reason, but still documents it. A crate
      # without a lib skips both.
      rustdocPostBuild = docTests: ''
        # If any other variable read here were unset, the rustdoc calls
        # would fail. An unset $buildTests would skip them without an error,
        # so make that case an error.
        : "''${buildTests?buildRustCrate no longer sets buildTests}"
        if [ -n "$buildTests" ]; then
          docLib="$LIB_PATH"
          [ -e "$docLib" ] || docLib=src/lib.rs
          if [ -e "$docLib" ]; then
            docOpts=""
            for opt in $LIB_RUSTC_OPTS; do
              case "$opt" in
                --remap-path-prefix=*) ;;
                *) docOpts="$docOpts $opt" ;;
              esac
            done
            # Rendering the docs resolves every intra-doc link, so a broken
            # or private link is a rustdoc lint error, denied by the
            # `[workspace.lints]` flags in $LIB_RUSTC_OPTS. Only the exit
            # code matters; nothing installs the rendered docs.
            # No $EXTRA_LIB: a crate is not an extern of its own docs.
            rustdoc "$docLib" \
              --crate-name "$CRATE_NAME" \
              --out-dir target/doc \
              --cap-lints "$CAP_LINTS" \
              --color always \
              -L dependency=target/deps \
              $docOpts $LINK $BUILD_OUT_DIR $EXTRA_FEATURES
            ${nixpkgs.lib.optionalString docTests ''
              echo "Doc-testing $CRATE_NAME ($docLib)"
              # $EXTRA_LIB is the --extern for this crate's own rlib, built
              # by build_lib before the --test pass; the rest is verbatim
              # what build_lib passes rustc.
              rustdoc --test "$docLib" \
                --crate-name "$CRATE_NAME" \
                --cap-lints "$CAP_LINTS" \
                --color always \
                -L dependency=target/deps \
                $docOpts $EXTRA_LIB $LINK $BUILD_OUT_DIR $EXTRA_FEATURES
            ''}
          fi
        fi
      '';

      cargoToml = fromTOML (builtins.readFile ./Cargo.toml);

      # Cargo.nix is generated — don't hand-edit (regen: CLAUDE.md). Build
      # with our pinned rustc, not nixpkgs'; the virtual workspace has no
      # rootCrate, so callers select a member via `workspaceMembers."<name>"`.
      # Our own crates (local `src`, not a crates.io `sha256`) compile with
      # clippy-driver under Cargo.toml's `[workspace.lints]`. Dependencies
      # compile with plain rustc.
      # On Linux every crate links statically against musl, as the release
      # binary does, so the checks test what ships. Darwin has no
      # pkgsStatic and builds natively.
      cargoNixFor =
        pkgs:
        let
          cargoPkgs = if pkgs.stdenv.hostPlatform.isLinux then pkgs.pkgsStatic else pkgs;
          rustToolchain = rustToolchainFor cargoPkgs.buildPackages;
          lints = cargoToml.workspace.lints;
        in
        cargoPkgs.callPackage ./Cargo.nix {
          buildRustCrateForPkgs =
            p:
            let
              base = p.buildRustCrate.override {
                rustc = rustToolchain;
                cargo = rustToolchain;
                clippy = rustToolchain;
                # buildRustCrate defaults to codegen-units=1 (serial codegen),
                # ~3x slower per crate than cargo's release default. Match cargo.
                defaultCodegenUnits = 16;
              };
            in
            crate:
            let
              drv = base crate;
            in
            if crate ? sha256 then
              drv
            else
              drv.override (_: {
                useClippy = true;
                inherit lints;
                # buildRustCrate builds the bins in a `make` subshell that
                # inherits only the variables it exports, and RUSTC_DRIVER is
                # not one of them, so the bins compile with plain rustc and
                # skip the lints. Marking the variable for export before
                # build-crate.nix assigns it makes the subshell inherit it.
                preBuild = "export RUSTC_DRIVER";
                postBuild = rustdocPostBuild (nixpkgs.lib.elem "lib" (crate.type or [ "lib" ]));
              });
        };
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            # rustc/cargo/clippy/rustfmt/rust-analyzer all come from the one
            # pinned toolchain.
            (rustToolchainFor pkgs)

            # Regenerates Cargo.nix
            crate2nix

            # Formatting — treefmt runs the per-language formatters
            # configured in treefmt.toml (rustfmt from the toolchain above)
            treefmt
            nixfmt
            prettier
            shfmt
            taplo
          ];
        };
      });

      packages = forAllSystems (pkgs: rec {
        # Builds only. The `test` check runs the tests.
        noise = (cargoNixFor pkgs).workspaceMembers."noise".build;
        default = noise;
      });

      checks = forAllSystems (pkgs: {
        build = self.packages.${pkgs.system}.noise;
        # Also clippy-checks the test targets (lib/bins are linted in every
        # build). Tests run here, not in `nix build`. Rustdoc runs in the same
        # build (rustdocPostBuild): the docs render and the doc tests run.
        test = (cargoNixFor pkgs).workspaceMembers."noise".build.override {
          runTests = true;
        };
        treefmt = pkgs.stdenvNoCC.mkDerivation {
          name = "treefmt-check";
          src = self;
          nativeBuildInputs = [ self.formatter.${pkgs.system} ];
          buildPhase = "HOME=$TMPDIR treefmt --ci --tree-root .";
          installPhase = "touch $out";
        };
      });

      # `nix fmt` = the same whole-tree treefmt the devShell runs,
      # self-contained (formatters on PATH without entering the shell).
      # rustfmt comes from the pinned toolchain, as in the devShell.
      formatter = forAllSystems (
        pkgs:
        pkgs.writeShellApplication {
          name = "treefmt";
          runtimeInputs = with pkgs; [
            treefmt
            (rustToolchainFor pkgs)
            nixfmt
            prettier
            shfmt
            taplo
          ];
          text = ''exec treefmt "$@"'';
        }
      );
    };
}
