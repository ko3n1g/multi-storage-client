#
# Nix flake.
#
# https://nix.dev/manual/nix/stable/command-ref/new-cli/nix3-flake#flake-format
# https://wiki.nixos.org/wiki/Flakes#Flake_schema
#
{
  description = "Nix flake.";

  inputs = {
    # https://search.nixos.org/packages?channel=unstable
    nixpkgs = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      ref = "refs/heads/nixos-unstable";
    };

    # Python 3.9 (EOL October 2025) was dropped early for NixOS 25.05.
    #
    # https://github.com/NixOS/nixpkgs/pull/397258
    nixpkgs-python39 = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      # Commit right before drop.
      rev = "1076d576f5e8916f240d094a3c381d3e121800ba";
    };
  };

  outputs =
    inputs:
    let
      # Output systems.
      #
      # https://github.com/NixOS/nixpkgs/blob/master/lib/systems/flake-systems.nix
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];

      # Return an attribute set of system to the result of applying `f`.
      genSystemAttrs =
        f:
        # https://nixos.org/manual/nixpkgs/stable#function-library-lib.attrsets.genAttrs
        inputs.nixpkgs.lib.genAttrs systems (
          system:
          f {
            # System packages.
            packages = {
              self = inputs.self.packages.${system};

              nixpkgs = inputs.nixpkgs.legacyPackages.${system};

              nixpkgs-python39 = inputs.nixpkgs-python39.legacyPackages.${system};
            };
          }
        );
    in
    {
      # Development shells.
      #
      # For `nix develop` and direnv's `use flake`.
      devShells = genSystemAttrs (system-inputs: {
        # https://nixos.org/manual/nixpkgs/stable#sec-pkgs-mkShell
        default = system-inputs.packages.nixpkgs.mkShell {
          packages = with system-inputs.packages.nixpkgs; [
            # Nix.
            #
            # Nix is dynamically linked on some systems. If we set LD_LIBRARY_PATH,
            # running Nix commands with the system-installed Nix may fail due to mismatched library versions.
            nix
            nixfmt-rfc-style
            # Utilities.
            coreutils
            curl
            lsof
            netcat-gnu
            # Git.
            git
            git-lfs
            # Just.
            just
            # Python.
            system-inputs.packages.nixpkgs-python39.python39
            python310
            python311
            python312
            python313
            # uv.
            uv
            # Ruff.
            ruff
            # Pyright.
            pyright
            # Storage systems.
            azurite
            fake-gcs-server
            minio
            # Telemetry systems.
            grafana
            mimir
            tempo
            # JFrog CLI.
            jfrog-cli
            # OpenSSH.
            openssh
            # GitHub CLI.
            gh
          ];

          shellHook = ''
            # Dynamic linker.
            #
            # https://discourse.nixos.org/t/how-to-solve-libstdc-not-found-in-shell-nix/25458
            # https://discourse.nixos.org/t/poetry-pandas-issue-libz-so-1-not-found/17167
            export LD_LIBRARY_PATH=${
              with system-inputs.packages.nixpkgs;
              lib.makeLibraryPath [
                stdenv.cc.cc.lib
                zlib
              ]
            }

            # Disable Objective-C fork safety on macOS for pytest-xdist.
            #
            # https://github.com/python/cpython/issues/77906
            export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

            echo "⚗️"
          '';
        };
      });
    };
}
