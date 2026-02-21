{
  description = "AutoPkg FleetImporter processor with SeaweedFS and local GitOps support";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    autopkg = {
      url = "github:autopkg/autopkg";
      flake = false;
    };
  };

  outputs =
    { self, nixpkgs, autopkg }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;

      nixpkgsFor = forAllSystems (
        system:
        import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        }
      );
    in
    {
      # Output the FleetImporter processor directory and full recipes
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgsFor.${system};
          lib = pkgs.lib;

          # Build AutoPkg with Python dependencies
          autopkgDrv =
            let
              python = pkgs.python311;
              pythonPackages = python.pkgs;
              pythonDeps = with pythonPackages; [
                appdirs
                attrs
                boto3
                certifi
                lxml
                pyyaml
                six
                xattr
              ] ++ lib.optionals pkgs.stdenv.isDarwin [
                pyobjc-core
                pyobjc-framework-Cocoa
                pyobjc-framework-Quartz
              ];
            in
            pythonPackages.buildPythonApplication {
              pname = "autopkg";
              version = "3.0.0";
              src = autopkg;
              format = "other";

              propagatedBuildInputs = pythonDeps;
              dontBuild = true;
              doCheck = false;

              postPatch = ''
                sed -i '/^import sys$/a import grp' Code/autopkgserver/autopkgserver
                sed -i 's/admin_gid = 80$/&\n    try:\n        nixbld_gid = grp.getgrnam("nixbld").gr_gid\n    except KeyError:\n        nixbld_gid = None/' Code/autopkgserver/autopkgserver
                sed -i 's/if info.st_gid not in (wheel_gid, admin_gid):/if info.st_gid not in (wheel_gid, admin_gid, nixbld_gid):/' Code/autopkgserver/autopkgserver
              '';

              installPhase = ''
                mkdir -p $out/libexec $out/bin
                if [ -d "Code" ]; then
                  cp -R Code $out/libexec/autopkg
                else
                  cp -R . $out/libexec/autopkg
                fi

                makeWrapper ${python}/bin/python${python.pythonVersion} $out/bin/autopkg \
                  --add-flags "$out/libexec/autopkg/autopkg" \
                  --prefix PYTHONPATH : "$out/libexec/autopkg" \
                  --prefix PYTHONPATH : "${pythonPackages.makePythonPath pythonDeps}"
              '';

              nativeBuildInputs = [ pkgs.makeWrapper ];
            };
        in
        {
          fleetimporter = pkgs.stdenv.mkDerivation {
            pname = "autopkg-fleetimporter";
            version = "1.0.0";

            src = ./FleetImporter;

            installPhase = ''
              mkdir -p $out
              cp -r $src/* $out/
            '';

            meta = with pkgs.lib; {
              description = "AutoPkg FleetImporter processor with SeaweedFS and local GitOps support";
              homepage = "https://github.com/ocasazza/fleet-recipes";
              license = licenses.asl20;
              platforms = platforms.unix;
            };
          };

          # Full recipes directory for RECIPE_SEARCH_DIRS
          recipes = pkgs.stdenv.mkDerivation {
            pname = "autopkg-fleet-recipes";
            version = "1.0.0";

            src = ./.;

            installPhase = ''
              mkdir -p $out
              # Copy all processors
              cp -r FleetImporter $out/
              cp -r ChmodTool $out/
              cp -r ScriptInjector $out/
              cp -r ProductBuildCreator $out/
              cp -r FleetAgentBuilder $out/
              # Copy recipe directories (exclude hidden files, tests, etc)
              for dir in */; do
                if [[ ! "$dir" =~ ^(\..*|tests|\.github|nix-darwin|FleetImporter|ChmodTool|ScriptInjector|ProductBuildCreator|FleetAgentBuilder)/ ]]; then
                  cp -r "$dir" $out/
                fi
              done
            '';

            meta = with pkgs.lib; {
              description = "AutoPkg recipes and processors for Fleet";
              homepage = "https://github.com/ocasazza/fleet-recipes";
              license = licenses.asl20;
              platforms = platforms.unix;
            };
          };

          # AutoPkg package (for use in nix-darwin modules)
          autopkg = autopkgDrv;

          default = self.packages.${system}.recipes;
        }
      );

      # nix-darwin module for autopkgserver
      darwinModules.autopkgserver = ./nix-darwin/autopkgserver.nix;

      # Development shell for working on FleetImporter and testing recipes
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgsFor.${system};
          recipes = self.packages.${system}.recipes;
          autopkgDrv = self.packages.${system}.autopkg;
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              autopkgDrv
              pkgs.git
              pkgs.curl
              pkgs.jq
              pkgs.yq-go
            ];

            shellHook = ''
              echo "AutoPkg fleet-recipes development environment"
              echo ""
              echo "AutoPkg version: $(autopkg version)"
              echo "FleetImporter location: ${recipes}/FleetImporter"
              echo ""
              echo "RECIPE_SEARCH_DIRS=${recipes}"
              export RECIPE_SEARCH_DIRS="${recipes}"
              export AUTOPKG_CACHE_DIR="$HOME/Library/AutoPkg/Cache"
              echo ""
              echo "To test recipes:"
              echo "  autopkg run <recipe>.fleet.recipe.yaml"
            '';
          };
        }
      );

      # AutoPkg app with fleet-recipes processors pre-configured
      apps = forAllSystems (
        system:
        let
          pkgs = nixpkgsFor.${system};
          recipes = self.packages.${system}.recipes;
          autopkgDrv = self.packages.${system}.autopkg;
        in
        {
          autopkg-run = {
            type = "app";
            meta.description = "Run AutoPkg with fleet-recipes processors";
            program = toString (
              pkgs.writeShellScript "autopkg-run" ''
                set -euo pipefail
                export PATH="${pkgs.lib.makeBinPath [ autopkgDrv pkgs.git pkgs.curl pkgs.python3 ]}:$PATH"

                # Set up AutoPkg environment
                export AUTOPKG_CACHE_DIR="''${AUTOPKG_CACHE_DIR:-$HOME/Library/AutoPkg/Cache}"

                # Add fleet-recipes to search path via --search-dir
                # This adds to AutoPkg's configured repos instead of replacing them
                if [[ "$1" == "run" ]]; then
                  exec autopkg "$@" --search-dir="${recipes}"
                else
                  exec autopkg "$@"
                fi
              ''
            );
          };
        }
      );
    };
}
