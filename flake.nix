{
  description = "AutoPkg FleetImporter processor with SeaweedFS and local GitOps support";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { self, nixpkgs }:
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
              # Copy all processors and recipes
              cp -r FleetImporter $out/
              cp -r ChmodTool $out/
              # Copy recipe directories (exclude hidden files, tests, etc)
              for dir in */; do
                if [[ ! "$dir" =~ ^(\..*|tests|\.github)/ ]]; then
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

          default = self.packages.${system}.recipes;
        }
      );

      # Development shell for working on FleetImporter
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgsFor.${system};
        in
        {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              python311
              python311Packages.boto3
              python311Packages.pyyaml
              python311Packages.requests
            ];

            shellHook = ''
              echo "FleetImporter development environment"
              echo ""
              echo "Python version: $(python3 --version)"
              echo "FleetImporter location: ${self}/FleetImporter"
              echo ""
              echo "To test FleetImporter locally:"
              echo "  cd path/to/recipe/dir"
              echo "  autopkg run recipe.yaml"
            '';
          };
        }
      );
    };
}
