# NixOS/nix-darwin module for AutoPkg server
# Provides autopkgserver as a LaunchDaemon for building packages
#
# Usage in configuration.nix:
#   imports = [ (fleet-recipes + "/nix-darwin/autopkgserver.nix") ];
#   services.autopkgserver = {
#     enable = true;
#     autopkgDrv = fleet-recipes.packages.${system}.autopkg;
#     recipesPath = fleet-recipes.packages.${system}.recipes;
#   };
{ config, lib, pkgs, ... }:

let
  cfg = config.services.autopkgserver;
in
{
  options.services.autopkgserver = {
    enable = lib.mkEnableOption "AutoPkg server for package building";
    
    autopkgDrv = lib.mkOption {
      type = lib.types.package;
      description = "AutoPkg package derivation";
    };
    
    recipesPath = lib.mkOption {
      type = lib.types.path;
      description = "Path to AutoPkg recipes directory (fleet-recipes)";
    };
    
    cacheDir = lib.mkOption {
      type = lib.types.path;
      default = "/var/lib/autopkg/cache";
      description = "AutoPkg cache directory";
    };
    
    recipeOverrideDirs = lib.mkOption {
      type = lib.types.str;
      default = "lib/software";
      description = "AutoPkg recipe override directories";
    };
  };

  config = lib.mkIf cfg.enable {
    launchd.daemons.autopkgserver = {
      script = ''
        # Set up AutoPkg environment
        export RECIPE_SEARCH_DIRS="${cfg.recipesPath}"
        export AUTOPKG_CACHE_DIR="${cfg.cacheDir}"
        export AUTOPKG_RECIPE_OVERRIDE_DIRS="${cfg.recipeOverrideDirs}"

        # Ensure cache directory exists
        mkdir -p "$AUTOPKG_CACHE_DIR"

        # Run autopkgserver
        exec ${cfg.autopkgDrv}/libexec/autopkg/autopkgserver/autopkgserver
      '';
      
      serviceConfig = {
        Label = "com.github.autopkg.autopkgserver";
        KeepAlive = false;  # Socket-activated
        StandardOutPath = "/var/log/autopkgserver.log";
        StandardErrorPath = "/var/log/autopkgserver.error.log";
        
        # Socket configuration
        Sockets = {
          AutoPkgServer = {
            SockPathName = "/var/run/autopkgserver";
            SockPathMode = 438;  # 0666 in octal
          };
        };
      };
    };
  };
}
