#!/usr/local/autopkg/python
"""
FleetAgentBuilder - AutoPkg Processor

Builds Fleet osquery agent packages using fleetctl package command.
Requires Fleet server URL and enrollment secret.

Copyright 2024 Schrödinger, Inc.
"""

import os
import shutil
import subprocess
from autopkglib import Processor, ProcessorError

__all__ = ["FleetAgentBuilder"]


class FleetAgentBuilder(Processor):
    """Build Fleet osquery agent package with enrollment secret"""

    description = __doc__
    input_variables = {
        "fleet_url": {
            "required": True,
            "description": "Fleet server URL (e.g., https://fleet.example.com)",
        },
        "enroll_secret": {
            "required": True,
            "description": "Fleet enrollment secret for the team",
        },
        "team_name": {
            "required": True,
            "description": "Team name for logging/identification",
        },
        "output_path": {
            "required": True,
            "description": "Path where the Fleet agent package should be written",
        },
        "fleetctl_path": {
            "required": False,
            "description": "Path to fleetctl binary (default: fleetctl in PATH)",
            "default": "fleetctl",
        },
    }
    output_variables = {
        "fleet_agent_pkg": {
            "description": "Path to the built Fleet agent package",
        },
    }

    def main(self):
        fleet_url = self.env["fleet_url"]
        enroll_secret = self.env["enroll_secret"]
        team_name = self.env["team_name"]
        output_path = self.env["output_path"]
        fleetctl_path = self.env.get("fleetctl_path", "fleetctl")

        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.output(f"Building Fleet agent package for team: {team_name}")
        self.output(f"Fleet URL: {fleet_url}")

        # Check if fleetctl is in PATH, otherwise use npx
        if shutil.which(fleetctl_path) is None:
            # fleetctl not found, use npx to run it
            cmd = [
                "npx",
                "-y",
                "fleetctl",
                "package",
                "--type=pkg",
                f"--fleet-url={fleet_url}",
                f"--enroll-secret={enroll_secret}",
                f"--fleet-desktop",
                "--disable-open-folder",
            ]
        else:
            # Build the fleetctl package command
            cmd = [
                fleetctl_path,
                "package",
                "--type=pkg",
                f"--fleet-url={fleet_url}",
                f"--enroll-secret={enroll_secret}",
                f"--fleet-desktop",
                "--disable-open-folder",
            ]

        self.output(f"Running: {cmd[0]} {cmd[1] if cmd[0] == 'npx' else 'package'} --fleet-url=... --enroll-secret=***")

        try:
            # Run fleetctl package command
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                cwd=output_dir or ".",
            )

            # fleetctl package creates fleet-osquery.pkg in the current directory
            default_pkg = os.path.join(output_dir or ".", "fleet-osquery.pkg")

            if not os.path.exists(default_pkg):
                raise ProcessorError(
                    f"fleetctl package succeeded but output file not found: {default_pkg}"
                )

            # Move to desired output path if different
            if os.path.abspath(default_pkg) != os.path.abspath(output_path):
                os.rename(default_pkg, output_path)

            # Verify final package exists
            if not os.path.exists(output_path):
                raise ProcessorError(f"Fleet agent package not found at: {output_path}")

            # Get package size
            pkg_size = os.path.getsize(output_path)
            pkg_size_mb = pkg_size / (1024 * 1024)

            self.output(
                f"✓ Fleet agent package built: {output_path} ({pkg_size_mb:.2f} MB)"
            )

            # Set output variable
            self.env["fleet_agent_pkg"] = output_path

        except subprocess.CalledProcessError as e:
            error_msg = f"fleetctl package failed with exit code {e.returncode}"
            if e.stderr:
                error_msg += f"\nError output:\n{e.stderr}"
            if e.stdout:
                error_msg += f"\nStandard output:\n{e.stdout}"
            raise ProcessorError(error_msg)
        except FileNotFoundError as e:
            if cmd[0] == "npx":
                raise ProcessorError(
                    "npx command not found. Ensure Node.js/npm is installed."
                )
            else:
                raise ProcessorError(
                    f"fleetctl command not found at: {fleetctl_path}. "
                    "Ensure fleetctl is installed and in PATH or install Node.js for npx."
                )


if __name__ == "__main__":
    PROCESSOR = FleetAgentBuilder()
    PROCESSOR.execute_shell()
