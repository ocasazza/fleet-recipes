#!/usr/local/autopkg/python
"""
ProductBuildCreator - AutoPkg Processor

Creates macOS distribution packages using productbuild and optionally signs them with productsign.
Used for creating bootstrap packages with custom distribution XML.

Copyright 2024 Schrödinger, Inc.
"""

import os
import subprocess
from autopkglib import Processor, ProcessorError

__all__ = ["ProductBuildCreator"]


class ProductBuildCreator(Processor):
    """Create macOS distribution packages with productbuild"""

    description = __doc__
    input_variables = {
        "distribution_xml": {
            "required": True,
            "description": "Path to distribution.xml file",
        },
        "package_path": {
            "required": True,
            "description": "Directory containing component packages referenced in distribution.xml",
        },
        "output_pkg": {
            "required": True,
            "description": "Path for output distribution package",
        },
        "signing_identity": {
            "required": False,
            "description": (
                "Code signing identity for productsign (e.g., 'Developer ID Installer: Company (TEAMID)'). "
                "If not provided, package will be unsigned."
            ),
            "default": "",
        },
        "disable_timestamp": {
            "required": False,
            "description": (
                "Disable timestamp when signing (use --timestamp=none). "
                "Set to true if Apple's timestamp service is blocked by firewall."
            ),
            "default": False,
        },
        "notarize": {
            "required": False,
            "description": "Enable notarization after signing",
            "default": False,
        },
        "notarization_apple_id": {
            "required": False,
            "description": "Apple ID for notarization",
            "default": "",
        },
        "notarization_team_id": {
            "required": False,
            "description": "Apple Developer Team ID",
            "default": "",
        },
        "notarization_password": {
            "required": False,
            "description": "App-specific password for notarization (use @keychain: or @env: for security)",
            "default": "",
        },
        "notarization_wait": {
            "required": False,
            "description": "Wait for notarization to complete (default: True)",
            "default": True,
        },
    }
    output_variables = {
        "pkg_path": {
            "description": "Path to created package (for use by FleetImporter)",
        },
        "pkg_creator_summary_result": {
            "description": "Summary of package creation",
        },
    }

    def run_command(self, cmd, description):
        """Run a shell command and return output"""
        self.output(f"Running: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
            )
            if result.stdout:
                self.output(result.stdout)
            return result.stdout
        except subprocess.CalledProcessError as e:
            error_msg = f"{description} failed with exit code {e.returncode}"
            if e.stderr:
                error_msg += f"\nError output:\n{e.stderr}"
            raise ProcessorError(error_msg)

    def notarize_package(self, pkg_path, apple_id, team_id, password, wait=True):
        """Submit package to Apple notary service"""
        self.output(f"Submitting package for notarization...")

        # Find notarytool - try xcrun first, then direct path
        notarytool_cmd = None
        try:
            subprocess.run(["/usr/bin/xcrun", "--find", "notarytool"],
                         check=True, capture_output=True)
            notarytool_cmd = ["/usr/bin/xcrun", "notarytool"]
        except subprocess.CalledProcessError:
            # Fallback to direct path (Command Line Tools location)
            direct_path = "/Library/Developer/CommandLineTools/usr/bin/notarytool"
            if os.path.exists(direct_path):
                notarytool_cmd = [direct_path]
            else:
                raise ProcessorError(
                    "notarytool not found. Install Xcode Command Line Tools: xcode-select --install"
                )

        # Submit to notary service
        submit_cmd = notarytool_cmd + [
            "submit",
            pkg_path,
            "--apple-id", apple_id,
            "--team-id", team_id,
            "--password", password,
        ]

        if wait:
            submit_cmd.append("--wait")

        submit_output = self.run_command(submit_cmd, "notarytool submit")

        # Extract submission ID from output
        submission_id = None
        for line in submit_output.splitlines():
            if "id:" in line.lower():
                submission_id = line.split(":")[-1].strip()
                break

        if not submission_id and wait:
            raise ProcessorError("Failed to get notarization submission ID")

        if submission_id:
            self.output(f"Notarization submission ID: {submission_id}")

        # Staple notarization ticket to package
        if wait:
            self.output("Stapling notarization ticket to package...")
            # stapler is always available via xcrun, no fallback needed
            stapler_cmd = [
                "/usr/bin/xcrun",
                "stapler",
                "staple",
                pkg_path,
            ]
            self.run_command(stapler_cmd, "stapler")
            self.output("✓ Notarization ticket stapled")

            # Verify notarization
            self.output("Verifying notarization...")
            stapler_validate_cmd = [
                "/usr/bin/xcrun",
                "stapler",
                "validate",
                pkg_path,
            ]
            self.run_command(stapler_validate_cmd, "stapler validate")
            self.output("✓ Notarization verified")

        return submission_id

    def main(self):
        distribution_xml = self.env["distribution_xml"]
        package_path = self.env["package_path"]
        output_pkg = self.env["output_pkg"]
        signing_identity = self.env.get("signing_identity", "")
        disable_timestamp = self.env.get("disable_timestamp", False)

        # Validate inputs
        if not os.path.exists(distribution_xml):
            raise ProcessorError(f"Distribution XML not found: {distribution_xml}")

        if not os.path.exists(package_path):
            raise ProcessorError(f"Package path not found: {package_path}")

        # Create output directory if needed
        output_dir = os.path.dirname(output_pkg)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Determine if we need to sign
        if signing_identity:
            # Build unsigned package first
            unsigned_pkg = output_pkg.replace(".pkg", "-unsigned.pkg")
            build_output = unsigned_pkg
        else:
            build_output = output_pkg

        # Build distribution package with productbuild
        productbuild_cmd = [
            "/usr/bin/productbuild",
            "--distribution",
            distribution_xml,
            "--package-path",
            package_path,
            build_output,
        ]

        self.output(f"Building distribution package...")
        self.run_command(productbuild_cmd, "productbuild")

        # Sign if identity provided
        if signing_identity:
            timestamp_status = "disabled" if disable_timestamp else "enabled"
            self.output(f"Signing package with identity: {signing_identity} (timestamp: {timestamp_status})")

            productsign_cmd = [
                "/usr/bin/productsign",
                "--sign",
                signing_identity,
            ]

            # Add timestamp option
            if disable_timestamp:
                productsign_cmd.append("--timestamp=none")
            else:
                productsign_cmd.append("--timestamp")

            productsign_cmd.extend([unsigned_pkg, output_pkg])

            self.run_command(productsign_cmd, "productsign")

            # Remove unsigned package
            try:
                os.remove(unsigned_pkg)
            except OSError as e:
                self.output(f"Warning: Failed to remove unsigned package: {e}")

        # Notarize if requested
        if self.env.get("notarize", False):
            apple_id = self.env.get("notarization_apple_id", "")
            team_id = self.env.get("notarization_team_id", "")
            password = self.env.get("notarization_password", "")
            wait = self.env.get("notarization_wait", True)

            if not signing_identity:
                raise ProcessorError("Notarization requires a signed package (signing_identity must be set)")

            if not apple_id or not team_id or not password:
                raise ProcessorError(
                    "Notarization requires: notarization_apple_id, notarization_team_id, and notarization_password"
                )

            self.notarize_package(output_pkg, apple_id, team_id, password, wait)

        # Verify package was created
        if not os.path.exists(output_pkg):
            raise ProcessorError(f"Package was not created at {output_pkg}")

        # Get package size
        pkg_size = os.path.getsize(output_pkg)
        pkg_size_mb = pkg_size / (1024 * 1024)

        # Set output variables
        self.env["pkg_path"] = output_pkg

        # Get identifier and version from environment (set by recipe Input)
        identifier = self.env.get("IDENTIFIER", "unknown")
        version = self.env.get("VERSION", "1.0.0")

        self.env["pkg_creator_summary_result"] = {
            "summary_text": "The following distribution packages were built:",
            "report_fields": ["identifier", "version", "pkg_path"],
            "data": {
                "identifier": identifier,
                "version": version,
                "pkg_path": output_pkg,
            },
        }

        # Verify signature if signed
        if signing_identity:
            self.output("Verifying package signature...")
            pkgutil_cmd = ["/usr/sbin/pkgutil", "--check-signature", output_pkg]
            sig_output = self.run_command(pkgutil_cmd, "pkgutil signature check")
            if "signed" in sig_output.lower():
                self.output("✓ Package signature verified")

        self.output(f"✓ Distribution package created: {output_pkg} ({pkg_size_mb:.2f} MB)")


if __name__ == "__main__":
    PROCESSOR = ProductBuildCreator()
    PROCESSOR.execute_shell()
