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

    def main(self):
        distribution_xml = self.env["distribution_xml"]
        package_path = self.env["package_path"]
        output_pkg = self.env["output_pkg"]
        signing_identity = self.env.get("signing_identity", "")

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
            self.output(f"Signing package with identity: {signing_identity}")

            productsign_cmd = [
                "/usr/bin/productsign",
                "--sign",
                signing_identity,
                "--timestamp",
                unsigned_pkg,
                output_pkg,
            ]

            self.run_command(productsign_cmd, "productsign")

            # Remove unsigned package
            try:
                os.remove(unsigned_pkg)
            except OSError as e:
                self.output(f"Warning: Failed to remove unsigned package: {e}")

        # Verify package was created
        if not os.path.exists(output_pkg):
            raise ProcessorError(f"Package was not created at {output_pkg}")

        # Get package size
        pkg_size = os.path.getsize(output_pkg)
        pkg_size_mb = pkg_size / (1024 * 1024)

        # Set output variables
        self.env["pkg_path"] = output_pkg
        self.env["pkg_creator_summary_result"] = {
            "summary_text": "The following distribution packages were built:",
            "report_fields": ["package", "size_mb", "signed"],
            "data": {
                "package": os.path.basename(output_pkg),
                "size_mb": f"{pkg_size_mb:.2f}",
                "signed": "Yes" if signing_identity else "No",
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
