# -*- coding: utf-8 -*-
#
# FleetImporter AutoPkg Processor
#
# Uploads a package to Fleet for software deployment.
#
# Requires: Python 3.9+
#

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import certifi
import yaml
from autopkglib import Processor, ProcessorError

__all__ = ["FleetImporter"]

# boto3 is only required for GitOps mode (S3 uploads)
# It will be imported lazily when needed to avoid requiring it for direct mode
boto3 = None
ClientError = None
NoCredentialsError = None

# Constants for improved readability
DEFAULT_PLATFORM = "darwin"

# Fleet version constants
FLEET_MINIMUM_VERSION = "4.74.0"

# HTTP timeout constants (in seconds)
FLEET_VERSION_TIMEOUT = 30
FLEET_UPLOAD_TIMEOUT = 900  # 15 minutes for large packages

# PyYAML configuration for prettier compatibility
# Use default SafeDumper with explicit_start to ensure consistent formatting


class FleetImporter(Processor):
    """
    Upload AutoPkg-built installer packages to Fleet for software deployment.

    This processor uploads software packages (.pkg files) to Fleet and configures
    deployment settings including self-service availability, automatic installation,
    host targeting via labels, and custom install/uninstall scripts.

    Dependencies:
        - boto3>=1.18.0: Required for GitOps mode S3 operations. Will be automatically
          installed if not present when GitOps mode is used.
        - Native Python libraries only for direct mode (no external dependencies)
    """

    description = __doc__
    input_variables = {
        # --- Required basics ---
        "pkg_path": {
            "required": True,
            "description": "Path to the built .pkg from AutoPkg.",
        },
        "software_title": {
            "required": True,
            "description": "Human-readable software title, e.g., 'Firefox.app'.",
        },
        "version": {
            "required": True,
            "description": "Software version string.",
        },
        "platform": {
            "required": False,
            "default": DEFAULT_PLATFORM,
            "description": "Platform (darwin|windows|linux|ios|ipados). Default: darwin",
        },
        # --- Fleet API (required for direct mode, optional for GitOps mode) ---
        "fleet_api_base": {
            "required": False,
            "description": "Fleet base URL, e.g., https://fleet.example.com (required for direct mode)",
        },
        "fleet_api_token": {
            "required": False,
            "description": "Fleet API token (Bearer) (required for direct mode).",
        },
        "team_id": {
            "required": False,
            "description": "Fleet team ID to attach the uploaded package to (required for direct mode, unless team_ids or discover_teams is used).",
        },
        "team_ids": {
            "required": False,
            "description": "List of Fleet team IDs for multi-team upload (alternative to team_id). Package will be uploaded to all specified teams in parallel.",
        },
        "discover_teams": {
            "required": False,
            "default": False,
            "description": "Auto-discover teams from YAML files. If enabled, will scan teams directory to find all teams that reference this software package.",
        },
        "teams_dir": {
            "required": False,
            "description": "Path to teams directory for team discovery (defaults to ./teams or git-root/teams).",
        },
        # --- GitOps mode ---
        "gitops_mode": {
            "required": False,
            "default": False,
            "description": "Enable GitOps mode: upload to S3 and create PR instead of direct Fleet upload.",
        },
        "aws_s3_bucket": {
            "required": False,
            "description": "S3 bucket name for package storage (required for GitOps mode).",
        },
        "aws_cloudfront_domain": {
            "required": False,
            "description": "CloudFront distribution domain (required for GitOps mode), e.g., cdn.example.com",
        },
        "gitops_repo_url": {
            "required": False,
            "description": "GitOps repository URL (required for GitOps mode), e.g., https://github.com/org/fleet-gitops.git. Use FLEET_GITOPS_REPO_URL environment variable.",
        },
        "gitops_software_dir": {
            "required": False,
            "default": "lib/macos/software",
            "description": "Directory for software package YAMLs within GitOps repo (default: lib/macos/software). Use FLEET_GITOPS_SOFTWARE_DIR environment variable.",
        },
        "gitops_team_yaml_path": {
            "required": False,
            "description": "Path to team YAML file within GitOps repo (required for GitOps mode), e.g., teams/team-name.yml. Use FLEET_GITOPS_TEAM_YAML_PATH environment variable.",
        },
        "github_token": {
            "required": False,
            "description": "GitHub personal access token for cloning and creating PRs (required for GitOps mode). Use FLEET_GITOPS_GITHUB_TOKEN environment variable.",
        },
        "s3_retention_versions": {
            "required": False,
            "default": 0,
            "description": "Number of old versions to retain per software title in S3. Set to 0 to disable pruning (default: 0).",
        },
        # --- AWS Configuration (required for GitOps mode) ---
        "aws_access_key_id": {
            "required": False,
            "description": "AWS access key ID for S3 operations (required for GitOps mode).",
        },
        "aws_secret_access_key": {
            "required": False,
            "description": "AWS secret access key for S3 operations (required for GitOps mode).",
        },
        "aws_default_region": {
            "required": False,
            "default": "us-east-1",
            "description": "AWS region for S3 operations (default: us-east-1).",
        },
        "s3_endpoint_url": {
            "required": False,
            "description": "Custom S3 endpoint URL for S3-compatible storage (e.g., SeaweedFS). Leave empty for AWS S3.",
        },
        "dry_run": {
            "required": False,
            "default": False,
            "description": "If True, build package and calculate hash but skip S3 upload and YAML updates (for testing).",
        },
        # --- Fleet deployment options ---
        "self_service": {
            "required": False,
            "default": True,
            "description": "Whether the package is available for self-service installation.",
        },
        "automatic_install": {
            "required": False,
            "default": False,
            "description": "macOS-only: automatically install on hosts that don't have this software.",
        },
        "labels_include_any": {
            "required": False,
            "default": [],
            "description": "List of label names - software is available on hosts with ANY of these labels.",
        },
        "labels_exclude_any": {
            "required": False,
            "default": [],
            "description": "List of label names - software is excluded from hosts with ANY of these labels.",
        },
        "install_script": {
            "required": False,
            "default": "",
            "description": "Custom install script - either inline script body (string) or path to .sh file (relative to recipe dir or absolute).",
        },
        "uninstall_script": {
            "required": False,
            "default": "",
            "description": "Custom uninstall script - either inline script body (string) or path to .sh file (relative to recipe dir or absolute).",
        },
        "icon": {
            "required": False,
            "default": "",
            "description": "Path to PNG icon file (square, 120x120 to 1024x1024 px) to upload to Fleet. If not provided, will attempt to extract icon from app bundle automatically.",
        },
        "pre_install_query": {
            "required": False,
            "default": "",
            "description": "Pre-install osquery SQL condition.",
        },
        "post_install_script": {
            "required": False,
            "default": "",
            "description": "Post-install script - either inline script body (string) or path to .sh file (relative to recipe dir or absolute).",
        },
        "categories": {
            "required": False,
            "default": [],
            "description": "List of category names to group self-service software in Fleet Desktop (e.g., ['Productivity', 'Browser']).",
        },
        "display_name": {
            "required": False,
            "default": "",
            "description": "Custom display name for the software in Fleet (e.g., 'CrowdStrike Falcon' instead of 'Falcon.app'). If not provided, Fleet will use the software_title.",
        },
        # --- Auto-update policy options ---
        "automatic_update": {
            "required": False,
            "default": False,
            "description": "Enable auto-update policy creation. Creates a Fleet policy that automatically installs software on devices with outdated versions.",
        },
        "auto_update_policy_name": {
            "required": False,
            "default": "autopkg-auto-update-%NAME%",
            "description": "Template for auto-update policy name. Use %NAME% as placeholder for software title (default: autopkg-auto-update-%NAME%).",
        },
        "auto_update_policy_query": {
            "required": False,
            "default": "",
            "description": "Query template for auto-update policy. Use %VERSION% as placeholder for version number. If not specified, a default query using bundle_identifier will be generated (macOS apps only).",
        },
        "package_type": {
            "required": False,
            "default": "software",
            "description": "Package type: 'software' for regular installers (default), 'bootstrap' for MDM bootstrap packages.",
        },
    }

    output_variables = {
        "fleet_title_id": {"description": "Created/updated Fleet software title ID."},
        "fleet_installer_id": {"description": "Installer ID in Fleet."},
        "hash_sha256": {
            "description": "SHA-256 hash of the uploaded package, as returned by Fleet."
        },
        "cloudfront_url": {
            "description": "CloudFront URL for the uploaded package (GitOps mode only)."
        },
        "pull_request_url": {
            "description": "URL of the created pull request (GitOps mode only)."
        },
        "git_branch": {
            "description": "Name of the Git branch created for the PR (GitOps mode only)."
        },
    }

    def _get_ssl_context(self):
        """Create an SSL context using certifi's CA bundle."""
        return ssl.create_default_context(cafile=certifi.where())

    def _fleet_api_request_with_retry(
        self,
        request: urllib.request.Request,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 2.0,
        initial_delay: float = 1.0,
    ):
        """
        Execute a Fleet API request with exponential backoff for rate limiting.

        Handles 429 (Too Many Requests) responses with automatic retry using
        exponential backoff. Also retries on network errors and 5xx server errors.

        Args:
            request: urllib.request.Request object
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            backoff_factor: Multiplier for exponential backoff
            initial_delay: Initial delay in seconds before first retry

        Returns:
            urllib response object

        Raises:
            ProcessorError: If all retries are exhausted
        """
        last_error = None
        delay = initial_delay

        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                response = urllib.request.urlopen(
                    request, timeout=timeout, context=self._get_ssl_context()
                )
                return response

            except urllib.error.HTTPError as e:
                last_error = e

                # Handle rate limiting (429) with retry
                if e.code == 429:
                    if attempt < max_retries:
                        # Check for Retry-After header
                        retry_after = e.headers.get('Retry-After')
                        if retry_after:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                pass  # Use exponential backoff instead

                        self.output(
                            f"Rate limit hit (429). Retrying in {delay:.1f}s "
                            f"(attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(delay)
                        delay *= backoff_factor
                        continue
                    else:
                        raise ProcessorError(
                            f"Rate limit exceeded after {max_retries} retries: {e.code} {e.read().decode()}"
                        )

                # Handle server errors (5xx) with retry
                elif 500 <= e.code < 600:
                    if attempt < max_retries:
                        self.output(
                            f"Server error ({e.code}). Retrying in {delay:.1f}s "
                            f"(attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(delay)
                        delay *= backoff_factor
                        continue
                    else:
                        raise ProcessorError(
                            f"Server error after {max_retries} retries: {e.code} {e.read().decode()}"
                        )

                # Don't retry on other HTTP errors (4xx except 429)
                raise

            except (urllib.error.URLError, OSError) as e:
                last_error = e
                if attempt < max_retries:
                    self.output(
                        f"Network error: {e}. Retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(delay)
                    delay *= backoff_factor
                    continue
                else:
                    raise ProcessorError(
                        f"Network error after {max_retries} retries: {e}"
                    )

        # Should never reach here, but just in case
        raise ProcessorError(f"Request failed after {max_retries} retries: {last_error}")

    def _discover_teams_for_package(
        self, software_name: str, teams_dir: str = None, fleet_api_base: str = None, fleet_token: str = None
    ) -> list[dict]:
        """
        Discover which teams reference a software package in their YAML files.

        Scans team YAML files to find all teams that include the specified
        software package in their configuration. Team IDs are looked up from
        Fleet API using team names.

        Args:
            software_name: Name of the software package (e.g., "sentinelone")
            teams_dir: Path to teams directory (defaults to ./teams in repo root)
            fleet_api_base: Fleet API base URL (for looking up team IDs)
            fleet_token: Fleet API token (for looking up team IDs)

        Returns:
            List of dicts with team info: [{"file": "it-ops-general.yml", "team_id": 26, "name": "IT Ops General"}, ...]
        """
        if not teams_dir:
            # Try to find teams directory relative to current working directory
            teams_dir = Path.cwd() / "teams"
            if not teams_dir.exists():
                # Try git root
                try:
                    git_root = subprocess.run(
                        ["git", "rev-parse", "--show-toplevel"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        text=True,
                        check=True
                    ).stdout.strip()
                    teams_dir = Path(git_root) / "teams"
                except (subprocess.CalledProcessError, FileNotFoundError):
                    self.output(
                        f"Warning: Could not find teams directory. "
                        f"Tried: {Path.cwd() / 'teams'}"
                    )
                    return []

        teams_dir = Path(teams_dir)
        if not teams_dir.exists():
            self.output(f"Warning: Teams directory does not exist: {teams_dir}")
            return []

        # Fetch team ID mapping from Fleet API if credentials provided
        team_name_to_id = {}
        if fleet_api_base and fleet_token:
            try:
                url = f"{fleet_api_base}/api/v1/fleet/teams"
                headers = {
                    "Authorization": f"Bearer {fleet_token}",
                    "Accept": "application/json",
                }
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30, context=self._get_ssl_context()) as resp:
                    if resp.getcode() == 200:
                        data = json.loads(resp.read().decode())
                        teams = data.get("teams", [])
                        team_name_to_id = {t["name"]: t["id"] for t in teams}
                        self.output(f"Fetched {len(team_name_to_id)} teams from Fleet API for ID lookup")
            except Exception as e:
                self.output(f"Warning: Could not fetch teams from Fleet API: {e}. Team IDs will be null.")

        teams_with_package = []

        # Scan all YAML files in teams directory
        yaml_files = list(teams_dir.glob("*.yml")) + list(teams_dir.glob("*.yaml"))
        self.output(f"Scanning {len(yaml_files)} team YAML files in {teams_dir}...")
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    team_data = yaml.safe_load(f)

                if not team_data:
                    continue

                # Check if this team has a software section
                # Software can be at root level or under software.packages
                software_section = team_data.get('software')
                if not software_section:
                    # No software section at all
                    continue

                if isinstance(software_section, list):
                    # Backward compatibility: software as list
                    software_list = software_section
                    self.output(f"  [{yaml_file.name}] Found software list (legacy format) with {len(software_list)} items")
                elif isinstance(software_section, dict):
                    # New format: software.packages subsection
                    software_list = software_section.get('packages', [])
                    if software_list:
                        self.output(f"  [{yaml_file.name}] Found software.packages with {len(software_list)} items")
                else:
                    continue

                if not software_list:
                    continue

                # Check if our software package is in the list
                for idx, software_item in enumerate(software_list):
                    # Software items can be strings, dicts with 'name', or dicts with 'path'
                    if isinstance(software_item, str):
                        sw_name = software_item
                    elif isinstance(software_item, dict):
                        # Try 'name' field first
                        sw_name = software_item.get('name', '')
                        # If no 'name', extract from 'path' field
                        if not sw_name and 'path' in software_item:
                            # Extract filename from path: ../lib/software/homebrew/macos/homebrew.yml -> homebrew
                            path = software_item['path']
                            sw_name = Path(path).stem  # Gets filename without extension
                            if idx == 0:  # Only log first item to avoid spam
                                self.output(f"  [{yaml_file.name}] Extracted '{sw_name}' from path: {path}")
                    else:
                        continue

                    if sw_name == software_name:
                        # Found a match! Extract team info
                        team_name = team_data.get('name', yaml_file.stem)
                        team_id = team_name_to_id.get(team_name)  # Lookup from Fleet API

                        team_info = {
                            "file": yaml_file.name,
                            "team_id": team_id,
                            "name": team_name,
                        }
                        teams_with_package.append(team_info)
                        self.output(
                            f"Found {software_name} in team: {team_info['name']} "
                            f"(ID: {team_info['team_id']}, file: {team_info['file']})"
                        )
                        break  # Found it in this team, move to next file

            except Exception as e:
                self.output(
                    f"Warning: Failed to parse team file {yaml_file.name}: {e}"
                )
                continue

        if not teams_with_package:
            self.output(
                f"Warning: No teams found referencing package '{software_name}'. "
                f"Searched in: {teams_dir}"
            )
        else:
            self.output(
                f"Discovered {len(teams_with_package)} team(s) referencing {software_name}"
            )

        return teams_with_package

    def _upload_package_to_teams(
        self,
        base_url: str,
        token: str,
        pkg_path: Path,
        software_title: str,
        version: str,
        team_ids: list[int],
        self_service: bool,
        automatic_install: bool,
        labels_include_any: list[str],
        labels_exclude_any: list[str],
        install_script: str,
        uninstall_script: str,
        pre_install_query: str,
        post_install_script: str,
        categories: list[str],
        display_name: str = "",
        max_workers: int = 4,
    ) -> dict[int, dict]:
        """
        Upload a package to multiple teams in parallel with rate limit handling.

        Args:
            base_url: Fleet base URL
            token: Fleet API token
            pkg_path: Path to the package file
            software_title: Software title name
            version: Software version
            team_ids: List of team IDs to upload to
            self_service: Enable self-service installation
            automatic_install: Enable automatic installation
            labels_include_any: Labels to include
            labels_exclude_any: Labels to exclude
            install_script: Installation script
            uninstall_script: Uninstallation script
            pre_install_query: Pre-installation query
            post_install_script: Post-installation script
            categories: Software categories
            display_name: Display name for the software
            max_workers: Maximum number of concurrent uploads (default: 4)

        Returns:
            Dict mapping team_id to upload response: {team_id: response_dict, ...}
        """
        if not team_ids:
            self.output("Warning: No team IDs provided for upload")
            return {}

        self.output(
            f"Starting parallel upload to {len(team_ids)} team(s): {team_ids}"
        )

        results = {}
        errors = {}

        def upload_to_team(team_id: int) -> tuple[int, dict]:
            """Upload package to a single team."""
            try:
                self.output(f"[Team {team_id}] Starting upload...")
                response = self._fleet_upload_package(
                    base_url=base_url,
                    token=token,
                    pkg_path=pkg_path,
                    software_title=software_title,
                    version=version,
                    team_id=team_id,
                    self_service=self_service,
                    automatic_install=automatic_install,
                    labels_include_any=labels_include_any,
                    labels_exclude_any=labels_exclude_any,
                    install_script=install_script,
                    uninstall_script=uninstall_script,
                    pre_install_query=pre_install_query,
                    post_install_script=post_install_script,
                    categories=categories,
                    display_name=display_name,
                )
                self.output(f"[Team {team_id}] ✅ Upload successful")
                return (team_id, response)
            except Exception as e:
                error_msg = str(e)
                self.output(f"[Team {team_id}] ❌ Upload failed: {error_msg}")
                return (team_id, {"error": error_msg})

        # Execute uploads in parallel with thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all upload tasks
            future_to_team = {
                executor.submit(upload_to_team, team_id): team_id
                for team_id in team_ids
            }

            # Collect results as they complete
            for future in as_completed(future_to_team):
                team_id, response = future.result()
                if "error" in response:
                    errors[team_id] = response["error"]
                else:
                    results[team_id] = response

        # Summary
        success_count = len(results)
        failure_count = len(errors)

        if success_count > 0:
            self.output(
                f"✅ Successfully uploaded to {success_count} team(s): {list(results.keys())}"
            )

        if failure_count > 0:
            self.output(
                f"❌ Failed to upload to {failure_count} team(s): {list(errors.keys())}"
            )
            for team_id, error in errors.items():
                self.output(f"  Team {team_id}: {error}")

            # Raise error if all uploads failed
            if failure_count == len(team_ids):
                raise ProcessorError(
                    f"All team uploads failed. Errors: {json.dumps(errors, indent=2)}"
                )

        return results

    def _build_version_query(
        self, version: str, query_template: str = None, bundle_id: str = None
    ) -> str:
        """Build osquery query to detect outdated software versions.

        Supports two modes:
        1. Template mode: Use provided query_template with %VERSION% placeholder
        2. Default mode: Generate query using bundle_identifier and version_compare()

        Args:
            version: Current version to check against
            query_template: Optional query template with %VERSION% placeholder
            bundle_id: App bundle identifier (required for default mode)

        Returns:
            osquery SQL query string with version substituted

        Raises:
            ProcessorError: If template mode is used without query_template,
                          or default mode is used without bundle_id
        """
        # Sanitize version for SQL (escape single quotes)
        safe_version = version.replace("'", "''")

        if query_template:
            # Template mode: Replace %VERSION% placeholder with actual version
            query = query_template.replace("%VERSION%", safe_version)
            return query
        elif bundle_id:
            # Default mode: Generate query using apps table and version_compare
            # This is the legacy behavior for macOS apps
            safe_bundle_id = bundle_id.replace("'", "''")

            # Build query using apps table for version checking
            # Policy passes when no instances exist with incorrect version
            # This means: app not installed OR all instances have correct version
            # Policy fails when any instance has wrong version (needs update)
            safe_bundle_id = bundle_id.replace("'", "''")

            query = (
                f"SELECT 1 WHERE NOT EXISTS ("
                f"SELECT 1 FROM apps WHERE bundle_identifier = '{safe_bundle_id}' "
                f"AND bundle_short_version != '{safe_version}'"
                f");"
            )
            return query
        else:
            raise ProcessorError(
                "Either query_template or bundle_id must be provided to build version query"
            )

    def _format_policy_name(self, software_title: str, template: str = None) -> str:
        """Format policy name from template.

        Args:
            software_title: Software title to use in policy name
            template: Optional template string with %NAME% placeholder

        Returns:
            Formatted policy name
        """
        if template is None:
            template = self.env.get(
                "auto_update_policy_name", "autopkg-auto-update-%NAME%"
            )

        # Create slug from software title (lowercase, hyphens only)
        slug = self._slugify(software_title)

        # Replace %NAME% placeholder with slug
        policy_name = template.replace("%NAME%", slug)

        return policy_name

    def _find_existing_policy(
        self, fleet_api_base: str, fleet_token: str, team_id: int, policy_name: str
    ) -> dict | None:
        """Find existing policy by name.

        Args:
            fleet_api_base: Fleet base URL
            fleet_token: Fleet API token
            team_id: Team ID (0 for global)
            policy_name: Policy name to search for

        Returns:
            Policy dict if found, None otherwise
        """
        try:
            # Determine endpoint based on team_id
            if team_id == 0:
                endpoint = f"{fleet_api_base}/api/v1/fleet/global/policies"
            else:
                endpoint = f"{fleet_api_base}/api/v1/fleet/teams/{team_id}/policies"

            headers = {
                "Authorization": f"Bearer {fleet_token}",
                "Accept": "application/json",
            }
            req = urllib.request.Request(endpoint, headers=headers)

            with urllib.request.urlopen(
                req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                if resp.getcode() == 200:
                    data = json.loads(resp.read().decode())
                    policies = data.get("policies", [])

                    # Search for policy by name
                    for policy in policies:
                        if policy.get("name") == policy_name:
                            return policy
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            json.JSONDecodeError,
        ) as e:
            self.output(f"Warning: Could not query policies: {e}")

        return None

    def _create_or_update_policy_direct(
        self,
        fleet_api_base: str,
        fleet_token: str,
        team_id: int,
        software_title: str,
        version: str,
        title_id: int,
        pkg_path: str,
    ):
        """Create or update auto-update policy via Fleet API.

        Args:
            fleet_api_base: Fleet base URL
            fleet_token: Fleet API token
            team_id: Team ID (0 for global)
            software_title: Software title
            version: Software version
            title_id: Software title ID for linking policy to package
            pkg_path: Path to package file for bundle ID extraction
        """
        # Build policy name
        policy_name = self._format_policy_name(software_title)
        self.output(f"Auto-update policy name: {policy_name}")

        # Build version detection query
        query_template = self.env.get("auto_update_policy_query", "").strip()

        if query_template:
            # Use custom query template from recipe
            self.output("Using custom query template from recipe")
            query = self._build_version_query(version, query_template=query_template)
        else:
            # Fall back to default bundle_identifier-based query
            self.output(
                "No query template specified, using default bundle_identifier detection"
            )
            bundle_id = self._extract_bundle_id_from_pkg(Path(pkg_path))
            if not bundle_id:
                self.output(
                    f"Warning: Could not extract bundle ID from package and no query template provided. "
                    "Skipping auto-update policy creation."
                )
                return
            query = self._build_version_query(version, bundle_id=bundle_id)

        self.output(f"Auto-update policy query: {query}")

        # Check if policy already exists
        existing_policy = self._find_existing_policy(
            fleet_api_base, fleet_token, team_id, policy_name
        )

        # Prepare policy payload
        payload = {
            "name": policy_name,
            "query": query,
            "description": f"Auto-update policy for {software_title}. Managed by AutoPkg.",
            "resolution": f"This device will automatically install {software_title} {version}",
            "platform": "darwin",
            "critical": False,
            "software_title_id": title_id,
        }

        # Determine endpoint based on team_id
        if team_id == 0:
            base_endpoint = f"{fleet_api_base}/api/v1/fleet/global/policies"
        else:
            base_endpoint = f"{fleet_api_base}/api/v1/fleet/teams/{team_id}/policies"

        headers = {
            "Authorization": f"Bearer {fleet_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            if existing_policy:
                # Update existing policy
                policy_id = existing_policy["id"]
                endpoint = f"{base_endpoint}/{policy_id}"
                self.output(f"Updating existing auto-update policy (ID: {policy_id})")

                req = urllib.request.Request(
                    endpoint,
                    data=json.dumps(payload).encode(),
                    headers=headers,
                    method="PATCH",
                )
            else:
                # Create new policy
                endpoint = base_endpoint
                self.output("Creating new auto-update policy")

                req = urllib.request.Request(
                    endpoint,
                    data=json.dumps(payload).encode(),
                    headers=headers,
                    method="POST",
                )

            with urllib.request.urlopen(
                req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                if resp.getcode() in (200, 201):
                    response_data = json.loads(resp.read().decode())
                    policy_id = response_data.get("policy", {}).get("id")
                    self.output(
                        f"Auto-update policy {'updated' if existing_policy else 'created'} successfully (ID: {policy_id})"
                    )
                else:
                    raise ProcessorError(
                        f"Failed to create/update policy: {resp.getcode()}"
                    )
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            raise ProcessorError(
                f"Failed to create/update auto-update policy: {e.code} {error_body}"
            )
        except urllib.error.URLError as e:
            raise ProcessorError(
                f"Failed to connect to Fleet API for policy creation: {e}"
            )

    def _create_or_update_policy_gitops(
        self,
        repo_dir: str,
        software_title: str,
        version: str,
        pkg_path: str,
    ):
        """Create or update auto-update policy in GitOps repository.

        Args:
            repo_dir: Path to Git repository
            software_title: Software title (used to reference the software package)
            version: Software version
            pkg_path: Path to package file for bundle ID extraction

        Note:
            In GitOps mode, the policy references software by name rather than ID.
            Fleet will resolve the software_title to the appropriate software_title_id
            when it processes the GitOps configuration.
        """
        # Build policy name
        policy_name = self._format_policy_name(software_title)
        self.output(f"Auto-update policy name: {policy_name}")

        # Build version detection query
        query_template = self.env.get("auto_update_policy_query", "").strip()

        if query_template:
            # Use custom query template from recipe
            self.output("Using custom query template from recipe")
            query = self._build_version_query(version, query_template=query_template)
        else:
            # Fall back to default bundle_identifier-based query
            self.output(
                "No query template specified, using default bundle_identifier detection"
            )
            bundle_id = self._extract_bundle_id_from_pkg(Path(pkg_path))
            if not bundle_id:
                self.output(
                    f"Warning: Could not extract bundle ID from package and no query template provided. "
                    "Skipping auto-update policy creation."
                )
                return None
            query = self._build_version_query(version, bundle_id=bundle_id)

        self.output(f"Auto-update policy query: {query}")

        # Create policy YAML structure
        # In GitOps mode, reference software by name - Fleet will resolve to software_title_id
        policy_yaml = {
            "name": policy_name,
            "query": query,
            "description": f"Auto-update policy for {software_title}. Managed by AutoPkg.",
            "resolution": f"This device will automatically install {software_title} {version}",
            "platform": "darwin",
            "critical": False,
            "install_software": {
                "name": software_title,
            },
        }

        # Create lib/policies directory if it doesn't exist
        policies_dir = Path(repo_dir) / "lib" / "policies"
        policies_dir.mkdir(parents=True, exist_ok=True)

        # Write policy file
        slug = self._slugify(software_title)
        policy_filename = f"{slug}.yml"
        policy_path = policies_dir / policy_filename

        self.output(f"Writing auto-update policy to: lib/policies/{policy_filename}")
        self._write_yaml(policy_path, policy_yaml)

        # Return relative path for Git operations
        return f"lib/policies/{policy_filename}"

    def _parse_bool(self, value):
        """Parse boolean from string or bool value.

        AutoPkg passes booleans as strings via --key, so "false" becomes string
        and bool("false") = True (non-empty string). This properly parses it.

        Handles:
        - Python literals: 'True', 'False' via ast.literal_eval()
        - Common bool strings: 'true', 'false', 'yes', 'no', '1', '0'
        - Bool/int values: passthrough
        """
        import ast

        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            # Try ast.literal_eval for Python literals (True/False)
            try:
                return bool(ast.literal_eval(value))
            except (ValueError, SyntaxError):
                pass
            # Fallback to string matching for common bool strings
            val = value.lower()
            if val in ('y', 'yes', 't', 'true', 'on', '1'):
                return True
            elif val in ('n', 'no', 'f', 'false', 'off', '0', ''):
                return False
        return bool(value)

    def main(self):
        # Check if GitOps mode is enabled
        gitops_mode = self._parse_bool(self.env.get("gitops_mode", False))

        if gitops_mode:
            self._run_gitops_workflow()
        else:
            self._run_direct_upload_workflow()

    def _run_direct_upload_workflow(self):
        """Run the original direct upload workflow to Fleet API."""
        # Validate inputs
        pkg_path = Path(self.env["pkg_path"]).expanduser().resolve()
        if not pkg_path.is_file():
            raise ProcessorError(f"pkg_path not found: {pkg_path}")

        software_title = self.env["software_title"].strip()
        version = self.env["version"].strip()
        # Platform parameter for software YAML files
        platform = self.env.get("platform", DEFAULT_PLATFORM)

        # Check for dry_run mode
        dry_run = self._parse_bool(self.env.get("dry_run", False))

        # Get local GitOps parameters for YAML updates (optional)
        gitops_software_dir = self.env.get("gitops_software_dir")
        gitops_software_subpath = self.env.get("gitops_software_subpath")
        gitops_software_filename = self.env.get("gitops_software_filename")

        # Validate required direct mode parameters (skip in dry_run)
        fleet_api_base = self.env.get("fleet_api_base")
        fleet_token = self.env.get("fleet_api_token")
        team_id = self.env.get("team_id")
        team_ids = self.env.get("team_ids")
        discover_teams = self._parse_bool(self.env.get("discover_teams", False))
        teams_dir = self.env.get("teams_dir")

        # Multi-team support: Determine which teams to upload to
        target_team_ids = []

        if discover_teams:
            # Extract software name from YAML filename for team discovery
            # Teams reference packages by YAML filename (e.g., "homebrew" from "homebrew.yml")
            # NOT by software_title (which includes version and git hash)
            if gitops_software_filename:
                software_name = gitops_software_filename.replace('.yml', '').replace('.yaml', '')
            else:
                # Fallback: try to guess from software_title (remove version/hash patterns)
                software_name = software_title.split()[0].lower()

            # Auto-discover teams from YAML files
            self.output(f"Team discovery enabled. Scanning for teams that reference package '{software_name}'...")
            discovered_teams = self._discover_teams_for_package(
                software_name,
                teams_dir,
                fleet_api_base=fleet_api_base,
                fleet_token=fleet_token
            )
            target_team_ids = [t["team_id"] for t in discovered_teams if t["team_id"] is not None]

            if not target_team_ids:
                self.output(
                    f"Warning: Team discovery found no teams for '{software_name}'. "
                    f"Falling back to team_id or team_ids if provided."
                )

        if team_ids:
            # Explicit list of team IDs provided
            if isinstance(team_ids, (list, tuple)):
                explicit_teams = [int(tid) for tid in team_ids]
            else:
                # Single team ID passed as team_ids
                explicit_teams = [int(team_ids)]

            # Merge with discovered teams if any
            target_team_ids = list(set(target_team_ids + explicit_teams))

        if not target_team_ids and team_id:
            # Fall back to single team_id
            target_team_ids = [int(team_id)]

        # Validate we have at least one team
        if not dry_run and not all([fleet_api_base, fleet_token]):
            raise ProcessorError(
                "Direct mode requires: fleet_api_base and fleet_api_token. "
                "These can be set via recipe Input variables or AutoPkg preferences:\n"
                "  defaults write com.github.autopkg FLEET_API_BASE 'https://fleet.example.com'\n"
                "  defaults write com.github.autopkg FLEET_API_TOKEN 'your-token'"
            )

        if not dry_run and not target_team_ids:
            raise ProcessorError(
                "Direct mode requires at least one team. Provide one of:\n"
                "  - team_id: Single team ID\n"
                "  - team_ids: List of team IDs\n"
                "  - discover_teams: true (auto-discover from YAML files)\n"
                "Example: defaults write com.github.autopkg FLEET_TEAM_ID '1'"
            )

        # Use first team as primary for legacy single-team operations
        team_id = target_team_ids[0] if target_team_ids else None

        # Dry run mode: calculate hash and exit early
        if dry_run:
            self.output("DRY RUN MODE: Package will be built and hashed, but Fleet API upload and YAML updates will be skipped.")

            # Calculate hash from local package
            hash_sha256 = self._calculate_file_sha256(pkg_path)
            self.output(f"Calculated SHA-256 hash from local file: {hash_sha256}")
            self.env["hash_sha256"] = hash_sha256

            # Output what would be done
            self.output(f"[DRY RUN] Would upload package to Fleet API:")
            self.output(f"  Software: {software_title}")
            self.output(f"  Version: {version}")
            self.output(f"  Hash: {hash_sha256}")

            if gitops_software_dir and gitops_software_subpath and gitops_software_filename:
                yaml_file_path = Path(gitops_software_dir) / gitops_software_subpath / gitops_software_filename
                self.output(f"[DRY RUN] Would update local software YAML: {yaml_file_path}")

            self.output("DRY RUN COMPLETE - No changes were made to Fleet API or local files")
            return

        # Now safe to use - strip/convert values
        fleet_api_base = fleet_api_base.rstrip("/")
        team_id = int(team_id)

        # Package type: software (default) or bootstrap
        package_type = self.env.get("package_type", "software").strip().lower()
        if package_type not in ["software", "bootstrap"]:
            raise ProcessorError(
                f"Invalid package_type '{package_type}'. Must be 'software' or 'bootstrap'."
            )

        # Fleet deployment options
        self_service = self._parse_bool(self.env.get("self_service", False))
        automatic_install = self._parse_bool(self.env.get("automatic_install", False))
        labels_include_any = list(self.env.get("labels_include_any", []))
        labels_exclude_any = list(self.env.get("labels_exclude_any", []))
        categories = list(self.env.get("categories", []))

        # Display name: optional custom display name for Fleet UI
        # If not provided, use software_title as default
        display_name = self.env.get("display_name", "").strip()
        if not display_name:
            display_name = software_title

        # Read script files if paths are provided, otherwise use inline content
        install_script_input = self.env.get("install_script", "")
        uninstall_script_input = self.env.get("uninstall_script", "")
        pre_install_query = self.env.get("pre_install_query", "")
        post_install_script_input = self.env.get("post_install_script", "")

        # Check if inputs look like file paths (end with .sh or contain /) or inline scripts
        # If they look like paths, read the file content
        install_script = (
            self._read_script_file(install_script_input)
            if (
                install_script_input
                and (
                    install_script_input.endswith(".sh") or "/" in install_script_input
                )
            )
            else install_script_input
        )

        uninstall_script = (
            self._read_script_file(uninstall_script_input)
            if (
                uninstall_script_input
                and (
                    uninstall_script_input.endswith(".sh")
                    or "/" in uninstall_script_input
                )
            )
            else uninstall_script_input
        )

        post_install_script = (
            self._read_script_file(post_install_script_input)
            if (
                post_install_script_input
                and (
                    post_install_script_input.endswith(".sh")
                    or "/" in post_install_script_input
                )
            )
            else post_install_script_input
        )

        # Validate label targeting - only one of include/exclude allowed
        if labels_include_any and labels_exclude_any:
            raise ProcessorError(
                "Only one of labels_include_any or labels_exclude_any may be specified, not both."
            )

        # Validate categories - required when self_service is enabled (not for bootstrap packages)
        if package_type == "software" and self_service and not categories:
            raise ProcessorError(
                "CATEGORIES is required when SELF_SERVICE is true. Please specify at least one category."
            )

        # Query Fleet API to get server version
        self.output("Querying Fleet server version...")
        fleet_version = self._get_fleet_version(fleet_api_base, fleet_token)
        self.output(f"Detected Fleet version: {fleet_version}")

        # Check minimum version requirements
        if not self._is_fleet_minimum_supported(fleet_version):
            raise ProcessorError(
                f"Fleet version {fleet_version} is not supported. "
                f"This processor requires Fleet v{FLEET_MINIMUM_VERSION} or higher. "
                f"Please upgrade your Fleet server to a supported version."
            )

        # Check if package already exists in Fleet
        self.output(
            f"Checking if {software_title} {version} already exists in Fleet..."
        )
        existing_package = self._check_existing_package(
            fleet_api_base, fleet_token, team_id, software_title, version
        )

        if existing_package:
            # Calculate hash from local package file
            hash_sha256 = self._calculate_file_sha256(pkg_path)
            fleet_hash = existing_package.get("hash_sha256", "")

            self.output(
                f"Package {software_title} {version} already exists in Fleet (hash: {fleet_hash[:16] + '...' if fleet_hash else 'none'})"
            )
            self.output(
                f"Calculated SHA-256 hash from local file: {hash_sha256[:16]}..."
            )

            # Compare hashes - if they differ, attempt to delete and re-upload
            if hash_sha256 != fleet_hash:
                self.output(
                    f"Hash mismatch detected! Local hash differs from Fleet hash. "
                    f"Attempting to delete old package and re-upload..."
                )
                delete_success = self._fleet_delete_package(fleet_api_base, fleet_token, software_title, team_id)
                if delete_success:
                    self.output("Old package deleted successfully. Continuing with upload of new package...")
                    # Continue to upload section below
                else:
                    # Delete failed - package is in use (bootstrap, policy, etc.)
                    # Update yml with Fleet's hash instead of local hash to keep in sync
                    self.output(
                        f"Warning: Could not delete existing package (likely in use for bootstrap or policy). "
                        f"Keeping Fleet's existing package. Updating yml with Fleet hash to maintain sync."
                    )
                    self.output(
                        f"To update this package, manually remove it from bootstrap/policies in Fleet UI first."
                    )

                    # Set output variables for existing package
                    title_id = existing_package.get("title_id")
                    self.env["fleet_title_id"] = title_id
                    self.env["fleet_installer_id"] = None
                    self.env["hash_sha256"] = fleet_hash  # Use Fleet's hash, not local

                    # Update local yml with Fleet's hash
                    if gitops_software_dir and gitops_software_subpath and gitops_software_filename and fleet_hash and title_id:
                        yaml_file_path = Path(gitops_software_dir) / gitops_software_subpath / gitops_software_filename

                        if not yaml_file_path.is_absolute():
                            yaml_file_path = yaml_file_path.resolve()

                        self.output(f"Updating local software YAML with Fleet hash: {yaml_file_path}")

                        self._update_local_software_yaml(
                            yaml_file_path,
                            fleet_hash,  # Use Fleet's hash
                            version,
                            package_url=None,
                            display_name=display_name,
                            software_title=software_title,
                            platform=platform,
                        )

                    return  # Skip upload, keep existing package
            else:
                # Hashes match - skip upload
                self.output(
                    f"Package {software_title} {version} already exists with matching hash. Skipping upload."
                )
                # Set output variables for existing package
                title_id = existing_package.get("title_id")
                self.env["fleet_title_id"] = title_id
                self.env["fleet_installer_id"] = None
                self.env["hash_sha256"] = hash_sha256

                # Update display name if provided (even for existing packages)
                if title_id and display_name:
                    self.output(
                        f"Updating display name for existing software title ID {title_id}..."
                    )
                    try:
                        self._fleet_update_display_name(
                            fleet_api_base,
                            fleet_token,
                            title_id,
                            team_id,
                            display_name,
                        )
                    except Exception as e:
                        # Log warning but don't fail the entire workflow
                        self.output(
                            f"Warning: Failed to update display name: {e}. "
                            "Display name may show default value."
                        )

                # Still create/update auto-update policy if enabled
                automatic_update = self._parse_bool(self.env.get("automatic_update", False))
                if automatic_update and title_id:
                    self.output("Auto-update policy enabled - creating/updating policy...")
                    try:
                        self._create_or_update_policy_direct(
                            fleet_api_base,
                            fleet_token,
                            team_id,
                            software_title,
                            version,
                            title_id,
                            pkg_path,
                        )
                    except Exception as e:
                        # Log warning but don't fail the entire workflow
                        self.output(
                            f"Warning: Failed to create auto-update policy: {e}. "
                            "Package already exists, but policy creation failed."
                        )

                # Update local software YAML file if GitOps parameters are provided
                # (even when package already exists, to ensure yml is in sync)
                if gitops_software_dir and gitops_software_subpath and gitops_software_filename and hash_sha256 and title_id:
                    yaml_file_path = Path(gitops_software_dir) / gitops_software_subpath / gitops_software_filename

                    if not yaml_file_path.is_absolute():
                        yaml_file_path = yaml_file_path.resolve()

                    self.output(f"Updating local software YAML (package already exists): {yaml_file_path}")

                    # Update the YAML file with hash only
                    # Do NOT include Fleet API URLs - they're internal/temporary
                    self._update_local_software_yaml(
                        yaml_file_path,
                        hash_sha256,
                        version,
                        package_url=None,  # No URL in Fleet API mode
                        display_name=display_name,
                        software_title=software_title,
                        platform=platform,
                    )

                return

        # Upload to Fleet
        if package_type == "bootstrap":
            # Bootstrap packages use different endpoint and don't support deployment options
            # Bootstrap packages don't support multi-team upload yet
            if len(target_team_ids) > 1:
                self.output(
                    f"Warning: Bootstrap packages only support single team upload. "
                    f"Using first team: {target_team_ids[0]}"
                )
            self.output("Uploading bootstrap package to Fleet...")
            upload_info = self._fleet_upload_bootstrap(
                fleet_api_base,
                fleet_token,
                pkg_path,
                team_id,
            )
        else:
            # Regular software package upload
            if len(target_team_ids) > 1:
                # Multi-team upload with parallel execution
                self.output(f"Uploading package to {len(target_team_ids)} teams in parallel...")
                upload_results = self._upload_package_to_teams(
                    base_url=fleet_api_base,
                    token=fleet_token,
                    pkg_path=pkg_path,
                    software_title=software_title,
                    version=version,
                    team_ids=target_team_ids,
                    self_service=self_service,
                    automatic_install=automatic_install,
                    labels_include_any=labels_include_any,
                    labels_exclude_any=labels_exclude_any,
                    install_script=install_script,
                    uninstall_script=uninstall_script,
                    pre_install_query=pre_install_query,
                    post_install_script=post_install_script,
                    categories=categories,
                    display_name=display_name,
                )

                # Summarize upload results
                successful_teams = []
                failed_teams = []
                for team_id_key, result in upload_results.items():
                    if result and "error" not in result:
                        # Validate that upload response has required fields
                        software_package = result.get("software_package", {})
                        title_id = software_package.get("title_id")
                        hash_sha256 = software_package.get("hash_sha256")

                        if title_id and hash_sha256:
                            successful_teams.append((team_id_key, result))
                        else:
                            missing_fields = []
                            if not title_id:
                                missing_fields.append("title_id")
                            if not hash_sha256:
                                missing_fields.append("hash_sha256")
                            error_msg = f"Upload response missing required fields: {', '.join(missing_fields)}"
                            failed_teams.append((team_id_key, error_msg))
                    else:
                        error_msg = result.get("error", "Unknown error") if result else "No response"
                        failed_teams.append((team_id_key, error_msg))

                # Log summary
                self.output(f"\n{'='*60}")
                self.output(f"Multi-team upload summary:")
                self.output(f"  Total teams: {len(upload_results)}")
                self.output(f"  Successful: {len(successful_teams)} - Team IDs: {[t[0] for t in successful_teams]}")
                self.output(f"  Failed: {len(failed_teams)}")
                if failed_teams:
                    for team_id_key, error_msg in failed_teams:
                        self.output(f"    Team {team_id_key}: {error_msg}")
                self.output(f"{'='*60}\n")

                if not successful_teams:
                    raise ProcessorError("All team uploads failed")

                # Update display_name for all successful teams
                if display_name and display_name.strip():
                    self.output(f"Updating display_name for {len(successful_teams)} successful team(s)...")
                    for team_id_key, result in successful_teams:
                        software_package = result.get("software_package", {})
                        title_id = software_package.get("title_id")
                        if title_id:
                            try:
                                self.output(f"  [Team {team_id_key}] Updating display_name for title ID {title_id}...")
                                self._fleet_update_display_name(
                                    fleet_api_base,
                                    fleet_token,
                                    title_id,
                                    team_id_key,
                                    display_name,
                                )
                                self.output(f"  [Team {team_id_key}] ✅ Display name updated successfully")
                            except Exception as e:
                                self.output(
                                    f"  [Team {team_id_key}] ⚠️  Warning: Failed to update display name: {e}"
                                )
                        else:
                            self.output(f"  [Team {team_id_key}] ⚠️  Warning: No title_id in upload response, skipping display_name update")

                # Use first successful upload for output variables
                upload_info = successful_teams[0][1]
                successful_team_id = successful_teams[0][0]
                self.output(f"Using upload result from team {successful_team_id} for output variables")

                # Update team_id to the successful upload's team for subsequent queries
                team_id = successful_team_id
            else:
                # Single team upload (legacy path)
                self.output("Uploading package to Fleet...")
                upload_info = self._fleet_upload_package(
                    fleet_api_base,
                    fleet_token,
                    pkg_path,
                    software_title,
                    version,
                    team_id,
                    self_service,
                    automatic_install,
                    labels_include_any,
                    labels_exclude_any,
                    install_script,
                    uninstall_script,
                    pre_install_query,
                    post_install_script,
                    categories,
                    display_name,
                )

        if upload_info is None:
            raise ProcessorError("Fleet package upload failed; no data returned")

        # Bootstrap packages return empty dict on success - handle specially
        if package_type == "bootstrap":
            self.output("Bootstrap package uploaded successfully")
            return  # No further processing needed for bootstrap packages

        # Extract upload results
        software_package = upload_info.get("software_package", {})
        title_id = software_package.get("title_id")
        installer_id = software_package.get("installer_id")
        hash_sha256 = software_package.get("hash_sha256")

        # DEBUG: Log upload response structure
        self.output(f"DEBUG: Upload response - software_package keys: {list(software_package.keys())}")
        if hash_sha256:
            self.output(f"DEBUG: Hash from upload response: {hash_sha256}")
        else:
            self.output("DEBUG: No hash in upload response")

        # Set output variables
        self.output(
            f"Package uploaded successfully. Title ID: {title_id}, Installer ID: {installer_id}"
        )
        self.env["fleet_title_id"] = title_id
        self.env["fleet_installer_id"] = installer_id

        # If upload response doesn't include hash, query Fleet to get it
        if not hash_sha256 and title_id:
            self.output(f"Hash not in upload response, querying Fleet for title {title_id}...")
            try:
                title_url = f"{fleet_api_base}/api/v1/fleet/software/titles/{title_id}?team_id={team_id}"
                headers = {
                    "Authorization": f"Bearer {fleet_token}",
                    "Accept": "application/json",
                }
                req = urllib.request.Request(title_url, headers=headers)
                with urllib.request.urlopen(req, timeout=30, context=self._get_ssl_context()) as resp:
                    if resp.getcode() == 200:
                        title_data = json.loads(resp.read().decode())
                        package_data = title_data.get("software_title", {}).get("software_package", {})
                        hash_sha256 = package_data.get("hash_sha256")
                        self.output(f"DEBUG: Fleet title query - package_data: {package_data}")
                        if hash_sha256:
                            self.output(f"Retrieved hash from Fleet API: {hash_sha256}")
                        else:
                            self.output("Warning: Fleet API did not return package hash - package may not be attached to title")
            except Exception as e:
                self.output(f"Warning: Failed to query Fleet for hash: {e}")

        if hash_sha256:
            self.env["hash_sha256"] = hash_sha256
            self.output(f"DEBUG: Set hash_sha256 environment variable: {hash_sha256}")
        else:
            self.output("WARNING: No hash available - YAML file will NOT be updated")

        # Update local software YAML file if GitOps parameters are provided
        if gitops_software_dir and gitops_software_subpath and gitops_software_filename and hash_sha256:
            yaml_file_path = Path(gitops_software_dir) / gitops_software_subpath / gitops_software_filename

            if not yaml_file_path.is_absolute():
                yaml_file_path = yaml_file_path.resolve()

            self.output(f"Updating local software YAML: {yaml_file_path}")
            self.output(f"DEBUG: Writing hash to YAML: {hash_sha256}")

            # Update or create the YAML file with hash only
            # Do NOT include Fleet API URLs - they're internal/temporary and break when packages are deleted
            self._update_local_software_yaml(
                yaml_file_path,
                hash_sha256,
                version,
                package_url=None,  # No URL in Fleet API mode
                display_name=display_name,
                software_title=software_title,
                platform=platform,
            )

            # Update any policy files that install this software with the new hash
            self.output(f"Checking for policies that install {software_title}...")
            software_name = gitops_software_filename.replace('.yml', '')
            self._update_policy_hashes(
                gitops_software_dir,
                software_name,
                hash_sha256,
            )

        # Upload icon if provided
        icon_path_str = self.env.get("icon", "").strip()

        extracted_icon_path = None  # Track if we need to clean up

        if icon_path_str and title_id:
            # Manual icon path provided - use it
            # Try to resolve icon path relative to recipe directory first
            icon_path = Path(icon_path_str)
            if not icon_path.is_absolute():
                # Get recipe directory from AutoPkg environment
                recipe_dir = self.env.get("RECIPE_DIR")
                if recipe_dir:
                    icon_path = (Path(recipe_dir) / icon_path_str).resolve()
                else:
                    icon_path = icon_path.expanduser().resolve()
            else:
                icon_path = icon_path.expanduser().resolve()

            if icon_path.exists():
                self.output(f"Using manual icon file: {icon_path}")
                self._fleet_upload_icon(
                    fleet_api_base,
                    fleet_token,
                    title_id,
                    team_id,
                    icon_path,
                )
            else:
                self.output(
                    f"Warning: Icon file not found: {icon_path}. Skipping icon upload."
                )
        elif title_id:
            # No manual icon - try to extract from package automatically
            self.output("Attempting to extract icon from package automatically...")
            extracted_icon_path = self._extract_icon_from_pkg(pkg_path)

            if extracted_icon_path and extracted_icon_path.exists():
                self.output(f"Successfully extracted icon: {extracted_icon_path.name}")
                try:
                    self._fleet_upload_icon(
                        fleet_api_base,
                        fleet_token,
                        title_id,
                        team_id,
                        extracted_icon_path,
                    )
                finally:
                    # Clean up extracted icon temp directory
                    if extracted_icon_path.parent.exists():
                        try:
                            shutil.rmtree(extracted_icon_path.parent)
                        except Exception as e:
                            self.output(
                                f"Warning: Failed to cleanup icon temp dir: {e}"
                            )
            else:
                self.output(
                    "Could not extract icon from package. Skipping icon upload."
                )

        # Update display name if provided and different from software_title
        if title_id and display_name:
            self.output(f"Updating display name for software title ID {title_id}...")
            try:
                self._fleet_update_display_name(
                    fleet_api_base,
                    fleet_token,
                    title_id,
                    team_id,
                    display_name,
                )
            except Exception as e:
                # Log warning but don't fail the entire workflow
                self.output(
                    f"Warning: Failed to update display name: {e}. "
                    "Package upload succeeded, but display name may show default value."
                )

        # Create auto-update policy if enabled
        automatic_update = self._parse_bool(self.env.get("automatic_update", False))
        if automatic_update and title_id:
            self.output("Auto-update policy enabled - creating/updating policy...")
            try:
                self._create_or_update_policy_direct(
                    fleet_api_base,
                    fleet_token,
                    team_id,
                    software_title,
                    version,
                    title_id,
                    pkg_path,
                )
            except Exception as e:
                # Log warning but don't fail the entire workflow
                self.output(
                    f"Warning: Failed to create auto-update policy: {e}. "
                    "Package upload succeeded, but policy creation failed."
                )
        elif automatic_update and not title_id:
            self.output(
                "Warning: Auto-update policy enabled but no software title ID available. "
                "Skipping policy creation."
            )

    def _run_gitops_workflow(self):
        """Run the GitOps workflow: upload to S3, update YAML, create PR."""
        # Import boto3 for GitOps mode (required for S3 operations)
        global boto3, ClientError, NoCredentialsError
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
        except ImportError:
            raise ProcessorError(
                "boto3 is required for GitOps mode.\n\n"
                "Install it into AutoPkg's Python environment with:\n"
                "  /Library/AutoPkg/Python3/Python.framework/Versions/Current/bin/python3 -m pip install boto3>=1.18.0\n\n"
                "Or use direct mode to upload directly to Fleet API without S3/GitOps:\n"
                "  Set gitops_mode to false in your recipe or AutoPkg preferences."
            )

        # Validate inputs
        pkg_path = Path(self.env["pkg_path"]).expanduser().resolve()
        if not pkg_path.is_file():
            raise ProcessorError(f"pkg_path not found: {pkg_path}")

        software_title = self.env["software_title"].strip()
        version = self.env["version"].strip()

        # GitOps mode required parameters
        aws_s3_bucket = self.env.get("aws_s3_bucket")
        aws_cloudfront_domain = self.env.get("aws_cloudfront_domain")
        gitops_repo_url = self.env.get("gitops_repo_url")
        gitops_software_dir = self.env.get("gitops_software_dir", "lib/macos/software")
        gitops_team_yaml_path = self.env.get("gitops_team_yaml_path")
        github_token = self.env.get("github_token")
        s3_retention_versions = int(self.env.get("s3_retention_versions", 0))
        dry_run = self._parse_bool(self.env.get("dry_run", False))

        # Detect GitOps mode type: local-only or full GitHub PR workflow
        local_gitops_mode = not bool(gitops_repo_url)

        # Validate required GitOps parameters based on mode
        if dry_run:
            # Dry run only requires S3 config
            if not all([aws_s3_bucket]):
                raise ProcessorError("Dry run mode requires: aws_s3_bucket")
        elif local_gitops_mode:
            # Local GitOps mode: upload to S3 and update local YAML files
            gitops_software_subpath = self.env.get("gitops_software_subpath")
            gitops_software_filename = self.env.get("gitops_software_filename")

            if not all([aws_s3_bucket, gitops_software_dir, gitops_software_subpath, gitops_software_filename]):
                raise ProcessorError(
                    "Local GitOps mode requires: aws_s3_bucket, gitops_software_dir, "
                    "gitops_software_subpath, and gitops_software_filename"
                )
            self.output("Using LOCAL GitOps mode (S3 upload + local YAML update)")
        else:
            # Full GitHub PR GitOps mode requires all parameters
            if not all(
                [
                    aws_s3_bucket,
                    aws_cloudfront_domain,
                    gitops_repo_url,
                    gitops_team_yaml_path,
                    github_token,
                ]
            ):
                raise ProcessorError(
                    "Full GitOps mode requires: aws_s3_bucket, aws_cloudfront_domain, "
                    "gitops_repo_url, gitops_team_yaml_path, and github_token. "
                    "For local-only mode (no GitHub PRs), omit gitops_repo_url."
                )
            self.output("Using FULL GitOps mode (S3 upload + GitHub PR)")

        # Store mode for later use
        self.env["_local_gitops_mode"] = local_gitops_mode

        # Fleet deployment options
        self_service = self._parse_bool(self.env.get("self_service", True))
        automatic_install = self._parse_bool(self.env.get("automatic_install", False))
        labels_include_any = list(self.env.get("labels_include_any", []))
        labels_exclude_any = list(self.env.get("labels_exclude_any", []))
        categories = list(self.env.get("categories", []))

        # Display name: optional custom display name for Fleet UI
        # If not provided, use software_title as default
        display_name = self.env.get("display_name", "").strip()
        if not display_name:
            display_name = software_title

        # Read script files if paths are provided, otherwise use inline content
        install_script_input = self.env.get("install_script", "")
        uninstall_script_input = self.env.get("uninstall_script", "")
        pre_install_query = self.env.get("pre_install_query", "")
        post_install_script_input = self.env.get("post_install_script", "")

        # Check if inputs look like file paths (end with .sh or contain /) or inline scripts
        # If they look like paths, read the file content
        install_script = (
            self._read_script_file(install_script_input)
            if (
                install_script_input
                and (
                    install_script_input.endswith(".sh") or "/" in install_script_input
                )
            )
            else install_script_input
        )

        uninstall_script = (
            self._read_script_file(uninstall_script_input)
            if (
                uninstall_script_input
                and (
                    uninstall_script_input.endswith(".sh")
                    or "/" in uninstall_script_input
                )
            )
            else uninstall_script_input
        )

        post_install_script = (
            self._read_script_file(post_install_script_input)
            if (
                post_install_script_input
                and (
                    post_install_script_input.endswith(".sh")
                    or "/" in post_install_script_input
                )
            )
            else post_install_script_input
        )

        icon_path_str = self.env.get("icon", "").strip()

        # Dry run notification
        if dry_run:
            self.output("DRY RUN MODE: Package will be built and hashed, but S3 upload and YAML updates will be skipped.")

        # Validate label targeting - only one of include/exclude allowed
        if labels_include_any and labels_exclude_any:
            raise ProcessorError(
                "Only one of labels_include_any or labels_exclude_any may be specified, not both."
            )

        # Validate categories - required when self_service is enabled (not for bootstrap packages)
        if package_type == "software" and self_service and not categories:
            raise ProcessorError(
                "CATEGORIES is required when SELF_SERVICE is true. Please specify at least one category."
            )

        # Initialize variables
        temp_dir = None
        extracted_icon_path = None
        icon_relative_path = None

        try:
            # Clone GitOps repository and handle icons (skip in dry run or local mode)
            if not dry_run and not local_gitops_mode:
                self.output(f"Cloning GitOps repository: {gitops_repo_url}")
                temp_dir = self._clone_gitops_repo(gitops_repo_url, github_token)
                self.output(f"Repository cloned to: {temp_dir}")

                # Handle icon - either from manual path or auto-extraction
                if icon_path_str:
                    # Manual icon path provided
                    icon_relative_path = self._copy_icon_to_gitops_repo(
                        temp_dir, icon_path_str, software_title
                    )
                else:
                    # Try to extract icon from package automatically
                    self.output("Attempting to extract icon from package automatically...")
                    extracted_icon_path = self._extract_icon_from_pkg(pkg_path)

                    if extracted_icon_path and extracted_icon_path.exists():
                        self.output(
                            f"Successfully extracted icon: {extracted_icon_path.name}"
                        )
                        # Copy extracted icon to GitOps repo
                        icon_relative_path = self._copy_icon_to_gitops_repo(
                            temp_dir, str(extracted_icon_path), software_title
                        )
                    else:
                        self.output(
                            "Could not extract icon from package. Skipping icon in GitOps."
                        )

            # Calculate SHA-256 hash (always done, even in dry run)
            self.output(f"Calculating SHA-256 hash from local file: {pkg_path.name}")
            hash_sha256 = self._calculate_file_sha256(pkg_path)
            self.output(f"SHA-256: {hash_sha256}")
            self.env["hash_sha256"] = hash_sha256

            if dry_run:
                # Dry run: Output what would be done without actually doing it
                s3_key = f"{software_title}/{software_title}-{version}.pkg"

                # Only construct CloudFront URL if domain is provided
                if aws_cloudfront_domain:
                    cloudfront_url = self._construct_cloudfront_url(aws_cloudfront_domain, s3_key)
                elif self.env.get("s3_endpoint_url"):
                    # Use S3 endpoint URL if available (for SeaweedFS/MinIO)
                    s3_endpoint = self.env.get("s3_endpoint_url")
                    cloudfront_url = f"{s3_endpoint}/{aws_s3_bucket}/{s3_key}"
                else:
                    cloudfront_url = "N/A"

                self.output("[DRY RUN] Would upload package to S3:")
                self.output(f"  Bucket: {aws_s3_bucket}")
                self.output(f"  Key: {s3_key}")
                self.output(f"  CloudFront URL: {cloudfront_url}")
                self.output(f"  SHA-256: {hash_sha256}")

                if s3_retention_versions > 0:
                    self.output(f"[DRY RUN] Would clean up old S3 versions (retaining {s3_retention_versions} most recent)")

                self.env["cloudfront_url"] = cloudfront_url
            else:
                # Normal mode: Actually upload to S3
                self.output(f"Uploading package to S3 bucket: {aws_s3_bucket}")
                s3_key, package_was_uploaded = self._upload_to_s3(
                    aws_s3_bucket, software_title, version, pkg_path
                )
                self.output(f"Package in S3: {s3_key}")

                # Construct package URL (CloudFront or direct S3)
                if local_gitops_mode:
                    # Local mode: use S3 endpoint URL directly
                    s3_endpoint = self.env.get("s3_endpoint_url", "")
                    if s3_endpoint:
                        # Remove trailing slash
                        s3_endpoint = s3_endpoint.rstrip("/")
                        package_url = f"{s3_endpoint}/{aws_s3_bucket}/{s3_key}"
                    else:
                        # Fallback to AWS S3 URL
                        package_url = f"https://{aws_s3_bucket}.s3.amazonaws.com/{s3_key}"
                    self.output(f"Package URL: {package_url}")
                else:
                    # Full GitOps mode: use CloudFront
                    package_url = self._construct_cloudfront_url(aws_cloudfront_domain, s3_key)
                    self.output(f"CloudFront URL: {package_url}")

                self.env["cloudfront_url"] = package_url  # Keep same env var name for compatibility

                # Clean up old versions in S3
                if s3_retention_versions > 0:
                    self.output(
                        f"Cleaning up old S3 versions (retaining {s3_retention_versions} most recent)..."
                    )
                    self._cleanup_old_s3_versions(
                        aws_s3_bucket, software_title, version, s3_retention_versions
                    )
                else:
                    self.output("S3 pruning disabled (s3_retention_versions = 0)")

            if dry_run:
                # Dry run: Output what would be done without creating YAML or Git operations
                self.output("[DRY RUN] Would create software package YAML:")
                self.output(f"  Directory: {gitops_software_dir}")
                self.output(f"  Software: {software_title}")
                self.output(f"  Version: {version}")
                self.output(f"  CloudFront URL: {cloudfront_url}")
                self.output(f"  Hash: {hash_sha256}")

                self.output(f"[DRY RUN] Would update team YAML: {gitops_team_yaml_path}")
                self.output(f"  Self-service: {self_service}")
                self.output(f"  Automatic install: {automatic_install}")
                if labels_include_any:
                    self.output(f"  Labels (include any): {labels_include_any}")
                if labels_exclude_any:
                    self.output(f"  Labels (exclude any): {labels_exclude_any}")
                if categories:
                    self.output(f"  Categories: {categories}")

                automatic_update = self._parse_bool(self.env.get("automatic_update", False))
                if automatic_update:
                    self.output("[DRY RUN] Would create auto-update policy YAML")

                branch_name = f"autopkg/{self._slugify(software_title)}-{version}"
                self.output(f"[DRY RUN] Would create Git branch: {branch_name}")
                self.output(f"[DRY RUN] Would create pull request to: {gitops_repo_url}")
                self.output("")
                self.output("DRY RUN COMPLETE - No changes were made to S3 or Git repository")

                self.env["git_branch"] = branch_name
                self.env["pull_request_url"] = ""
            elif local_gitops_mode:
                # Local GitOps mode: Update local software.yml file
                gitops_software_subpath = self.env.get("gitops_software_subpath")
                gitops_software_filename = self.env.get("gitops_software_filename")

                # Construct full path to software.yml file
                yaml_file_path = Path(gitops_software_dir) / gitops_software_subpath / gitops_software_filename
                yaml_file_path = yaml_file_path.resolve()

                self.output(f"Updating local software YAML: {yaml_file_path}")

                # Update or create the YAML file with package URL and hash
                self._update_local_software_yaml(
                    yaml_file_path,
                    hash_sha256,
                    version,
                    package_url=package_url,
                    display_name=display_name,
                )

                self.output(f"Successfully updated {yaml_file_path}")
                self.output(f"  URL: {package_url}")
                self.output(f"  Hash: {hash_sha256}")
                self.output(f"  Version: {version}")

                # Set env vars for compatibility
                self.env["git_branch"] = ""
                self.env["pull_request_url"] = ""
            else:
                # Full GitHub PR GitOps mode: create YAML files and Git operations
                # Create software package YAML file
                self.output(f"Creating software package YAML in {gitops_software_dir}")
                package_yaml_path = self._create_software_package_yaml(
                    temp_dir,
                    gitops_software_dir,
                    software_title,
                    package_url,
                    hash_sha256,
                    install_script,
                    uninstall_script,
                    pre_install_query,
                    post_install_script,
                    icon_relative_path,
                    display_name,
                )

                # Update team YAML file to reference the package
                self.output(f"Updating team YAML: {gitops_team_yaml_path}")
                team_yaml_path = Path(temp_dir) / gitops_team_yaml_path
                self._update_team_yaml(
                    team_yaml_path,
                    package_yaml_path,
                    software_title,
                    self_service,
                    automatic_install,
                    labels_include_any,
                    labels_exclude_any,
                    categories,
                )

                # Create auto-update policy if enabled
                policy_yaml_path = None
                automatic_update = self._parse_bool(self.env.get("automatic_update", False))
                if automatic_update:
                    self.output("Auto-update policy enabled - creating policy YAML...")
                    try:
                        policy_yaml_path = self._create_or_update_policy_gitops(
                            temp_dir,
                            software_title,
                            version,
                            pkg_path,
                        )
                    except Exception as e:
                        # Log warning but don't fail the entire workflow
                        self.output(
                            f"Warning: Failed to create auto-update policy YAML: {e}. "
                            "Package upload succeeded, but policy creation failed."
                        )

                # Create Git branch, commit, and push
                branch_name = f"autopkg/{self._slugify(software_title)}-{version}"
                self.output(f"Creating Git branch: {branch_name}")
                self._commit_and_push(
                    temp_dir,
                    branch_name,
                    software_title,
                    version,
                    package_yaml_path,
                    team_yaml_path,
                    icon_relative_path,
                    policy_yaml_path,
                )
                self.env["git_branch"] = branch_name

                # Create pull request
                self.output("Creating pull request...")
                pr_url = self._create_pull_request(
                    gitops_repo_url, github_token, branch_name, software_title, version
                )
                self.output(f"Pull request created: {pr_url}")
                self.env["pull_request_url"] = pr_url

        except Exception as e:
            # If we have a CloudFront URL, log it so it can be manually added
            if "cloudfront_url" in self.env:
                self.output(
                    f"ERROR: GitOps workflow failed, but package was uploaded to: {self.env['cloudfront_url']}"
                )
            raise ProcessorError(f"GitOps workflow failed: {e}")
        finally:
            # Clean up extracted icon temp directory
            if extracted_icon_path and extracted_icon_path.parent.exists():
                try:
                    shutil.rmtree(extracted_icon_path.parent)
                except Exception as e:
                    self.output(f"Warning: Failed to cleanup icon temp dir: {e}")
            # Always clean up temporary directory
            if temp_dir and Path(temp_dir).exists():
                self.output(f"Cleaning up temporary directory: {temp_dir}")
                try:
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    self.output(f"Warning: Failed to cleanup temp dir: {e}")

    # ------------------- helpers -------------------

    def _update_local_software_yaml(
        self,
        yaml_file_path: Path,
        hash_sha256: str,
        version: str,
        package_url: str = None,
        display_name: str = "",
        software_title: str = "",
        platform: str = "",
    ):
        """Update local software.yml file with package hash and version.

        Args:
            yaml_file_path: Path to the software YAML file
            hash_sha256: SHA256 hash of the package
            version: Package version
            package_url: URL of the package (optional, only written if provided)
            display_name: Optional display name for Fleet UI
            software_title: Software title (name field, required for new files)
            platform: Platform (darwin/linux, required for new files)

        Raises:
            ProcessorError: If YAML file cannot be read or written
        """
        # Import yaml library
        try:
            import yaml
        except ImportError:
            raise ProcessorError(
                "PyYAML is required for local GitOps mode.\n"
                "Install it into AutoPkg's Python environment."
            )

        # Read existing YAML file or create new structure
        if yaml_file_path.exists():
            self.output(f"Reading existing YAML file: {yaml_file_path}")
            try:
                with open(yaml_file_path, 'r') as f:
                    data = yaml.safe_load(f) or {}
            except Exception as e:
                raise ProcessorError(f"Failed to read YAML file {yaml_file_path}: {e}")
        else:
            self.output(f"Creating new YAML file: {yaml_file_path}")
            # Create parent directory if it doesn't exist
            yaml_file_path.parent.mkdir(parents=True, exist_ok=True)
            data = {}

        # Update fields
        if package_url:
            data['url'] = package_url
        data['hash_sha256'] = hash_sha256
        data['version'] = version

        # Write name and platform for new files (required by Fleet GitOps)
        if software_title and 'name' not in data:
            data['name'] = software_title
        if platform and 'platform' not in data:
            data['platform'] = platform

        # Add display_name if provided
        if display_name:
            data['display_name'] = display_name

        # Write updated YAML with verification
        #
        # IMPORTANT: This verification logging helps debug hash commit issues.
        # Previous bug: CI artifact download/decompression failures caused stale
        # hashes to be committed even though this write succeeded. The verification
        # logs prove the correct hash was written here, allowing us to trace where
        # the hash was lost in the CI artifact workflow.
        #
        # If hash commits fail but these logs show success, check:
        # 1. CI artifact compression/decompression (.yml.gz handling)
        # 2. YAML filename matches recipe name (gitops_software_filename)
        # 3. Artifact upload/download endpoints and permissions
        try:
            with open(yaml_file_path, 'w') as f:
                yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
            self.output(f"DEBUG: YAML file written successfully to: {yaml_file_path}")
            self.output(f"DEBUG: File exists after write: {yaml_file_path.exists()}")
            self.output(f"DEBUG: File size: {yaml_file_path.stat().st_size if yaml_file_path.exists() else 0} bytes")

            # Read back to verify hash was written correctly
            with open(yaml_file_path, 'r') as f:
                verify_data = yaml.safe_load(f)
            self.output(f"DEBUG: Hash in written file (verification): {verify_data.get('hash_sha256', 'NOT FOUND')}")
            if verify_data.get('hash_sha256') == hash_sha256:
                self.output("✓ YAML write verified - hash matches")
            else:
                self.output(f"✗ YAML write verification FAILED - Expected: {hash_sha256}, Got: {verify_data.get('hash_sha256')}")
        except Exception as e:
            raise ProcessorError(f"Failed to write YAML file {yaml_file_path}: {e}")

    def _update_policy_hashes(
        self,
        gitops_software_dir: str,
        software_name: str,
        new_hash: str,
    ):
        """Update policy files that install this software with the new package hash.

        Searches for policy files in lib/policies/*/install_software.hash_sha256
        and updates them to reference the new package hash.

        Edge cases handled:
        - Name variations (okta-verify-pkg -> okta-verify, sentinel-one -> sentinelone)
        - Multiple policies for same software
        - Cross-platform policy directories (macos, windows, linux)
        - Non-standard policy formats (gracefully skipped)

        Args:
            gitops_software_dir: Base GitOps directory path (e.g., /path/to/git-fleet/lib/software)
            software_name: Name of the software package (e.g., "sentinelone")
            new_hash: New SHA256 hash to write to policy files

        Raises:
            ProcessorError: If policy files cannot be updated
        """
        try:
            import yaml
        except ImportError:
            self.output("Warning: PyYAML not available, skipping policy hash updates")
            return

        # Navigate up from software dir to find policies dir
        software_path = Path(gitops_software_dir)
        if not software_path.is_absolute():
            software_path = software_path.resolve()

        # Find git root by going up until we find lib/policies
        repo_root = software_path.parent.parent  # /path/to/git-fleet
        policies_base_dir = repo_root / "lib" / "policies"

        if not policies_base_dir.exists():
            self.output(f"Policies directory not found: {policies_base_dir}")
            self.output("Skipping policy hash updates")
            return

        # Normalize software name for matching (remove -pkg suffix, normalize separators)
        normalized_name = software_name.lower()
        normalized_name = normalized_name.replace('-pkg', '').replace('_', '').replace('-', '')

        self.output(f"Looking for policies matching: {software_name} (normalized: {normalized_name})")

        # Search all platform subdirectories (macos, windows, linux)
        platform_dirs = [d for d in policies_base_dir.iterdir() if d.is_dir()]

        total_updated = 0
        total_scanned = 0

        for platform_dir in platform_dirs:
            platform_name = platform_dir.name
            policy_files = list(platform_dir.glob("*.yml"))

            if not policy_files:
                continue

            self.output(f"Scanning {len(policy_files)} policies in {platform_name}/")

            for policy_file in policy_files:
                total_scanned += 1
                try:
                    # Read policy file
                    with open(policy_file, 'r') as f:
                        policy_data = yaml.safe_load(f)

                    # Handle both list format and dict format
                    if isinstance(policy_data, list):
                        if len(policy_data) == 0:
                            continue
                        policy = policy_data[0]
                    elif isinstance(policy_data, dict):
                        policy = policy_data
                    else:
                        self.output(f"  Skipping {policy_file.name}: unexpected format")
                        continue

                    # Check if this policy has install_software
                    if 'install_software' not in policy:
                        continue

                    install_software = policy['install_software']
                    if 'hash_sha256' not in install_software:
                        continue

                    old_hash = install_software['hash_sha256']

                    # Match policy to software using multiple strategies
                    policy_name = policy.get('name', '').lower()
                    policy_file_name = policy_file.stem.lower()

                    # Normalize policy identifiers for matching
                    norm_policy_name = policy_name.replace('-', '').replace('_', '').replace(' ', '')
                    norm_file_name = policy_file_name.replace('-', '').replace('_', '')

                    # Match if software name appears in policy name OR filename
                    # Also match variations like "sentinel-one" <-> "sentinelone"
                    name_match = (
                        normalized_name in norm_policy_name or
                        normalized_name in norm_file_name or
                        software_name.lower() in policy_name or
                        software_name.lower() in policy_file_name
                    )

                    if not name_match:
                        continue

                    self.output(f"  Found matching policy: {policy_file.name}")
                    self.output(f"    Policy name: {policy.get('name', 'N/A')}")
                    self.output(f"    Old hash: {old_hash[:16]}...")
                    self.output(f"    New hash: {new_hash[:16]}...")

                    # Only update if hash actually changed
                    if old_hash == new_hash:
                        self.output(f"    ⏭️  Hash unchanged, skipping")
                        continue

                    # Update the hash
                    install_software['hash_sha256'] = new_hash

                    # Update in the original structure
                    if isinstance(policy_data, list):
                        policy['install_software'] = install_software
                        policy_data[0] = policy
                    else:
                        policy['install_software'] = install_software
                        policy_data = policy

                    # Write back to file
                    with open(policy_file, 'w') as f:
                        yaml.safe_dump(policy_data, f, default_flow_style=False, sort_keys=False)

                    self.output(f"    ✅ Updated successfully")
                    total_updated += 1

                except Exception as e:
                    self.output(f"  ⚠️  Failed to update {policy_file.name}: {e}")
                    continue

        self.output(f"Policy update summary: scanned {total_scanned} policies, updated {total_updated}")

        if total_updated == 0 and total_scanned > 0:
            self.output(f"ℹ️  No policies needed updating (this is normal if no policies install {software_name})")

    def _slugify(self, text: str) -> str:
        """Convert text to a URL-safe slug.

        Args:
            text: Text to slugify

        Returns:
            Lowercase slug with hyphens instead of spaces/special chars
        """
        # Convert to lowercase and replace non-alphanumeric with hyphens
        slug = re.sub(r"[^a-z0-9]+", "-", text.lower())
        # Remove leading/trailing hyphens
        return slug.strip("-")

    def _read_script_file(self, script_path_str: str) -> str:
        """Read script content from a file path.

        Args:
            script_path_str: Path to script file (relative or absolute)

        Returns:
            Script content as string, or empty string if file not found

        Notes:
            - If path is relative, resolves relative to recipe directory
            - Returns empty string if file doesn't exist (with warning)
        """
        if not script_path_str:
            return ""

        script_path = Path(script_path_str)

        # Resolve relative paths relative to recipe directory
        if not script_path.is_absolute():
            recipe_dir = self.env.get("RECIPE_DIR")
            if recipe_dir:
                script_path = (Path(recipe_dir) / script_path_str).resolve()
            else:
                script_path = script_path.expanduser().resolve()
        else:
            script_path = script_path.expanduser().resolve()

        if script_path.exists():
            try:
                with open(script_path, "r", encoding="utf-8") as f:
                    content = f.read()
                self.output(f"Read script file: {script_path}")
                return content
            except Exception as e:
                self.output(
                    f"Warning: Could not read script file {script_path}: {e}. Using empty script."
                )
                return ""
        else:
            self.output(
                f"Warning: Script file not found: {script_path}. Using empty script."
            )
            return ""

    def _extract_icon_from_pkg(self, pkg_path: Path) -> Path | None:
        """Extract and convert app icon from a package to PNG format.

        Args:
            pkg_path: Path to .pkg file

        Returns:
            Path to extracted PNG icon file in temp directory, or None if extraction fails

        Raises:
            ProcessorError: If icon extraction fails critically
        """
        try:
            # Create temporary directory for extraction
            temp_dir = Path(tempfile.mkdtemp(prefix="fleetimporter-icon-"))

            # First, expand the pkg to find the app bundle
            # Note: pkgutil --expand will create the target directory, so don't create it beforehand
            pkg_expand_dir = temp_dir / "pkg_contents"

            self.output(f"Expanding package to find app bundle: {pkg_path.name}")
            result = subprocess.run(
                ["pkgutil", "--expand", str(pkg_path), str(pkg_expand_dir)],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                self.output(
                    f"Warning: Could not expand package: {result.stderr}. Skipping icon extraction."
                )
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            # Find .app bundles within the expanded package
            app_bundles = list(pkg_expand_dir.rglob("*.app"))

            # If no .app bundles found directly, check for Payload archives
            if not app_bundles:
                self.output(
                    "No .app bundle found directly in package. Checking Payload archives..."
                )
                payload_files = list(pkg_expand_dir.rglob("Payload"))

                for payload_file in payload_files:
                    if payload_file.is_file():
                        self.output(f"Found Payload archive: {payload_file}")
                        # Extract Payload archive to find .app bundles
                        payload_extract_dir = temp_dir / "payload_extracted"
                        payload_extract_dir.mkdir(exist_ok=True)

                        try:
                            # Try to extract as gzip compressed tar (most common)
                            result = subprocess.run(
                                [
                                    "tar",
                                    "-xzf",
                                    str(payload_file),
                                    "-C",
                                    str(payload_extract_dir),
                                ],
                                capture_output=True,
                                text=True,
                            )

                            if result.returncode != 0:
                                # Try as bzip2 compressed tar
                                result = subprocess.run(
                                    [
                                        "tar",
                                        "-xjf",
                                        str(payload_file),
                                        "-C",
                                        str(payload_extract_dir),
                                    ],
                                    capture_output=True,
                                    text=True,
                                )

                            if result.returncode != 0:
                                # Try as uncompressed tar
                                result = subprocess.run(
                                    [
                                        "tar",
                                        "-xf",
                                        str(payload_file),
                                        "-C",
                                        str(payload_extract_dir),
                                    ],
                                    capture_output=True,
                                    text=True,
                                )

                            if result.returncode == 0:
                                # Search for .app bundles in extracted payload
                                app_bundles = list(payload_extract_dir.rglob("*.app"))
                                if app_bundles:
                                    self.output(
                                        f"Found {len(app_bundles)} .app bundle(s) in Payload archive"
                                    )
                                    break
                                else:
                                    # Some packages have app bundle contents without the .app wrapper
                                    # Look for directories containing Contents/Info.plist OR
                                    # a Contents directory with Info.plist directly inside it
                                    self.output(
                                        "No .app bundles found. Checking for unwrapped app bundle contents..."
                                    )
                                    for candidate_dir in payload_extract_dir.iterdir():
                                        if candidate_dir.is_dir():
                                            # Case 1: Directory contains Contents/Info.plist
                                            info_plist = (
                                                candidate_dir
                                                / "Contents"
                                                / "Info.plist"
                                            )
                                            if info_plist.exists():
                                                self.output(
                                                    f"Found unwrapped app bundle contents at: {candidate_dir}"
                                                )
                                                # Treat this as an app bundle (the directory containing Contents/)
                                                app_bundles.append(candidate_dir)
                                                break

                                            # Case 2: Directory IS the Contents directory with Info.plist inside
                                            if candidate_dir.name == "Contents":
                                                info_plist = (
                                                    candidate_dir / "Info.plist"
                                                )
                                                if info_plist.exists():
                                                    self.output(
                                                        f"Found Contents directory directly at: {candidate_dir}"
                                                    )
                                                    # Treat the parent directory as the app bundle
                                                    app_bundles.append(
                                                        payload_extract_dir
                                                    )
                                                    break
                                    if app_bundles:
                                        break
                        except Exception as e:
                            self.output(f"Warning: Could not extract Payload: {e}")
                            continue

            if not app_bundles:
                self.output(
                    "Warning: No .app bundle found in package or Payload archives. Skipping icon extraction."
                )
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            # Use the first app bundle found
            app_bundle = app_bundles[0]
            self.output(f"Found app bundle: {app_bundle.name}")

            # Extract icon using the _extract_icon_from_app helper
            icon_path = self._extract_icon_from_app(app_bundle, temp_dir)

            if icon_path and icon_path.exists():
                # Verify the icon meets size requirements
                icon_size_bytes = icon_path.stat().st_size
                icon_size_kb = icon_size_bytes / 1024

                if icon_size_bytes > 100 * 1024:  # 100KB limit
                    self.output(
                        f"Warning: Extracted icon is {icon_size_kb:.1f} KB, which exceeds Fleet's 100 KB limit. "
                        f"Attempting to compress..."
                    )
                    # Try to compress the icon
                    compressed_icon = self._compress_icon(icon_path, temp_dir)
                    if compressed_icon:
                        icon_path = compressed_icon
                        icon_size_kb = icon_path.stat().st_size / 1024
                        self.output(
                            f"Compressed icon to {icon_size_kb:.1f} KB successfully"
                        )
                    else:
                        self.output(
                            "Warning: Could not compress icon below 100 KB. Skipping icon upload."
                        )
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        return None

                self.output(
                    f"Successfully extracted icon: {icon_path.name} ({icon_size_kb:.1f} KB)"
                )
                return icon_path
            else:
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

        except Exception as e:
            self.output(
                f"Warning: Icon extraction failed with error: {e}. Skipping icon extraction."
            )
            if temp_dir and temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            return None

    def _extract_icon_from_app(self, app_bundle: Path, temp_dir: Path) -> Path | None:
        """Extract icon from an .app bundle and convert to PNG.

        Args:
            app_bundle: Path to .app bundle or directory containing Contents/
            temp_dir: Temporary directory for output

        Returns:
            Path to PNG icon file, or None if extraction fails
        """
        try:
            # Handle both .app bundles and unwrapped app bundle contents
            # Check if we have Contents/Info.plist directly (unwrapped bundle)
            info_plist = app_bundle / "Contents" / "Info.plist"
            if not info_plist.exists():
                # Maybe we were passed the Contents directory itself?
                # This shouldn't happen with current code, but handle it for robustness
                if (
                    app_bundle.name == "Contents"
                    and (app_bundle / "Info.plist").exists()
                ):
                    # Adjust app_bundle to be the parent directory
                    app_bundle = app_bundle.parent
                    info_plist = app_bundle / "Contents" / "Info.plist"

            if not info_plist.exists():
                self.output(f"Warning: Info.plist not found in {app_bundle.name}")
                return None

            icon_file = None

            # Try CFBundleIconFile first (legacy .icns approach)
            result = subprocess.run(
                ["plutil", "-extract", "CFBundleIconFile", "raw", str(info_plist)],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0 and result.stdout.strip():
                icon_name = result.stdout.strip()
                # Add .icns extension if not present
                if not icon_name.endswith(".icns"):
                    icon_name += ".icns"

                # Find the icon file in the app bundle
                icon_file = app_bundle / "Contents" / "Resources" / icon_name
                if not icon_file.exists():
                    # Try without extension
                    icon_name_no_ext = icon_name.replace(".icns", "")
                    icon_file = (
                        app_bundle
                        / "Contents"
                        / "Resources"
                        / f"{icon_name_no_ext}.icns"
                    )

                if icon_file.exists():
                    self.output(f"Found icon file: {icon_file.name}")
                else:
                    icon_file = None

            # If CFBundleIconFile not found, try CFBundleIconName (modern asset catalog approach)
            if not icon_file:
                result = subprocess.run(
                    ["plutil", "-extract", "CFBundleIconName", "raw", str(info_plist)],
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0 and result.stdout.strip():
                    icon_name = result.stdout.strip()
                    self.output(
                        f"App uses asset catalog (CFBundleIconName: {icon_name}). Searching for icon..."
                    )
                    resources_dir = app_bundle / "Contents" / "Resources"

                    # First try: Look for .icns files in Resources
                    icns_files = list(resources_dir.glob("*.icns"))
                    if icns_files:
                        icon_file = icns_files[0]
                        self.output(f"Found icon file: {icon_file.name}")

                    # Second try: Use macOS icon services via Python/Cocoa to extract icon
                    if not icon_file:
                        self.output(
                            "No .icns file found. Attempting to extract icon using macOS icon services..."
                        )
                        temp_png = (
                            Path(tempfile.gettempdir()) / f"{app_bundle.stem}_icon.png"
                        )
                        try:
                            # Use Python with Cocoa (PyObjC) to get the app's icon
                            # This is available in macOS's system Python
                            import Cocoa

                            workspace = Cocoa.NSWorkspace.sharedWorkspace()
                            app_icon = workspace.iconForFile_(str(app_bundle))

                            if app_icon:
                                # Get the largest representation (usually 512x512 or 1024x1024)
                                tiff_data = app_icon.TIFFRepresentation()
                                bitmap_rep = Cocoa.NSBitmapImageRep.imageRepWithData_(
                                    tiff_data
                                )

                                # Convert to PNG
                                png_data = (
                                    bitmap_rep.representationUsingType_properties_(
                                        Cocoa.NSBitmapImageFileTypePNG, None
                                    )
                                )

                                # Write PNG file
                                png_data.writeToFile_atomically_(str(temp_png), True)

                                if temp_png.exists() and temp_png.stat().st_size > 0:
                                    icon_file = temp_png
                                    self.output(
                                        f"Successfully extracted icon using macOS icon services ({temp_png.stat().st_size} bytes)"
                                    )
                                else:
                                    self.output("Warning: Icon file created but empty")
                            else:
                                self.output(
                                    "Warning: Could not get app icon from macOS"
                                )

                        except ImportError:
                            self.output(
                                "Warning: PyObjC (Cocoa module) not available. Cannot extract icon from asset catalog."
                            )
                        except Exception as e:
                            self.output(f"Warning: Error extracting icon: {str(e)}")
                            if temp_png.exists():
                                temp_png.unlink()

                    if not icon_file:
                        self.output(
                            f"Warning: Could not find or extract icon for {app_bundle.name}"
                        )
                        return None
                else:
                    self.output(
                        f"Warning: Neither CFBundleIconFile nor CFBundleIconName found in Info.plist for {app_bundle.name}"
                    )
                    return None

            if not icon_file or not icon_file.exists():
                self.output(f"Warning: Icon file not found in {app_bundle.name}")
                return None

            # Convert .icns to PNG using sips (macOS built-in tool)
            output_png = temp_dir / "icon.png"
            result = subprocess.run(
                [
                    "sips",
                    "-s",
                    "format",
                    "png",
                    str(icon_file),
                    "--out",
                    str(output_png),
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                self.output(f"Warning: Could not convert icon to PNG: {result.stderr}")
                return None

            if not output_png.exists():
                self.output("Warning: PNG conversion produced no output file")
                return None

            return output_png

        except Exception as e:
            self.output(f"Warning: Icon extraction from app bundle failed: {e}")
            return None

    def _compress_icon(self, icon_path: Path, temp_dir: Path) -> Path | None:
        """Compress a PNG icon to meet Fleet's 100 KB size limit.

        Uses sips to resize the icon progressively until it's under 100 KB.

        Args:
            icon_path: Path to original PNG icon
            temp_dir: Temporary directory for output

        Returns:
            Path to compressed PNG icon, or None if compression fails
        """
        try:
            # Try progressively smaller sizes: 512, 256, 128
            for size in [512, 256, 128]:
                compressed_path = temp_dir / f"icon_compressed_{size}.png"

                result = subprocess.run(
                    [
                        "sips",
                        "-Z",
                        str(size),
                        str(icon_path),
                        "--out",
                        str(compressed_path),
                    ],
                    capture_output=True,
                    text=True,
                )

                if result.returncode != 0:
                    continue

                if compressed_path.exists():
                    compressed_size = compressed_path.stat().st_size
                    if compressed_size <= 100 * 1024:  # Under 100 KB
                        self.output(
                            f"Compressed icon to {size}x{size}px ({compressed_size / 1024:.1f} KB)"
                        )
                        return compressed_path

            # If we get here, even 128px was too large - this is unusual
            self.output("Warning: Could not compress icon below 100 KB even at 128px")
            return None

        except Exception as e:
            self.output(f"Warning: Icon compression failed: {e}")
            return None

    def _extract_bundle_id_from_pkg(self, pkg_path: Path) -> str | None:
        """Extract bundle identifier from a package file.

        Args:
            pkg_path: Path to .pkg file

        Returns:
            Bundle identifier string, or None if extraction fails
        """
        temp_dir = None
        try:
            # Create temporary directory for extraction
            temp_dir = Path(tempfile.mkdtemp(prefix="fleetimporter-bundleid-"))

            # Expand the pkg to find the app bundle
            pkg_expand_dir = temp_dir / "pkg_contents"

            self.output(f"Extracting bundle ID from package: {pkg_path.name}")
            result = subprocess.run(
                ["pkgutil", "--expand-full", str(pkg_path), str(pkg_expand_dir)],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                self.output(
                    f"Warning: Could not expand package for bundle ID extraction: {result.stderr}"
                )
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            # Find .app bundles within the expanded package
            app_bundles = list(pkg_expand_dir.rglob("*.app"))
            if not app_bundles:
                self.output(
                    "Warning: No .app bundle found in package for bundle ID extraction."
                )
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            # Use the first app bundle found
            app_bundle = app_bundles[0]
            info_plist = app_bundle / "Contents" / "Info.plist"

            if not info_plist.exists():
                self.output(f"Warning: Info.plist not found in {app_bundle.name}")
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None

            # Extract CFBundleIdentifier using PlistBuddy
            result = subprocess.run(
                [
                    "/usr/libexec/PlistBuddy",
                    "-c",
                    "Print :CFBundleIdentifier",
                    str(info_plist),
                ],
                capture_output=True,
                text=True,
            )

            # Clean up temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)

            if result.returncode != 0:
                self.output(
                    f"Warning: Could not read CFBundleIdentifier from Info.plist: {result.stderr}"
                )
                return None

            bundle_id = result.stdout.strip()
            if not bundle_id:
                self.output("Warning: CFBundleIdentifier is empty in Info.plist")
                return None

            self.output(f"Extracted bundle identifier: {bundle_id}")
            return bundle_id

        except Exception as e:
            self.output(f"Warning: Bundle ID extraction failed with error: {e}")
            if temp_dir and temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            return None

    def _get_aws_credentials(self) -> tuple[str, str, str]:
        """Get AWS credentials from processor environment.

        Uses standard AutoPkg variable precedence: recipe arguments override
        AutoPkg preferences, which override defaults.

        Returns:
            Tuple of (access_key_id, secret_access_key, region)

        Raises:
            ProcessorError: If required credentials are missing
        """
        access_key = self.env.get("aws_access_key_id")
        secret_key = self.env.get("aws_secret_access_key")
        region = self.env.get("aws_default_region", "us-east-1")

        if not access_key or not secret_key:
            raise ProcessorError(
                "AWS credentials not found. Please provide aws_access_key_id and "
                "aws_secret_access_key as recipe arguments or set in AutoPkg preferences:\n"
                "  defaults write com.github.autopkg AWS_ACCESS_KEY_ID 'your-key'\n"
                "  defaults write com.github.autopkg AWS_SECRET_ACCESS_KEY 'your-secret'"
            )

        return access_key, secret_key, region

    def _get_s3_client(self):
        """Get configured boto3 S3 client.

        Returns:
            boto3 S3 client

        Raises:
            ProcessorError: If boto3 is not available or credentials are missing
        """
        if boto3 is None:
            raise ProcessorError(
                "boto3 is required for S3 operations but could not be imported or installed. "
                "Please install it manually: pip install boto3"
            )

        access_key, secret_key, region = self._get_aws_credentials()
        endpoint_url = self.env.get("s3_endpoint_url")

        try:
            client_args = {
                "service_name": "s3",
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
                "region_name": region,
            }

            # Add custom endpoint URL for S3-compatible storage (e.g., SeaweedFS)
            if endpoint_url:
                client_args["endpoint_url"] = endpoint_url
                self.output(f"Using custom S3 endpoint: {endpoint_url}")

            s3_client = boto3.client(**client_args)
            return s3_client
        except Exception as e:
            raise ProcessorError(f"Failed to create S3 client: {e}")

    def _upload_to_s3(
        self, bucket: str, software_title: str, version: str, pkg_path: Path
    ) -> tuple[str, bool]:
        """Upload package to S3 and return the S3 key.

        Args:
            bucket: S3 bucket name
            software_title: Software title for path construction
            version: Software version for path construction
            pkg_path: Path to the package file

        Returns:
            Tuple of (S3 key, was_uploaded: bool)
            - S3 key: path within bucket
            - was_uploaded: True if file was uploaded, False if it already existed

        Raises:
            ProcessorError: If upload fails
        """
        try:
            # Get S3 client
            s3_client = self._get_s3_client()

            # Use AutoPkg standard naming: software/Title/Title-Version.pkg
            extension = pkg_path.suffix
            s3_key = f"software/{software_title}/{software_title}-{version}{extension}"

            # Check if package already exists in S3
            try:
                head_response = s3_client.head_object(Bucket=bucket, Key=s3_key)
                # Package exists - verify it matches local file
                s3_etag = head_response.get("ETag", "").strip('"')
                s3_size = head_response.get("ContentLength", 0)
                local_size = pkg_path.stat().st_size

                if s3_size != local_size:
                    self.output(
                        f"Warning: S3 package size ({s3_size} bytes) differs from local file ({local_size} bytes). "
                        f"Re-uploading package."
                    )
                    # Continue to upload
                else:
                    self.output(
                        f"Package {software_title} {version} already exists in S3 at {s3_key}. "
                        f"Skipping upload (size: {s3_size} bytes, ETag: {s3_etag})."
                    )
                    return s3_key, False
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    self.output("Package not found in S3, proceeding with upload")
                else:
                    raise ProcessorError(f"S3 HEAD request failed: {e}")

            # Upload file to S3
            self.output(f"Uploading to s3://{bucket}/{s3_key}")
            s3_client.upload_file(
                str(pkg_path),
                bucket,
                s3_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
            self.output(f"Upload complete: s3://{bucket}/{s3_key}")
            return s3_key, True

        except NoCredentialsError:
            raise ProcessorError(
                "AWS credentials not found. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
            )
        except ClientError as e:
            raise ProcessorError(f"S3 upload failed: {e}")
        except Exception as e:
            raise ProcessorError(f"S3 upload failed: {e}")

    def _construct_cloudfront_url(self, cloudfront_domain: str, s3_key: str) -> str:
        """Construct CloudFront URL from S3 key.

        Args:
            cloudfront_domain: CloudFront distribution domain
            s3_key: S3 key (path within bucket)

        Returns:
            Full CloudFront HTTPS URL
        """
        # Remove any leading/trailing slashes from domain
        domain = cloudfront_domain.strip("/")
        # Ensure s3_key doesn't start with /
        key = s3_key.lstrip("/")
        return f"https://{domain}/{key}"

    def _cleanup_old_s3_versions(
        self,
        bucket: str,
        software_title: str,
        current_version: str,
        retention_count: int,
    ):
        """Clean up old package versions in S3, keeping the N most recent.

        Args:
            bucket: S3 bucket name
            software_title: Software title
            current_version: Current version (just uploaded)
            retention_count: Number of versions to keep (0 means no pruning)

        Safety rules:
        - Never delete the only remaining version
        - Keep the N most recent versions based on version sort
        - If retention_count is 0, skip pruning entirely
        """
        # Skip pruning if retention_count is 0
        if retention_count <= 0:
            self.output("S3 version pruning disabled (retention_count <= 0)")
            return

        try:
            # Get S3 client
            s3_client = self._get_s3_client()
            prefix = f"software/{software_title}/"

            # List all objects for this software title
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            if "Contents" not in response:
                self.output(f"No existing versions found in S3 for {software_title}")
                return

            # Extract version information from S3 keys
            # Key format: software/Title/Title-Version.pkg
            versions = {}
            for obj in response["Contents"]:
                key = obj["Key"]
                # Extract version from filename pattern: Title-Version.pkg
                # Match: software/Title/Title-Version.ext
                match = re.search(rf"{re.escape(software_title)}-([^/]+)\.", key)
                if match:
                    ver = match.group(1)
                    if ver not in versions:
                        versions[ver] = []
                    versions[ver].append(key)

            self.output(
                f"Found {len(versions)} version(s) in S3: {list(versions.keys())}"
            )

            # Safety check: never delete if only one version exists
            if len(versions) <= 1:
                self.output("Only one version exists, skipping cleanup")
                return

            # Sort versions (semantic versioning)
            try:
                from packaging import version as pkg_version

                sorted_versions = sorted(
                    versions.keys(),
                    key=lambda v: pkg_version.parse(v),
                    reverse=True,
                )
            except Exception:
                # Fallback to string sort if packaging not available
                sorted_versions = sorted(versions.keys(), reverse=True)

            # Determine which versions to delete
            versions_to_keep = sorted_versions[:retention_count]
            versions_to_delete = [
                v for v in sorted_versions if v not in versions_to_keep
            ]

            if not versions_to_delete:
                self.output(
                    f"All versions within retention limit ({retention_count}), skipping cleanup"
                )
                return

            # Delete old versions
            for ver in versions_to_delete:
                for key in versions[ver]:
                    self.output(f"Deleting old version from S3: {key}")
                    try:
                        s3_client.delete_object(Bucket=bucket, Key=key)
                    except ClientError as e:
                        self.output(f"Warning: Failed to delete {key}: {e}")

            self.output(
                f"Cleanup complete. Kept versions: {versions_to_keep}, "
                f"Deleted versions: {versions_to_delete}"
            )

        except ClientError as e:
            # Log error but don't fail the entire workflow
            self.output(f"Warning: S3 cleanup failed: {e}")
        except Exception as e:
            # Log error but don't fail the entire workflow
            self.output(f"Warning: S3 cleanup failed: {e}")

    def _copy_icon_to_gitops_repo(
        self, repo_dir: str, icon_path_str: str, software_title: str
    ) -> str:
        """Copy icon file to GitOps repository under lib/icons.

        Args:
            repo_dir: Path to Git repository
            icon_path_str: Path to icon file (relative to recipe or absolute)
            software_title: Software title for naming

        Returns:
            Relative path from software YAML to icon (e.g., ../icons/claude.png)

        Raises:
            ProcessorError: If icon file not found or invalid
        """
        # Resolve icon path relative to recipe directory first
        icon_path = Path(icon_path_str)
        if not icon_path.is_absolute():
            # Get recipe directory from AutoPkg environment
            recipe_dir = self.env.get("RECIPE_DIR")
            if recipe_dir:
                icon_path = (Path(recipe_dir) / icon_path_str).resolve()
            else:
                icon_path = icon_path.expanduser().resolve()
        else:
            icon_path = icon_path.expanduser().resolve()

        if not icon_path.exists():
            raise ProcessorError(f"Icon file not found: {icon_path}")

        # Validate icon is PNG
        if icon_path.suffix.lower() != ".png":
            self.output(
                f"Warning: Icon file {icon_path.name} is not a PNG file. Fleet requires PNG format."
            )

        # Check file size (must be <= 100KB)
        icon_size_bytes = icon_path.stat().st_size
        icon_size_kb = icon_size_bytes / 1024
        if icon_size_bytes > 100 * 1024:  # 100KB in bytes
            raise ProcessorError(
                f"Icon file {icon_path.name} is too large ({icon_size_kb:.1f} KB). "
                f"Maximum allowed size is 100 KB. Please use a smaller icon file."
            )

        # Create icons directory in GitOps repo
        icons_dir = Path(repo_dir) / "lib" / "icons"
        icons_dir.mkdir(parents=True, exist_ok=True)

        # Use slugified software title for icon filename
        slug = self._slugify(software_title)
        icon_filename = f"{slug}.png"
        dest_icon_path = icons_dir / icon_filename

        # Copy icon to GitOps repo
        self.output(f"Copying icon to GitOps repo: lib/icons/{icon_filename}")
        shutil.copy2(icon_path, dest_icon_path)

        # Return relative path from lib/macos/software to lib/icons
        # From lib/macos/software/package.yml to lib/icons/icon.png = ../../icons/icon.png
        return f"../../icons/{icon_filename}"

    def _clone_gitops_repo(self, repo_url: str, github_token: str) -> str:
        """Clone GitOps repository to a temporary directory.

        Args:
            repo_url: Git repository URL
            github_token: GitHub personal access token

        Returns:
            Path to temporary directory containing cloned repo

        Raises:
            ProcessorError: If clone fails
        """
        temp_dir = tempfile.mkdtemp(prefix="fleetimporter-gitops-")
        askpass_script = None

        try:
            # Create a temporary GIT_ASKPASS script to provide credentials securely
            # This avoids embedding tokens in URLs where they could be logged
            askpass_fd, askpass_script = tempfile.mkstemp(
                prefix="git-askpass-", suffix=".sh", text=True
            )
            os.write(askpass_fd, f'#!/bin/sh\necho "{github_token}"\n'.encode())
            os.close(askpass_fd)
            os.chmod(askpass_script, 0o700)

            # Set up minimal environment for git clone
            git_env = {
                "GIT_ASKPASS": askpass_script,
                "GIT_TERMINAL_PROMPT": "0",
                "PATH": os.environ.get("PATH", ""),
                "HOME": os.environ.get("HOME", ""),
            }

            # Clone repository using GIT_ASKPASS for authentication
            subprocess.run(
                ["git", "clone", repo_url, temp_dir],
                check=True,
                capture_output=True,
                text=True,
                env=git_env,
            )
            return temp_dir
        except subprocess.CalledProcessError as e:
            # Clean up temp dir on failure
            if Path(temp_dir).exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise ProcessorError(
                f"Failed to clone GitOps repository: {e.stderr or e.stdout}"
            )
        finally:
            # Clean up the askpass script
            if askpass_script and os.path.exists(askpass_script):
                try:
                    os.unlink(askpass_script)
                except Exception:
                    pass  # Best effort cleanup

    def _read_yaml(self, yaml_path: Path) -> dict:
        """Read and parse YAML file.

        Args:
            yaml_path: Path to YAML file

        Returns:
            Parsed YAML data as dict

        Raises:
            ProcessorError: If file cannot be read or parsed
        """
        try:
            if not yaml_path.exists():
                # Return empty structure if file doesn't exist
                return {"software": []}
            with open(yaml_path, "r") as f:
                data = yaml.safe_load(f) or {}
                # Ensure software array exists
                if "software" not in data:
                    data["software"] = []
                return data
        except (yaml.YAMLError, IOError) as e:
            raise ProcessorError(f"Failed to read YAML file {yaml_path}: {e}")

    def _write_yaml(self, yaml_path: Path, data: dict):
        """Write data to YAML file.

        Args:
            yaml_path: Path to YAML file
            data: Data to write

        Raises:
            ProcessorError: If file cannot be written
        """
        try:
            # Ensure parent directory exists
            yaml_path.parent.mkdir(parents=True, exist_ok=True)
            with open(yaml_path, "w") as f:
                yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False, indent=2)
        except (yaml.YAMLError, IOError) as e:
            raise ProcessorError(f"Failed to write YAML file {yaml_path}: {e}")

    def _create_software_package_yaml(
        self,
        repo_dir: str,
        software_dir: str,
        software_title: str,
        cloudfront_url: str,
        hash_sha256: str,
        install_script: str,
        uninstall_script: str,
        pre_install_query: str,
        post_install_script: str,
        icon_path: str = None,
        display_name: str = "",
    ) -> str:
        """Create software package YAML file in lib/ directory.

        Args:
            repo_dir: Path to Git repository
            software_dir: Directory for software YAMLs (e.g., lib/macos/software)
            software_title: Software title
            cloudfront_url: CloudFront URL for package
            hash_sha256: SHA-256 hash of package
            install_script: Custom install script
            uninstall_script: Custom uninstall script
            pre_install_query: Pre-install query
            post_install_script: Post-install script
            icon_path: Relative path to icon file in GitOps repo (e.g., ../icons/claude.png)
            display_name: Custom display name for the software in Fleet UI

        Returns:
            Relative path to created package YAML file (for use in team YAML)

        Raises:
            ProcessorError: If YAML creation fails
        """
        # Create slugified filename
        slug = self._slugify(software_title)
        package_filename = f"{slug}.yml"
        package_path = Path(repo_dir) / software_dir / package_filename

        # Build package entry (Fleet expects a list with single item)
        package_entry = {
            "url": cloudfront_url,
            "hash_sha256": hash_sha256,
        }

        # Add optional display name if provided
        if display_name:
            package_entry["display_name"] = display_name

        # Add optional icon path if provided
        if icon_path:
            package_entry["icon"] = {"path": icon_path}

        # Add optional script paths if provided
        if install_script:
            package_entry["install_script"] = {"path": install_script}
        if uninstall_script:
            package_entry["uninstall_script"] = {"path": uninstall_script}
        if pre_install_query:
            package_entry["pre_install_query"] = {"path": pre_install_query}
        if post_install_script:
            package_entry["post_install_script"] = {"path": post_install_script}

        # Package YAML is a list with single entry
        self._write_yaml(package_path, [package_entry])

        # Return relative path from team YAML to package YAML
        # E.g., if team YAML is teams/team-name.yml and package is lib/macos/software/chrome.yml
        # then relative path is ../lib/macos/software/chrome.yml
        return f"../{software_dir}/{package_filename}"

    def _update_team_yaml(
        self,
        team_yaml_path: Path,
        package_yaml_relative_path: str,
        software_title: str,
        self_service: bool,
        automatic_install: bool,
        labels_include_any: list,
        labels_exclude_any: list,
        categories: list,
    ):
        """Update team YAML file to include software package reference.

        Args:
            team_yaml_path: Path to team YAML file
            package_yaml_relative_path: Relative path to package YAML
            software_title: Software title (for logging)
            self_service: Self-service flag
            automatic_install: Automatic install flag (setup_experience in Fleet)
            labels_include_any: Include labels
            labels_exclude_any: Exclude labels
            categories: List of category names for grouping software

        Raises:
            ProcessorError: If YAML update fails
        """
        data = self._read_yaml(team_yaml_path)

        # Ensure software section exists
        if "software" not in data:
            data["software"] = {}
        if "packages" not in data["software"]:
            data["software"]["packages"] = []

        packages_list = data["software"]["packages"]

        # Find existing entry for this package path
        existing_entry = None
        for entry in packages_list:
            if entry.get("path") == package_yaml_relative_path:
                existing_entry = entry
                break

        # Build package reference entry
        new_entry = {
            "path": package_yaml_relative_path,
            "self_service": self_service,
        }

        # Add optional fields according to Fleet docs
        if categories:
            new_entry["categories"] = categories
        if automatic_install:
            new_entry["setup_experience"] = True
        if labels_include_any:
            new_entry["labels_include_any"] = labels_include_any
        if labels_exclude_any:
            new_entry["labels_exclude_any"] = labels_exclude_any

        if existing_entry:
            # Update existing entry
            self.output(f"Updating existing team entry for {software_title}")
            existing_entry.update(new_entry)
        else:
            # Add new entry
            self.output(f"Adding new team entry for {software_title}")
            packages_list.append(new_entry)

        data["software"]["packages"] = packages_list
        self._write_yaml(team_yaml_path, data)

    def _commit_and_push(
        self,
        repo_dir: str,
        branch_name: str,
        software_title: str,
        version: str,
        package_yaml_path: str,
        team_yaml_path: str,
        icon_path: str = None,
        policy_yaml_path: str = None,
    ):
        """Create Git branch, commit changes, and push to remote.

        Args:
            repo_dir: Path to Git repository
            branch_name: Name of branch to create
            software_title: Software title for commit message
            version: Software version for commit message
            package_yaml_path: Relative path to package YAML file
            team_yaml_path: Relative path to team YAML file
            icon_path: Optional relative path to icon file (e.g., ../../icons/claude.png)
            policy_yaml_path: Optional relative path to policy YAML file (e.g., lib/policies/chrome.yml)

        Raises:
            ProcessorError: If Git operations fail
        """
        try:
            # Use explicit allowlist of environment variables for Git operations
            # Only pass what Git actually needs, avoiding leakage of secrets
            git_env = {
                "GIT_TERMINAL_PROMPT": "0",
                "PATH": os.environ.get("PATH", ""),
                "HOME": os.environ.get("HOME", ""),
            }

            # Create and checkout new branch
            subprocess.run(
                ["git", "checkout", "-b", branch_name],
                cwd=repo_dir,
                check=True,
                capture_output=True,
                text=True,
                env=git_env,
            )

            # Stage YAML files and icon
            # Convert relative paths (with ../) to paths relative to repo root
            # package_yaml_path is like ../lib/macos/software/chrome.yml
            # team_yaml_path is like Path object to teams/team-name.yml
            pkg_file = package_yaml_path.replace("../", "")
            team_file = str(team_yaml_path.relative_to(repo_dir))

            files_to_add = [pkg_file, team_file]

            # Add icon file if provided
            if icon_path:
                # icon_path is like ../../icons/claude.png, need to convert to lib/icons/claude.png
                icon_file = icon_path.replace("../../", "lib/")
                files_to_add.append(icon_file)

            # Add policy file if provided
            if policy_yaml_path:
                # policy_yaml_path is already relative to repo root (e.g., lib/policies/chrome.yml)
                files_to_add.append(policy_yaml_path)

            subprocess.run(
                ["git", "add"] + files_to_add,
                cwd=repo_dir,
                check=True,
                capture_output=True,
                text=True,
                env=git_env,
            )

            # Commit
            commit_msg = f"Add {software_title} {version}"
            subprocess.run(
                ["git", "commit", "-m", commit_msg],
                cwd=repo_dir,
                check=True,
                capture_output=True,
                text=True,
                env=git_env,
            )

            # Push to remote
            subprocess.run(
                ["git", "push", "origin", branch_name],
                cwd=repo_dir,
                check=True,
                capture_output=True,
                text=True,
                env=git_env,
            )
        except subprocess.CalledProcessError as e:
            raise ProcessorError(f"Git operation failed: {e.stderr or e.stdout}")

    def _create_pull_request(
        self,
        repo_url: str,
        github_token: str,
        branch_name: str,
        software_title: str,
        version: str,
    ) -> str:
        """Create a pull request using GitHub API.

        Args:
            repo_url: Git repository URL
            github_token: GitHub personal access token
            branch_name: Name of branch to create PR from
            software_title: Software title for PR title
            version: Software version for PR title

        Returns:
            URL of created pull request

        Raises:
            ProcessorError: If PR creation fails
        """
        # Parse repository owner and name from URL
        # Expected format: https://github.com/owner/repo.git
        match = re.search(r"github\.com[:/]([^/]+)/([^/\.]+)", repo_url)
        if not match:
            raise ProcessorError(
                f"Could not parse GitHub repository from URL: {repo_url}"
            )

        owner = match.group(1)
        repo = match.group(2)

        # Construct PR details
        pr_title = f"Add {software_title} {version}"
        pr_body = f"""
## AutoPkg Package Upload

This PR adds a new version of {software_title}.

- **Version**: {version}
- **Source**: AutoPkg FleetImporter
- **Branch**: `{branch_name}`

### Changes
- Updated software definition in GitOps YAML
- Package uploaded to S3 and available via CloudFront

This PR was automatically generated by the FleetImporter AutoPkg processor.
""".strip()

        # Create PR using GitHub API
        api_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
        }
        data = {
            "title": pr_title,
            "body": pr_body,
            "head": branch_name,
            "base": "main",  # TODO: Make this configurable
        }

        try:
            req = urllib.request.Request(
                api_url,
                data=json.dumps(data).encode(),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(
                req, timeout=30, context=self._get_ssl_context()
            ) as resp:
                if resp.getcode() in (200, 201):
                    response_data = json.loads(resp.read().decode())
                    pr_url = response_data.get("html_url")
                    return pr_url
                else:
                    raise ProcessorError(
                        f"GitHub API returned unexpected status: {resp.getcode()}"
                    )
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            raise ProcessorError(
                f"Failed to create pull request: {e.code} {error_body}"
            )
        except urllib.error.URLError as e:
            raise ProcessorError(f"Failed to connect to GitHub API: {e}")

    def _calculate_file_sha256(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of a file.

        Args:
            file_path: Path to the file to hash

        Returns:
            Lowercase hexadecimal SHA-256 hash string
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def _calculate_s3_file_sha256(self, bucket: str, s3_key: str) -> str:
        """Calculate SHA-256 hash of a file in S3 by downloading it.

        Args:
            bucket: S3 bucket name
            s3_key: S3 key (path within bucket)

        Returns:
            Lowercase hexadecimal SHA-256 hash string

        Raises:
            ProcessorError: If download fails
        """
        try:
            s3_client = self._get_s3_client()

            # Download file in chunks and calculate hash
            sha256_hash = hashlib.sha256()
            response = s3_client.get_object(Bucket=bucket, Key=s3_key)

            # Read body in chunks
            for chunk in iter(lambda: response["Body"].read(8192), b""):
                sha256_hash.update(chunk)

            return sha256_hash.hexdigest()
        except ClientError as e:
            raise ProcessorError(
                f"Failed to download S3 file for hash calculation: {e}"
            )
        except Exception as e:
            raise ProcessorError(f"Failed to calculate S3 file hash: {e}")

    def _is_fleet_minimum_supported(self, fleet_version: str) -> bool:
        """Check if Fleet version meets minimum requirements."""
        try:
            # Parse version string like "4.70.0" or "4.70.0-dev"
            version_parts = fleet_version.split("-")[0].split(".")
            major = int(version_parts[0])
            minor = int(version_parts[1])
            patch = int(version_parts[2]) if len(version_parts) > 2 else 0

            # Parse minimum version from constant
            min_parts = FLEET_MINIMUM_VERSION.split(".")
            min_major = int(min_parts[0])
            min_minor = int(min_parts[1])
            min_patch = int(min_parts[2]) if len(min_parts) > 2 else 0

            # Check if >= minimum version
            if major > min_major:
                return True
            elif major == min_major and minor > min_minor:
                return True
            elif major == min_major and minor == min_minor and patch >= min_patch:
                return True
            return False
        except (ValueError, IndexError):
            # If we can't parse the version, assume it's supported to avoid blocking
            return True

    def _check_existing_package(
        self,
        fleet_api_base: str,
        fleet_token: str,
        team_id: int,
        software_title: str,
        version: str,
    ) -> dict | None:
        """Query Fleet API to check if a package version already exists.

        Returns a dict with package info if it exists, None otherwise.
        The dict includes: version, hash_sha256 if the version matches.

        The API response includes a versions array with all uploaded versions.
        We check if our version exists in that array.
        """
        try:
            # Search for the software title
            query_param = urllib.parse.quote(software_title)
            search_url = f"{fleet_api_base}/api/v1/fleet/software/titles?available_for_install=true&team_id={team_id}&query={query_param}"
            headers = {
                "Authorization": f"Bearer {fleet_token}",
                "Accept": "application/json",
            }
            req = urllib.request.Request(search_url, headers=headers)

            with urllib.request.urlopen(
                req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                if resp.getcode() == 200:
                    data = json.loads(resp.read().decode())
                    software_titles = data.get("software_titles", [])

                    self.output(
                        f"Found {len(software_titles)} software title(s) matching '{software_title}'"
                    )

                    # Look for title match - try exact match first, then case-insensitive, then fuzzy
                    matching_title = None
                    for title in software_titles:
                        title_name = title.get("name", "")
                        # Exact match (preferred)
                        if title_name == software_title:
                            matching_title = title
                            self.output(
                                f"Found exact match for '{software_title}' (title_id: {title.get('id')})"
                            )
                            break
                        # Case-insensitive match as fallback
                        elif title_name.lower() == software_title.lower():
                            matching_title = title
                            self.output(
                                f"Found case-insensitive match: '{title_name}' for '{software_title}' (title_id: {title.get('id')})"
                            )
                            break

                    # If no exact match, try fuzzy matching (e.g., "Zoom" matches "zoom.us", "Caffeine" matches "Caffeine.app")
                    if not matching_title and software_titles:
                        for title in software_titles:
                            title_name = title.get("name", "")
                            # Check if search term is contained in title name or vice versa (case-insensitive)
                            search_lower = software_title.lower()
                            title_lower = title_name.lower()
                            if (
                                search_lower in title_lower
                                or title_lower in search_lower
                            ):
                                matching_title = title
                                self.output(
                                    f"Found fuzzy match: '{title_name}' for '{software_title}' (title_id: {title.get('id')})"
                                )
                                break

                    if not matching_title:
                        # No exact or case-insensitive match - log what we found for debugging
                        if software_titles:
                            for title in software_titles:
                                self.output(
                                    f"No match found - searched for '{software_title}', found '{title.get('name', '')}'"
                                )
                        return None

                    # Check if our version exists in the versions array
                    versions = matching_title.get("versions", [])
                    if versions:
                        self.output(
                            f"Checking {len(versions)} version(s) for '{matching_title.get('name')}'"
                        )
                        for idx, ver in enumerate(versions):
                            # Debug: show what fields are in the version object
                            if isinstance(ver, dict):
                                ver_string = ver.get("version", "")
                                self.output(
                                    f"  Version {idx + 1}: '{ver_string}' (fields: {list(ver.keys())})"
                                )
                            elif isinstance(ver, str):
                                # Sometimes versions might be returned as strings directly
                                ver_string = ver
                                self.output(
                                    f"  Version {idx + 1}: '{ver_string}' (string)"
                                )
                            else:
                                self.output(
                                    f"  Version {idx + 1}: unexpected type {type(ver)}"
                                )
                                continue

                            if ver_string == version:
                                # Hash is at the title level, not version level
                                hash_sha256 = matching_title.get("hash_sha256")
                                title_id = matching_title.get("id")
                                self.output(
                                    f"Package {software_title} {version} already exists in Fleet (hash: {hash_sha256[:16] + '...' if hash_sha256 else 'none'})"
                                )
                                return {
                                    "version": ver_string,
                                    "hash_sha256": hash_sha256,
                                    "package_name": software_title,
                                    "title_id": title_id,
                                }

                    # Check the currently available software_package as well
                    sw_package = matching_title.get("software_package")
                    if sw_package:
                        pkg_version = sw_package.get("version", "")
                        if pkg_version == version:
                            hash_sha256 = matching_title.get("hash_sha256")
                            title_id = matching_title.get("id")
                            self.output(
                                f"Package {software_title} {version} already exists in Fleet as current package (hash: {hash_sha256[:16] + '...' if hash_sha256 else 'none'})"
                            )
                            return {
                                "version": pkg_version,
                                "hash_sha256": hash_sha256,
                                "package_name": sw_package.get("name", software_title),
                                "title_id": title_id,
                            }

                    # Version not found in this title
                    self.output(
                        f"Version {version} not found for '{matching_title.get('name')}'"
                    )

        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            json.JSONDecodeError,
            KeyError,
        ) as e:
            # If query fails, log and continue with upload
            self.output(f"Warning: Could not check for existing package: {e}")

        return None

    def _get_fleet_version(self, fleet_api_base: str, fleet_token: str) -> str:
        """Query Fleet API to get the server version.

        Returns the semantic version string (e.g., "4.74.0").
        If the query fails, defaults to "4.74.0" (minimum supported) assuming a modern deployment.
        """
        try:
            url = f"{fleet_api_base}/api/v1/fleet/version"
            headers = {
                "Authorization": f"Bearer {fleet_token}",
                "Accept": "application/json",
            }
            req = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(
                req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                if resp.getcode() == 200:
                    data = json.loads(resp.read().decode())
                    version = data.get("version", "")
                    if version:
                        # Parse version string like "4.74.0-dev", "4.74.0", or "0.0.0-SNAPSHOT"
                        # Extract just the semantic version part
                        base_version = version.split("-")[0]
                        # If version is 0.0.0, it's a snapshot/development build
                        # Treat it as meeting minimum version requirements
                        if base_version == "0.0.0":
                            self.output(
                                f"Detected Fleet snapshot build: {version}. "
                                "Assuming compatibility with minimum version requirements."
                            )
                            return FLEET_MINIMUM_VERSION
                        return base_version

        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            json.JSONDecodeError,
            KeyError,
        ):
            # If we can't get the version, assume minimum supported version for modern deployments
            pass

        # Default to minimum supported version if query fails (assume modern Fleet deployment)
        return FLEET_MINIMUM_VERSION

    def _fleet_delete_package(
        self,
        base_url: str,
        token: str,
        software_title: str,
        team_id: int,
    ) -> bool:
        """Delete a software package from Fleet by finding its title ID.

        Args:
            base_url: Fleet API base URL
            token: Fleet API token
            software_title: Name of the software to delete
            team_id: Team ID

        Returns:
            True if deleted successfully, False if not found or error
        """
        try:
            # First, find the software title ID by searching
            query_param = urllib.parse.quote(software_title)
            search_url = f"{base_url}/api/v1/fleet/software/titles?team_id={team_id}&query={query_param}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            req = urllib.request.Request(search_url, headers=headers)

            with urllib.request.urlopen(
                req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                if resp.getcode() == 200:
                    data = json.loads(resp.read().decode())
                    software_titles = data.get("software_titles", [])

                    # Find matching title
                    title_id = None
                    for title in software_titles:
                        if title.get("name", "").lower() == software_title.lower():
                            title_id = title.get("id")
                            break

                    if not title_id:
                        self.output(f"Software title '{software_title}' not found, cannot delete")
                        return False

                    # Now delete the package
                    delete_url = f"{base_url}/api/v1/fleet/software/titles/{title_id}/package?team_id={team_id}"
                    self.output(f"Deleting existing package (title_id: {title_id})...")

                    delete_req = urllib.request.Request(
                        delete_url, headers=headers, method="DELETE"
                    )

                    with urllib.request.urlopen(
                        delete_req,
                        timeout=FLEET_UPLOAD_TIMEOUT,
                        context=self._get_ssl_context(),
                    ) as delete_resp:
                        if delete_resp.getcode() == 204:
                            self.output("Package deleted successfully")
                            return True
                        else:
                            self.output(
                                f"Unexpected delete response: {delete_resp.getcode()}"
                            )
                            return False

        except urllib.error.HTTPError as e:
            self.output(f"Failed to delete package: {e.code} - {e.read().decode()}")
            return False
        except Exception as e:
            self.output(f"Error deleting package: {e}")
            return False

        return False

    def _fleet_upload_package(
        self,
        base_url,
        token,
        pkg_path: Path,
        software_title: str,
        version: str,
        team_id: int,
        self_service: bool,
        automatic_install: bool,
        labels_include_any: list[str],
        labels_exclude_any: list[str],
        install_script: str,
        uninstall_script: str,
        pre_install_query: str,
        post_install_script: str,
        categories: list[str],
        display_name: str = "",
    ) -> dict:
        url = f"{base_url}/api/v1/fleet/software/package"
        self.output(f"Uploading file to Fleet: {pkg_path}")
        self.output(f"DEBUG: Package path type: {type(pkg_path)}, exists: {pkg_path.exists() if isinstance(pkg_path, Path) else 'N/A'}")
        self.output(f"DEBUG: Package filename: {pkg_path.name if isinstance(pkg_path, Path) else pkg_path}")
        self.output(f"DEBUG: Team ID: {team_id}, Self-service: {self_service}")
        # API rules: only one of include/exclude
        if labels_include_any and labels_exclude_any:
            raise ProcessorError(
                "Only one of labels_include_any or labels_exclude_any may be specified."
            )

        boundary = "----FleetUploadBoundary" + hashlib.sha1(os.urandom(16)).hexdigest()
        body = io.BytesIO()

        def write_field(name: str, value: str):
            body.write(f"--{boundary}\r\n".encode())
            body.write(
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            )
            body.write(str(value).encode())
            body.write(b"\r\n")

        def write_file(name: str, filename: str, path: Path):
            body.write(f"--{boundary}\r\n".encode())
            body.write(
                f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode()
            )
            body.write(b"Content-Type: application/octet-stream\r\n\r\n")
            with open(path, "rb") as f:
                shutil.copyfileobj(f, body)
            body.write(b"\r\n")

        write_field("team_id", str(team_id))
        write_field("self_service", json.dumps(bool(self_service)).lower())
        # Note: display_name is NOT supported by POST /api/v1/fleet/software/package
        # It must be set via PATCH /api/v1/fleet/software/titles/:id/package after upload
        if install_script:
            write_field("install_script", install_script)
        if uninstall_script:
            write_field("uninstall_script", uninstall_script)
        if pre_install_query:
            write_field("pre_install_query", pre_install_query)
        if post_install_script:
            write_field("post_install_script", post_install_script)
        if automatic_install:
            write_field("automatic_install", "true")

        for label in labels_include_any:
            write_field("labels_include_any", label)
        for label in labels_exclude_any:
            write_field("labels_exclude_any", label)
        for category in categories:
            write_field("categories", category)

        write_file("software", pkg_path.name, pkg_path)
        body.write(f"--{boundary}--\r\n".encode())

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }
        req = urllib.request.Request(url, data=body.getvalue(), headers=headers)
        try:
            with urllib.request.urlopen(
                req, timeout=FLEET_UPLOAD_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                resp_body = resp.read()
                status = resp.getcode()
        except urllib.error.HTTPError as e:
            if e.code == 409:
                # Package already exists - try to delete and retry
                self.output(
                    f"Package already exists in Fleet (409 Conflict). Attempting to delete and retry..."
                )
                # Delete the old package
                if self._fleet_delete_package(base_url, token, software_title, team_id):
                    # Retry upload after successful delete
                    self.output("Retrying upload after deletion...")
                    # Recursively call with retry protection by checking for 409 again
                    req_retry = urllib.request.Request(url, data=body.getvalue(), headers=headers)
                    try:
                        with urllib.request.urlopen(
                            req_retry, timeout=FLEET_UPLOAD_TIMEOUT, context=self._get_ssl_context()
                        ) as resp_retry:
                            resp_body = resp_retry.read()
                            status = resp_retry.getcode()
                            if status != 200:
                                raise ProcessorError(f"Fleet upload retry failed: {status} {resp_body.decode()}")
                            return json.loads(resp_body or b"{}")
                    except urllib.error.HTTPError as e_retry:
                        raise ProcessorError(f"Fleet upload retry failed: {e_retry.code} {e_retry.read().decode()}")
                else:
                    # Delete failed (likely 405 Method Not Allowed or package in use)
                    # With unique identifiers (git hash), 409 is rare but can happen if:
                    # - Same git hash built multiple times
                    # - Package is in active use (bootstrap, policy)
                    # - Fleet API restrictions
                    #
                    # Instead of failing, query Fleet for existing package info and use that
                    self.output(
                        "Warning: Cannot delete existing package (likely in use or API restriction). "
                        "Querying Fleet for existing package information..."
                    )
                    self.output(
                        "Note: Package identifiers include git hash, so 409 conflicts should be rare. "
                        "If this happens frequently, check Fleet API permissions or package usage."
                    )

                    # Query Fleet to get existing package info
                    try:
                        query_param = urllib.parse.quote(software_title)
                        search_url = f"{base_url}/api/v1/fleet/software/titles?team_id={team_id}&query={query_param}"
                        search_req = urllib.request.Request(search_url, headers=headers)

                        with urllib.request.urlopen(search_req, timeout=30, context=self._get_ssl_context()) as search_resp:
                            search_data = json.loads(search_resp.read())
                            titles = search_data.get("software_titles", [])

                            # Find exact match
                            for title in titles:
                                if title.get("name", "").lower() == software_title.lower():
                                    # Get detailed info for this title
                                    title_id = title.get("id")
                                    versions = title.get("versions", [])

                                    # Use the first version's hash (should be the existing one)
                                    if versions:
                                        existing_hash = versions[0].get("sha256", "")
                                        self.output(f"Using existing package hash from Fleet: {existing_hash[:16]}...")

                                        # Return minimal response matching upload response structure
                                        return {
                                            "software_package": {
                                                "title_id": title_id,
                                                "hash_sha256": existing_hash,
                                                "version": version,
                                                "name": software_title,
                                            }
                                        }

                            self.output("Warning: Could not find existing package in Fleet search results.")

                    except Exception as query_err:
                        self.output(f"Warning: Could not query existing package: {query_err}.")

                    # Fallback: Calculate hash from local file
                    # This ensures YAML gets updated even when Fleet query fails
                    self.output("Calculating hash from local package file as fallback...")
                    try:
                        local_hash = self._calculate_file_sha256(pkg_path)
                        self.output(f"Calculated local hash: {local_hash[:16]}...")
                        return {
                            "software_package": {
                                "title_id": None,  # Unknown since upload failed
                                "hash_sha256": local_hash,
                                "version": version,
                                "name": software_title,
                            }
                        }
                    except Exception as hash_err:
                        self.output(f"Error calculating local hash: {hash_err}")
                        return {}
            raise ProcessorError(f"Fleet upload failed: {e.code} {e.read().decode()}")
        if status != 200:
            raise ProcessorError(f"Fleet upload failed: {status} {resp_body.decode()}")

        upload_response = json.loads(resp_body or b"{}")
        self.output(f"DEBUG: Upload response keys: {list(upload_response.keys())}")
        self.output(f"DEBUG: Upload response: {json.dumps(upload_response, indent=2)[:500]}...")
        return upload_response

    def _fleet_upload_bootstrap(
        self,
        base_url: str,
        token: str,
        pkg_path: Path,
        team_id: int,
    ) -> dict:
        """
        Upload a bootstrap package to Fleet.

        Bootstrap packages are uploaded to /api/v1/fleet/bootstrap and installed
        during DEP enrollment.

        Args:
            base_url: Fleet base URL
            token: Fleet API token
            pkg_path: Path to .pkg file
            team_id: Team ID (0 for "No team")

        Returns:
            dict with upload response
        """
        url = f"{base_url}/api/v1/fleet/bootstrap"
        self.output(f"Uploading bootstrap package to Fleet (team_id: {team_id}): {pkg_path}")

        boundary = "----FleetBootstrapBoundary" + hashlib.sha1(os.urandom(16)).hexdigest()
        body = io.BytesIO()

        def write_field(name: str, value: str):
            body.write(f"--{boundary}\r\n".encode())
            body.write(
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            )
            body.write(str(value).encode())
            body.write(b"\r\n")

        def write_file(name: str, filename: str, path: Path):
            body.write(f"--{boundary}\r\n".encode())
            body.write(
                f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode()
            )
            body.write(b"Content-Type: application/octet-stream\r\n\r\n")
            with open(path, "rb") as f:
                shutil.copyfileobj(f, body)
            body.write(b"\r\n")

        write_field("team_id", str(team_id))
        write_file("package", pkg_path.name, pkg_path)
        body.write(f"--{boundary}--\r\n".encode())

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }
        req = urllib.request.Request(url, data=body.getvalue(), headers=headers)

        try:
            with urllib.request.urlopen(
                req, timeout=FLEET_UPLOAD_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                resp_body = resp.read()
                status = resp.getcode()
                self.output(f"Bootstrap package uploaded successfully (HTTP {status})")
                return json.loads(resp_body or b"{}")
        except urllib.error.HTTPError as e:
            if e.code == 409:
                # Bootstrap package already exists - delete and retry
                self.output(
                    "Bootstrap package already exists (HTTP 409). Deleting existing package..."
                )
                delete_url = f"{base_url}/api/v1/fleet/mdm/apple/bootstrap/{team_id}"
                delete_req = urllib.request.Request(delete_url, headers=headers)
                delete_req.get_method = lambda: 'DELETE'

                try:
                    with urllib.request.urlopen(
                        delete_req, timeout=30, context=self._get_ssl_context()
                    ) as delete_resp:
                        self.output("Existing bootstrap package deleted successfully")
                except urllib.error.HTTPError as delete_err:
                    raise ProcessorError(
                        f"Failed to delete existing bootstrap package: {delete_err.code} {delete_err.read().decode()}"
                    )

                # Retry upload
                self.output("Retrying bootstrap package upload...")
                req = urllib.request.Request(url, data=body.getvalue(), headers=headers)
                try:
                    with urllib.request.urlopen(
                        req, timeout=FLEET_UPLOAD_TIMEOUT, context=self._get_ssl_context()
                    ) as resp:
                        resp_body = resp.read()
                        status = resp.getcode()
                        self.output(f"Bootstrap package uploaded successfully (HTTP {status})")
                        return json.loads(resp_body or b"{}")
                except urllib.error.HTTPError as retry_err:
                    raise ProcessorError(
                        f"Bootstrap upload failed after delete: {retry_err.code} {retry_err.read().decode()}"
                    )
            else:
                raise ProcessorError(f"Bootstrap upload failed: {e.code} {e.read().decode()}")

    def _fleet_upload_icon(
        self, base_url: str, token: str, title_id: int, team_id: int, icon_path: Path
    ) -> None:
        """
        Upload a software icon to Fleet with retry logic for race condition errors.

        Fleet has a known issue (#33917, #34281, #36090) where icon uploads can fail
        with "500 sql: no rows in result set" due to a race condition in activity
        logging. The icon usually uploads successfully despite the error, but the
        activity feed entry fails. Retrying after a brief delay typically succeeds.

        Args:
            base_url: Fleet base URL
            token: Fleet API token
            title_id: Software title ID from package upload
            team_id: Team ID for the icon
            icon_path: Path to PNG icon file (square, 120x120 to 1024x1024 px)
        """
        url = (
            f"{base_url}/api/v1/fleet/software/titles/{title_id}/icon?team_id={team_id}"
        )
        self.output(f"Uploading icon to Fleet: {icon_path}")

        # Validate icon file exists and is PNG
        if not icon_path.exists():
            raise ProcessorError(f"Icon file not found: {icon_path}")

        # Check file extension
        if icon_path.suffix.lower() != ".png":
            self.output(
                f"Warning: Icon file {icon_path.name} is not a PNG file. Fleet requires PNG format."
            )

        # Check file size (must be <= 100KB)
        icon_size_bytes = icon_path.stat().st_size
        icon_size_kb = icon_size_bytes / 1024
        if icon_size_bytes > 100 * 1024:  # 100KB in bytes
            raise ProcessorError(
                f"Icon file {icon_path.name} is too large ({icon_size_kb:.1f} KB). "
                f"Maximum allowed size is 100 KB. Please use a smaller icon file."
            )

        boundary = (
            "----FleetIconUploadBoundary" + hashlib.sha1(os.urandom(16)).hexdigest()
        )
        body = io.BytesIO()

        # Write the icon file
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            f'Content-Disposition: form-data; name="icon"; filename="{icon_path.name}"\r\n'.encode()
        )
        body.write(b"Content-Type: image/png\r\n\r\n")
        with open(icon_path, "rb") as f:
            shutil.copyfileobj(f, body)
        body.write(b"\r\n")
        body.write(f"--{boundary}--\r\n".encode())

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }

        # Retry logic to work around Fleet race condition bug
        max_retries = 3
        retry_delays = [2, 4, 8]  # Exponential backoff: 2s, 4s, 8s

        last_error = None
        for attempt in range(max_retries):
            if attempt > 0:
                delay = retry_delays[attempt - 1]
                self.output(
                    f"Retrying icon upload after {delay}s delay (attempt {attempt + 1}/{max_retries})..."
                )
                time.sleep(delay)

            req = urllib.request.Request(
                url, data=body.getvalue(), headers=headers, method="PUT"
            )

            try:
                with urllib.request.urlopen(
                    req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
                ) as resp:
                    status = resp.getcode()

                if status != 200:
                    last_error = f"Fleet icon upload failed with status: {status}"
                    continue

                self.output(
                    f"Icon uploaded successfully for software title ID: {title_id}"
                )
                return  # Success!

            except urllib.error.HTTPError as e:
                error_body = e.read().decode()
                # Check if this is the known race condition error
                if e.code == 500 and "sql: no rows in result set" in error_body:
                    last_error = f"Fleet race condition error (known bug): {error_body}"
                    self.output(
                        f"Encountered Fleet race condition bug on attempt {attempt + 1}/{max_retries}"
                    )
                    continue  # Retry
                else:
                    # Different error - don't retry
                    raise ProcessorError(
                        f"Fleet icon upload failed: {e.code} {error_body}"
                    )

        # All retries exhausted
        raise ProcessorError(
            f"Fleet icon upload failed after {max_retries} attempts. Last error: {last_error}"
        )

    def _fleet_update_display_name(
        self, base_url: str, token: str, title_id: int, team_id: int, display_name: str
    ) -> None:
        """
        Update the display name for a software title in Fleet.

        The POST /api/v1/fleet/software/package endpoint does not accept display_name,
        so we must use PATCH /api/v1/fleet/software/titles/:id/package to set it
        after the initial upload. This fixes the issue where software titles show
        helper process names instead of friendly names.

        Args:
            base_url: Fleet base URL
            token: Fleet API token
            title_id: Software title ID from package upload
            team_id: Team ID for the software
            display_name: Human-readable display name (e.g., "Claude" instead of "Claude Helper (Renderer)")
        """
        if not display_name or not display_name.strip():
            return  # Nothing to update

        url = f"{base_url}/api/v1/fleet/software/titles/{title_id}/package"
        self.output(f"Updating display name to: {display_name}")

        # Build multipart form data for PATCH request
        boundary = (
            "----FleetDisplayNameUpdateBoundary"
            + hashlib.sha1(os.urandom(16)).hexdigest()
        )
        body = io.BytesIO()

        def write_field(name: str, value: str):
            body.write(f"--{boundary}\r\n".encode())
            body.write(
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            )
            body.write(str(value).encode())
            body.write(b"\r\n")

        write_field("team_id", str(team_id))
        write_field("display_name", display_name)
        body.write(f"--{boundary}--\r\n".encode())

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }

        # Use PATCH method via Request with method override
        req = urllib.request.Request(
            url, data=body.getvalue(), headers=headers, method="PATCH"
        )

        try:
            with urllib.request.urlopen(
                req, timeout=FLEET_VERSION_TIMEOUT, context=self._get_ssl_context()
            ) as resp:
                status = resp.getcode()
                resp_body = resp.read().decode()
                self.output(f"DEBUG: Display name update response - Status: {status}, Body: {resp_body[:200]}")
                if status in (200, 204):
                    self.output(
                        f"Display name updated successfully for software title ID: {title_id}"
                    )
                else:
                    self.output(
                        f"Warning: Unexpected status {status} when updating display name"
                    )
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            self.output(
                f"Warning: Failed to update display name: {e.code} {error_body}. "
                "Package was uploaded successfully, but display name may show default value."
            )


if __name__ == "__main__":
    PROCESSOR = FleetImporter()
    PROCESSOR.execute_shell()
