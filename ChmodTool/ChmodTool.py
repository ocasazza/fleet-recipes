#!/usr/bin/env python3
"""AutoPkg processor to change file permissions."""

import os
import stat
from autopkglib import Processor, ProcessorError

__all__ = ["ChmodTool"]


class ChmodTool(Processor):
    """Changes file permissions (chmod)."""

    description = __doc__
    input_variables = {
        "file_path": {
            "required": True,
            "description": "Path to file or directory to chmod.",
        },
        "mode": {
            "required": True,
            "description": "Octal mode string (e.g., '0755', '0644').",
        },
    }
    output_variables = {}

    def main(self):
        file_path = self.env["file_path"]
        mode_str = self.env["mode"]

        if not os.path.exists(file_path):
            raise ProcessorError(f"File not found: {file_path}")

        # Convert octal string to integer
        try:
            mode = int(mode_str, 8)
        except ValueError:
            raise ProcessorError(
                f"Invalid mode: {mode_str}. Must be octal string like '0755'."
            )

        try:
            os.chmod(file_path, mode)
            self.output(f"Changed permissions of {file_path} to {mode_str}")
        except OSError as e:
            raise ProcessorError(f"Failed to chmod {file_path}: {e}")


if __name__ == "__main__":
    PROCESSOR = ChmodTool()
    PROCESSOR.execute_shell()
