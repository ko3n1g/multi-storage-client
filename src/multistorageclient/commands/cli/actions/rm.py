# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys

import multistorageclient as msc

from .action import Action


class RmAction(Action):
    """Action for batch deletion of files with a given prefix."""

    def name(self) -> str:
        return "rm"

    def help(self) -> str:
        return "Delete files with a given prefix"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.formatter_class = argparse.RawDescriptionHelpFormatter

        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug output with deletion details",
        )
        parser.add_argument(
            "--dryrun",
            action="store_true",
            help="Show what would be deleted without actually deleting",
        )
        parser.add_argument(
            "--quiet",
            action="store_true",
            help="Suppress output of operations performed",
        )
        parser.add_argument(
            "--recursive",
            action="store_true",
            help="Delete all files or objects under the specified directory or prefix",
        )
        parser.add_argument(
            "--only-show-errors",
            action="store_true",
            help="Only errors and warnings are displayed. All other output is suppressed",
        )

        parser.add_argument("prefix", help="The prefix of files to delete (POSIX path or msc:// URL)")

        # Add examples as description
        parser.description = """Delete files with a given prefix. Supports:
  1. Simple prefix matching
  2. Dry run mode
  3. Directory deletion
"""

        # Add examples as epilog (appears after argument help)
        parser.epilog = """examples:
  # Delete all files with prefix
  msc rm "msc://profile/data/old_"
  msc rm "/path/to/files/temp_"

  # Delete including directories
  msc rm "msc://profile/temp/" --recursive

  # Dry run to see what would be deleted
  msc rm "msc://profile/temp/" --dryrun

  # Debug output
  msc rm "msc://profile/old/" --debug

  # Quiet mode
  msc rm "msc://profile/old/" --quiet

  # Only show errors
  msc rm "msc://profile/old/" --only-show-errors
"""

    def run(self, args: argparse.Namespace) -> int:
        if args.debug and not args.quiet and not args.only_show_errors:
            print("Arguments:", vars(args))

        try:
            if args.dryrun:
                # For dryrun, we need to list first to show what would be deleted
                if args.recursive:
                    results = msc.list(
                        url=args.prefix,
                        include_directories=False,  # list all the files that match the prefix
                    )
                else:
                    results = msc.list(
                        url=args.prefix,
                        include_directories=True,  # list only first level of files and directories
                    )
                if not args.quiet and not args.only_show_errors:
                    print("\nFiles that would be deleted:")
                    count = 0
                    for result in results:
                        count += 1
                        print(f"  {result.key}")
                    print(f"\nTotal: {count} file(s)")
                return 0

            # Perform actual deletion
            msc.delete(args.prefix, recursive=args.recursive)

            if not args.quiet and not args.only_show_errors:
                print(f"Successfully deleted files with prefix: {args.prefix}")
            return 0

        except ValueError as e:
            print(f"Error in command arguments: {str(e)}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error during deletion: {str(e)}", file=sys.stderr)
            return 1
