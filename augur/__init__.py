"""
The top-level augur command which dispatches to subcommands.
"""

import argparse
import os
import sys
import importlib
import traceback
from textwrap import dedent
from types import SimpleNamespace

from .errors import AugurError
from .io import print_err
from .utils import first_line

recursion_limit = os.environ.get("AUGUR_RECURSION_LIMIT")
if recursion_limit:
    sys.setrecursionlimit(int(recursion_limit))

COMMAND_MODULE_NAMES = [
    "parse",
    "index",
    "filter",
    "mask",
    "align",
    "tree",
    "refine",
    "ancestral",
    "translate",
    "reconstruct_sequences",
    "clades",
    "traits",
    "sequence_traits",
    "lbi",
    "distance",
    "titers",
    "frequencies",
    "export",
    "validate",
    "version",
    "import",
    "measurements",
]

# Convert snake_case module names to kebab-case command names for CLI
COMMANDS = [(c.replace("_", "-"), importlib.import_module(f'augur.{c}')) for c in COMMAND_MODULE_NAMES]

def make_parser():
    parser = argparse.ArgumentParser(
        prog        = "augur",
        description = "Augur: A bioinformatics toolkit for phylogenetic analysis.")

    subparsers = parser.add_subparsers()

    add_default_command(parser)
    add_version_alias(parser)

    for command_name, command in COMMANDS:
        # Add a subparser for each command.
        subparser = subparsers.add_parser(
            command_name,
            help        = first_line(command.__doc__),
            description = command.__doc__)

        subparser.set_defaults(__command__ = command)

        # Let the command register arguments on its subparser.
        command.register_arguments(subparser)

        # Use the same formatting class for every command for consistency.
        # Set here to avoid repeating it in every command's register_parser().
        subparser.formatter_class = argparse.ArgumentDefaultsHelpFormatter

    return parser


def run(argv):
    args = make_parser().parse_args(argv)
    try:
        return args.__command__.run(args)
    except AugurError as e:
        print_err(f"ERROR: {e}")
        sys.exit(2)
    except RecursionError:
        print_err("FATAL: Maximum recursion depth reached. You can set the env variable AUGUR_RECURSION_LIMIT to adjust this (current limit: {})".format(sys.getrecursionlimit()))
        sys.exit(2)
    except FileNotFoundError as e:
        print_err(f"ERROR: {e.strerror}: '{e.filename}'")
        sys.exit(2)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        print_err("\n")
        print_err(dedent("""\
            An error occurred (see above) that has not been properly handled by Augur.
            To report this, please open a new issue including the original command and the error above:
                <https://github.com/nextstrain/augur/issues/new/choose>
            """))
        sys.exit(2)


def add_default_command(parser):
    """
    Sets the default command to run when none is provided.
    """
    class default_command():
        def run(args):
            parser.print_help()
            return 2

    parser.set_defaults(__command__ = default_command)


def add_version_alias(parser):
    """
    Add --version as a (hidden) alias for the version command.

    It's not uncommon to blindly run a command with --version as the sole
    argument, so its useful to make that Just Work.
    """

    class run_version_command(argparse.Action):
        def __call__(self, *args, **kwargs):
            opts = SimpleNamespace()
            sys.exit( version.run(opts) )

    return parser.add_argument(
        "--version",
        nargs  = 0,
        help   = argparse.SUPPRESS,
        action = run_version_command)
