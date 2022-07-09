import argparse
import importlib
import sys
from types import SimpleNamespace

from .subparsers import add_subparsers


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

    add_subparsers(COMMANDS, subparsers)
    return parser


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
