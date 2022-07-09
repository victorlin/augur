"""
Create JSON files suitable for visualization within the measurements panel of Auspice.
"""
import importlib
from augur.cli.subparsers import add_subparsers

SUBCOMMAND_MODULE_NAMES = [
    "export",
    "concat",
]

SUBCOMMANDS = [(c.replace("_", "-"), importlib.import_module(f'augur.measurements.{c}')) for c in SUBCOMMAND_MODULE_NAMES]


def register_arguments(parser):
    subparsers = parser.add_subparsers(dest='subcommand')
    subparsers.required = True

    add_subparsers(SUBCOMMANDS, subparsers)


def run(args):
    return args.__command__.run(args)
