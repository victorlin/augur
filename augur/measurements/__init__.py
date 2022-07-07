"""
Create JSON files suitable for visualization within the measurements panel of Auspice.
"""
import argparse
import importlib
from augur.utils import first_line

SUBCOMMAND_MODULE_NAMES = [
    "export",
    "concat",
]

SUBCOMMANDS = [(c.replace("_", "-"), importlib.import_module(f'augur.measurements.{c}')) for c in SUBCOMMAND_MODULE_NAMES]


def register_arguments(parser):
    subparsers = parser.add_subparsers(dest='subcommand')
    subparsers.required = True

    for subcommand_name, subcommand in SUBCOMMANDS:
        # Add a subparser for each subcommand
        subparser = subparsers.add_parser(
            subcommand_name,
            help        = first_line(subcommand.__doc__),
            description = subcommand.__doc__
        )

        # Allows us to run subcommands directly with `args.__subcommand__.run()`
        subparser.set_defaults(__subcommand__ = subcommand)

        # Let the subcommand register arguments on its subparser
        subcommand.register_arguments(subparser)

        # Use the same formatting class for every command for consistency.
        # Set here to avoid repeating it in every command's register_parser().
        subparser.formatter_class = argparse.ArgumentDefaultsHelpFormatter


def run(args):
    return args.__subcommand__.run(args)
