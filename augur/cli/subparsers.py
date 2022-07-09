import argparse
from types import ModuleType
from typing import List, Tuple
from augur.utils import first_line


def add_subparsers(subcommands: List[Tuple[str, ModuleType]], subparsers: argparse._SubParsersAction):
    """Add a subparser for each subcommand."""
    for name, module in subcommands:
        subparser = subparsers.add_parser(
            name,
            help        = first_line(module.__doc__),
            description = module.__doc__)

        # Allows us to run subcommands directly with `args.__command__.run()`
        subparser.set_defaults(__command__ = module)

        # Let the command register arguments on its subparser.
        module.register_arguments(subparser)

        # Use the same formatting class for every command for consistency.
        # Set here to avoid repeating it in every command's register_parser().
        subparser.formatter_class = argparse.ArgumentDefaultsHelpFormatter
