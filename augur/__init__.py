"""
The top-level augur command which dispatches to subcommands.
"""

import os
import sys
import traceback
from textwrap import dedent

from .cli.commands import make_parser
from .errors import AugurError
from .io import print_err

recursion_limit = os.environ.get("AUGUR_RECURSION_LIMIT")
if recursion_limit:
    sys.setrecursionlimit(int(recursion_limit))


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
