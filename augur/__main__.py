"""
Stub function and module used as a setuptools entry point.
"""

import sys
import augur
from sys import argv, exit
import cProfile

# Entry point for setuptools-installed script and bin/augur dev wrapper.
def main():
    sys.stdout.reconfigure(
        # Support non-Unicode encodings by replacing Unicode characters instead of erroring.
        errors="backslashreplace",

        # Explicitly enable universal newlines mode so we do the right thing.
        newline=None,
    )
    # Apply the above to stderr as well.
    sys.stderr.reconfigure(
        errors="backslashreplace",
        newline=None,
    )

    profiler = cProfile.Profile()
    profiler.enable()
    try:
        result = augur.run( argv[1:] )
    finally:
        profiler.disable()
        profiler.dump_stats("stats.prof") # Creates file in working directory. Visualize with snakeviz or https://nejc.saje.info/pstats-viewer.html
    return result

# Run when called as `python -m augur`, here for good measure.
if __name__ == "__main__":
    exit( main() )
