"""
Stub function and module used as a setuptools entry point.
"""

import augur
from sys import argv, exit
import cProfile

# Entry point for setuptools-installed script and bin/augur dev wrapper.
def main():
    pr = cProfile.Profile()
    pr.enable()
    res = augur.run( argv[1:] )
    pr.disable()
    pr.dump_stats("stats.prof") # Creates file in working directory. Visualize with https://nejc.saje.info/pstats-viewer.html
    return res

# Run when called as `python -m augur`, here for good measure.
if __name__ == "__main__":
    exit( main() )
