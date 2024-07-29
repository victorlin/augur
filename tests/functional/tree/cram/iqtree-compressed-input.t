Setup

  $ export AUGUR="${AUGUR:-$TESTDIR/../../../../bin/augur}"

Build a tree with excluded sites using a compressed input file.

  $ ${AUGUR} tree \
  >  --alignment "$TESTDIR/../data/aligned.fasta.xz" \
  >  --exclude-sites "$TESTDIR/../data/excluded_sites.txt" \
  >  --output tree_raw.nwk &> /dev/null
