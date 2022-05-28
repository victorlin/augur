Setup

  $ pushd "$TESTDIR" > /dev/null
  $ source _setup.sh

Try to filter without any outputs.

Pandas engine
-------------

  $ ${AUGUR} filter \
  >  --sequence-index filter/data/sequence_index.tsv \
  >  --metadata filter/data/metadata.tsv \
  >  --min-length 10000 > /dev/null
  ERROR: You need to select at least one output.
  [1]

SQLite engine
-------------

  $ ${AUGUR} filter --engine sqlite \
  >  --sequence-index filter/data/sequence_index.tsv \
  >  --metadata filter/data/metadata.tsv \
  >  --min-length 10000 > /dev/null
  ERROR: You need to select at least one output.
  [1]
