Setup

  $ source "$TESTDIR"/_setup.sh

Error on duplicates in metadata within same chunk.

  $ cat >metadata-duplicates.tsv <<~~
  > strain	date
  > a	2010-10-10
  > a	2010-10-10
  > b	2010-10-10
  > c	2010-10-10
  > d	2010-10-10
  > ~~
  $ ${AUGUR} filter \
  >   --metadata metadata-duplicates.tsv \
  >   --group-by year \
  >   --sequences-per-group 2 \
  >   --subsample-seed 0 \
  >   --metadata-chunk-size 10 \
  >   --output-metadata metadata-filtered.tsv > /dev/null
  ERROR: The following strains are duplicated in .* (re)
  a
  [2]
  $ cat metadata-filtered.tsv
  cat: .*: No such file or directory (re)
  [1]

Error on duplicates in metadata in separate chunks.

  $ ${AUGUR} filter \
  >   --metadata metadata-duplicates.tsv \
  >   --group-by year \
  >   --sequences-per-group 2 \
  >   --subsample-seed 0 \
  >   --metadata-chunk-size 1 \
  >   --output-metadata metadata-filtered.tsv > /dev/null
  ERROR: The following strains are duplicated in .* (re)
  a
  [2]
  $ cat metadata-filtered.tsv
  cat: .*: No such file or directory (re)
  [1]
