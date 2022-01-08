Integration tests for augur filter.

  $ pushd "$TESTDIR" > /dev/null
  $ export AUGUR="../../bin/augur"

Filter with exclude query for two regions that comprise all but one strain.
This filter should leave a single record from Oceania.
Force include one South American record by country to get two total records.

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --exclude-where "region=South America" "region=North America" "region=Southeast Asia" \
  >  --include-where "country=Ecuador" \
  >  --output-metadata "$TMP/filtered_metadata.tsv" \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*2 .* (re)
  $ cat "$TMP/filtered_strains.txt"
  EcEs062_16
  ZKC2/2016
  $ wc -l "$TMP/filtered_metadata.tsv"
  \s*3 .* (re)
  $ rm -f "$TMP/filtered_strains.txt"
  $ rm -f "$TMP/filtered_metadata.tsv"

Force include one South American record by country.

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --exclude-all \
  >  --include-where "country=Ecuador" \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*1 .* (re)
  $ rm -f "$TMP/filtered_strains.txt"

Query-exclude all but one Ecuador

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --query "country = 'Ecuador'" \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*1 .* (re)
  $ rm -f "$TMP/filtered_strains.txt"

Force-include without any exclude should return original metadata

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --include-where "country=Ecuador" \
  >  --output-metadata "$TMP/filtered_metadata.tsv" > /dev/null
  $ diff "filter/metadata.tsv" "$TMP/filtered_metadata.tsv"
  $ rm -f "$TMP/filtered_metadata.tsv"

Sequence index is missing 1 strain

  $ ${AUGUR} filter \
  >  --sequence-index filter/sequence_index.tsv \
  >  --metadata filter/metadata.tsv \
  >  --output-metadata "$TMP/filtered_metadata.tsv" > /dev/null
  $ wc -l "$TMP/filtered_metadata.tsv"
  \s*12 .* (re)

Filter using only metadata without sequence input or output and save results as filtered metadata.

  $ ${AUGUR} filter \
  >  --sequence-index filter/sequence_index.tsv \
  >  --metadata filter/metadata.tsv \
  >  --min-length 10500 \
  >  --output-metadata "$TMP/filtered_metadata.tsv" > /dev/null
  $ wc -l "$TMP/filtered_metadata.tsv"
  \s*10 .* (re)

Filter out a sequence with invalid nucleotides.

  $ ${AUGUR} filter \
  >  --sequence-index filter/sequence_index.tsv \
  >  --metadata filter/metadata.tsv \
  >  --non-nucleotide \
  >  --output-metadata "$TMP/filtered_metadata.tsv" > /dev/null
  $ wc -l "$TMP/filtered_metadata.tsv"
  \s*11 .* (re)

Filter by min date.

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --min-date 2016 \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*9 .* (re)

Filter out ambiguous days

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --exclude-ambiguous-dates-by day \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*7 .* (re)

Filter out ambiguous months

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --exclude-ambiguous-dates-by month \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*10 .* (re)

Filter out ambiguous years

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --exclude-ambiguous-dates-by year \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*11 .* (re)

Filter out any ambiguous date

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --exclude-ambiguous-dates-by any \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null
  $ wc -l "$TMP/filtered_strains.txt"
  \s*7 .* (re)

Subsample one strain per year with priorities.
There are two years (2015 and 2016) represented in the metadata.
The two highest priority strains are in these two years.

  $ ${AUGUR} filter \
  >  --metadata filter/metadata.tsv \
  >  --group-by year \
  >  --priority filter/priorities.tsv \
  >  --sequences-per-group 1 \
  >  --output-strains "$TMP/filtered_strains.txt" > /dev/null

  $ diff -u <(sort -k 2,2rn -k 1,1 filter/priorities.tsv | head -n 2 | cut -f 1) <(sort -k 1,1 "$TMP/filtered_strains.txt")
