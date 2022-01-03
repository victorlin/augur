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
