pushd ingest
hadoop fs -mkdir /input/acled
hadoop fs -put Nigeria_ACLED.csv /input/acled
hadoop fs -mkdir /input/acled_cleaned
hadoop fs -put Nigeria_ACLED_cleaned.tsv /input/acled_cleaned
hive -f load_acled.sql
hive -f clean_and_transform_acled.sql
./load_acled_accumulo.sh
popd
