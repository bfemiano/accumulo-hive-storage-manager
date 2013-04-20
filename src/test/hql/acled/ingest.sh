pushd ingest
hadoop fs -mkdir /input/acled
if command -v wget 2>/dev/null; then
	wget http://pficloud.s3.amazonaws.com/Nigeria_ACLED.csv
elif command -v curl 2>/dev/null; then
	curl -O http://pficloud.s3.amazonaws.com/Nigeria_ACLED.csv
else 
	echo "need to install curl or wget to fetch acled data"
	exit 1;
fi
hadoop fs -put Nigeria_ACLED.csv /input/acled
hadoop fs -mkdir /input/acled_cleaned
if command -v wget 2>/dev/null; then
	wget http://pficloud.s3.amazonaws.com/Nigeria_ACLED_cleaned.tsv
elif command -v curl 2>/dev/null; then
	curl -O http://pficloud.s3.amazonaws.com/Nigeria_ACLED_cleaned.tsv
else 
	echo "need to install curl or wget to fetch acled data"
	exit 1;
fi
hadoop fs -put Nigeria_ACLED_cleaned.tsv /input/acled_cleaned
hive -f load_acled.sql
hive -f clean_and_transform_acled.sql
./load_acled_accumulo.sh
popd
