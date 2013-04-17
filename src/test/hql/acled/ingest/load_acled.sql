DROP TABLE IF EXISTS acled_nigeria;
CREATE EXTERNAL TABLE acled_nigeria (
       loc STRING,
       event_date STRING,
       year STRING,
       event_type STRING,
       actor STRING,
       latitude DOUBLE,
       longitude DOUBLE,
       source STRING,
       fatalities STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/input/acled/';

