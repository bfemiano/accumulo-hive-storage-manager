SET mapred.child.java.opts=-Xmx512M;

DROP TABLE IF EXISTS acled_nigeria_cleaned;
CREATE TABLE acled_nigeria_cleaned (
    loc STRING,
    event_date STRING,
    event_type STRING,
    actor STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    source STRING,
    fatalities INT
) ROW FORMAT DELIMITED;

ADD FILE ./clean_acled_nigeria.py;
INSERT OVERWRITE TABLE acled_nigeria_cleaned
    SELECT TRANSFORM(
            if(loc != "", loc, 'Unknown'),
            event_date,
            year,
            event_type,
            actor,
            latitude,
            longitude,
            source,
            if(fatalities != "", fatalities, 'ZERO_FLAG'))
    USING 'python clean_acled_nigeria.py'
    AS (loc, event_date, event_type, actor, latitude, longitude, source, fatalities)
    FROM acled_nigeria;
