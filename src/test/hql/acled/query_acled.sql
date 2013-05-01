add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/commons-collections-3.2.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/commons-configuration-1.5.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/commons-io-1.4.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/commons-lang-2.4.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/commons-logging-1.0.4.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/commons-vfs2-2.0.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/jline-0.9.94.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/guava-13.0.1.jar;
add jar /Users/bfemiano/cloud/zookeeper-3.4.2/zookeeper-3.4.2.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/accumulo-trace-1.6.0-SNAPSHOT.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/accumulo-fate-1.6.0-SNAPSHOT.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/accumulo-core-1.6.0-SNAPSHOT.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/accumulo-server-1.6.0-SNAPSHOT.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/accumulo-start-1.6.0-SNAPSHOT.jar;
add jar /Users/bfemiano/cloud/accumulo-trunk/trunk/lib/accumulo-hive-storage-handler-1.6.0-SNAPSHOT.jar;

DROP TABLE IF EXISTS acled;
CREATE EXTERNAL TABLE acled(rowid STRING, lat DOUBLE, lon DOUBLE, loc STRING, src STRING, type STRING, fid INT, pid BIGINT) 
STORED BY 'org.apache.accumulo.storagehandler.AccumuloStorageHandler' 
WITH SERDEPROPERTIES ('accumulo.columns.mapping' = 'rowID,cf|lat,cf|lon,cf|loc,cf|src,cf|type,cf|fid,cf|pid',
	'accumulo.table.name' = 'acled'); 
		
select type,lat,lon from acled where pid = 3333 and fid = 20 and type like 'Violence%';

select pid,fid from acled where pid > 3333 and fid < 30 limit 100;

select lat,lon from acled where lat > 0.0 and lat < 10.0 and lon > 0.0 and lon < 10.0 limit 100;

select * from acled where src = 'Reuters News';

select * from acled where loc = 'Clough Creek';

DROP TABLE IF EXISTS acled2;
CREATE EXTERNAL TABLE acled2(rowid STRING, lat DOUBLE, lon DOUBLE, loc STRING, src STRING, type STRING) 
STORED BY 'org.apache.accumulo.storagehandler.AccumuloStorageHandler' 
WITH SERDEPROPERTIES ('accumulo.columns.mapping' = 'rowID,cf|lat,cf|lon,cf|loc,cf|src,cf|type',
	'accumulo.no.iterators' = 'true',
	'accumulo.table.name' = 'acled');

select count(1) from acled2 where type = 'Violence against civilians';

select a1.lat,a1.lon,a2.src from acled a1 JOIN acled2 a2 on a1.rowid = a2.rowid; 



