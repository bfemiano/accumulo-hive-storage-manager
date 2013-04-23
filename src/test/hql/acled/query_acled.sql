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
CREATE EXTERNAL TABLE acled(rowid STRING, lat DOUBLE, lon DOUBLE, loc STRING, src STRING, type STRING) 
STORED BY 'org.apache.accumulo.storagehandler.AccumuloStorageHandler' 
WITH SERDEPROPERTIES ('accumulo.columns.mapping' = 'rowID,cf|lat,cf|lon,cf|loc,cf|src,cf|type', 
	'accumulo.table.name' = 'acled'); 
	
select count(1) from acled where type = 'Violence against civilians' and lon > 84.4;


