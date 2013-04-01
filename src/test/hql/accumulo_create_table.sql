add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/commons-collections-3.2.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/commons-configuration-1.5.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/commons-io-1.4.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/commons-lang-2.4.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/commons-logging-1.0.4.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/commons-vfs2-2.0.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/jline-0.9.94.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/cloudtrace-1.5.0-SNAPSHOT.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/accumulo-fate-1.5.0-SNAPSHOT.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/accumulo-core-1.5.0-SNAPSHOT.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/accumulo-server-1.5.0-SNAPSHOT.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/accumulo-start-1.5.0-SNAPSHOT.jar;
add jar /opt/cloud/accumulo-1.5.0-source/accumulo-trunk-cdh3/lib/accumulo-hive-storage-handler-1.5.0-SNAPSHOT.jar;

DROP TABLE IF EXISTS test_table;
CREATE EXTERNAL TABLE test_table(blah STRING, value STRING) 
STORED BY 'org.apache.accumulo.storagehandler.AccumuloStorageHandler' 
WITH SERDEPROPERTIES ('accumulo.columns.mapping' = 'cf|f1', 
	'accumulo.key.mapping' = 'blah', 
	'accumulo.table.name' = 'foo') 