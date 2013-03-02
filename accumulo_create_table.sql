add jar /opt/cloud/accumulo-1.4.1/lib/accumulo-core-1.4.1.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/accumulo-server-1.4.1.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/accumulo-start-1.4.1.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/cloudtrace-1.4.1.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-collections-3.2.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-configuration-1.5.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-io-1.4.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-jci-core-1.0.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-jci-fam-1.0.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-lang-2.4.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-logging-1.0.4.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/commons-logging-api-1.0.4.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/examples-simple-1.4.1.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/libthrift-0.6.1.jar;
add jar /opt/cloud/accumulo-1.4.1/lib/log4j-1.2.16.jar;
add jar /opt/cloud/accumulo-trunk/trunk/contrib/hive-storage-handler/target/hive-storage-handler-1.4.0.jar;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(key int, value string) 
STORED BY 'org.apache.accumulo.storagehandler.AccumuloStorageHandler'
WITH SERDEPROPERTIES ("accumulo.columns.mapping" = ":key,cf1:val", "accumulo.table.name" = "xyz", "accumulo.instance.id" = "test", "accumulo.user.name" = "root", "accumulo.user.pass" = "password")
TBLPROPERTIES ("accumulo.table.name" = "xyz");
