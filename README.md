Accumulo-hive-storage-manager
=============================

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143

Manage your Accumulo tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Requires Hive 0.10 and Accumulo 1.5+ which both use Thrift 0.9. Otherwise there are binary incompatibilities. 

Setup:
=================

Before you can build this storage handler, checkout and build Accumulo from the latest source. <code>svn co https://svn.apache.org/repos/asf/accumulo/trunk/</code> then <code>mvn clean install</code> to get 1.6.0 installed in your local repo. This will
have to do until Accumulo 1.5+ is hosted in maven central.

ACLED examples:
=================

The query examples use a cleaned up version of the structured Acled Nigeria dataset. (http://www.acleddata.com/) 

1.	Navigate to and run [ingest.sh](src/test/hql/acled/ingest.sh) from relative directory. This will run standalone to both load ACLED data into HDFS for a Hive external table named 'acled_nigeria', and create a table in Accumulo named 'acled'. The script handles both ETL ingest automatically, so long as $ACCUMULO_HOME/bin/ and hive are on the environment path. 

2.	See [query_acled.sql](src/test/hql/query_acled.sql) for CREATE EXTERNAL TABLE example, required aux jars, and several sample queries that utilize both the Hive and Accumulo tables. The number of hive columns in table definition must be equal to accumulo.column.mapping.

3.	Setup Accumulo parameters and launch with [query_acled.sh](src/test/hql/query_acled.sh) 

TODO: 
====================

*	Simple Predicate pushdown to iterators.

*	More testing with joins.

*	Output to Accumulo from Hive. The OutputFormat has not yet been wired into the Serde for field serialization to Accumulo.

*	Statistics

