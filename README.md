Accumulo-hive-storage-manager
=============================

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143

Manage your Accumulo tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Requires Hive 0.10 and Accumulo 1.5+ which both use Thrift 0.9. Otherwise there are binary incompatibilities. 

Setup:
=================

Before you can build this storage handler, checkout and build Accumulo from the latest source. <code>svn co https://svn.apache.org/repos/asf/accumulo/trunk/</code> then <code>mvn clean install</code> to get 1.6.0 installed in your local repo. This will
have to do until Accumulo 1.5+ is hosted in maven central.

See [create.sh](src/test/hql/create.sh) for how to initialize required Accumulo parameters. 
See [accumulo_create_table.sql](src/test/hql/accumulo_create_table.sql) for CREATE EXTERNAL TABLE example and required aux jars. The number of hive columns in table definition must be equal to accumulo.column.mapping + accumulo.rowid.mapping (if present). 

TODO: 
====================

*	Simple Predicate pushdown to iterators.

*	More testing with joins.

*	Output to Accumulo from Hive. The OutputFormat has not yet been wired into the Serde for field serialization to Accumulo.

*	Statistics


