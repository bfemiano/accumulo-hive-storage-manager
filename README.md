Accumulo-hive-storage-manager
=============================

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143

Manage your Accumulo tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Requires Hive 0.10 and Accumulo 1.5+ which both use Thrift 0.9. Otherwise there are binary incompatibilities. 

Setup:
=================

Before you can build this storage handler, checkout and build Accumulo from the latest source. svn co https://svn.apache.org/repos/asf/accumulo/trunk/ <code>mvn clean install</code> to get 1.6.0 installed in your local repo. This will
have to do until Accumulo 1.5+ is hosted in maven central.

See (src/test/hql/create.sh) for how to initialize required Accumulo parameters. 
See (src/test/hql/accumulo_create_table.sql) for example syntax. The number of hive columns in table definition must be equal to accumulo.column.mapping + accumulo.rowid.mapping (if present). 

There are a dozen or so jars that need to be added. CREATE EXTERNAL TABLE is the only example provided. 

TODO: 
====================

*   Behaves oddly with key/value pairs with heterogeneous column families. I'm looking into this more.   

*	Output to Accumulo from Hive. The OutputFormat has not yet been wired into the Serde for field serialization to Accumulo.

*	Simple Predictate pushdown to iterators.

*	More testing with joins. 

*	Statistics


