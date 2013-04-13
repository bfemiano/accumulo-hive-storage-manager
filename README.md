accumulo-hive-storage-manager
=============================

Manage your Accumulo tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Requires Hive 0.10 which uses Thrift 0.9. Otherwise there are binary incompatibilities. 

Requires Accumulo 1.5 or later (with Authenticators). 

Basic rowIDs and column qualifiers are returning as Strings. The record reader logic is currently returning an AccumuloHiveRow for each key/value pair, which is
incorrect. I'll fix this tomorrow. I also need to get basic Predictate pushdown working. 

One of the tests is broken that depends on the record reader working properly. 

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143
