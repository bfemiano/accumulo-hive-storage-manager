accumulo-hive-storage-manager
=============================

Manage your Accumulo tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Requires Hive 0.10 which uses Thrift 0.9. Otherwise there are binary incompatibilities. 

I'm close to having basic rows returning from Accumulo, but for some reason the deserialize method is not getting fed AccumuloHiveRow. My goal is to get basic predicate pushdown to work. My first commit will not have serialization, even though I've started on those classes.

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143
