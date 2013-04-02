accumulo-hive-storage-manager
=============================

Manage your Accumulo tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Requires Hive 0.10 which uses Thrift 0.9. Otherwise there are binary incompatibilities. 

Requires Accumulo 1.5 or later (with Authenticators). 

Currently produces a lazyfield error when reading back rows. One of the tests is broken that depends on the record reader working properly, which is also the likely cause
of the lazyfield issues.

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143
