accumulo-hive-storage-manager
=============================

Manage your Accumulo 1.4.1 tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Working commits for Hive connector to Accumulo. This will eventually be checked directly into Accumulo. 

The input format and deserialization is close to done, but not working yet. Output format hasn't even been started yet, nor the predicate pushdown. 

Avoiding using for now, if you happen to find this project before I post the patch to https://issues.apache.org/jira/browse/ACCUMULO-143
