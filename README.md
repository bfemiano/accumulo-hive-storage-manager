accumulo-hive-storage-manager
=============================

Manage your Accumulo 1.4.1 tables through the Hive metastore, and issue queries directly over the underlying column familes and qualifiers. 

Working commits for Hive connector to Accumulo. This will eventually be patched in Accumulo trunk. 

About 70% done. Still needs serialization support and predicate pushdown, plus a shit ton of testing. All the mistakes I made probably means it's more like 50%. 

Avoiding using for now, if you happen to find this project before I post the patch to https://issues.apache.org/jira/browse/ACCUMULO-143

Soon Neil will have a new shiny toy to dangle in front of Jonathan. 
