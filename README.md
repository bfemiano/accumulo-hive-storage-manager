Query data stored in Accumulo tables directly with HiveQL. 

Pertains to patch: https://issues.apache.org/jira/browse/ACCUMULO-143

Setup:
=================

Before you can build this storage handler, checkout and build Accumulo from the latest source. <code>svn co https://svn.apache.org/repos/asf/accumulo/trunk/</code> then <code>mvn clean install</code> to get 1.6.0 installed in your local repo. This will
have to do until Accumulo 1.5+ is hosted in maven central.

Documentation:
=================

<p style='font-size: 16pt'><a href="https://github.com/bfemiano/accumulo-hive-storage-manager/wiki/Basic-Tutorial">Basic Tutorial</a></p>

<a href="http://storage-handler-docs.s3.amazonaws.com/javadocs/index.html">Javadocs</a>

<a href="https://github.com/bfemiano/accumulo-hive-storage-manager/wiki/Iterator Predicate pushdown">How Iterator Predicate pushdown works</a>

<a href="https://github.com/bfemiano/accumulo-hive-storage-manager/wiki/Required-Aux-Jars">List of required aux jars</a>

ACLED examples:
=================
 
$ACCUMULO_HOME/bin, $HADOOP_HOME/bin, $HIVE_HOME/bin on environment path. Either wget or curl installed. 

The query examples use a cleaned up version of the structured Acled Nigeria dataset. (http://www.acleddata.com/) 

1.	Navigate to [src/test/hql/acled](src/test/hql/acled) and run [ingest.sh](src/test/hql/acled/ingest.sh). The script handles creating and loading data for both the Hive and Accumulo acled tables named 'acled_nigeria' and 'acled' respectively. The ETL and data for both processes runs standalone from the  [ingest](src/test/hql/acled) directory. 

2.	See [query_acled.sql](src/test/hql/query_acled.sql) for CREATE EXTERNAL TABLE example, required aux jars, and several sample queries that utilize both the Hive and Accumulo tables. The number of hive columns in table definition must be equal to accumulo.column.mapping.

3.	Run [query_acled.sh](src/test/hql/query_acled.sh) to see the different query results. Make sure to configure the -hiveconf variables for your local Accumulo instance. 

Known limitations:
===================

* 	Requires Hive 0.10 and Accumulo 1.5+ which both use Thrift 0.9. Otherwise there are binary incompatibilities. 
*	Supported Hive column types limited to int, double, string and bigint.
*	Security Authorizations cannot be supplied from an external source. 
*	Hive column type mapping assumes value type consistency for the same qualifier across different rows. For example, r1/cf/q/v cannot hold an int while r2/cf/q/v is a double. 
*	The Hive column types must match Accumulo value types. An Accumulo value holding integer bytes should be mapped as a hive column of type int. 
* 	Does not yet support INSERT.
* 	Iterator pushdown only works on WHERE clauses consisting of purely conjunctive predicates. This is a known Hive limitation with the IndexPredicateAnalyzer.
* 	'Like' CompareOpt is not considered decomposable by the predicate analyzer. This has to do with the Hive UDFLike not extending GenericUDF. 
*	Iterator pushdown only kicks in for operators <, >, =, >=, <=, !=.  

Future enhancements: 
====================

*	Allow INSERT for field serialization to Accumulo. OutputFormat exists but is not wired to Serde or tested.  
*   Serde property for setting fixed timestamp during mutations. 
*   Allow per-qualifier type hints in the serde property, similar to the latest build of the HBase StorageHandler.  
*   Support for remaining hive primitive column types.
*   Support for complex value types (Struct, Map, Array, Union).
