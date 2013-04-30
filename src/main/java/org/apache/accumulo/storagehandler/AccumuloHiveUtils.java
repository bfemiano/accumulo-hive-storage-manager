package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.storagehandler.predicate.AccumuloPredicateHandler;
import org.apache.accumulo.storagehandler.predicate.compare.PrimitiveCompare;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * User: bfemiano
 * Date: 3/4/13
 * Time: 12:44 AM
 */
public class AccumuloHiveUtils {

    public static final long WRITER_MAX_MEMORY = 1000000L; // bytes to store before sending a batch
    public static final long WRITER_TIMEOUT = 1000L; // milliseconds to wait before sending
    public static final int WRITER_NUM_THREADS = 10;
    private static final String ROWID = "rowID";
    private static final Pattern COMMA = Pattern.compile("[,]");

    public static String getFromConf(Configuration conf, String property)
            throws MissingArgumentException {
        String propValue = conf.get(property);
        if (propValue == null)
            throw new MissingArgumentException("Forgot to set " + property + " in your script");
        return propValue;
    }

    public static BatchWriter createBatchWriter(Configuration conf)
            throws IOException {
        try {
            String table  = getFromConf(conf, AccumuloSerde.TABLE_NAME);
            Connector connector = getConnector(conf);
            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxLatency(WRITER_TIMEOUT, TimeUnit.MILLISECONDS);
            config.setMaxMemory(WRITER_MAX_MEMORY);
            config.setMaxWriteThreads(WRITER_NUM_THREADS);
            return connector.createBatchWriter(table,config);
        } catch (MissingArgumentException e){
            throw new IOException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    public static List<String> parseColumnMapping(String columnMapping) {

        if(columnMapping == null)
            throw new IllegalArgumentException("null columnMapping not allowed.");
        return Lists.newArrayList(COMMA.split(columnMapping));
    }

    public static String hiveColForRowID (JobConf conf) {
        String hiveColProp = conf.get(serdeConstants.LIST_COLUMNS);
        List<String> hiveCols = AccumuloHiveUtils.parseColumnMapping(hiveColProp);
        int rowidIndex = getRowIdIndex(conf);
        if(rowidIndex >= 0) {
            return hiveCols.get(rowidIndex);
        }
        return null;
    }

    public static boolean containsRowID(String colName) {
        return colName.contains(ROWID);
    }

    public static int getRowIdIndex(JobConf conf) {
        int index = -1;
        String accumuloProp = conf.get(AccumuloSerde.COLUMN_MAPPINGS);
        if(accumuloProp == null)
            throw new IllegalArgumentException(AccumuloSerde.COLUMN_MAPPINGS + " cannot be null");
        List<String> accumCols = AccumuloHiveUtils.parseColumnMapping(accumuloProp);
        for (int i = 0; i < accumCols.size(); i++) {
            if(containsRowID(accumCols.get(i))) {
                return i;
            }
        }
        return index;
    }

    public static String hiveToAccumulo(String column, JobConf conf) {
        String accumuloProp = conf.get(AccumuloSerde.COLUMN_MAPPINGS);
        String hiveProp = conf.get(serdeConstants.LIST_COLUMNS);
        if(accumuloProp == null)
            throw new IllegalArgumentException(AccumuloSerde.COLUMN_MAPPINGS + " cannot be null");
        List<String> accumCols = parseColumnMapping(accumuloProp);
        List<String> hiveCols = parseColumnMapping(hiveProp);
        for (int i = 0; i < hiveCols.size(); i++) {
            String hiveCol = hiveCols.get(i);
            if(hiveCol.equals(column))
                return accumCols.get(i);
        }
        throw new IllegalArgumentException("column " + column + " is not mapped in the hive table definition");
    }

    public static String accumuloToHive(String column, JobConf conf) {
        String accumuloProp = conf.get(AccumuloSerde.COLUMN_MAPPINGS);
        String hiveProp = conf.get(serdeConstants.LIST_COLUMNS);
        if(accumuloProp == null)
            throw new IllegalArgumentException(AccumuloSerde.COLUMN_MAPPINGS + " cannot be null");
        List<String> accumCols = parseColumnMapping(accumuloProp);
        List<String> hiveCols = parseColumnMapping(hiveProp);
        for (int i = 0; i < accumCols.size(); i++) {
            String accuCol = accumCols.get(i);
            if(accuCol.equals(column))
                return hiveCols.get(i);
        }
        throw new IllegalArgumentException("column " + column + " is not mapped in " + AccumuloSerde.COLUMN_MAPPINGS);
    }

    public static String hiveColType(String col, JobConf conf) {
        List<String> hiveCols = parseColumnMapping(conf.get(serdeConstants.LIST_COLUMNS));
        List<String> types =  parseColumnMapping(conf.get(serdeConstants.LIST_COLUMN_TYPES));
        if(types.size() != hiveCols.size())
            throw new IllegalArgumentException("num of hive cols (" + hiveCols.size() + ") does not match " +
                    "number of types (" + types.size() + ")");
        for(int i = 0; i < hiveCols.size(); i++) {
            String hiveCOl = hiveCols.get(i);
            if(hiveCOl.equals(col))
                return types.get(i);
        }
        throw new IllegalArgumentException("not type index found for column: " + col);
    }

    public static byte[] valueAsUTF8bytes(JobConf conf, Key k, Value v)
            throws IOException {
        String cf = k.getColumnFamily().toString();
        String qual = k.getColumnQualifier().toString();
        String combined = cf + "|" + qual;
        String type = hiveColType(accumuloToHive(combined, conf), conf);
        if(type.equals("string")) {
            return v.get();
        } else if (type.equals("int")) {
            int val = ByteBuffer.wrap(v.get()).asIntBuffer().get();
            return String.valueOf(val).getBytes();
        } else if (type.equals("double")) {
            double val = ByteBuffer.wrap(v.get()).asDoubleBuffer().get();
            return String.valueOf(val).getBytes();
        } else if (type.equals("bigint")) {
            long val = ByteBuffer.wrap(v.get()).asLongBuffer().get();
            return String.valueOf(val).getBytes();
        } else {
            throw new IOException("Unsupported type: " + type + " currently only string,int,long,double supported");
        }
    }

    public static Connector getConnector(Configuration conf)
            throws IOException {
        try {
            String instance = getFromConf(conf, AccumuloSerde.INSTANCE_ID);
            String user = getFromConf(conf, AccumuloSerde.USER_NAME);
            String pass = getFromConf(conf, AccumuloSerde.USER_PASS);
            String zookeepers = getFromConf(conf, AccumuloSerde.ZOOKEEPERS);
            ZooKeeperInstance inst = new ZooKeeperInstance(instance, zookeepers);
            return  inst.getConnector(user, pass.getBytes());
        } catch (MissingArgumentException e){
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }
}
