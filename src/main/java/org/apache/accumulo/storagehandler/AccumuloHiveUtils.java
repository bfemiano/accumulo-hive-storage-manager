package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;
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

    public static String getHiveColNameOfRowID (JobConf conf) {
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
        List<String> accumCols = AccumuloHiveUtils.parseColumnMapping(accumuloProp);
        for (int i = 0; i < accumCols.size(); i++) {
            if(containsRowID(accumCols.get(i))) {
                return i;
            }
        }
        return index;
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
