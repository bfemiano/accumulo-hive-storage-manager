package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.Key;
import java.util.List;

/**
 * User: bfemiano
 * Date: 3/2/13
 * Time: 2:43 AM
 */
public class HiveAccumuloTableInputFormat extends AccumuloRowInputFormat
        implements InputFormat<Key, Value>{
    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public RecordReader<Key, Value> getRecordReader(InputSplit inputSplit,
                                                    JobConf jobConf,
                                                    Reporter reporter) throws IOException {
        String tableName = jobConf.get(AccumuloSerde.TABLE_NAME);
        String instance = jobConf.get(AccumuloSerde.INSTANCE_ID);
        String user = jobConf.get(AccumuloSerde.USER_NAME);
        String pass = jobConf.get(AccumuloSerde.USER_PASS);
        String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
        try {
            Connector connector = new ZooKeeperInstance(instance, zookeepers).getConnector(user, pass.getBytes());
            Authorizations auths = connector.securityOperations().getUserAuthorizations(user);
            setInputInfo(jobConf, user, pass.getBytes(), tableName, auths);
            String colMapping = jobConf.get(AccumuloSerde.COLUMN_MAPPINGS);
            List<String> colQualFamPairs = Lists.newArrayList();
            try {
               colQualFamPairs = AccumuloSerde.parseColumnMapping(colMapping);
            } catch (SerDeException e) {
                throw new IOException(StringUtils.stringifyException(e));
            }

            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            if (colQualFamPairs.size() < readColIds.size())
                throw new IOException("Can't read more columns than the given table contains.");

            boolean addAll = (readColIds.size() == 0);
            if (!addAll) {

            }
            for (int i = readColIds) {

            }

        } catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }

    }
}
