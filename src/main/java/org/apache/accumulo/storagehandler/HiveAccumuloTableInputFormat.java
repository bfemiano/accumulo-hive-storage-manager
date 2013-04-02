package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * User: bfemiano
 * Date: 3/2/13
 * Time: 2:43 AM
 */
public class HiveAccumuloTableInputFormat
        extends AccumuloInputFormat
        implements org.apache.hadoop.mapred.InputFormat<Text, AccumuloHiveRow> {
    private static final Pattern PIPE = Pattern.compile("[|]");

    private Instance instance;

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        String tableName = jobConf.get(AccumuloSerde.TABLE_NAME);
        String id = jobConf.get(AccumuloSerde.INSTANCE_ID);
        String user = jobConf.get(AccumuloSerde.USER_NAME);
        String pass = jobConf.get(AccumuloSerde.USER_PASS);
        String key = jobConf.get(AccumuloSerde.ACCUMULO_KEY_MAPPING);
        String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
        instance = getInstance(id, zookeepers);
        Job job = new Job(jobConf);
        try {
            Connector connector =  instance.getConnector(user,  new PasswordToken(pass.getBytes()));
            String colMapping = jobConf.get(AccumuloSerde.COLUMN_MAPPINGS);
            System.out.println("col mapping: " + colMapping);
            System.out.println("table name: " + tableName);
            System.out.println("key : " + key);
            configure(job, connector);
            List<String> colQualFamPairs;
            try {
                colQualFamPairs = AccumuloSerde.parseColumnMapping(colMapping);
            } catch (SerDeException e) {
                throw new IOException(StringUtils.stringifyException(e));
            }
            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            int incForKey = key == null ? 0 : 1;
            if (colQualFamPairs.size() + incForKey < readColIds.size())
                throw new IOException("Number of colfam:qual pairs + rowkey (" + (colQualFamPairs.size() + incForKey) + ")" +
                        " numbers less than the hive table columns. (" + readColIds.size() + ") "  +
                        "Did you forget the serde property " + AccumuloSerde.ACCUMULO_KEY_MAPPING + "?");


            fetchColumns(jobConf, getPairCollection(colQualFamPairs));


            JobContext context = new JobContext(job.getConfiguration(), job.getJobID());
            Path[] tablePaths = FileInputFormat.getInputPaths(context);
            List<org.apache.hadoop.mapreduce.InputSplit> splits = super.getSplits(job);
            InputSplit[] newSplits = new InputSplit[splits.size()];
            for (int i = 0; i < splits.size(); i++) {
                RangeInputSplit ris = (RangeInputSplit)splits.get(i);
                newSplits[i] = new AccumuloSplit(ris, tablePaths[0]);

            }
            return newSplits;
        }  catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    private Instance getInstance(String id,
                                 String zookeepers) {
        if(instance != null) {
            return instance;
        } else {
            return new ZooKeeperInstance(id, zookeepers);
        }
    }

    //for testing purposes
    public void setInstance(Instance instance) {
        this.instance = instance;
    }

    @Override
    public RecordReader<Text, AccumuloHiveRow> getRecordReader(InputSplit inputSplit,
                                                               JobConf jobConf,
                                                               final Reporter reporter) throws IOException {


        String user = jobConf.get(AccumuloSerde.USER_NAME);
        String pass = jobConf.get(AccumuloSerde.USER_PASS);
        String tableName = jobConf.get(AccumuloSerde.TABLE_NAME);
        String id = jobConf.get(AccumuloSerde.INSTANCE_ID);
        String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
        instance = getInstance(id, zookeepers);
        AccumuloSplit as = (AccumuloSplit)inputSplit;
        RangeInputSplit ris = as.getSplit();
        Job job = new Job(jobConf);
        try {
            String colMapping = jobConf.get(AccumuloSerde.COLUMN_MAPPINGS);
            List<String> colQualFamPairs;
            try {
                colQualFamPairs = AccumuloSerde.parseColumnMapping(colMapping);
            } catch (SerDeException e) {
                throw new IOException(StringUtils.stringifyException(e));
            }

            String key = jobConf.get(AccumuloSerde.ACCUMULO_KEY_MAPPING);
            Connector connector = instance.getConnector(user, new PasswordToken(pass.getBytes()));
            configure(job, connector);

            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            int incForKey = key == null ? 0 : 1;
            if (colQualFamPairs.size() + incForKey < readColIds.size())
                throw new IOException("Number of colfam:qual pairs + rowkey (" + (colQualFamPairs.size() + incForKey) + ")" +
                        " numbers less than the hive table columns. (" + readColIds.size() + ") "  +
                        "Did you forget the serde property " + AccumuloSerde.ACCUMULO_KEY_MAPPING + "?");



            TaskAttemptContext tac =
                    new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {

                        @Override
                        public void progress() {
                            reporter.progress();;
                        }
                    };
            fetchColumns(job, getPairCollection(colQualFamPairs));
            final org.apache.hadoop.mapreduce.RecordReader
                    <Key,Value> recordReader =
                    createRecordReader(ris, tac);
            recordReader.initialize(ris, tac);

            return new RecordReader<Text, AccumuloHiveRow>() {

                protected Text currentK;
                protected AccumuloHiveRow currentV;

                @Override
                public void close() throws IOException {
                    recordReader.close();
                }

                @Override
                public Text createKey() {
                    return new Text();
                }

                @Override
                public AccumuloHiveRow createValue() {
                    return new AccumuloHiveRow();
                }

                @Override
                public long getPos() throws IOException {
                    return 0;
                }

                @Override
                public float getProgress() throws IOException {
                    float progress = 0.0F;

                    try {
                        progress = recordReader.getProgress();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }

                    return progress;
                }

                @Override
                public boolean next(Text rowKey, AccumuloHiveRow value) throws IOException {
                    boolean next;
                    try {
                        next = recordReader.nextKeyValue();
                        Key key = recordReader.getCurrentKey();
                        Value val = recordReader.getCurrentValue();
                        rowKey.set(key.getRow());

                        value = new AccumuloHiveRow(key.getRow().toString());
                        if(next) {
                            value.add(key.getColumnFamily().toString(),
                                    key.getColumnQualifier().toString(),
                                    val.get());

                            log.info("key.cf" + key.getColumnFamily().toString());
                            log.info("key.qf" + key.getColumnQualifier().toString());
                            log.info("getting row " + value.toString());
                        }
                    } catch (InterruptedException e) {
                        throw new IOException(StringUtils.stringifyException(e));
                    }
                    log.info("dont processing");
                    return next;
                }
            };

        } catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (InterruptedException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    private void configure(Job job, Connector connector)
            throws AccumuloSecurityException, AccumuloException {
        String instanceId = job.getConfiguration().get(AccumuloSerde.INSTANCE_ID);
        String zookeepers = job.getConfiguration().get(AccumuloSerde.ZOOKEEPERS);
        String user = job.getConfiguration().get(AccumuloSerde.USER_NAME);
        String pass = job.getConfiguration().get(AccumuloSerde.USER_PASS);
        String tableName = job.getConfiguration().get(AccumuloSerde.TABLE_NAME);
        if (instance instanceof MockInstance) {
            setMockInstance(job, instanceId);
        }  else {
            setZooKeeperInstance(job, instanceId, zookeepers);
        }
        setConnectorInfo(job, user, new PasswordToken(pass.getBytes()));
        setInputTableName(job, tableName);
        setScanAuthorizations(job, connector.securityOperations().getUserAuthorizations(user));

    }

    private boolean inputConfigured(JobConf jobConf) {
        return jobConf.getBoolean(AccumuloInputFormat.class.getSimpleName() + ".configured", false);
    }

    private boolean instanceConfigured(JobConf jobConf) {
        return jobConf.getBoolean(AccumuloInputFormat.class.getSimpleName() + ".instanceConfigured", false);
    }

    private Collection<Pair<Text, Text>> getPairCollection(List<String> colQualFamPairs) {
        List<Pair<Text, Text>> pairs = Lists.newArrayList();
        for (String colQualFam : colQualFamPairs) {
            String[] qualFamPieces = PIPE.split(colQualFam);
            Text fam = new Text(qualFamPieces[0]);
            Text qual = qualFamPieces.length > 1 ? new Text(qualFamPieces[1]) : null;
            pairs.add(new Pair<Text, Text>(fam, qual));
        }
        return pairs;
    }
}
