package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * User: bfemiano
 * Date: 3/2/13
 * Time: 2:43 AM
 */
public class HiveAccumuloTableInputFormat
        extends AccumuloInputFormat
        implements org.apache.hadoop.mapred.InputFormat<Text, AccumuloHiveRow> {
    private static final Pattern COLON = Pattern.compile("[:]");

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
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
            List<String> colQualFamPairs;
            try {
                colQualFamPairs = AccumuloSerde.parseColumnMapping(colMapping);
            } catch (SerDeException e) {
                throw new IOException(StringUtils.stringifyException(e));
            }

            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            if (colQualFamPairs.size() < readColIds.size())
                throw new IOException("Can't read more columns than the given table contains.");


            fetchColumns(jobConf, getPairCollection(colQualFamPairs));

            Job job = new Job(jobConf);
            JobContext context = new JobContext(job.getConfiguration(), job.getJobID());
            Path[] tablePaths = FileInputFormat.getInputPaths(context);
            List<org.apache.hadoop.mapreduce.InputSplit> splits = super.getSplits(job);
            InputSplit[] newSplits = new InputSplit[splits.size()];
            for (int j = 0; j < splits.size(); j++) {
                newSplits[i] = new AccumuloSplit((RangeInputSplit)splits.get(i), tablePaths[0]);

            }
            return newSplits;
        }  catch (AccumuloException e) {
            throw new IOException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public RecordReader<Text, AccumuloHiveRow> getRecordReader(InputSplit inputSplit,
                                                               JobConf jobConf,
                                                               final Reporter reporter) throws IOException {
        String tableName = jobConf.get(AccumuloSerde.TABLE_NAME);
        String instance = jobConf.get(AccumuloSerde.INSTANCE_ID);
        String user = jobConf.get(AccumuloSerde.USER_NAME);
        String pass = jobConf.get(AccumuloSerde.USER_PASS);
        String zookeepers = jobConf.get(AccumuloSerde.ZOOKEEPERS);
        AccumuloSplit as = (AccumuloSplit)inputSplit;
        RangeInputSplit ris = as.getSplit();
        try {
            Connector connector = new ZooKeeperInstance(instance, zookeepers).getConnector(user, pass.getBytes());
            Authorizations auths = connector.securityOperations().getUserAuthorizations(user);
            setInputInfo(jobConf, user, pass.getBytes(), tableName, auths);
            String colMapping = jobConf.get(AccumuloSerde.COLUMN_MAPPINGS);
            List<String> colQualFamPairs;
            try {
                colQualFamPairs = AccumuloSerde.parseColumnMapping(colMapping);
            } catch (SerDeException e) {
                throw new IOException(StringUtils.stringifyException(e));
            }

            List<Integer> readColIds = ColumnProjectionUtils.getReadColumnIDs(jobConf);
            if (colQualFamPairs.size() < readColIds.size())
                throw new IOException("Can't read more columns than the given table contains.");

            fetchColumns(jobConf, getPairCollection(colQualFamPairs));

            Job job = new Job(jobConf);
            TaskAttemptContext tac =
                    new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {

                        @Override
                        public void progress() {
                            reporter.progress();;
                        }
                    };

            final org.apache.hadoop.mapreduce.RecordReader
                    <Key,Value> recordReader =
                    createRecordReader(ris, tac);

            return new RecordReader<Text, AccumuloHiveRow>() {

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
                        while (key.getRow().equals(rowKey) && next) {
                            value.add(key.getColumnFamily().toString(),
                                    key.getColumnQualifier().toString(),
                                    val.get());

                            next = recordReader.nextKeyValue();
                            key = recordReader.getCurrentKey();
                            val = recordReader.getCurrentValue();
                        }
                    } catch (InterruptedException e) {
                        throw new IOException(StringUtils.stringifyException(e));
                    }

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

    private Collection<Pair<Text, Text>> getPairCollection(List<String> colQualFamPairs) {
        List<Pair<Text, Text>> pairs = Lists.newArrayList();
        for (String colQualFam : colQualFamPairs) {
            String[] qualFamPieces = COLON.split(colQualFam);
            Text fam = new Text(qualFamPieces[0]);
            Text qual = qualFamPieces.length > 1 ? new Text(qualFamPieces[1]) : null;
            pairs.add(new Pair<Text, Text>(fam, qual));
        }
        return pairs;
    }
}
