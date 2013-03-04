package org.apache.accumulo.storagehandler;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * User: bfemiano
 * Date: 3/3/13
 * Time: 11:08 PM
 */
public class HiveAccumuloTableOutputFormat
        extends AccumuloOutputFormat
        implements HiveOutputFormat<Text, Mutation>,
        OutputFormat<Text, Mutation>
{

    private static Logger log = Logger.getLogger(HiveAccumuloTableOutputFormat.class);


    @Override
    public RecordWriter getHiveRecordWriter(JobConf jobConf,
                                            Path path,
                                            Class<? extends Writable> aClass,
                                            boolean b,
                                            Properties properties,
                                            final Progressable progressable)
            throws IOException {
        Job job = new Job(jobConf);

        final TaskAttemptContext tac =
                new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {

                    @Override
                    public void progress() {
                        progressable.progress();
                    }
                };
        try {
            String user =  AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.USER_NAME);
            byte[] pass = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.USER_PASS).getBytes();
            String table = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.TABLE_NAME);
            String instance = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.INSTANCE_ID);
            String zookeepers = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.ZOOKEEPERS);
            setOutputInfo(jobConf, user, pass, true, table);
            setZooKeeperInstance(jobConf, instance, zookeepers);
        } catch (MissingArgumentException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }

        final org.apache.hadoop.mapreduce.RecordWriter<Text, Mutation> accumuloRecordWriter = getRecordWriter(tac);
        return new RecordWriter() {
            @Override
            public void write(Writable writable) throws IOException {
                if(!(writable instanceof Mutation))
                    throw new IOException("Trying to write something other than Mutation. " + writable.getClass().getName());
                try {
                    accumuloRecordWriter.write(null, (Mutation) writable);
                } catch (InterruptedException e) {

                }
            }

            @Override
            public void close(boolean abort) throws IOException {
                try {
                    if(abort){
                        accumuloRecordWriter.close(tac);
                    }
                }  catch (InterruptedException e) {
                    throw new IOException(StringUtils.stringifyException(e));
                }
            }
        };
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Text, Mutation>
    getRecordWriter(FileSystem fileSystem,
                    JobConf jobConf,
                    String s,
                    Progressable progressable) throws IOException {
        throw new RuntimeException("Hive should not invoke this method");
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        Job job = new Job(jobConf);
        JobContext context = new JobContext(job.getConfiguration(), job.getJobID());

        try {
            String user =  AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.USER_NAME);
            byte[] pass = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.USER_PASS).getBytes();
            String table = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.TABLE_NAME);
            String instance = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.INSTANCE_ID);
            String zookeepers = AccumuloHiveUtils.getFromConf(jobConf, AccumuloSerde.ZOOKEEPERS);
            setOutputInfo(jobConf, user, pass, true, table);
            setZooKeeperInstance(jobConf, instance, zookeepers);
        } catch (MissingArgumentException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }

        checkOutputSpecs(context);
        if (log.isInfoEnabled()) {
            log.info("successfully checked output specs " + getClass().getName());
        }
    }
}
