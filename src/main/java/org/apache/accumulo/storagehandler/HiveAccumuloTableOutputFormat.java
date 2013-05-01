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
 * Required for AccumuloSerde. Not yet implemented.
 */
public class HiveAccumuloTableOutputFormat
        extends AccumuloOutputFormat
        implements HiveOutputFormat<Text, Mutation>,
        OutputFormat<Text, Mutation>
{

    @Override
    public RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Path path,
            Class<? extends Writable> aClass,
            boolean b,
            Properties properties,
            final Progressable progressable)
      throws IOException {
        throw new UnsupportedOperationException("INSERT not yet supported to Accumulo");
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
        throw new UnsupportedOperationException("INSERT not yet supported to Accumulo");
    }
}
