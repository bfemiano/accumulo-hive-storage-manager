import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.storagehandler.AccumuloHiveRow;
import org.apache.accumulo.storagehandler.AccumuloSerde;
import org.apache.accumulo.storagehandler.HiveAccumuloTableInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.*;

/**
 * User: bfemiano
 * Date: 3/30/13
 * Time: 4:53 PM
 */
public class HiveAccumuloInputFormatTest {


    private Instance mockInstance;
    public static final String MOCK_INSTANCE_NAME = "test_instance";
    public static final String USER = "user";
    public static final String PASS = "pass";

    public static final long MAX_MEMORY= 10000L;
    public static final long MAX_LATENCY=1000L;
    public static final int MAX_WRITE_THREADS = 4;
    public static final String TEST_TABLE = "table1";
    public static final Text COLUMN_FAMILY = new Text("cf");
    public static final Text FIELD_1 = new Text("f1");
    private HiveAccumuloTableInputFormat inputformat;
    private JobConf conf;

    private static final Logger log = Logger.getLogger(HiveAccumuloInputFormatTest.class);

    @BeforeClass
    public void createMockKeyValues() {
        mockInstance =  new MockInstance(MOCK_INSTANCE_NAME);
        inputformat = new HiveAccumuloTableInputFormat();
        conf = new JobConf();
        conf.set(AccumuloSerde.TABLE_NAME, TEST_TABLE);
        conf.set(AccumuloSerde.INSTANCE_ID, MOCK_INSTANCE_NAME);
        conf.set(AccumuloSerde.USER_NAME, USER);
        conf.set(AccumuloSerde.USER_PASS, PASS);
        conf.set(AccumuloSerde.ZOOKEEPERS, "localhost:2181"); //not used for mock, but required by input format.
        conf.set(AccumuloSerde.COLUMN_MAPPINGS, "cf|f1");
        try {
            Connector con = mockInstance.getConnector(USER, PASS.getBytes());
            con.tableOperations().create(TEST_TABLE);
            con.securityOperations().changeUserAuthorizations(USER, new Authorizations("blah"));
            BatchWriter writer = con.createBatchWriter(TEST_TABLE, MAX_MEMORY, MAX_LATENCY, MAX_WRITE_THREADS);

            Mutation m1 = new Mutation(new Text("r1"));
            m1.put(COLUMN_FAMILY, FIELD_1, new Value("v1".getBytes()));
            writer.addMutation(m1);

            writer.close();
            inputformat.setInstance(mockInstance);

        } catch (AccumuloException e) {
            log.error(e);
            fail();
        } catch (AccumuloSecurityException e) {
            log.error(e);
            fail();
        } catch (TableNotFoundException e) {
            log.error(e);
            fail();
        } catch (TableExistsException e) {
            log.error(e);
            fail();
        }
    }

    @Test
    public void getHiveAccumuloRecord() {

        FileInputFormat.addInputPath(conf, new Path("unused"));
        try {
            InputSplit[] splits = inputformat.getSplits(conf, 0);
            assertEquals(splits.length, 1);
            RecordReader reader = inputformat.getRecordReader(splits[0], conf, null);
            Text rowId = new Text("r1");
            AccumuloHiveRow row = new AccumuloHiveRow();
            assertFalse(reader.next(rowId, row));
        } catch (IOException e) {
            log.error(e);
            fail();
        }
    }

}
