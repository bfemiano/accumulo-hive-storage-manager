import org.apache.accumulo.storagehandler.AccumuloHiveRow;
import org.apache.accumulo.storagehandler.AccumuloSerde;
import org.apache.accumulo.storagehandler.LazyAccumuloRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import static org.testng.Assert.*;

import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * User: bfemiano
 * Date: 3/30/13
 * Time: 4:53 PM
 */
public class AccumuloSerdeTest {

    AccumuloSerde serde = new AccumuloSerde();

    private static final Logger log = Logger.getLogger(AccumuloSerdeTest.class);

    @Test
    public void initialize() {
        Properties properties = new Properties();
        Configuration conf = new Configuration();
        try {
            serde.initialize(conf, properties);
            fail("Missing columnMapping. Should have failed");
        } catch (SerDeException e) {
            assertTrue(e.getMessage().contains("null columnMapping not allowed."));
        }

        properties.setProperty(AccumuloSerde.COLUMN_MAPPINGS, "cf|f1,cf|f2,cf|f3");
        try {
            serde.initialize(conf, properties);
            assertNotNull(serde.getCachedRow());
        } catch (SerDeException e) {
            log.error(e);
            fail();
        }
    }

    @Test
    public void withRowID() {
        Properties properties = new Properties();
        Configuration conf = new Configuration();
        properties.setProperty(AccumuloSerde.COLUMN_MAPPINGS, "cf|f1,cf|f2,cf|f3");
        properties.setProperty(AccumuloSerde.ACCUMULO_KEY_MAPPING, "key");
        try {
            serde.initialize(conf, properties);
            assertNotNull(serde.getCachedRow());
        } catch (SerDeException e) {
            log.error(e);
            fail();
        }
    }

    @Test
    public void invalidColMapping() {
        Properties properties = new Properties();
        Configuration conf = new Configuration();
        properties.setProperty(AccumuloSerde.ACCUMULO_KEY_MAPPING, "blah");
        properties.setProperty(AccumuloSerde.COLUMN_MAPPINGS, "cf,cf|f2,cf|f3");

        try {
            serde.initialize(conf, properties);
            AccumuloHiveRow row = new AccumuloHiveRow("r1");
            row.add("cf", "f1", "v1".getBytes());
            row.add("cf", "f2", "v2".getBytes());
            row.add("cf", "f3", "v3".getBytes());
            Object obj = serde.deserialize(row);
            assertTrue(obj instanceof LazyAccumuloRow);
            LazyAccumuloRow lazyRow = (LazyAccumuloRow)obj;
            LazyString field1 = (LazyString)lazyRow.getField(0);
            fail("Should have throw SerdeException for Malformed famQualPair");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Malformed famQualPair"));
        } catch (SerDeException e) {
            log.error(e);
            fail();
        }
    }

    @Test
    public void deserialize() {
        Properties properties = new Properties();
        Configuration conf = new Configuration();
        properties.setProperty(AccumuloSerde.ACCUMULO_KEY_MAPPING, "blah");
        properties.setProperty(AccumuloSerde.COLUMN_MAPPINGS, "cf|f1,cf|f2,cf|f3");

        try {
            serde.initialize(conf, properties);
            serde.deserialize(new Text("fail"));
            fail("Not instance of AccumuloHiveRow");
        } catch (SerDeException e) {
            assertTrue(e.getMessage().contains("Expects AccumuloHiveRow"));
        }

        try {
            AccumuloHiveRow row = new AccumuloHiveRow("r1");
            row.add("cf", "f1", "v1".getBytes());
            row.add("cf", "f2", "v2".getBytes());
            row.add("cf", "f3", "v3".getBytes());
            Object obj = serde.deserialize(row);
            assertTrue(obj instanceof LazyAccumuloRow);
            LazyAccumuloRow lazyRow = (LazyAccumuloRow)obj;

            Object field0 = lazyRow.getField(0);
            assertTrue(field0 instanceof LazyString);
            assertEquals(field0.toString(), "v1");

            Object field1 = lazyRow.getField(1);
            assertTrue(field1 instanceof LazyString);
            assertEquals(field1.toString(), "v2");

            Object field2 = lazyRow.getField(2);
            assertTrue(field2 instanceof LazyString);
            assertEquals(field2.toString(), "v3");

            Object field3= lazyRow.getField(3);
            assertTrue(field3 instanceof LazyString);
            assertEquals(field3.toString(), "r1");

        } catch (SerDeException e){
            log.error(e);
            fail();
        }
    }
}
