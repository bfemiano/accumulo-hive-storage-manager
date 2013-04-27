package org.apache.accumulo.storagehandler;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.storagehandler.predicate.PrimativeComparisonFilter;
import org.apache.accumulo.storagehandler.predicate.compare.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.datanucleus.sco.backed.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;

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
    public static final String TEST_TABLE = "table1";
    public static final Text COLUMN_FAMILY = new Text("cf");
    private static final Text NAME = new Text("name");
    private static final Text SID = new Text("sid");
    private static final Text DEGREES = new Text("dgrs");
    private static final Text MILLIS = new Text("mills");
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
        conf.set(AccumuloSerde.COLUMN_MAPPINGS, "cf|name,cf|sid,cf|dgrs,cf|mills");
        conf.set(serdeConstants.LIST_COLUMNS, "name,sid,dgrs,mills");
        conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,int,double,bigint");
        try {
            Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
            con.tableOperations().create(TEST_TABLE);
            con.securityOperations().changeUserAuthorizations(USER, new Authorizations("blah"));
            BatchWriterConfig writerConf = new BatchWriterConfig();
            BatchWriter writer = con.createBatchWriter(TEST_TABLE, writerConf);

            Mutation m1 = new Mutation(new Text("r1"));
            m1.put(COLUMN_FAMILY, NAME, new Value("brian".getBytes()));
            m1.put(COLUMN_FAMILY, SID, new Value(parseIntBytes("1")));
            m1.put(COLUMN_FAMILY, DEGREES, new Value(parseDoubleBytes("44.5")));
            m1.put(COLUMN_FAMILY, MILLIS, new Value(parseLongBytes("555")));

            Mutation m2 = new Mutation(new Text("r2"));
            m2.put(COLUMN_FAMILY, NAME, new Value("mark".getBytes()));
            m2.put(COLUMN_FAMILY, SID, new Value(parseIntBytes("2")));
            m2.put(COLUMN_FAMILY, DEGREES, new Value(parseDoubleBytes("55.5")));
            m2.put(COLUMN_FAMILY, MILLIS, new Value(parseLongBytes("666")));

            Mutation m3 = new Mutation(new Text("r3"));
            m3.put(COLUMN_FAMILY, NAME, new Value("dennis".getBytes()));
            m3.put(COLUMN_FAMILY, SID, new Value(parseIntBytes("3")));
            m3.put(COLUMN_FAMILY, DEGREES, new Value(parseDoubleBytes("65.5")));
            m3.put(COLUMN_FAMILY, MILLIS, new Value(parseLongBytes("777")));

            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);

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

    private byte[] parseIntBytes(String s) {
        int val = Integer.parseInt(s);
        byte [] valBytes = new byte[4];
        ByteBuffer.wrap(valBytes).putInt(val);
        return valBytes;
    }

    private byte[] parseLongBytes(String s) {
        long val = Long.parseLong(s);
        byte [] valBytes = new byte[8];
        ByteBuffer.wrap(valBytes).putLong(val);
        return valBytes;
    }

    private byte[] parseDoubleBytes(String s) {
        double val = Double.parseDouble(s);
        byte [] valBytes = new byte[8];
        ByteBuffer.wrap(valBytes).putDouble(val);
        return valBytes;
    }

    @Test
    public void hiveAccumuloRecord() {

        FileInputFormat.addInputPath(conf, new Path("unused"));
        try {
            InputSplit[] splits = inputformat.getSplits(conf, 0);
            assertEquals(splits.length, 1);
            RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
            Text rowId = new Text("r1");
            AccumuloHiveRow row = new AccumuloHiveRow();
            row.add(COLUMN_FAMILY.toString(), NAME.toString(), "brian".getBytes());
            row.add(COLUMN_FAMILY.toString(), SID.toString(), parseIntBytes("1"));
            row.add(COLUMN_FAMILY.toString(), DEGREES.toString(), parseDoubleBytes("44.5"));
            row.add(COLUMN_FAMILY.toString(), MILLIS.toString(), parseLongBytes("555"));
            assertTrue(reader.next(rowId, row));
            assertEquals(row.getRowId(), rowId.toString());
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), NAME.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), NAME.toString()), "brian".getBytes());
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), SID.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), SID.toString()), parseIntBytes("1"));
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), DEGREES.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), DEGREES.toString()), parseDoubleBytes("44.5"));
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), MILLIS.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), MILLIS.toString()), parseLongBytes("555"));

        } catch (IOException e) {
            log.error(e);
            fail();
        }
    }


    @Test
    public void getOnlyName() {
        FileInputFormat.addInputPath(conf, new Path("unused"));

        try {
            InputSplit[] splits = inputformat.getSplits(conf, 0);
            assertEquals(splits.length, 1);
            RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
            Text rowId = new Text("r1");
            AccumuloHiveRow row = new AccumuloHiveRow();
            assertTrue(reader.next(rowId, row));
            assertEquals(row.getRowId(), rowId.toString());
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), NAME.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), NAME.toString()), "brian".getBytes());

            rowId = new Text("r2");
            assertTrue(reader.next(rowId, row));
            assertEquals(row.getRowId(), rowId.toString());
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), NAME.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), NAME.toString()), "mark".getBytes());

            rowId = new Text("r3");
            assertTrue(reader.next(rowId, row));
            assertEquals(row.getRowId(), rowId.toString());
            assertTrue(row.hasFamAndQual(COLUMN_FAMILY.toString(), NAME.toString()));
            assertEquals(row.getValue(COLUMN_FAMILY.toString(), NAME.toString()), "dennis".getBytes());

            assertFalse(reader.next(rowId, row));
        }catch (IOException e) {
            log.error(e);
            fail();
        }
    }

    @Test
    public void degreesAndMillis() {
        try {
            Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
            Scanner scan = con.createScanner(TEST_TABLE, new Authorizations("blah"));
            IteratorSetting is = new IteratorSetting(1, PrimativeComparisonFilter.FILTER_PREFIX + 1,
                    PrimativeComparisonFilter.class);

            is.addOption(PrimativeComparisonFilter.P_COMPARE_CLASS, DoubleCompare.class.getName());
            is.addOption(PrimativeComparisonFilter.COMPARE_OPT_CLASS, GreaterThanOrEqual.class.getName());
            is.addOption(PrimativeComparisonFilter.CONST_VAL,  new String(Base64.encodeBase64(parseDoubleBytes("55.6"))));
            is.addOption(PrimativeComparisonFilter.COLUMN, "cf|dgrs");
            scan.addScanIterator(is);

            IteratorSetting is2 = new IteratorSetting(2, PrimativeComparisonFilter.FILTER_PREFIX + 2,
                    PrimativeComparisonFilter.class);

            is2.addOption(PrimativeComparisonFilter.P_COMPARE_CLASS, LongCompare.class.getName());
            is2.addOption(PrimativeComparisonFilter.COMPARE_OPT_CLASS, LessThan.class.getName());
            is2.addOption(PrimativeComparisonFilter.CONST_VAL,  new String(Base64.encodeBase64(parseLongBytes("778"))));
            is2.addOption(PrimativeComparisonFilter.COLUMN, "cf|mills");

            scan.addScanIterator(is2);

            boolean foundDennis = false;
            int totalCount = 0;
            for(Map.Entry<Key,Value> kv : scan) {
                boolean foundName = false;
                boolean foundSid = false;
                boolean foundDegrees = false;
                boolean foundMillis = false;
                SortedMap<Key,Value> items = PrimativeComparisonFilter.decodeRow(kv.getKey(), kv.getValue());
                for(Map.Entry<Key, Value> item: items.entrySet()) {
                    SortedMap<Key, Value> nestedItems = PrimativeComparisonFilter.decodeRow(item.getKey(), item.getValue());
                    for(Map.Entry<Key,Value> nested : nestedItems.entrySet()) {
                        if(nested.getKey().getRow().toString().equals("r3")) {
                            foundDennis = true;
                        }
                        if(nested.getKey().getColumnQualifier().equals(NAME)){
                            foundName = true;
                        } else if (nested.getKey().getColumnQualifier().equals(SID)) {
                            foundSid = true;
                        } else if (nested.getKey().getColumnQualifier().equals(DEGREES)) {
                            foundDegrees = true;
                        } else if (nested.getKey().getColumnQualifier().equals(MILLIS)) {
                            foundMillis = true;
                        }
                    }
                }
                totalCount++;
                assertTrue(foundDegrees & foundMillis & foundName & foundSid);
            }
            assertTrue(foundDennis);
            assertEquals(totalCount, 1);
        }  catch (AccumuloSecurityException e) {
            fail(StringUtils.stringifyException(e));
        } catch (AccumuloException e) {
            fail(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            fail(StringUtils.stringifyException(e));
        } catch (IOException e) {
            fail(StringUtils.stringifyException(e));
        }
    }

    @Test
    public void greaterThan1Sid() {
        try {
            Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
            Scanner scan = con.createScanner(TEST_TABLE, new Authorizations("blah"));
            IteratorSetting is = new IteratorSetting(1, PrimativeComparisonFilter.FILTER_PREFIX + 1,
                    PrimativeComparisonFilter.class);

            is.addOption(PrimativeComparisonFilter.P_COMPARE_CLASS, IntCompare.class.getName());
            is.addOption(PrimativeComparisonFilter.COMPARE_OPT_CLASS, GreaterThan.class.getName());
            is.addOption(PrimativeComparisonFilter.CONST_VAL,  new String(Base64.encodeBase64(parseIntBytes("1"))));
            is.addOption(PrimativeComparisonFilter.COLUMN, "cf|sid");
            scan.addScanIterator(is);
            boolean foundMark = false;
            boolean foundDennis = false;
            int totalCount = 0;
            for(Map.Entry<Key,Value> kv : scan) {
                boolean foundName = false;
                boolean foundSid = false;
                boolean foundDegrees = false;
                boolean foundMillis = false;
                SortedMap<Key,Value> items = PrimativeComparisonFilter.decodeRow(kv.getKey(), kv.getValue());
                for(Map.Entry<Key, Value> item: items.entrySet()) {
                    if(item.getKey().getRow().toString().equals("r2")) {
                        foundMark = true;
                    } else if(item.getKey().getRow().toString().equals("r3")) {
                        foundDennis = true;
                    }
                    if(item.getKey().getColumnQualifier().equals(NAME)){
                        foundName = true;
                    } else if (item.getKey().getColumnQualifier().equals(SID)) {
                        foundSid = true;
                    } else if (item.getKey().getColumnQualifier().equals(DEGREES)) {
                        foundDegrees = true;
                    } else if (item.getKey().getColumnQualifier().equals(MILLIS)) {
                        foundMillis = true;
                    }
                }
                totalCount++;
                assertTrue(foundDegrees & foundMillis & foundName & foundSid);
            }
            assertTrue(foundDennis & foundMark);
            assertEquals(totalCount, 2);
        }  catch (AccumuloSecurityException e) {
            fail(StringUtils.stringifyException(e));
        } catch (AccumuloException e) {
            fail(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            fail(StringUtils.stringifyException(e));
        } catch (IOException e) {
            fail(StringUtils.stringifyException(e));
        }
    }

    @Test
    public void nameEqualBrian() {
        try {
            Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
            Scanner scan = con.createScanner(TEST_TABLE, new Authorizations("blah"));
            IteratorSetting is = new IteratorSetting(1, PrimativeComparisonFilter.FILTER_PREFIX + 1,
                    PrimativeComparisonFilter.class);

            is.addOption(PrimativeComparisonFilter.P_COMPARE_CLASS, StringCompare.class.getName());
            is.addOption(PrimativeComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
            is.addOption(PrimativeComparisonFilter.CONST_VAL,  new String(Base64.encodeBase64("brian".getBytes())));
            is.addOption(PrimativeComparisonFilter.COLUMN, "cf|name");
            scan.addScanIterator(is);
            boolean foundName = false;
            boolean foundSid = false;
            boolean foundDegrees = false;
            boolean foundMillis = false;
            for(Map.Entry<Key,Value> kv : scan) {
                SortedMap<Key,Value> items = PrimativeComparisonFilter.decodeRow(kv.getKey(), kv.getValue());
                for(Map.Entry<Key, Value> item: items.entrySet()) {
                    assertEquals(item.getKey().getRow().toString(), "r1");
                    if(item.getKey().getColumnQualifier().equals(NAME)){
                        foundName = true;
                        assertEquals(item.getValue().get(), "brian".getBytes());
                    } else if (item.getKey().getColumnQualifier().equals(SID)) {
                        foundSid = true;
                        assertEquals(item.getValue().get(), parseIntBytes("1"));
                    } else if (item.getKey().getColumnQualifier().equals(DEGREES)) {
                        foundDegrees = true;
                        assertEquals(item.getValue().get(), parseDoubleBytes("44.5"));
                    } else if (item.getKey().getColumnQualifier().equals(MILLIS)) {
                        foundMillis = true;
                        assertEquals(item.getValue().get(), parseLongBytes("555"));
                    }
                }
            }
            assertTrue(foundDegrees & foundMillis & foundName & foundSid);

        }  catch (AccumuloSecurityException e) {
            fail(StringUtils.stringifyException(e));
        } catch (AccumuloException e) {
            fail(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            fail(StringUtils.stringifyException(e));
        } catch (IOException e) {
            fail(StringUtils.stringifyException(e));
        }
    }

    @Test
    public void getNone() {
        FileInputFormat.addInputPath(conf, new Path("unused"));
        conf.set(AccumuloSerde.COLUMN_MAPPINGS, "cf|f1");
        try {
            InputSplit[] splits = inputformat.getSplits(conf, 0);
            assertEquals(splits.length, 1);
            RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
            Text rowId = new Text("r1");
            AccumuloHiveRow row = new AccumuloHiveRow();
            row.setRowId("r1");
            assertFalse(reader.next(rowId, row));
        }catch (IOException e) {
            log.error(e);
            fail();
        }
        //for whatever reason, this errors unless I manually reset. mockKeyValues() ignored @BeforeTest
        conf.set(AccumuloSerde.COLUMN_MAPPINGS, "cf|name,cf|sid,cf|dgrs,cf|mills");
    }

}
