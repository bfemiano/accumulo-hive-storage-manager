package org.apache.accumulo.storagehandler;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import java.util.List;

public class AccumuloHiveUtilsTest {

    private JobConf conf = new JobConf();

    @BeforeClass
    public void setup() {
        conf.set(serdeConstants.LIST_COLUMNS, "event_date,source,lat,lon,event_millis,id");
        conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string,double,double,long,int");
        conf.set(AccumuloSerde.COLUMN_MAPPINGS, "cf|dt,cf|src,cf|lat,cf|lon,cf|dtm,rowID");
    }

    @Test
    public void parseColumn() {
        List<String> hiveCols = AccumuloHiveUtils.parseColumnMapping(conf.get(serdeConstants.LIST_COLUMNS));
        assertEquals(hiveCols.size(), 6);

        List<String> accuCols = AccumuloHiveUtils.parseColumnMapping(conf.get(AccumuloSerde.COLUMN_MAPPINGS));
        assertEquals(accuCols.size(), 6);

        List<String> types = AccumuloHiveUtils.parseColumnMapping(conf.get(serdeConstants.LIST_COLUMN_TYPES));
        assertEquals(types.size(), 6);
    }

    @Test(dependsOnMethods = "parseColumn")
    public void rowId () {
        int index = AccumuloHiveUtils.getRowIdIndex(conf);
        assertEquals(index, 5);

        List<String> accuCols = AccumuloHiveUtils.parseColumnMapping(conf.get(AccumuloSerde.COLUMN_MAPPINGS));
        assertTrue(AccumuloHiveUtils.containsRowID(accuCols.get(5)));

        String hiveCol = AccumuloHiveUtils.hiveColForRowID(conf);
        assertEquals(hiveCol, "id");
    }


    @Test(dependsOnMethods = "parseColumn")
    public void hiveToAccumuloLookup() {
        String hiveExpected = "event_millis";
        String accumuloExpected = "cf|dtm";
        String accumCol = AccumuloHiveUtils.hiveToAccumulo(hiveExpected, conf);
        assertEquals(accumCol, accumuloExpected);


        String hiveCol = AccumuloHiveUtils.accumuloToHive(accumCol, conf);
        assertEquals(hiveCol, hiveExpected);
        String type = AccumuloHiveUtils.hiveColType(hiveCol, conf);
        assertEquals(type, "long");

        try {
            AccumuloHiveUtils.hiveToAccumulo("blah", conf);
            fail("column blah does not exist");
        } catch (IllegalArgumentException e) {
           assertTrue(e.getMessage().contains("column blah is not mapped in the hive table definition"));
        }

        try {
            AccumuloHiveUtils.accumuloToHive("blah", conf);
            fail("column blah does not exist");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("column blah is not mapped in " + AccumuloSerde.COLUMN_MAPPINGS));
        }

    }
}
