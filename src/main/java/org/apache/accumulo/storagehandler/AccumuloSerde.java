package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;

/**
 * User: bfemiano
 * Date: 2/27/13
 * Time: 4:49 PM
 */
public class AccumuloSerde implements SerDe {
    public static final String TABLE_NAME = "accumulo.table.name";
    public static final String USER_NAME = "accumulo.user.name";
    public static final String USER_PASS = "accumulo.user.pass";
    public static final String ZOOKEEPERS = "accumuo.zookeepers";
    public static final String INSTANCE_ID = "accumulo.instance.id";
    public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
    public static final String ACCUMULO_KEY_COL = ":key";
    private LazySimpleSerDe.SerDeParameters serDeParameters;
    private LazyAccumuloRow cachedRow;
    private List<String> fams;
    private String colMapping;
    private byte[] separators;
    private boolean escaped;
    private byte escapedChar;
    private boolean[] needsEscape;

    private static final Logger log = Logger.getLogger(AccumuloSerde.class);

    private ObjectInspector cachedObjectInspector;
    private static final String MAP_STRING_STRING_NAME = Constants.MAP_TYPE_NAME + "<" +
            Constants.STRING_TYPE_NAME + "," +
            Constants.STRING_TYPE_NAME + ">";

    public void initialize(Configuration conf, Properties properties) throws SerDeException {
        initAccumuloSerdeParameters(conf, properties);

        cachedObjectInspector = LazyFactory.createLazyStructInspector(
                serDeParameters.getColumnNames(),
                serDeParameters.getColumnTypes(),
                serDeParameters.getSeparators(),
                serDeParameters.getNullSequence(),
                serDeParameters.isLastColumnTakesRest(),
                serDeParameters.isEscaped(),
                serDeParameters.getEscapeChar());

        cachedRow = new LazyAccumuloRow((LazySimpleStructObjectInspector) cachedObjectInspector);

        if(log.isInfoEnabled()) {
           log.info("Initialized with " + serDeParameters.getColumnNames() +
                    " type: " + serDeParameters.getColumnTypes());
        }
    }

    public static boolean isKeyField(String colName) {
        return colName.equals(ACCUMULO_KEY_COL);
    }

    private void initAccumuloSerdeParameters(Configuration conf, Properties properties)
            throws SerDeException{
        colMapping = properties.getProperty(COLUMN_MAPPINGS);
        String colTypeProperty = properties.getProperty(Constants.LIST_COLUMN_TYPES);
        String name = getClass().getName();
        fams = parseColumnMapping(colMapping);
        //TODO: eventually make the key num instance global.
        if (colTypeProperty == null) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < fams.size(); i++) {
                builder.append(MAP_STRING_STRING_NAME + ":");
            }
            int indexOfLastColon = builder.lastIndexOf(":");
            builder.replace(indexOfLastColon, indexOfLastColon+1, "");
            properties.setProperty(Constants.LIST_COLUMN_TYPES, builder.toString());
        }
        serDeParameters = LazySimpleSerDe.initSerdeParams(conf, properties, name);

        if (fams.size() != serDeParameters.getColumnNames().size()) {
            throw new SerDeException(name + ": columns has "
                    + serDeParameters.getColumnNames().size() +
                    " elements while accumulo.column.mapping has " +
                    fams.size() + " elements, +1 for the key.");
        }
        separators = serDeParameters.getSeparators();
        escaped = serDeParameters.isEscaped();
        escapedChar = serDeParameters.getEscapeChar();
        needsEscape = serDeParameters.getNeedsEscape();

        if(log.isInfoEnabled())
            log.info("Serde initialized successfully for column mapping: " + colMapping);
    }

    public static List<String> parseColumnMapping(String columnMapping)
            throws SerDeException{
        List<String> colQualFamPairs = Lists.newArrayList();
        colQualFamPairs.add("cf");
        return colQualFamPairs;

    }

    public Class<? extends Writable> getSerializedClass() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object deserialize(Writable writable) throws SerDeException {
        if(!(writable instanceof AccumuloHiveRow)) {
            throw new SerDeException(getClass().getName() + " : " +
                    "Expects AccumuloHiveRow. Got " + writable.getClass().getName());
        }

        return writable;
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return cachedObjectInspector;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }
}
