package org.apache.accumulo.storagehandler;

<<<<<<< HEAD
import com.google.common.collect.Lists;
=======
>>>>>>> 4f6e14e44cc5e3899403b947faa0d8b19b686f1f
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
<<<<<<< HEAD
=======
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
>>>>>>> 4f6e14e44cc5e3899403b947faa0d8b19b686f1f
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * User: bfemiano
 * Date: 2/27/13
 * Time: 4:49 PM
 */
public class AccumuloSerde implements SerDe {
    public static final String TABLE_NAME = "accumulo.table.name";
    public static final String USER_NAME = "accumulo.user.name";
    public static final String USER_PASS = "accumulo.user.pass";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    public static final String INSTANCE_ID = "accumulo.instance.id";
    public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
    public static final String ACCUMULO_KEY_MAPPING = "accumulo.key.mapping";
    private static final String KEY = "key=";
    private LazySimpleSerDe.SerDeParameters serDeParameters;
    private LazyAccumuloRow cachedRow;
    private List<String> fetchCols;
    private String colMapping;
    private String rowIdMapping;
    private byte[] separators;
    private boolean escaped;
    private byte escapedChar;
    private boolean[] needsEscape;

    private static final Logger log = Logger.getLogger(AccumuloSerde.class);
    private static final Pattern COMMA = Pattern.compile("[,]");

    private ObjectInspector cachedObjectInspector;
    //private static final String MAP_STRING_STRING_NAME = Constants.MAP_TYPE_NAME + "<" +
            //Constants.STRING_TYPE_NAME + "," +
            //Constants.STRING_TYPE_NAME + ">";

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

    /***
     * For testing purposes.
     * @return
     */
    public LazyAccumuloRow getCachedRow() {
        return cachedRow;
    }

    public static boolean isKeyField(String colName) {
        return colName.contains(KEY);
    }

    private void initAccumuloSerdeParameters(Configuration conf, Properties properties)
            throws SerDeException{
        colMapping = properties.getProperty(COLUMN_MAPPINGS);
        rowIdMapping = properties.getProperty(ACCUMULO_KEY_MAPPING);
        String colTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        String name = getClass().getName();
        fetchCols = parseColumnMapping(colMapping);
        if(rowIdMapping != null) {
            String key = KEY + rowIdMapping;
            fetchCols.add(key);
            colMapping = colMapping + "," + key;
            conf.set(ACCUMULO_KEY_MAPPING, rowIdMapping);
        }
        properties.setProperty(serdeConstants.LIST_COLUMNS, colMapping);
        if (colTypeProperty == null) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < fetchCols.size(); i++) {
                builder.append(serdeConstants.STRING_TYPE_NAME + ":");
            }
            int indexOfLastColon = builder.lastIndexOf(":");
            builder.replace(indexOfLastColon, indexOfLastColon+1, "");
            properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, builder.toString());
        }
        serDeParameters = LazySimpleSerDe.initSerdeParams(conf, properties, name);
        if (fetchCols.size() != serDeParameters.getColumnNames().size()) {
            throw new SerDeException(name + ": columns has "
                    + serDeParameters.getColumnNames().size() +
                    " elements while accumulo.column.mapping has " +
                    fetchCols.size() + " elements. Did you forget the key column type?");
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

        if(columnMapping == null)
            throw new SerDeException("null columnMapping not allowed.");
        return Lists.newArrayList(COMMA.split(columnMapping));
    }

    public Class<? extends Writable> getSerializedClass() {
        return Mutation.class;
    }

    public Writable serialize(Object o, ObjectInspector objectInspector)
            throws SerDeException {
         throw new UnsupportedOperationException("Serialization to Accumulo not yet supported");
    }

    public Object deserialize(Writable writable) throws SerDeException {
        if(!(writable instanceof AccumuloHiveRow)) {
            throw new SerDeException(getClass().getName() + " : " +
                    "Expects AccumuloHiveRow. Got " + writable.getClass().getName());
        }
        if(log.isInfoEnabled())
            log.info("Got accumulo row.");

        cachedRow.init((AccumuloHiveRow)writable, fetchCols);
        return cachedRow;
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return cachedObjectInspector;
    }

    public SerDeStats getSerDeStats() {
        throw new UnsupportedOperationException("Stats for AccumuloSerde not yet supported.");
    }
}
