package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * User: bfemiano
 * Date: 7/10/12
 * Time: 1:37 AM
 */
public class AccumuloStorageHandler extends DefaultStorageHandler
        implements HiveMetaHook {
    //TODO predicate pushdown.

    private Configuration conf;

    private TableOperations tableOpts;
    private Connector connector;
    private ZooKeeperInstance instance;

    private final long MAX_MEMORY = 1000000L; // bytes to store before sending a batch
    private final long TIMEOUT = 1000L; // milliseconds to wait before sending
    private final int NUM_THREADS = 10;

    private static final Logger log = Logger.getLogger(AccumuloStorageHandler.class);

    private Connector getConnector(Map<String,String> parameters)
            throws MetaException{
        String user = getFromConf(AccumuloSerde.USER_NAME);
        String pass = getFromConf(AccumuloSerde.USER_PASS);
        String inst = getFromConf(AccumuloSerde.INSTANCE_ID);
        String zookeepers = getFromConf(AccumuloSerde.ZOOKEEPERS);

        if (connector == null){
            try {
                connector = instance.getConnector(user, pass.getBytes());
                instance = new ZooKeeperInstance(inst, zookeepers);
            } catch (AccumuloException e) {
                throw new MetaException(StringUtils.stringifyException(e));
            } catch (AccumuloSecurityException e) {
                throw new MetaException(StringUtils.stringifyException(e));
            }
        }
        return connector;
    }

    @Override
    public void configureTableJobProperties(TableDesc desc,
                                            Map<String, String> jobProps) {
        Properties tblProperties = desc.getProperties();
        jobProps.put(AccumuloSerde.COLUMN_MAPPINGS,
                tblProperties.getProperty(AccumuloSerde.COLUMN_MAPPINGS));
        String tableName = tblProperties.getProperty(AccumuloSerde.TABLE_NAME);
        jobProps.put(AccumuloSerde.TABLE_NAME, tableName);

    }

    private String getTableName(Table table) throws MetaException{
        String tableName = table.getParameters().get(AccumuloSerde.TABLE_NAME);
        if (tableName == null)   {
            throw new MetaException("Please specify table name in TBLPROPERTIES");
        }
        return tableName;
    }

    private String getProperty(Map<String, String> parameters, String propName)
            throws MetaException{
        String prop = parameters.get(propName);
        if (prop == null)
            throw new MetaException("Property " + propName + " is missing from TBLPROPERTIES");
        return prop;
    }

    private String getFromConf(String property)
        throws MetaException{
         String propValue = conf.get(property);
        if (propValue == null)
            throw new MetaException("Forgot to set property: " + property + " in your script");
        return propValue;
    }



    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return AccumuloSerde.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
         return HiveAccumuloTableInputFormat.class;
    }


    @Override
    public void preCreateTable(Table table) throws MetaException {
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        if (table.getSd().getLocation() != null){
            throw new MetaException("Location may not be specified for Accumulo");
        }
        try {
            String tblName = getTableName(table);
            Connector connector = getConnector(table.getParameters());
            TableOperations tableOpts = connector.tableOperations();
            Map<String, String> serdeParams = table.getSd().getParameters();
            String columnMapping = serdeParams.get(AccumuloSerde.COLUMN_MAPPINGS);
            if (columnMapping == null)
                throw new MetaException(AccumuloSerde.COLUMN_MAPPINGS + " missing from SERDEPROPERTIES");
            //TODO: Parse within Serde, for now just hardcode it for testing.
            // since it supports blank table creates.
            List<String> colQualFamPairs = AccumuloSerde.parseColumnMapping(columnMapping);
            if (!tableOpts.exists(tblName)) {
                if(!isExternal) {
                    tableOpts.create(tblName);
                    tableOpts.online(tblName);
//                    BatchWriter writer = connector.createBatchWriter(tblName, MAX_MEMORY, TIMEOUT, NUM_THREADS);
//                    Mutation m = new Mutation(fams.get(0));
//                    m.put(new Text(fams.get(0)), new Text(quals.get(0)));
                } else {
                    throw new MetaException("Accumulo table " + tblName + " doesn't existing even though declared external");
                }
            } else {
                if (!isExternal) {
                    throw new MetaException("Table " + tblName + " already exists. Use CREATE EXTERNAL TABLE to register with Hive.");
                }
            }

        } catch (AccumuloSecurityException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableExistsException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (AccumuloException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (SerDeException e) {
            log.info("Error parsing column mapping in Serde");
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        String tblName = getTableName(table);
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        try {
            TableOperations tblOpts = getConnector(table.getParameters()).tableOperations();
            if(!isExternal && tblOpts.exists(tblName)){
                tblOpts.delete(tblName);
            }
        } catch (AccumuloException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void commitDropTable(Table table, boolean deleteData) throws MetaException {
        String tblName = getTableName(table);
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        try {
            if(!isExternal && deleteData){
                TableOperations tblOpts = getConnector(table.getParameters()).tableOperations();
                if(tblOpts.exists(tblName))
                    tblOpts.delete(tblName);
            }
        } catch (AccumuloException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (AccumuloSecurityException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        } catch (TableNotFoundException e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void preDropTable(Table table) throws MetaException {
        //do nothing
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {
        //do nothing
    }

}
