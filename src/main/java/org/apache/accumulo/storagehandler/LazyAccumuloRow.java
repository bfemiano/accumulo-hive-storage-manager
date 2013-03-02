package org.apache.accumulo.storagehandler;

import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;

import java.util.List;

/**
 * User: bfemiano
 * Date: 3/2/13
 * Time: 2:26 AM
 */
public class LazyAccumuloRow extends LazyStruct{


    public LazyAccumuloRow(LazySimpleStructObjectInspector inspector) {
        super(inspector);
    }

    public void init(AccumuloHiveRow hiveRow, List<String> fams, List<String> quals) {

    }
}
