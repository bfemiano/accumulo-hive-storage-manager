package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * User: bfemiano
 * Date: 3/2/13
 * Time: 2:35 AM
 */
public class AccumuloHiveRow implements Writable{
    // create a list of column familes, qualifiers, and values.

    private String rowId;
    List<ColumnTuple> tuples = Lists.newArrayList();

    public AccumuloHiveRow() {
    }

    public AccumuloHiveRow(String rowId) {
        this.rowId = rowId;
    }

    public boolean hasFamAndQual(String fam, String qual) {
        for (ColumnTuple tuple : tuples) {
            if(tuple.getCf().equals(fam) && tuple.getQual().equals(qual)){
                return true;
            }
        }
        return false;
    }

    public byte[] getValue(String fam, String qual) {
        for (ColumnTuple tuple : tuples) {
            if(tuple.getCf().equals(fam) && tuple.getQual().equals(qual)){
                return tuple.getValue();
            }
        }
        return null;
    }

    public String getRowId() {
        return rowId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int size = tuples.size();
        dataOutput.writeInt(size);
        for (ColumnTuple tuple : tuples){
            dataOutput.writeUTF(tuple.getCf());
            dataOutput.writeUTF(tuple.getQual());
            dataOutput.writeInt(tuple.getValue().length);
            dataOutput.write(tuple.getValue());
        }
    }

    @Override
    public String toString() {
        return "AccumuloHiveRow{" +
                "rowId='" + rowId + '\'' +
                ", tuples=" + tuples +
                '}';
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++){
            String cf = dataInput.readUTF();
            String qual = dataInput.readUTF();
            int valSize = dataInput.readInt();
            byte[] val = new byte[valSize];
            for (int j = 0; j < valSize; j++) {
                val[i] = dataInput.readByte();
            }
            tuples.add(new ColumnTuple(cf, qual, val));
        }
    }

    public void add(String cf, String qual, byte[] val) {
        tuples.add(new ColumnTuple(cf, qual, val));
    }

    public static class ColumnTuple {
        private String cf;
        private String qual;
        private byte[] value;

        public ColumnTuple( String cf, String qual, byte[] value) {
            this.value = value;
            this.cf = cf;
            this.qual = qual;
        }

        public byte[] getValue() {
            return value;
        }

        public String getCf() {
            return cf;
        }

        public String getQual() {
            return qual;
        }
    }
}
