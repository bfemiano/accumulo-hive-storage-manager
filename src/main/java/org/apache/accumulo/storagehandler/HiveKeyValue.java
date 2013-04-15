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
 *
 * Simple class that wraps the rowID and qualifier portions of an
 * Accumulo key together with the value byte[].
 */
public class HiveKeyValue implements Writable{

    private String rowId;
    private String qual;
    private byte[] val;

    public HiveKeyValue() {
    }

    public HiveKeyValue(String rowId) {
        this.rowId = rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getRowId() {
        return rowId;
    }

    public byte[] getVal() {
        return val;
    }

    public String getQual() {
        return qual;
    }

    public void setQual(String qual) {
        this.qual = qual;
    }

    public void setVal(byte[] val) {
        this.val = val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(rowId);
        dataOutput.writeUTF(qual);
        dataOutput.writeInt(val.length);
        dataOutput.write(val);
    }

    @Override
    public String toString() {
        return rowId + " " + qual + " " + new String(val);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowId = dataInput.readUTF();
        qual = dataInput.readUTF();
        int size = dataInput.readInt();
        val = new byte[size];
        for (int i = 0; i < val.length; i++) {
             val[i] = dataInput.readByte();

        }
    }
}
