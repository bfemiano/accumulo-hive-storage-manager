package org.apache.accumulo.storagehandler.predicate.compare;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 1:00 AM
 * To change this template use File | Settings | File Templates.
 */
public interface CompareOp {

    public void setPrimativeCompare(PrimitiveCompare comp);
    public PrimitiveCompare getPrimativeCompare();
    public boolean accept(byte [] val);
}
