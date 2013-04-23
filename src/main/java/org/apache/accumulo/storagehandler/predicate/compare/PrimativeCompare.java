package org.apache.accumulo.storagehandler.predicate.compare;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 12:27 AM
 * To change this template use File | Settings | File Templates.
 */
public interface PrimativeCompare {

    public boolean isEqual(byte[] value);
    public boolean isNotEqual(byte [] value);
    public boolean greaterThanOrEqual(byte [] value);
    public boolean greaterThan(byte [] value);
    public boolean lessThanOrEqual(byte[] value);
    public boolean lessThan(byte [] value);
    public boolean like(byte[] value);
    public Object serialize(byte[] value);
}
