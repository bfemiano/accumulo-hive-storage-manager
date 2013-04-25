package org.apache.accumulo.storagehandler.predicate.compare;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 1:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class LongCompare implements PrimitiveCompare {

    private long constant;

    @Override
    public void init(byte[] constant) {
        this.constant = serialize(constant);
    }

    @Override
    public boolean isEqual(byte[] value) {
        long lonVal = serialize(value);
        return lonVal == constant;
    }

    @Override
    public boolean isNotEqual(byte[] value) {
        return serialize(value) != constant;
    }

    @Override
    public boolean greaterThanOrEqual(byte[] value) {
        return serialize(value) >= constant;
    }

    @Override
    public boolean greaterThan(byte[] value) {
        return serialize(value) > constant;
    }

    @Override
    public boolean lessThanOrEqual(byte[] value) {
        return serialize(value) <= constant;
    }

    @Override
    public boolean lessThan(byte[] value) {
        return serialize(value) < constant;
    }

    @Override
    public boolean like(byte[] value) {
        throw new UnsupportedOperationException("Like not supported for " + getClass().getName());
    }

    public Long serialize(byte[] value) {
        try {
            return ByteBuffer.wrap(value).asLongBuffer().get();
        } catch (Exception e) {
            throw new RuntimeException(e.toString() + " occurred trying to build long value. " +
                    "Make sure the value type for the byte[] is long ");
        }
    }
}
