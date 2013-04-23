package org.apache.accumulo.storagehandler.predicate.compare;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 1:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class IntCompare implements PrimativeCompare {

    private int constant;

    public IntCompare(int constant) {
        this.constant = constant;
    }

    @Override
    public boolean isEqual(byte[] value) {
        return  serialize(value) == constant;
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

    public Integer serialize(byte[] value) {
        try {
            return ByteBuffer.wrap(value).asIntBuffer().get();
        } catch (Exception e) {
            throw new RuntimeException(e.toString() + " occurred trying to build int value. " +
                    "Make sure the value type for the byte[] is int ");
        }
    }
}
