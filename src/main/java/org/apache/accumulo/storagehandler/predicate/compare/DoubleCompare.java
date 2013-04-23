package org.apache.accumulo.storagehandler.predicate.compare;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 1:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class DoubleCompare implements PrimativeCompare {

    private BigDecimal constant;

    public DoubleCompare(double constant) {
        this.constant = BigDecimal.valueOf(constant);
    }

    public BigDecimal serialize(byte[] value) {
        try {
            return new BigDecimal(ByteBuffer.wrap(value).asDoubleBuffer().get());
        } catch (Exception e) {
            throw new RuntimeException(e.toString() + " occurred trying to build double value. " +
                    "Make sure the value type for the byte[] is double ");
        }
    }

    @Override
    public boolean isEqual(byte[] value) {
        return serialize(value).compareTo(constant) == 0;
    }

    @Override
    public boolean isNotEqual(byte[] value) {
        return serialize(value).compareTo(constant) != 0;
    }

    @Override
    public boolean greaterThanOrEqual(byte[] value) {
        return serialize(value).compareTo(constant) >= 0;
    }

    @Override
    public boolean greaterThan(byte[] value) {
        return serialize(value).compareTo(constant) > 0;
    }

    @Override
    public boolean lessThanOrEqual(byte[] value) {
        return serialize(value).compareTo(constant) <= 0;
    }

    @Override
    public boolean lessThan(byte[] value) {
        return serialize(value).compareTo(constant) < 0;
    }

    @Override
    public boolean like(byte[] value) {
        throw new UnsupportedOperationException("Like not supported for " + getClass().getName());
    }
}
