package org.apache.accumulo.storagehandler.predicate.compare;

/**
 *
 * Wraps call to greaterThanOrEqual over {@link PrimitiveCompare} instance.
 *
 * Used by {@link org.apache.accumulo.storagehandler.predicate.PrimitiveComparisonFilter}
 */
public class GreaterThanOrEqual implements CompareOp {

    private PrimitiveCompare comp;

    public GreaterThanOrEqual(){}

    public GreaterThanOrEqual(PrimitiveCompare comp) {
        this.comp = comp;
    }

    @Override
    public void setPrimitiveCompare(PrimitiveCompare comp) {
        this.comp = comp;
    }

    @Override
    public PrimitiveCompare getPrimitiveCompare() {
        return comp;
    }

    @Override
    public boolean accept(byte[] val) {
        return comp.greaterThanOrEqual(val);
    }
}
