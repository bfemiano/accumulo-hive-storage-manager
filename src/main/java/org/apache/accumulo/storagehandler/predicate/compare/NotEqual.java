package org.apache.accumulo.storagehandler.predicate.compare;

/**
 *
 * Wraps call to isEqual over {@link PrimitiveCompare} instance and returns the negation.
 *
 * Used by {@link org.apache.accumulo.storagehandler.predicate.PrimitiveComparisonFilter}
 */
public class NotEqual implements CompareOp {

    private PrimitiveCompare comp;

    public NotEqual(){}

    public NotEqual(PrimitiveCompare comp) {
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
        return !comp.isEqual(val);
    }
}
