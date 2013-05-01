package org.apache.accumulo.storagehandler.predicate.compare;

/**
 *
 * Wraps call to lessThanOrEqual over {@link PrimitiveCompare} instance.
 *
 * Used by {@link org.apache.accumulo.storagehandler.predicate.PrimitiveComparisonFilter}
 */
public class LessThanOrEqual implements CompareOp {

    private PrimitiveCompare comp;

    public LessThanOrEqual(){}

    public LessThanOrEqual(PrimitiveCompare comp) {
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
        return comp.lessThanOrEqual(val);
    }
}
