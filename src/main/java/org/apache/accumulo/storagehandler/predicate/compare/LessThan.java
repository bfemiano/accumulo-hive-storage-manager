package org.apache.accumulo.storagehandler.predicate.compare;

/**
 *
 * Wraps call to lessThan over {@link PrimitiveCompare} instance.
 *
 * Used by {@link org.apache.accumulo.storagehandler.predicate.PrimitiveComparisonFilter}
 */
public class LessThan implements CompareOp {

    private PrimitiveCompare comp;

    public LessThan(){}

    public LessThan(PrimitiveCompare comp) {
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
        return comp.lessThan(val);
    }
}
