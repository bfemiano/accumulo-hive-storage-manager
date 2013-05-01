package org.apache.accumulo.storagehandler.predicate.compare;

/**
 *
 * Wraps call to like over {@link PrimitiveCompare} instance. Currently only supported by StringCompare.
 *
 * Used by {@link org.apache.accumulo.storagehandler.predicate.PrimitiveComparisonFilter}
 */
public class Like implements CompareOp {

    PrimitiveCompare comp;

    public Like(){}

    public Like(PrimitiveCompare comp) {
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
        return comp.like(val);
    }
}
