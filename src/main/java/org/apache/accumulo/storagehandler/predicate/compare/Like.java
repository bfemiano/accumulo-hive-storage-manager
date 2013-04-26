package org.apache.accumulo.storagehandler.predicate.compare;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 10:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class Like implements CompareOp {

    PrimitiveCompare comp;

    public Like(){}

    public Like(PrimitiveCompare comp) {
        this.comp = comp;
    }

    @Override
    public void setPrimativeCompare(PrimitiveCompare comp) {
       this.comp = comp;
    }

    @Override
    public PrimitiveCompare getPrimativeCompare() {
        return comp;
    }

    @Override
    public boolean accept(byte[] val) {
        return comp.like(val);
    }
}
