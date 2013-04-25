package org.apache.accumulo.storagehandler.predicate.compare;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.testng.Assert.*;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/23/13
 * Time: 1:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class DoubleCompareTest {

    private DoubleCompare doubleCompare;


    @BeforeClass
    public void setup() {
        doubleCompare = new DoubleCompare();
        byte[] db = new byte[8];
        ByteBuffer.wrap(db).putDouble(10.5d);
        doubleCompare.init(db);
    }

    public byte[] getBytes(double val) {
        byte [] dBytes = new byte[8];
        ByteBuffer.wrap(dBytes).putDouble(val);
        BigDecimal bd = doubleCompare.serialize(dBytes);
        assertEquals(bd.doubleValue(), val);
        return dBytes;
    }

    @Test
    public void equal() {
        Equal equalObj = new Equal(doubleCompare);
        byte[] val = getBytes(10.5d);
        assertTrue(equalObj.accept(val));
    }

    @Test
    public void notEqual() {
        NotEqual notEqualObj = new NotEqual(doubleCompare);
        byte [] val = getBytes(11.0d);
        assertTrue(notEqualObj.accept(val));

        val = getBytes(10.5d);
        assertFalse(notEqualObj.accept(val));

    }

    @Test
    public void greaterThan() {
        GreaterThan greaterThanObj = new GreaterThan(doubleCompare);
        byte [] val = getBytes(11.0d);

        assertTrue(greaterThanObj.accept(val));

        val = getBytes(4.5d);
        assertFalse(greaterThanObj.accept(val));

        val = getBytes(10.5d);
        assertFalse(greaterThanObj.accept(val));
    }

    @Test
    public void greaterThanOrEqual() {
        GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(doubleCompare);

        byte [] val = getBytes(11.0d);

        assertTrue(greaterThanOrEqualObj.accept(val));

        val = getBytes(4.0d);
        assertFalse(greaterThanOrEqualObj.accept(val));

        val = getBytes(10.5d);
        assertTrue(greaterThanOrEqualObj.accept(val));
    }

    @Test
    public void lessThan() {

        LessThan lessThanObj = new LessThan(doubleCompare);

        byte [] val = getBytes(11.0d);

        assertFalse(lessThanObj.accept(val));

        val = getBytes(4.0d);
        assertTrue(lessThanObj.accept(val));

        val = getBytes(10.5d);
        assertFalse(lessThanObj.accept(val));

    }

    @Test
    public void lessThanOrEqual() {

        LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(doubleCompare);

        byte [] val = getBytes(11.0d);

        assertFalse(lessThanOrEqualObj.accept(val));

        val = getBytes(4.0d);
        assertTrue(lessThanOrEqualObj.accept(val));

        val = getBytes(10.5d);
        assertTrue(lessThanOrEqualObj.accept(val));
    }

    @Test
    public void like() {
        try {
            Like likeObj = new Like(doubleCompare);
            assertTrue(likeObj.accept(new byte[]{}));
            fail("should not accept");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Like not supported for " + doubleCompare.getClass().getName()));
        }
    }

    @Test
    public void invalidSerialization() {
        try {
            byte[] badVal = new byte[4];
            ByteBuffer.wrap(badVal).putInt(1);
            doubleCompare.serialize(badVal);
            fail("Should fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(" occurred trying to build double value"));
        }
    }
}
