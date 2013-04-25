package org.apache.accumulo.storagehandler.predicate.compare;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.*;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/23/13
 * Time: 1:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class IntCompareTest {
    private IntCompare intCompare;


    @BeforeClass
    public void setup() {
        byte[] ibytes = new byte[4];
        ByteBuffer.wrap(ibytes).putInt(10);
        intCompare = new IntCompare();
        intCompare.init(ibytes);
    }

    public byte[] getBytes(int val) {
        byte [] intBytes = new byte[4];
        ByteBuffer.wrap(intBytes).putInt(val);
        int serializedVal = intCompare.serialize(intBytes);
        assertEquals(serializedVal, val);
        return intBytes;
    }

    @Test
    public void equal() {
        Equal equalObj = new Equal(intCompare);
        byte[] val = getBytes(10);
        assertTrue(equalObj.accept(val));
    }

    @Test
    public void notEqual() {
        NotEqual notEqualObj = new NotEqual(intCompare);
        byte [] val = getBytes(11);
        assertTrue(notEqualObj.accept(val));

        val = getBytes(10);
        assertFalse(notEqualObj.accept(val));

    }

    @Test
    public void greaterThan() {
        GreaterThan greaterThanObj = new GreaterThan(intCompare);
        byte [] val = getBytes(11);

        assertTrue(greaterThanObj.accept(val));

        val = getBytes(4);
        assertFalse(greaterThanObj.accept(val));

        val = getBytes(10);
        assertFalse(greaterThanObj.accept(val));
    }

    @Test
    public void greaterThanOrEqual() {
        GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(intCompare);

        byte [] val = getBytes(11);

        assertTrue(greaterThanOrEqualObj.accept(val));

        val = getBytes(4);
        assertFalse(greaterThanOrEqualObj.accept(val));

        val = getBytes(10);
        assertTrue(greaterThanOrEqualObj.accept(val));
    }

    @Test
    public void lessThan() {

        LessThan lessThanObj = new LessThan(intCompare);

        byte [] val = getBytes(11);

        assertFalse(lessThanObj.accept(val));

        val = getBytes(4);
        assertTrue(lessThanObj.accept(val));

        val = getBytes(10);
        assertFalse(lessThanObj.accept(val));

    }

    @Test
    public void lessThanOrEqual() {

        LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(intCompare);

        byte [] val = getBytes(11);

        assertFalse(lessThanOrEqualObj.accept(val));

        val = getBytes(4);
        assertTrue(lessThanOrEqualObj.accept(val));

        val = getBytes(10);
        assertTrue(lessThanOrEqualObj.accept(val));
    }

    @Test
    public void like() {
        try {
            Like likeObj = new Like(intCompare);
            assertTrue(likeObj.accept(new byte[]{}));
            fail("should not accept");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Like not supported for " + intCompare.getClass().getName()));
        }
    }
}
