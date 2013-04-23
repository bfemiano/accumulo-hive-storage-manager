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
 * Time: 2:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class StringCompareTest {

    private StringCompare strCompare;


    @BeforeClass
    public void setup() {
        strCompare = new StringCompare("aaa");
    }


    @Test
    public void equal() {
        Equal equalObj = new Equal(strCompare);
        byte[] val = "aaa".getBytes();
        assertTrue(equalObj.accept(val));
    }

    @Test
    public void notEqual() {
        NotEqual notEqualObj = new NotEqual(strCompare);
        byte [] val = "aab".getBytes();
        assertTrue(notEqualObj.accept(val));

        val = "aaa".getBytes();
        assertFalse(notEqualObj.accept(val));

    }

    @Test
    public void greaterThan() {
        GreaterThan greaterThanObj = new GreaterThan(strCompare);
        byte [] val = "aab".getBytes();

        assertTrue(greaterThanObj.accept(val));

        val = "aa".getBytes();
        assertFalse(greaterThanObj.accept(val));

        val = "aaa".getBytes();
        assertFalse(greaterThanObj.accept(val));
    }

    @Test
    public void greaterThanOrEqual() {
        GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(strCompare);
        byte [] val = "aab".getBytes();

        assertTrue(greaterThanOrEqualObj.accept(val));

        val = "aa".getBytes();
        assertFalse(greaterThanOrEqualObj.accept(val));

        val = "aaa".getBytes();
        assertTrue(greaterThanOrEqualObj.accept(val));
    }

    @Test
    public void lessThan() {

        LessThan lessThanObj = new LessThan(strCompare);

        byte [] val = "aab".getBytes();

        assertFalse(lessThanObj.accept(val));

        val = "aa".getBytes();
        assertTrue(lessThanObj.accept(val));

        val = "aaa".getBytes();
        assertFalse(lessThanObj.accept(val));

    }

    @Test
    public void lessThanOrEqual() {

        LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(strCompare);

        byte [] val = "aab".getBytes();

        assertFalse(lessThanOrEqualObj.accept(val));

        val = "aa".getBytes();
        assertTrue(lessThanOrEqualObj.accept(val));

        val = "aaa".getBytes();
        assertTrue(lessThanOrEqualObj.accept(val));
    }

    @Test
    public void like() {
        Like likeObj = new Like(strCompare);
        String condition = "%a";
        assertTrue(likeObj.accept(condition.getBytes()));

        condition = "%a%";
        assertTrue(likeObj.accept(condition.getBytes()));

        condition = "a%";
        assertTrue(likeObj.accept(condition.getBytes()));

        condition = "a%aa";
        assertFalse(likeObj.accept(condition.getBytes()));

        condition = "b%";
        assertFalse(likeObj.accept(condition.getBytes()));

        condition = "%ab%";
        assertFalse(likeObj.accept(condition.getBytes()));

        condition = "%ba";
        assertFalse(likeObj.accept(condition.getBytes()));
    }
}
