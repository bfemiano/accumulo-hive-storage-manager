package org.apache.accumulo.storagehandler.predicate;

import org.apache.accumulo.storagehandler.predicate.compare.*;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 10:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class PredicateHandlerTest {

    private static final Logger log = Logger.getLogger(PredicateHandlerTest.class);

    private AccumuloPredicateHandler handler = AccumuloPredicateHandler.getInstance();

    @Test
    public void basicOptLookup() {
        boolean foundEqual = false;
        boolean foundNotEqual = false;
        boolean foundGreaterThanOrEqual = false;
        boolean foundGreaterThan = false;
        boolean foundLessThanOrEqual = false;
        boolean foundLessThan = false;
        for(String opt : handler.compareOpKeyset()) {
            Class<? extends CompareOp> compOpt = handler.getCompareOp(opt);
            if(compOpt.getName().equals(Equal.class.getName())) {
                foundEqual = true;
            } else if (compOpt.getName().equals(NotEqual.class.getName())) {
                foundNotEqual = true;
            } else if (compOpt.getName().equals(GreaterThan.class.getName())) {
                foundGreaterThan = true;
            } else if (compOpt.getName().equals(GreaterThanOrEqual.class.getName())) {
                foundGreaterThanOrEqual = true;
            } else if (compOpt.getName().equals(LessThan.class.getName())) {
                foundLessThan = true;
            } else if (compOpt.getName().equals(LessThanOrEqual.class.getName())) {
                foundLessThanOrEqual = true;
            }
        }
        assertTrue(foundEqual & foundNotEqual & foundGreaterThan
                & foundGreaterThanOrEqual & foundLessThan & foundLessThanOrEqual);
    }

    @Test
    public void noOptFound() {
        try {
            handler.getCompareOp("blah");
            fail("Should not contain key blah");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Null compare op for specified key"));
        }
    }

    @Test
    public void pComparsionLookup() {
        boolean foundLong = false;
        boolean foundString = false;
        boolean foundInt = false;
        boolean foundDouble = false;
        for(String type : handler.pComparisonKeyset()) {
            Class<? extends PrimitiveCompare> pCompare = handler.getPrimativeComparison(type);
            if(pCompare.getName().equals(DoubleCompare.class.getName())) {
                foundDouble = true;
            } else if (pCompare.getName().equals(LongCompare.class.getName())) {
                foundLong = true;
            } else if (pCompare.getName().equals(IntCompare.class.getName())) {
                foundInt = true;
            } else if (pCompare.getName().equals(StringCompare.class.getName())) {
                foundString = true;
            }
        }
        assertTrue(foundDouble & foundLong & foundInt & foundString);
    }
}
