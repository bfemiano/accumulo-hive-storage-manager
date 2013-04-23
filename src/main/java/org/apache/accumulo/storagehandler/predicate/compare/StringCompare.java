package org.apache.accumulo.storagehandler.predicate.compare;

import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/22/13
 * Time: 1:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class StringCompare implements PrimativeCompare {

    private String constant;

    public StringCompare(String constant) {
        this.constant = constant;
    }

    @Override
    public boolean isEqual(byte[] value) {
        return serialize(value).equals(constant);
    }

    @Override
    public boolean isNotEqual(byte[] value) {
        return !isEqual(value);
    }

    @Override
    public boolean greaterThanOrEqual(byte[] value) {
        return serialize(value).compareTo(constant) >= 0;
    }

    @Override
    public boolean greaterThan(byte[] value) {
        return serialize(value).compareTo(constant) > 0;
    }

    @Override
    public boolean lessThanOrEqual(byte[] value) {
        return serialize(value).compareTo(constant) <= 0;
    }

    @Override
    public boolean lessThan(byte[] value) {
        return serialize(value).compareTo(constant) < 0;
    }

    @Override
    public boolean like(byte[] value) {
        String temp = new String(value).replaceAll("%", "[\\\\\\w]+?");
        Pattern pattern = Pattern.compile(temp);
        boolean match = pattern.matcher(constant).matches();
        return match;
    }

    public String serialize(byte[] value) {
        return new String(value);
    }
}
