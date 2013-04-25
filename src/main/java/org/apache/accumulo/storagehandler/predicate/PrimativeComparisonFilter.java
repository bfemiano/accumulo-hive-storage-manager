package org.apache.accumulo.storagehandler.predicate;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.storagehandler.predicate.compare.CompareOp;
import org.apache.accumulo.storagehandler.predicate.compare.PrimitiveCompare;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/23/13
 * Time: 11:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrimativeComparisonFilter extends Filter {

    public static final String FILTER_PREFIX = "accumulo.filter.compare.iterator.";
    public static final String P_COMPARE_CLASS = "accumulo.filter.iterator.p.compare.class";
    public static final String COMPARE_OPT_CLASS = "accumulo.filter.iterator.compare.opt.class";
    public static final String CONST_VAL = "accumulo.filter.iterator.const.val";
    public static final String QUAL = "accumulo.filter.iterator.qual";
    private String qual;

    private CompareOp compOpt;

    @Override
    public boolean accept(Key k, Value v) {
       return compOpt.accept(v.get());
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source,
                     Map<String,String> options,
                     IteratorEnvironment env) throws IOException {

       try {
           qual = options.get(QUAL);
           Class<?> pClass = Class.forName(options.get(P_COMPARE_CLASS));
           Class<?> cClazz = Class.forName(options.get(COMPARE_OPT_CLASS));
           PrimitiveCompare pCompare = pClass.asSubclass(PrimitiveCompare.class).newInstance();
           compOpt = cClazz.asSubclass(CompareOp.class).newInstance();
           byte [] constant = Base64.decodeBase64(options.get(CONST_VAL).getBytes());
           pCompare.init(constant);
           compOpt.setPrimativeCompare(pCompare);
       } catch (ClassNotFoundException e) {
           throw new IOException(e);
       } catch (InstantiationException e) {
           throw new IOException(e);
       } catch (IllegalAccessException e) {
           throw new IOException(e);
       }
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String, String> options = Maps.newHashMap();
        String name = getClass().getName();
        String description = "Filter key/value pairs based on qualifier, typed value, and comparison opt.";
        options.put(QUAL, "Qualifier to compare against");
        options.put(P_COMPARE_CLASS, "PrimitiveComparison subclass that encapsulates a constant value.");
        options.put(COMPARE_OPT_CLASS, "Comparison operation type. Takes in PrimitiveComparison subclass instance.");
        options.put(CONST_VAL, "Base64 encoded byte[] value of constant to initialize PrimitiveComparison.");
        return new IteratorOptions(name, description, options, null);

    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
       boolean valid = true;
       valid = validate(QUAL, options);
       valid = validate(P_COMPARE_CLASS, options);
       valid = validate(COMPARE_OPT_CLASS, options);
       valid = validate(CONST_VAL, options);
       return valid;
    }

    private boolean validate(String key, Map<String,String> options) {
        if(options.containsKey(key)) {
            if(options.get(key) == null)
                return false;
        } else {
            return false;
        }
        return true;
    }
}
