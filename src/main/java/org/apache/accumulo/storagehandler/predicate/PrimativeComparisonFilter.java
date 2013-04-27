package org.apache.accumulo.storagehandler.predicate;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.storagehandler.predicate.compare.CompareOp;
import org.apache.accumulo.storagehandler.predicate.compare.PrimitiveCompare;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/23/13
 * Time: 11:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrimativeComparisonFilter extends WholeRowIterator {

    public static final String FILTER_PREFIX = "accumulo.filter.compare.iterator.";
    public static final String P_COMPARE_CLASS = "accumulo.filter.iterator.p.compare.class";
    public static final String COMPARE_OPT_CLASS = "accumulo.filter.iterator.compare.opt.class";
    public static final String CONST_VAL = "accumulo.filter.iterator.const.val";
    public static final String COLUMN = "accumulo.filter.iterator.qual";
    private String qual;
    private String cf;

    private CompareOp compOpt;

    private static final Logger log = Logger.getLogger(PrimativeComparisonFilter.class);
    private static final Pattern PIPE_PATTERN = Pattern.compile("[|]");

    @Override
    protected boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
        SortedMap<Key,Value> items;
        boolean allow;
        try {
            while(keys.get(0).getColumnFamily().getBytes().length == 0) {
                items = decodeRow(keys.get(0), values.get(0));
                keys = Lists.newArrayList(items.keySet());
                values = Lists.newArrayList(items.values());
            }
            allow = filterNormal(keys, values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return allow;
    }

    private boolean filterNormal(Collection<Key> keys, Collection<Value> values) {
        Iterator<Key> kIter = keys.iterator();
        Iterator<Value> vIter = values.iterator();
        while(kIter.hasNext()) {
            Key k = kIter.next();
            Value v = vIter.next();
            if(matchQualAndFam(k) ) {
                return compOpt.accept(v.get());
            }
        }
        return false;
    }

    private boolean matchQualAndFam(Key k) {
        return k.getColumnQualifier().toString().equals(qual) &&
               k.getColumnFamily().toString().equals(cf);
    }


    @Override
    public void init(SortedKeyValueIterator<Key,Value> source,
                     Map<String,String> options,
                     IteratorEnvironment env) throws IOException {

        try {
            super.init(source, options ,env);
            String col = options.get(COLUMN);
            String[] splits = PIPE_PATTERN.split(col);
            if(splits.length !=2)
                throw new IOException("Malformed " + COLUMN + ": " + col);
            cf = splits[0];
            qual = splits[1];
            Class<?> pClass = Class.forName(options.get(P_COMPARE_CLASS));
            Class<?> cClazz = Class.forName(options.get(COMPARE_OPT_CLASS));
            PrimitiveCompare pCompare = pClass.asSubclass(PrimitiveCompare.class).newInstance();
            compOpt = cClazz.asSubclass(CompareOp.class).newInstance();
            String b64Const = options.get(CONST_VAL);
            String constStr = new String(Base64.decodeBase64(b64Const.getBytes()));
            log.info("constant: " + constStr);
            log.info("copt: " + cClazz.getName());
            log.info("pCompare: " + pClass.getName());
            log.info("cf: " + cf);
            log.info("qual:" + qual);
            byte [] constant = constStr.getBytes();
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
}
