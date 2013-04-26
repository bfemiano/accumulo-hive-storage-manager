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

import java.io.IOException;
import java.util.*;

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
    public static final String QUAL = "accumulo.filter.iterator.qual";
    private String qual;

    private CompareOp compOpt;


    @Override
    protected boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
        SortedMap<Key,Value> items;
        boolean keepDecoding = true;
        boolean allow = true;
        while(keepDecoding) {
            try {
                items = decodeRow(keys.get(0), values.get(0));
                keys = Lists.newArrayList(items.keySet());
                values = Lists.newArrayList(items.values());

            } catch (IOException e) {
                allow = filterNormal(keys, values);
                keepDecoding = false;
            }
        }
        return allow;
    }

    private boolean filterNormal(Collection<Key> keys, Collection<Value> values) {
        Iterator<Key> kIter = keys.iterator();
        Iterator<Value> vIter = values.iterator();
        while(kIter.hasNext()) {
            Key k = kIter.next();
            Value v = vIter.next();
            if(k.getColumnQualifier().toString().equals(qual)) {
                return compOpt.accept(v.get());
            }
        }
        return false;
    }


    @Override
    public void init(SortedKeyValueIterator<Key,Value> source,
                     Map<String,String> options,
                     IteratorEnvironment env) throws IOException {

       try {
           super.init(source, options ,env);
           qual = options.get(QUAL);
           Class<?> pClass = Class.forName(options.get(P_COMPARE_CLASS));
           Class<?> cClazz = Class.forName(options.get(COMPARE_OPT_CLASS));
           PrimitiveCompare pCompare = pClass.asSubclass(PrimitiveCompare.class).newInstance();
           compOpt = cClazz.asSubclass(CompareOp.class).newInstance();
           String b64Const = options.get(CONST_VAL);
           String constStr = new String(Base64.decodeBase64(b64Const.getBytes()));
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
