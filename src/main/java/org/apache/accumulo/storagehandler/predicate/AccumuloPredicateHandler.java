package org.apache.accumulo.storagehandler.predicate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.storagehandler.AccumuloHiveUtils;
import org.apache.accumulo.storagehandler.AccumuloSerde;
import org.apache.accumulo.storagehandler.predicate.compare.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: bfemiano
 * Date: 4/21/13
 * Time: 12:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class AccumuloPredicateHandler {

    private static AccumuloPredicateHandler handler = new AccumuloPredicateHandler();
    private static final Logger log = Logger.getLogger(AccumuloPredicateHandler.class);
    private static Map<String, Class<? extends CompareOp>> compareOps = Maps.newHashMap();
    private static Map<String, Class<? extends PrimitiveCompare>> pComparisons = Maps.newHashMap();
    private static int iteratorCount = 0;
    static {
        log.setLevel(Level.INFO);
        compareOps.put(GenericUDFOPEqual.class.getName(), Equal.class);
        compareOps.put(GenericUDFOPNotEqual.class.getName(), NotEqual.class);
        compareOps.put(GenericUDFOPGreaterThan.class.getName(), GreaterThan.class);
        compareOps.put(GenericUDFOPEqualOrGreaterThan.class.getName(), GreaterThanOrEqual.class);
        compareOps.put(GenericUDFOPEqualOrLessThan.class.getName(), LessThanOrEqual.class);
        compareOps.put(GenericUDFOPLessThan.class.getName(), LessThan.class);
        compareOps.put(UDFLike.class.getName(), Like.class);

        pComparisons.put("bigint", LongCompare.class);
        pComparisons.put("int", IntCompare.class);
        pComparisons.put("double", DoubleCompare.class);
        pComparisons.put("string", StringCompare.class);
    }

    public static AccumuloPredicateHandler getInstance() {
        return handler;
    }

    public Set<String> compareOpKeyset() {
        return compareOps.keySet();
    }

    public Set<String> pComparisonKeyset() {
        return pComparisons.keySet();
    }

    public Class<? extends CompareOp> getCompareOp(String udfType) {
        if(!compareOps.containsKey(udfType))
            throw new RuntimeException("Null compare op for specified key: " + udfType);
        return compareOps.get(udfType);
    }

    public Class<? extends PrimitiveCompare> getPrimativeComparison(String type) {
        if(!pComparisons.containsKey(type))
            throw new RuntimeException("Null primitive comparison for specified key: " + type);
        return pComparisons.get(type);
    }

    private AccumuloPredicateHandler(){}

    /**
     * Loop through search conditions. Build ranges
     * for predicates involving rowID column, if any.
     *
     * @param conf
     * @return
     */
    public Collection<Range> getRanges(JobConf conf)
        throws SerDeException {
        List<Range> ranges = Lists.newArrayList();
        String rowIdCol = AccumuloHiveUtils.hiveColForRowID(conf);
        if(rowIdCol == null)
            return ranges;
        for(IndexSearchCondition sc : getSearchConditions(conf)) {
            if(rowIdCol.equals(sc.getColumnDesc().getColumn()))
                ranges.add(toRange(sc));
        }
        return ranges;
    }

    public List<IteratorSetting> getIterators(JobConf conf)
            throws SerDeException{
        List<IteratorSetting> itrs = Lists.newArrayList();
        if(conf.get(AccumuloSerde.NO_ITERATOR_PUSHDOWN) != null)  {
            log.info("Iterator pushdown is disabled for this table");
            return itrs;
        }

        String rowIdCol = AccumuloHiveUtils.hiveColForRowID(conf);
        for(IndexSearchCondition sc : getSearchConditions(conf)) {
            log.info(sc.getComparisonExpr().getExprString());
            log.info("comp op " + sc.getComparisonOp());
            log.info("const " + sc.getConstantDesc());
            log.info("num iterators = " + itrs.size());
            String col = sc.getColumnDesc().getColumn();
            if(rowIdCol == null || !rowIdCol.equals(col))
                itrs.add(toSetting(conf, col, sc));
        }
        return itrs;
    }

    public Range toRange(IndexSearchCondition sc)
        throws SerDeException {
        Range range;
        PushdownTuple tuple = new PushdownTuple(sc);
        Text constText = new Text(tuple.getConstVal());
        if(tuple.getcOpt() instanceof Equal) {
            range = new Range(constText, true, constText, true); //start inclusive - end inclusive
        } else if (tuple.getcOpt() instanceof GreaterThanOrEqual) {
            range = new Range(constText, null); //start inclusive to infinity inclusive
        } else if (tuple.getcOpt() instanceof GreaterThan) {
            range = new Range(constText, false, null, true);  //start exclusive to infinity inclusive
        } else if (tuple.getcOpt() instanceof LessThanOrEqual) {
            range = new Range(null, true, constText, true); //neg-infinity - start inclusive
        } else if (tuple.getcOpt() instanceof LessThan) {
            range = new Range(null, true, constText, false); //neg-infinity - start exclusive
        } else {
            throw new SerDeException("Unsupported comparison operator involving rowid: " +
                    tuple.getcOpt().getClass().getName() + " only =, <, <=, >, >=");
        }
        return range;
    }

    public IteratorSetting toSetting(JobConf conf, String hiveCol, IndexSearchCondition sc)
            throws SerDeException{
        IteratorSetting is = new IteratorSetting(1, PrimativeComparisonFilter.FILTER_PREFIX + iteratorCount++,
                PrimativeComparisonFilter.class);

        PushdownTuple tuple = new PushdownTuple(sc);
        is.addOption(PrimativeComparisonFilter.P_COMPARE_CLASS, tuple.getpCompare().getClass().getName());
        is.addOption(PrimativeComparisonFilter.COMPARE_OPT_CLASS, tuple.getcOpt().getClass().getName());
        is.addOption(PrimativeComparisonFilter.CONST_VAL,  Arrays.toString(Base64.encodeBase64(tuple.getConstVal())));
        is.addOption(PrimativeComparisonFilter.QUAL, AccumuloHiveUtils.hiveToAccumulo(hiveCol, conf));

        return is;
    }

    public List<IndexSearchCondition> getSearchConditions(JobConf conf) {
        List<IndexSearchCondition> sConditions = Lists.newArrayList();
        String filteredExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filteredExprSerialized == null)
            return sConditions;
        ExprNodeDesc filterExpr = Utilities.deserializeExpression(filteredExprSerialized, conf);
        log.info("expr type: " + filterExpr.getClass().getName());
        IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
        log.info("full expression string: " + filterExpr.getExprString());
        for(ExprNodeDesc exp : filterExpr.getChildren()) {
            log.info("child: " + exp.getExprString());
        }
        ExprNodeDesc residual = analyzer.analyzePredicate(filterExpr, sConditions);
        if(residual != null)
            throw new RuntimeException("Unexpected residual predicate: " + residual.getExprString());
        return sConditions;
    }

    public DecomposedPredicate decompose(JobConf conf, ExprNodeDesc desc) {


        IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
        log.info("decomposing predicate");
        List<IndexSearchCondition> sConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(desc, sConditions);
        if(sConditions.size() == 0){
            log.info("nothing to decompose. Returning");
            return null;
        }
        log.info("setting up search indexes of length = " + sConditions.size());
        DecomposedPredicate decomposedPredicate  = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(sConditions);
        decomposedPredicate.residualPredicate = residualPredicate;
        return decomposedPredicate;
    }

    private IndexPredicateAnalyzer newAnalyzer(JobConf conf)
    {
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        analyzer.clearAllowedColumnNames();
        for(String op : compareOpKeyset()) {
            analyzer.addComparisonOp(op);
        }

        String hiveColProp = conf.get(serdeConstants.LIST_COLUMNS);
        List<String> hiveCols = AccumuloHiveUtils.parseColumnMapping(hiveColProp);
        for(String col : hiveCols)
            analyzer.allowColumnName(col);
        return analyzer;
    }

    public static class PushdownTuple {

        private byte[] constVal;
        private PrimitiveCompare pCompare;
        private CompareOp cOpt;

        public PushdownTuple(IndexSearchCondition sc)
                throws SerDeException{
            init(sc);
        }

        private void init(IndexSearchCondition sc) throws
                SerDeException {

            try{
                ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
                String type = sc.getColumnDesc().getTypeString();
                Class<? extends PrimitiveCompare> pClass = pComparisons.get(type);
                Class<? extends CompareOp> cClass = compareOps.get(sc.getComparisonOp());
                if(cClass == null)
                    throw new SerDeException("no CompareOp subclass mapped for operation: " + sc.getComparisonOp());
                if(pClass == null)
                    throw new SerDeException("no PrimitiveCompare subclass mapped for type: " + type);
                pCompare = pClass.newInstance();
                cOpt = cClass.newInstance();
                Writable writable  = (Writable)eval.evaluate(null);
                constVal = getConstantAsBytes(writable);
            } catch (ClassCastException cce) {
                log.info(StringUtils.stringifyException(cce));
                throw new SerDeException("Currently only int,double,string,bigint types are supported. Found: " +
                        sc.getConstantDesc().getTypeString());
            } catch (HiveException e) {
                throw new SerDeException(e);
            } catch (InstantiationException e) {
                throw new SerDeException(e);
            } catch (IllegalAccessException e) {
                throw new SerDeException(e);
            }

        }

        public byte[] getConstVal() {
            return constVal;
        }

        public PrimitiveCompare getpCompare() {
            return pCompare;
        }

        public CompareOp getcOpt() {
            return cOpt;
        }

        public byte[] getConstantAsBytes(Writable writable)
                throws SerDeException{
            if(pCompare instanceof StringCompare) {
                return writable.toString().getBytes();
            } else if (pCompare instanceof DoubleCompare) {
                byte [] bts = new byte[8];
                double val = ((DoubleWritable) writable).get();
                ByteBuffer.wrap(bts).putDouble(val);
                return bts;
            } else if (pCompare instanceof IntCompare) {
                byte [] bts = new byte[4];
                int val = ((IntWritable) writable).get();
                ByteBuffer.wrap(bts).putInt(val);
                return bts;
            } else if (pCompare instanceof LongCompare) {
                byte [] bts = new byte[8];
                long val = ((LongWritable) writable).get();
                ByteBuffer.wrap(bts).putLong(val);
                return bts;
            } else {
                throw new SerDeException("Unsupported primitive category: " + pCompare.getClass().getName());
            }
        }

    }
}
