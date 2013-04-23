package org.apache.accumulo.storagehandler.predicate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.storagehandler.AccumuloHiveUtils;
import org.apache.accumulo.storagehandler.AccumuloSerde;
import org.apache.accumulo.storagehandler.predicate.compare.*;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private static Map<String, Class<? extends PrimativeCompare>> pComparisons = Maps.newHashMap();
    static {
        log.setLevel(Level.INFO);
        compareOps.put(GenericUDFOPEqual.class.getName(), Equal.class);
        compareOps.put(GenericUDFOPNotEqual.class.getName(), NotEqual.class);
        compareOps.put(GenericUDFOPGreaterThan.class.getName(), GreaterThan.class);
        compareOps.put(GenericUDFOPEqualOrGreaterThan.class.getName(), GreaterThanOrEqual.class);
        compareOps.put(GenericUDFOPEqualOrLessThan.class.getName(), LessThan.class);
        compareOps.put(GenericUDFOPLessThan.class.getName(), LessThanOrEqual.class);

        pComparisons.put("long", LongCompare.class);
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

    public Class<? extends PrimativeCompare> getPrimativeComparison(String type) {
        if(!pComparisons.containsKey(type))
            throw new RuntimeException("Null primative comparison for specified key: " + type);
        return pComparisons.get(type);
    }

    private AccumuloPredicateHandler(){}

    public List<IteratorSetting> getIterators(JobConf conf) {
        List<IteratorSetting> iterators = Lists.newArrayList();
        String filteredExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filteredExprSerialized == null)
            return iterators;
        ExprNodeDesc filterExpr = Utilities.deserializeExpression(filteredExprSerialized, conf);

        List<IndexSearchCondition> sConditions = new ArrayList<IndexSearchCondition>();
        IndexPredicateAnalyzer analyzer = newAnalyzer(conf);
        log.info("full expression string: " + filterExpr.getExprString());
        for(ExprNodeDesc exp : filterExpr.getChildren()) {
            log.info("child: " + exp.getExprString());
        }
        ExprNodeDesc residual = analyzer.analyzePredicate(filterExpr, sConditions);
        if(residual != null)
            throw new RuntimeException("Unexpected residual predicate: " + residual.getExprString());
        //TODO: the difficult part. Convert each condition into one or more appropriate iterators.
        IndexSearchCondition sc = sConditions.get(0);
        log.info(sc.getComparisonExpr().getExprString());
        log.info("comp op " + sc.getComparisonOp());
        log.info("const " + sc.getConstantDesc());
        log.info("num iterators = " + iterators.size());

        return iterators;
    }

    public DecomposedPredicate decompose(JobConf conf,
                                         AccumuloSerde serde,
                                         ExprNodeDesc desc) {
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
}
