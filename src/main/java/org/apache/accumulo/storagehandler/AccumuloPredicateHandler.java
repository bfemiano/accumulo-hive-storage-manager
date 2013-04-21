package org.apache.accumulo.storagehandler;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
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
    static {
        log.setLevel(Level.INFO);
    }

    public static AccumuloPredicateHandler getInstance() {
        return handler;
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
        ExprNodeDesc residual = analyzer.analyzePredicate(filterExpr, sConditions);
        if(residual != null)
            throw new RuntimeException("Unexpected residual predicate: " + residual.getExprString());
        //now the difficult part. Convert each condition into one or more appropriate iterators.
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
        analyzer.addComparisonOp(GenericUDFOPEqual.class.getName());
        analyzer.addComparisonOp(GenericUDFOPNotEqual.class.getName());
        analyzer.addComparisonOp(GenericUDFOPGreaterThan.class.getName());
        analyzer.addComparisonOp(GenericUDFOPEqualOrGreaterThan.class.getName());
        analyzer.addComparisonOp(GenericUDFOPEqualOrLessThan.class.getName());
        analyzer.addComparisonOp(GenericUDFOPLessThan.class.getName());

        String hiveColProp = conf.get(serdeConstants.LIST_COLUMNS);
        List<String> hiveCols = AccumuloHiveUtils.parseColumnMapping(hiveColProp);
        for(String col : hiveCols)
            analyzer.allowColumnName(col);
        return analyzer;
    }
}
