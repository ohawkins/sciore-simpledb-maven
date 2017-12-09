package simpledb.query;

import simpledb.record.Schema;
import java.util.Collection;

/**
 * The Plan class corresponding to the <i>semijoin</i>
 * relational algebra operator.
 *
 * @author ohawkins
 */
public class SemiJoinPlan implements Plan {

    private Plan p1;
    private Plan p2;
    private Predicate pred;
    private Schema schema = new Schema();

    /**
     * Creates a new semijoin node in the query tree, having the specified
     * subquery and field list.
     *
     * @param p1 the first subquery
     * @param p2 the second subquery
     * @param pred the predicate
     */
    public SemiJoinPlan(Plan p1, Plan p2, Predicate pred) {
        this.p1 = p1;
        this.p2 = p2;
        this.pred = pred;
    }

    /**
     * Creates a project scan for this query.
     *
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new SemiJoinScan(s1, s2, this.pred);
    }

    /**
     * Estimates the number of block accesses in the projection, which is the
     * same as in the underlying query.
     *
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
    }

    /**
     * Estimates the number of output records in the projection, which is the
     * same as in the underlying query.
     *
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p1.recordsOutput(); // / pred.reductionFactor()
        //TODO: find out correct reduction factor
    }

    /**
     * Estimates the number of distinct field values in the projection, which is
     * the same as in the underlying query.
     *
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname)) {
            return p1.distinctValues(fldname);
        } else {
            return p2.distinctValues(fldname);
        }
    }

    /**
     * Returns the schema of the join, which is the union of the schemas of
     * the underlying queries.
     * 
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }
}
