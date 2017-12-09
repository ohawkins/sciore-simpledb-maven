package simpledb.query;

/**
 * The scan class corresponding to the <i>semijoin</i> relational algebra
 * operator.
 *
 * @author ohawkins
 */
public class SemiJoinScan implements Scan {

    private Scan s1, s2;
    private Predicate pred;
    private Scan left;

    /**
     * Creates a join scan having the two underlying scans.
     *
     * @param s1 the LHS scan
     * @param s2 the RHS scan
     */
    public SemiJoinScan(Scan s1, Scan s2, Predicate pred) {
        this.left = new SelectScan(s1, pred);
        this.pred = pred;
        left.next();
    }

    /**
     * Positions the scan before its first record. In other words, the LHS scan
     * is positioned at its first record, and the RHS scan is positioned before
     * its first record.
     *
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        left.beforeFirst();
    }

    /**
     * Moves the scan to the next record. The method moves to the next RHS
     * record, if possible. Otherwise, it moves to the next LHS record and the
     * first RHS record. If there are no more LHS records, the method returns
     * false.
     *
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        while (left.next()) {
            if (pred.isSatisfied(left)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Closes both underlying scans.
     *
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        left.close();
    }

    public Constant getVal(String fldname) {
        return left.getVal(fldname);
    }

    public int getInt(String fldname) {
        return left.getInt(fldname);
    }

    public String getString(String fldname) {
        return left.getString(fldname);
    }

    public boolean hasField(String fldname) {
        return left.hasField(fldname);
    }
}
