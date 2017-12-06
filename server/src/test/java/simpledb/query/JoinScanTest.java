/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.parse.Parser;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author hawkol01
 */
public class JoinScanTest {

    public JoinScanTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        SimpleDB.init("studentdb");
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test 
    public void testSelectByConstantScan() {
        System.out.println("JOIN");
        Transaction tx = new Transaction();
        String qry = "select sname, dname from student join dept on majorid = did";
        Parser p = new Parser(qry);
        Plan studentTblPlan = new TablePlan("student", tx);
        Plan deptTblPlan = new TablePlan("dept", tx);
//        Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
        Plan joinPlan = new JoinPlan(studentTblPlan, deptTblPlan, 
                new Predicate(new Term(
                        new FieldNameExpression("majorid"), 
                        new FieldNameExpression("did"))));
        Scan s = joinPlan.open();
        tx.commit();
        int records = 0;
        while (s.next()) {
            System.out.printf("%10s%n", s.getVal("sname").toString());
            records++;
        }
        assertEquals(9, records);
    }
}
