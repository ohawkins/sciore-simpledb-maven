/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package simpledb.tx;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import simpledb.server.SimpleDB;
/**
 *
 * @author hawkol01
 */
public class QCPThreadTest {
    private QCPThread instance;
    @Rule 
    public ExpectedException thrown = ExpectedException.none();
    
    public QCPThreadTest() {
        
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before 
    public void setUp() throws InterruptedException {
        SimpleDB.init("testdb");
        
//        QCPThreadUnitTest1 t1 = new QCPThreadUnitTest1(); new Thread(t1).start();
//        QCPThreadUnitTest1 t2 = new QCPThreadUnitTest1(); new Thread(t2).start();
//        QCPThreadUnitTest1 t3 = new QCPThreadUnitTest1(); new Thread(t3).start();
//        QCPThreadUnitTest1 t4 = new QCPThreadUnitTest1(); new Thread(t4).start();
//        QCPThreadUnitTest1 t5 = new QCPThreadUnitTest1(); new Thread(t5).start();
//        QCPThreadUnitTest1 t6 = new QCPThreadUnitTest1(); new Thread(t6).start();
//        QCPThreadUnitTest1 t7 = new QCPThreadUnitTest1(); new Thread(t7).start();
//        QCPThreadUnitTest1 t8 = new QCPThreadUnitTest1(); new Thread(t8).start();
//        QCPThreadUnitTest1 t9 = new QCPThreadUnitTest1(); new Thread(t9).start();
//        QCPThreadUnitTest1 t10 = new QCPThreadUnitTest1(); new Thread(t10).start();
//        Transaction t1 = new Transaction();
//        Transaction t2 = new Transaction();
//        Transaction t3 = new Transaction();
//        Transaction t4 = new Transaction();
//        Transaction t5 = new Transaction();
//        Transaction t6 = new Transaction();
//        Transaction t7 = new Transaction();
//        Transaction t8 = new Transaction();
//        Transaction t9 = new Transaction();
//        Transaction t10 = new Transaction();
//        Thread.sleep(2000);
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test of run method, of class QCPThread.
     */
    @Test
    public void testRun() {
        System.out.println("run");
        QCPThread instance = new QCPThread();
        instance.run();
    }
}

class QCPThreadUnitTest1 implements Runnable {
    public void run() {
        try {
            int i = 0;
            while (i < 10) {
                Transaction tx = new Transaction();
                Thread.sleep(1000);
                tx.commit();
            }
            
            System.out.println("10 TRANSACTIONS COMMITTED");
            
            int j = 0;
            while (j < 5) {
                Transaction tx = new Transaction();
                Thread.sleep(1000);
                tx.commit();
            }
            
            System.out.println("5 TRANSACTIONS COMMITTED");
            
        } catch (InterruptedException e) {}
    }
}

class QCPThreadUnitTest2 implements Runnable {
    public void run() {
        try {
            int i = 0;
            while (i < 10) {
                Transaction tx = new Transaction();
            }
            Thread.sleep(1000);
        } catch (InterruptedException ex) {}
    }
}
