package simpledb.buffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.file.Block;

/**
 *
 * @author yasiro01
 */
public class BasicBufferMgrTest {
  
  public BasicBufferMgrTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
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

  /**
   * Test of flushAll method, of class BasicBufferMgr.
   */
  @Test
  public void testFlushAll() {
    System.out.println("flushAll");
    int txnum = 0;
    BasicBufferMgr instance = null;
    instance.flushAll(txnum);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of pin method, of class BasicBufferMgr.
   */
  @Test
  public void testPin() {
    System.out.println("pin");
    Block blk = null;
    BasicBufferMgr instance = null;
    Buffer expResult = null;
    Buffer result = instance.pin(blk);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of pinNew method, of class BasicBufferMgr.
   */
  @Test
  public void testPinNew() {
    System.out.println("pinNew");
    String filename = "";
    PageFormatter fmtr = null;
    BasicBufferMgr instance = null;
    Buffer expResult = null;
    Buffer result = instance.pinNew(filename, fmtr);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of unpin method, of class BasicBufferMgr.
   */
  @Test
  public void testUnpin() {
    System.out.println("unpin");
    Buffer buff = null;
    BasicBufferMgr instance = null;
    instance.unpin(buff);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of available method, of class BasicBufferMgr.
   */
  @Test
  public void testAvailable() {
    System.out.println("available");
    BasicBufferMgr instance = null;
    int expResult = 0;
    int result = instance.available();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
  
}
