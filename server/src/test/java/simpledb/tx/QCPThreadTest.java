/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package simpledb.tx;

/**
 *
 * @author hawkol01
 */
public class QCPThreadTest {
    
    public void QCPThreadTest1() throws InterruptedException {
        int i = 0;
        while (i < 10) {
            Transaction t = new Transaction();
            Thread.sleep(1000);
            t.commit();
            i++;
        }
        
        int j = 0;
        while (j < 10) {
            Transaction t = new Transaction();
            
        }
    }
}
