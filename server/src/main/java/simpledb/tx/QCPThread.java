/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import java.util.logging.Level;
import java.util.logging.Logger;
import simpledb.server.SimpleDB;

/**
 *
 * @author hawkol01
 */
public class QCPThread implements Runnable {
    
    private static Object qcpLock = new Object();
    private static Boolean inProgress = true;
    
    @Override
    public void run() {
        while (!inProgress) {
            synchronized (qcpLock) {
                try {
                    Thread.sleep(1000);
                    qcpLock.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(QCPThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        
        // Flush all buffers
        SimpleDB.bufferMgr().definitelyFlushAll();
        }
    

    // flush buffers (use on each buffer in bufferpool)
        // write to log file
        // 
        }
    }
