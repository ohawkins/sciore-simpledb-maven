/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import java.util.logging.Level;
import java.util.logging.Logger;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.CheckpointRecord;

/**
 *
 * @author hawkol01
 */
public class QCPThread implements Runnable {
    
    private static Object qcpLock = new Object();
    
    @Override
    public void run() {
        while (!Transaction.inProgress) { // if there are any active transactions
            synchronized (qcpLock) {
                try {
                    qcpLock.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(QCPThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        
        // Flush all buffers
        SimpleDB.bufferMgr().definitelyFlushAll();
        
        // Write a new CheckpointRecord
        CheckpointRecord ckptRec = new CheckpointRecord();
        int recNum = ckptRec.writeToLog();
        // Flush the log
        SimpleDB.logMgr().flush(recNum);
        

        // Set inProgress to false
        Transaction.inProgress = false;
        // Transaction lock is available -> notifyAll()
        Transaction.tLock.notifyAll();
        }
    }
}
