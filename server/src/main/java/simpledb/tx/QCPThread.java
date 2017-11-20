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
public class QCPThread implements Runnable {

    @Override
    public void run() {
        while (currentTransactions.size != 0) {
            synchronized (Transaction.lock) 
                try {
                    ckptLock.wait();
                } catch  {} 
            }
        // flush buffers (use on each buffer in bufferpool)
        // write to log file
        // 
        }
    }
