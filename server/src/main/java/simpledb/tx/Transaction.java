package simpledb.tx;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.concurrency.ConcurrencyMgr;
import simpledb.tx.recovery.RecoveryMgr;

/**
 * Provides transaction management for clients, ensuring that all transactions
 * are serializable, recoverable, and in general satisfy the ACID properties.
 *
 * @author Edward Sciore
 */
public class Transaction {

    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private RecoveryMgr recoveryMgr;
    private ConcurrencyMgr concurMgr;
    private int txnum;
    private BufferList myBuffers = new BufferList();
    private static ArrayList<Transaction> activeTx = new ArrayList<Transaction>();
    private static boolean inProgress = false;
    private static Object tLock = new Object();

    /**
     * Creates a new transaction and its associated recovery and concurrency
     * managers. This constructor depends on the file, log, and buffer managers
     * that it gets from the class {@link simpledb.server.SimpleDB}. Those
     * objects are created during system initialization. Thus this constructor
     * cannot be called until either
     * {@link simpledb.server.SimpleDB#init(String)} or
     * {@link simpledb.server.SimpleDB#initFileLogAndBufferMgr(String)} or is
     * called first.
     */
    public Transaction() {
        // Get new transaction number
        txnum = nextTxNumber();
        
        // Acquire transaction lock
        while (inProgress) {
            try {
                tLock.wait(); // keep them waiting
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Transaction.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        // Create everything else
        recoveryMgr = new RecoveryMgr(txnum);
        concurMgr = new ConcurrencyMgr();
        
        // Add to list of active transactions
        activeTx.add(this);
        
        if (txnum % 10 == 0) {
            inProgress = true;
            QCPThread th = new QCPThread();
        }
    }

    /**
     * Commits the current transaction. Flushes all modified buffers (and their
     * log records), writes and flushes a commit record to the log, releases all
     * locks, and unpins any pinned buffers.
     */
    public void commit() {
        recoveryMgr.commit();
        concurMgr.release();
        myBuffers.unpinAll();
        activeTx.remove(this);
        //checkpointWaiting = frusle;
        tLock.notifyAll();
        System.out.println("transaction " + txnum + " committed");
    }

    /**
     * Rolls back the current transaction. Undoes any modified values, flushes
     * those buffers, writes and flushes a rollback record to the log, releases
     * all locks, and unpins any pinned buffers.
     */
    public void rollback() {
        recoveryMgr.rollback();
        concurMgr.release();
        myBuffers.unpinAll();
        activeTx.remove(this);
        System.out.println("transaction " + txnum + " rolled back");
    }

    /**
     * Flushes all modified buffers. Then goes through the log, rolling back all
     * uncommitted transactions. Finally, writes a quiescent checkpoint record
     * to the log. This method is called only during system startup, before user
     * transactions begin.
     */
    public void recover() {
        SimpleDB.bufferMgr().flushAll(txnum);
        recoveryMgr.recover();
    }

    /**
     * Pins the specified block. The transaction manages the buffer for the
     * client.
     *
     * @param blk a reference to the disk block
     */
    public void pin(Block blk) {
        myBuffers.pin(blk);
    }

    /**
     * Unpins the specified block. The transaction looks up the buffer pinned to
     * this block, and unpins it.
     *
     * @param blk a reference to the disk block
     */
    public void unpin(Block blk) {
        myBuffers.unpin(blk);
    }

    /**
     * Returns the integer value stored at the specified offset of the specified
     * block. The method first obtains an SLock on the block, then it calls the
     * buffer to retrieve the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the integer stored at that offset
     */
    public int getInt(Block blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getInt(offset);
    }

    /**
     * Returns the string value stored at the specified offset of the specified
     * block. The method first obtains an SLock on the block, then it calls the
     * buffer to retrieve the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the string stored at that offset
     */
    public String getString(Block blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getString(offset);
    }

    /**
     * Stores an integer at the specified offset of the specified block. The
     * method first obtains an XLock on the block. It then reads the current
     * value at that offset, puts it into an update log record, and writes that
     * record to the log. Finally, it calls the buffer to store the value,
     * passing in the LSN of the log record and the transaction's id.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     */
    public void setInt(Block blk, int offset, int val) {
        concurMgr.xLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        int lsn = recoveryMgr.setInt(buff, offset, val);
        buff.setInt(offset, val, txnum, lsn);
    }

    /**
     * Stores a string at the specified offset of the specified block. The
     * method first obtains an XLock on the block. It then reads the current
     * value at that offset, puts it into an update log record, and writes that
     * record to the log. Finally, it calls the buffer to store the value,
     * passing in the LSN of the log record and the transaction's id.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     */
    public void setString(Block blk, int offset, String val) {
        concurMgr.xLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        int lsn = recoveryMgr.setString(buff, offset, val);
        buff.setString(offset, val, txnum, lsn);
    }

    /**
     * Returns the number of blocks in the specified file. This method first
     * obtains an SLock on the "end of the file", before asking the file manager
     * to return the file size.
     *
     * @param filename the name of the file
     * @return the number of blocks in the file
     */
    public int size(String filename) {
        Block dummyblk = new Block(filename, END_OF_FILE);
        concurMgr.sLock(dummyblk);
        return SimpleDB.fileMgr().size(filename);
    }

    /**
     * Appends a new block to the end of the specified file and returns a
     * reference to it. This method first obtains an XLock on the "end of the
     * file", before performing the append.
     *
     * @param filename the name of the file
     * @param fmtr the formatter used to initialize the new page
     * @return a reference to the newly-created disk block
     */
    public Block append(String filename, PageFormatter fmtr) {
        Block dummyblk = new Block(filename, END_OF_FILE);
        concurMgr.xLock(dummyblk);
        Block blk = myBuffers.pinNew(filename, fmtr);
        unpin(blk);
        return blk;
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }

    /**
     * Creates a quiescent checkpoint, meaning it checks to make sure there are
     * no active transactions running and keeps other transactions from
     * occurring.
     *
     * @return
     */
    public static synchronized Boolean quiescentCkpt() {
        if (!activeTx.isEmpty()) {
            inProgress = true;
            while (!activeTx.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    Logger.getLogger(Transaction.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            QCPThread qcp = new QCPThread();
        }
        return true;
    }
}