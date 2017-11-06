package simpledb.buffer;

import java.sql.Timestamp;
import java.util.Arrays;
import simpledb.file.*;

/*
NOTES BY O. HAWKINS:

Added functionality for alternate buffer replacement algorithms FIFO, LRU, and Clock.
The hardest function by far was Clock, mainly since there wasn't much code shared between
the other functions. FIFO and LRU are basically the same, but for the Buffer attribute
to be sorted by. There were some changes in the Buffer class, mainly adding information
about when it was pinned / unpinned, and getting and setting this information. 

My main idea was to use timestamps to organize and sort the buffers in the bufferpool.
Each buffer would have two attributes about the the time it was pinned and the time it
was unpinned, timeAdded and timeAccessed, which would be updated each time the buffer 
was pinned or unpinned. Each strategy would use this information differently. The most 
frustration came from working with Timestamps; initially I had set the min value to an 
arbitrary date in the future (it was the estimated date of the singularity, if you're 
interested) and tried to compare the buffers' timeAdded's to it, but it just was not 
working, so I changed it to an arbitrary large number. Clock also was a bit of a pain, 
especially since I had it written (so I thought) and then realized that everything I 
had ever believed was a lie, and part of that fallout was having to rewrite the function. 
Thanks to a number of people also in the third floor lab in Olin, I managed to find the 
solution. It was a harrowing few hours.
*/


/**
 * Manages the pinning and unpinning of buffers to blocks.
 *
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {

    private Buffer[] bufferpool;
    private int numAvailable;
    private int strategy;
    private int buffPointer;

    /**
     * Creates a buffer manager having the specified number of buffer slots.
     * This constructor depends on both the {@link FileMgr} and
     * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
     * {@link simpledb.server.SimpleDB}. Those objects are created during system
     * initialization. Thus this constructor cannot be called until
     * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
     * first.
     *
     * @param numbuffs the number of buffer slots to allocate
     */
    BasicBufferMgr(int numbuffs) {
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        for (int i = 0; i < numbuffs; i++) {
            bufferpool[i] = new Buffer();
        }
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     *
     * @param txnum the transaction's id number
     */
    synchronized void flushAll(int txnum) {
        for (Buffer buff : bufferpool) {
            if (buff.isModifiedBy(txnum)) {
                buff.flush();
            }
        }
    }

    /**
     * Pins a buffer to the specified block. If there is already a buffer
     * assigned to that block then that buffer is used; otherwise, an unpinned
     * buffer from the pool is chosen. Returns a null value if there are no
     * available buffers.
     *
     * @param blk a reference to a disk block
     * @return the pinned buffer
     */
    synchronized Buffer pin(Block blk) {
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null) {
                return null;
            }
            buff.assignToBlock(blk);
        }

        if (!buff.isPinned()) {
            numAvailable--;
        }
        buff.pin();
        buff.updateTimeAdded();
        buffPointer = Arrays.asList(bufferpool).indexOf(buff) + 1;
        return buff;
    }

    /**
     * Allocates a new block in the specified file, and pins a buffer to it.
     * Returns null (without allocating the block) if there are no available
     * buffers.
     *
     * @param filename the name of the file
     * @param fmtr a pageformatter object, used to format the new block
     * @return the pinned buffer
     */
    synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
        Buffer buff = chooseUnpinnedBuffer();
        if (buff == null) {
            return null;
        }
        buff.assignToNew(filename, fmtr);
        numAvailable--;
        buff.pin();
        // When a buffer is read in from memory, update its attribute
        buff.updateTimeAdded();
        buffPointer = Arrays.asList(bufferpool).indexOf(buff) + 1;
        return buff;
    }

    /**
     * Unpins the specified buffer.
     *
     * @param buff the buffer to be unpinned
     */
    synchronized void unpin(Buffer buff) {
        buff.unpin();
        // Each time the buffer is unpinned, its attribute of timeLastAccessed is updated to the current time
        buff.updateTimeAccessed();
        if (!buff.isPinned()) {
            numAvailable++;
            buffPointer = Arrays.asList(bufferpool).indexOf(buff) + 1;
        }
    }

    /**
     * Returns the number of available (i.e. unpinned) buffers.
     *
     * @return the number of available buffers
     */
    int available() {
        return numAvailable;
    }

    private Buffer findExistingBuffer(Block blk) {
        for (Buffer buff : bufferpool) {
            Block b = buff.block();
            if (b != null && b.equals(blk)) {
                return buff;
            }
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        switch (this.strategy) {
            case 0:
                return useNaiveStrategy();
            case 1:
                return useFIFOStrategy();
            case 2:
                return useLRUStrategy();
            case 3:
                return useClockStrategy();
            default:
                return null;
        }
    }

    /**
     * @return Allocated buffers
     */
    public Buffer[] getBuffers() {
        return this.bufferpool;
    }

    /**
     * Set buffer selection strategy
     *
     * @param s (0 - Naive, 1 - FIFO, 2 - LRU, 3 - Clock)
     */
    public void setStrategy(int s) {
        this.strategy = s;
    }

    /**
     * Naive buffer selection strategy
     *
     * @return
     */
    private Buffer useNaiveStrategy() {
        for (Buffer buff : bufferpool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }

    /**
     * FIFO buffer selection strategy
     *
     * @return
     */
    private Buffer useFIFOStrategy() {
        long min = 1000000000000000L;
        Buffer leastRecentlyAddedBuffer = null;
        for (Buffer buff : bufferpool) {
            // if the buffer is pinned, ignore
            if (!buff.isPinned()) {
                // the first time around, this will be null
                if (leastRecentlyAddedBuffer == null) {
                    leastRecentlyAddedBuffer = buff;
                }
                // if the buffer had a time in
                if (buff.getTimeAdded() != 0) {
                    // compare its time in to the min value for time in
                    if (min - buff.getTimeAdded() > 0) {
                        // set the min to the new, smaller value and update buffer
                        min = buff.getTimeAdded();
                        leastRecentlyAddedBuffer = buff;
                    }
                }
            }
        }
        // this will be null if there are no unpinned buffers
        return leastRecentlyAddedBuffer;
    }

    /**
     * LRU buffer selection strategy
     *
     * @return
     */
    private Buffer useLRUStrategy() {
        long min = 1000000000000000L;
        Buffer leastRecentlyAccessedBuffer = null;
        for (Buffer buff : bufferpool) {
            // if the buffer is pinned, ignore
            if (!buff.isPinned()) {
                // the first time around, this will be null
                if (leastRecentlyAccessedBuffer == null) {
                    leastRecentlyAccessedBuffer = buff;
                }
                // if the buffer had a time out
                if (buff.getTimeAccessed() != 0) {
                    // compare its time in to the min value for time out
                    if (min - buff.getTimeAccessed() > 0) {
                        // set the min to the new, smaller value and update buffer
                        min = buff.getTimeAccessed();
                        leastRecentlyAccessedBuffer = buff;
                    }
                }
            }
        }
        return leastRecentlyAccessedBuffer;
    }

    /**
     * Clock buffer selection strategy
     *
     * @return
     */
 
    // I would like to dedicate this code to Alex Davis. May he never get a parking ticket.
    
    private Buffer useClockStrategy() {
        // make a new thing to iterate over
        Buffer[] newPool = new Buffer[bufferpool.length];
        
        // add everything after the pointer from bufferpool to the new pool
        int index = 0;
        for (int i = buffPointer; i < bufferpool.length; i++) {
            newPool[index] = bufferpool[i];
            index++;
        }
        
        // add everything before the pointer from bufferpool to the new pool
        for (int i = 0; i < buffPointer; i++) {
            newPool[index] = bufferpool[i];
            index++;
        }
        
        // use naive and iterate over the newly constructed pool
        for (Buffer buff : newPool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }
}
