/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.temafon.bufferizator;
import com.sleepycat.je.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


/**
 * Fast queue implementation on top of Berkley DB Java Edition.
 *

 * This class is thread-safe.
 */
public class PersistentQueue <T> implements AutoCloseable {

    /**
     * Berkley DB environment
     */
    private final Environment dbEnv;

    /**
     * Berkley DB instance for the queue
     */
    private Database queueDatabase;

    /**
     * Queue cache size - number of element operations it is allowed to loose in case of system crash.
     */
    private final int cacheSize;

    /**
     * This queue name.
     */
    private final String queueName;
    
    /**
     * Using deferred writing
     */
    private final boolean deferred;
    
    /**
     * Database config
     */
    private final DatabaseConfig dbConfig;

    /**
     * Queue operation counter, which is used to sync the queue database to disk periodically.
     */
    private int opsCounter;
    

    /**
     * Creates instance of persistent queue.
     *
     * @param queueEnvPath   queue database environment directory path
     * @param queueName      descriptive queue name
     * @param cacheSize      how often to sync the queue to disk
     */
    public PersistentQueue(final String queueEnvPath,
                 final String queueName,
                 final int cacheSize) {
        // Create parent dirs for queue environment directory
        new File(queueEnvPath).mkdirs();

        // Setup database environment
        final EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setTransactional(false);
        dbEnvConfig.setAllowCreate(true);
        this.dbEnv = new Environment(new File(queueEnvPath),
                                  dbEnvConfig);

        // Setup non-transactional deferred-write queue database
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        if (cacheSize > 0) {
            deferred = true;
        } else {
            deferred = false;
        }
        dbConfig.setDeferredWrite(deferred);
        
        dbConfig.setBtreeComparator(new KeyComparator());
        this.queueDatabase = dbEnv.openDatabase(null,
            queueName,
            dbConfig);
        this.queueName = queueName;
        this.cacheSize = cacheSize;
        this.opsCounter = 0;
    }
    
    public PersistentQueue(final String queueEnvPath, final String queueName) {
        this(queueEnvPath, queueName, 0);
    }

    /**
     * Retrieves and returns element from the head of this queue.
     *
     * @return element from the head of the queue or null if queue is empty
     *
     * @throws IOException in case of disk IO failure
     */
    public T poll() throws IOException, ClassNotFoundException {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = queueDatabase.openCursor(null, null)) {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null)
                return null;
            
            T result;
            try (ByteArrayInputStream bis = new ByteArrayInputStream(data.getData());ObjectInput in = new ObjectInputStream(bis)) {
                result = (T)in.readObject(); 
            }

            cursor.delete();
            if (deferred) {
                opsCounter++;
                if (opsCounter >= cacheSize) {
                    queueDatabase.sync();
                    opsCounter = 0;
                }
            }
            return result;
        }
    }
    
    /**
     * Retrieves and returns elements from the head of this queue.
     * @param pollCnt Maximum elements count per request
     * @return List of the elements from the head of the queue or null if queue is empty
     *
     * @throws IOException in case of disk IO failure
     */
    public List<T> poll(int pollCnt) throws IOException, ClassNotFoundException {
        List<T> result = new ArrayList<>(pollCnt);
        boolean finished = false;
        while (!finished && pollCnt > 0) {
            T obj = poll();
            if (obj == null) {
                finished = true;
            } else {
                result.add(obj);
            }
            pollCnt--;
        }
        return result;
        
    }

    /**
     * Pushes element to the tail of this queue.
     *
     * @param element element
     *
     * @throws IOException in case of disk IO failure
     */
    public synchronized void push(Serializable element) throws IOException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = queueDatabase.openCursor(null, null)) {
            cursor.getLast(key, data, LockMode.RMW);

            BigInteger prevKeyValue;
            if (key.getData() == null) {
                prevKeyValue = BigInteger.valueOf(-1);
            } else {
                prevKeyValue = new BigInteger(key.getData());
            }
            BigInteger newKeyValue = prevKeyValue.add(BigInteger.ONE);

            final DatabaseEntry newKey = new DatabaseEntry(newKeyValue.toByteArray());

            try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(element);
                byte[] bytes = bos.toByteArray();
                final DatabaseEntry newData = new DatabaseEntry(bytes);
                queueDatabase.put(null, newKey, newData);
            }

            if (deferred) {
                opsCounter++;
                if (opsCounter >= cacheSize) {
                    queueDatabase.sync();
                    opsCounter = 0;
                }
            }
        }
    }

   /**
     * Returns the size of this queue.
     *
     * @return the size of the queue
     */
    public long size() {
        return queueDatabase.count();
    }

    /**
     * Returns this queue name.
     *
     * @return this queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Closes this queue and frees up all resources associated to it.
     */
    @Override
    public void close() {
        queueDatabase.close();
        dbEnv.close();
    }
    
    /**
     * Clear database
     */
    public void clear() {
        queueDatabase.close();
        dbEnv.truncateDatabase(null, getQueueName(), false);
        this.queueDatabase = dbEnv.openDatabase(null,
            queueName,
            dbConfig);
    }
    
    /**
    * Key comparator for DB keys
    */
   static class KeyComparator implements Comparator, Serializable {

       /**
        * Compares two DB keys.
        *
        * @param key1 first key
        * @param key2 second key
        *
        * @return comparison result
        */
       @Override
       public int compare(Object o1, Object o2) {
           return new BigInteger((byte[])o1).compareTo(new BigInteger((byte[])o2));
       }

   }

}
