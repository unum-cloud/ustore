package com.unum.ukv;

import java.util.*; // Map abstract class
import java.lang.*; // Finalization

/**
 * Partially implements the JDBC interface, as well as a classical
 * Java `Dictionary` API using Java Native Interface.
 * https://www.ibm.com/docs/en/informix-servers/12.10?topic=operations-handle-transactions
 */
public class Transaction implements AutoCloseable {

    private long transactionAddress = 0;
    private long databaseAddress = 0;
    private boolean autoCommit = true;

    /**
     * When enabled, the transaction is being commited on close.
     * If the commit fails, an exception is raised.
     */
    public void setAutoCommit(boolean state) {
        autoCommit = state;
    }

    public native void rollback();

    public native boolean commit();

    /**
     * Tests if the specified object is a key in this collection.
     */
    public native boolean containsKey(String collection, long key);

    public boolean containsKey(long key) {
        return containsKey(null, key);
    }

    /**
     * Maps the specified key to the specified value in this collection.
     */
    public native void put(String collection, long key, byte[] value);

    public void put(long key, byte[] value) {
        put(null, key, value);
    }

    /**
     * Returns the value to which the specified key is mapped, or null if this map
     * contains no mapping for the key.
     */
    public native byte[] get(String collection, long key);

    public byte[] get(long key) {
        return get(null, key);
    }

    /**
     * Removes the key (and its corresponding value) from this collection.
     */
    public native byte[] remove(String collection, long key);

    public byte[] remove(long key) {
        return remove(null, key);
    }

    /**
     * If the specified key is not already associated with a value (or is mapped to
     * null) associates it with the given value and returns null, else returns the
     * current value.
     */
    public void putIfAbsent(String collection, long key, byte[] value) {
        if (!containsKey(collection, key))
            put(collection, key, value);
    }

    public void putIfAbsent(long key, byte[] value) {
        if (!containsKey(key))
            put(key, value);
    }

    /**
     * Copies all of the mappings from the specified map to this collection.
     */
    public void putAll(String collection, Map<Long, byte[]> t) {
        for (Map.Entry<Long, byte[]> entry : t.entrySet())
            put(collection, entry.getKey(), entry.getValue());
    }

    public void putAll(Map<Long, byte[]> t) {
        for (Map.Entry<Long, byte[]> entry : t.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    /**
     * Replaces the entry for the specified key only if it is currently mapped to
     * some value.
     */
    public byte[] replace(String collection, long key, byte[] value) {
        byte[] old = get(collection, key);
        if (old != value)
            put(collection, key, value);
        return old;
    }

    public byte[] replace(long key, byte[] value) {
        byte[] old = get(key);
        if (old != value)
            put(key, value);
        return old;
    }

    /**
     * Returns the value to which the specified key is mapped, or defaultValue if
     * this map contains no mapping for the key.
     */
    public byte[] getOrDefault(String collection, long key, byte[] defaultValue) {
        byte[] old = get(collection, key);
        if (old != null)
            return old;
        return defaultValue;
    }

    public byte[] getOrDefault(long key, byte[] defaultValue) {
        byte[] old = get(key);
        if (old != null)
            return old;
        return defaultValue;
    }

    /**
     * Removes the entry for the specified key only if it is currently
     * mapped to the specified value.
     * 
     * @return True, iff key was found, value was equal and the removal occured.
     */
    public boolean remove(String collection, long key, byte[] value) {
        byte[] old = get(collection, key);
        if (old == value) {
            remove(collection, key, value);
            return true;
        } else
            return false;
    }

    public boolean remove(long key, byte[] value) {
        byte[] old = get(key);
        if (old == value) {
            remove(key, value);
            return true;
        } else
            return false;
    }

    @Override
    public void close() throws Exception {
        if (autoCommit)
            if (!commit())
                throw new Exception("Failed to auto-commit on exit!");

    }
}
