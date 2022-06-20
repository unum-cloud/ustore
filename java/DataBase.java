package com.unum.ukv;

import java.util.*; // Map abstract class
import java.lang.*; // Finalization

/**
 * @brief An Embedded Persistent Key-Value Store with
 *        ACID Transactions and mind-boggling speed.
 * 
 *        Implemented using Java Native Interface:
 *        https://docs.oracle.com/en/graalvm/enterprise/21/docs/reference-manual/native-image/JNI/
 * 
 * @section API
 *          Mimics the interface of Javas native HashMap and Ditionary classes.
 *          https://docs.oracle.com/javase/7/docs/api/java/util/Dictionary.html
 *
 *
 * @section Alternatives
 *          Aside from classical C/C++ implementations of RocksDB, WiredTiger,
 *          LMDB and LevelDB being wrapped into JNI, there are some KVS written
 *          purely in Java:
 *          * https://github.com/jankotek/mapdb/
 *          * https://github.com/yahoo/HaloDB
 *          https://github.com/facebook/rocksdb/blob/main/java/src/main/java/org/rocksdb/RocksDB.java
 * 
 */
public class DataBase implements AutoCloseable {

    static {
        try {
            // TODO: During installation, put the library at the right global path
            System.loadLibrary("ukv_java");
            // System.load("/home/av/Code/DiskKV/bindings/jvm/libnative.so");
        } catch (UnsatisfiedLinkError e) {
            String paths = System.getProperty("java.library.path");
            System.err.println("Native code library failed to load.\n" + e);
            System.err.println("Shard libraries must be placed at:" + paths + "\n");
            System.exit(1);
        }
    }

    private long nativeAddress = 0;

    /**
     * Tests if the specified object is a key in this collection.
     */
    public native boolean containsKey(long key);

    /**
     * Maps the specified key to the specified value in this collection.
     */
    public native void put(long key, byte[] value);

    /**
     * Returns the value to which the specified key is mapped, or null if this map
     * contains no mapping for the key.
     */
    public native byte[] get(long key);

    /**
     * Removes the key (and its corresponding value) from this collection.
     */
    public native byte[] remove(long key);

    /**
     * Clears this collection so that it contains no keys.
     */
    public native void clear();

    public native void open(String config_json);

    public native void close_();

    /**
     * If the specified key is not already associated with a value (or is mapped to
     * null) associates it with the given value and returns null, else returns the
     * current value.
     */
    public void putIfAbsent(long key, byte[] value) {
        if (!containsKey(key))
            put(key, value);
    }

    /**
     * Copies all of the mappings from the specified map to this collection.
     */
    public void putAll(Map<Long, byte[]> t) {
        for (Map.Entry<Long, byte[]> entry : t.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    /**
     * Replaces the entry for the specified key only if it is currently mapped to
     * some value.
     */
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
    public byte[] getOrDefault(long key, byte[] defaultValue) {
        byte[] old = get(key);
        if (old != null)
            return old;
        return defaultValue;

    }

    /**
     * Removes the entry for the specified key only if it is currently mapped to the
     * specified value.
     * 
     * @return True, iff key was found, value was equal and the removal occured.
     */
    public boolean remove(long key, byte[] value) {
        byte[] old = get(key);
        if (old == value) {
            remove(key, value);
            return true;
        } else
            return false;
    }

    public DataBase(String config_json) {
        open(config_json);
    }

    /**
     * This is the preferred Java 9+ resource management
     * approach, as finalizers have been deprecated.
     * https://stackoverflow.com/a/52889247
     * https://stackoverflow.com/a/56454348
     */
    @Override
    public void close() {
        close_();
        System.out.println("Closed DataBase in the auto-close");
    }

    // Currently unsupported functions:

    public native boolean isEmpty();

    public native long size();

    public native void enumerate();

    public static void main(String[] args) {
        DataBase db = new DataBase("");
        db.put(1, "hey".getBytes());
        assert db.get(1) == "hey".getBytes() : "Received wrong value";
        db.close();
    }
}