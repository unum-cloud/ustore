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
public class DataBase extends Transaction implements AutoCloseable {

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

    /**
     * @brief Begins a new transaction with an auto-incremented identifier.
     *        By default, the transaction will be commited on `close`.
     */
    public native Transaction transaction();

    /**
     * @brief Clears this collection so that it contains no keys.
     *        Imposes a global lock on the entire collection, so use rarely.
     */
    public native void clear();

    public native void open(String config_json);

    public native void close_();

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
    }

    @Override
    public void rollback() {
    }

    @Override
    public boolean commit() {
        return false;
    }

    public static void main(String[] args) {
        DataBase db = new DataBase("");
        db.put(1, "hey".getBytes());
        assert db.get(1) == "hey".getBytes() : "Received wrong value";
        db.close();
    }
}