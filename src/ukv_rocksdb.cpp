/**
 * @file ukv_rocksdb.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded Persistent Key-Value Store on top of RocksDB.
 * It natively supports ACID transactions and iterators (range queries)
 * and is implemented via Log Structured Merge Tree. This makes RocksDB
 * great for write-intensive operations. It's already a common engine
 * choice for various Relational Database, built on top of it.
 * Examples: Yugabyte, TiDB, and, optionally: Mongo, MySQL, Cassandra, MariaDB.
 */

#include "ukv.h"
