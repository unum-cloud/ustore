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

#include <rocksdb/status.h>
#include <rocksdb/cache.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/comparator.h>
#include <rocksdb/filter_policy.h>

#include "ukv.h"
#include "helpers.hpp"

using namespace unum::ukv;
using namespace unum;

using rocks_db_t = rocksdb::TransactionDB;
using rocks_txn_t = rocksdb::Transaction;
using rocks_col_t = rocksdb::ColumnFamilyHandle;

inline rocksdb::Slice to_slice(ukv_key_t const* key) noexcept {
    return {reinterpret_cast<char const*>(key), sizeof(ukv_key_t)};
}

inline rocksdb::Slice to_slice(ukv_tape_ptr_t const value_ptr, ukv_val_len_t const length) noexcept {
    return {reinterpret_cast<const char*>(value_ptr), length};
}

struct read_task_t {
    rocks_col_t* collection;
    rocksdb::Slice key;
};

struct read_tasks_t {
    rocks_db_t* db;
    strided_ptr_gt<rocks_col_t*> cols;
    strided_ptr_gt<ukv_key_t const> keys;

    inline read_task_t operator[](ukv_size_t i) const noexcept {
        rocks_col_t* col = cols && cols[i] ? cols[i] : db->DefaultColumnFamily();
        rocksdb::Slice key = to_slice(&keys[i]);
        return {col, key};
    }
};

struct write_task_t {
    rocks_col_t* collection;
    rocksdb::Slice key;
    rocksdb::Slice value;
};

struct write_tasks_t {
    rocks_db_t* db;
    strided_ptr_gt<rocks_col_t*> cols;
    strided_ptr_gt<ukv_key_t const> keys;
    strided_ptr_gt<ukv_tape_ptr_t const> vals;
    strided_ptr_gt<ukv_val_len_t const> lens;

    inline write_task_t operator[](ukv_size_t i) const noexcept {
        rocks_col_t* col = cols && cols[i] ? cols[i] : db->DefaultColumnFamily();
        rocksdb::Slice key = to_slice(&keys[i]);
        rocksdb::Slice value = to_slice(vals[i], lens[i]);
        return {col, key, value};
    }
};

void ukv_open(char const* c_config, ukv_t* c_db, ukv_error_t* c_error) {
    rocks_db_t* db = NULL;

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
    rocksdb::Options options;
    rocksdb::ConfigOptions config_options;

    rocksdb::Status status = rocksdb::LoadLatestOptions(config_options, "./tmp", &options, &cf_descs);
    if (cf_descs.empty())
        cf_descs.push_back({ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()});

    options.create_if_missing = true;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    status = rocks_db_t::Open(options, rocksdb::TransactionDBOptions(), "./tmp", cf_descs, &handles, &db);
    if (!status.ok())
        *c_error = "Open Error";
    *c_db = db;
}

void write_head( //
    rocks_db_t* db,
    write_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    rocksdb::WriteBatch batch;
    for (size_t i = 0; i != n; ++i) {
        write_task_t task = tasks[i];
        batch.Put(task.collection, task.key, task.value);
    }

    rocksdb::Status status = db->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        *c_error = "Write Error";
}

void write_txn( //
    rocks_txn_t* txn,
    write_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    for (size_t i = 0; i != n; ++i) {
        write_task_t task = tasks[i];
        rocksdb::Status status = txn->Put(task.collection, task.key, task.value);
        if (!status.ok())
            *c_error = "Transaction Write Error";
    }
}

void ukv_write( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_tape_ptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_val_len_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    rocks_db_t* db = reinterpret_cast<rocks_db_t*>(c_db);
    rocks_txn_t* txn = (rocks_txn_t*)(c_txn);
    rocks_col_t** cols_ptr = (rocks_col_t**)(c_cols);

    strided_ptr_gt<rocks_col_t*> cols {cols_ptr, c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_ptr_gt<ukv_tape_ptr_t const> vals {c_vals, c_vals_stride};
    strided_ptr_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_t tasks {db, cols, keys, vals, lens};

    return txn ? write_txn(txn, tasks, c_keys_count, c_options, c_error)
               : write_head(db, tasks, c_keys_count, c_options, c_error);
}

void read_head( //
    rocks_db_t* db,
    read_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    std::vector<rocks_col_t*> handles(n);
    std::vector<rocksdb::Slice> keys(n);
    std::vector<std::string> values(n);

    for (size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];
        handles[i] = task.collection;
        keys[i] = task.key;
    }

    std::vector<rocksdb::Status> statuses = db->MultiGet(rocksdb::ReadOptions(), handles, keys, &values);
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    for (size_t i = 0; i != values.size(); ++i)
        total_bytes += values[i].size();

    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (!tape)
        return;

    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * n;

    for (size_t i = 0; i != values.size(); ++i) {
        auto len = values[i].size();
        std::memcpy(tape + exported_bytes, values[i].data(), len);
        lens[i] = static_cast<ukv_val_len_t>(len);
        exported_bytes += len;
    }
}
void read_txn( //
    rocks_txn_t* txn,
    read_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    std::vector<rocks_col_t*> handles(n);
    std::vector<rocksdb::Slice> keys(n);
    std::vector<std::string> values(n);

    for (size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];
        handles[i] = task.collection;
        keys[i] = task.key;
    }

    std::vector<rocksdb::Status> statuses = txn->MultiGet(rocksdb::ReadOptions(), handles, keys, &values);

    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    for (size_t i = 0; i != values.size(); ++i)
        total_bytes += values[i].size();

    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (!tape)
        return;

    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * n;

    for (size_t i = 0; i != values.size(); ++i) {
        auto len = values[i].size();
        std::memcpy(tape + exported_bytes, values[i].data(), len);
        lens[i] = static_cast<ukv_val_len_t>(len);
        exported_bytes += len;
    }
}

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    rocks_db_t* db = reinterpret_cast<rocks_db_t*>(c_db);
    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(c_txn);
    rocks_col_t** cols_ptr = (rocks_col_t**)(c_cols);

    strided_ptr_gt<rocks_col_t*> cols {cols_ptr, c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_t tasks {db, cols, keys};

    return txn ? read_txn(txn, tasks, c_keys_count, c_options, c_tape, c_capacity, c_error)
               : read_head(db, tasks, c_keys_count, c_options, c_tape, c_capacity, c_error);
}

void ukv_collection_upsert( //
    ukv_t const c_db,
    ukv_str_view_t name,
    ukv_collection_t* collection,
    ukv_error_t* error) {

    rocks_db_t* db = reinterpret_cast<rocks_db_t*>(c_db);
    rocks_col_t* coll;
    db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &coll);
    *collection = coll;
}

void ukv_collection_remove( //
    ukv_t const db,
    ukv_str_view_t name,
    ukv_error_t* error) {
}

void ukv_control( //
    ukv_t const c_db,
    ukv_str_view_t c_request,
    ukv_str_view_t* c_response,
    ukv_error_t* c_error) {

    *c_response = NULL;
    *c_error = "Controls aren't supported in this implementation!";
}

void ukv_txn_begin( //
    ukv_t const c_db,
    ukv_size_t const sequence_number,
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    rocks_db_t* db = reinterpret_cast<rocks_db_t*>(c_db);
    rocks_txn_t* txn = NULL;
    *c_txn = db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions(), txn);
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(c_txn);
    rocksdb::Status status = txn->Commit();
    if (!status.ok())
        *c_error = "Commit Error";
}

void ukv_tape_free(ukv_t const, ukv_tape_ptr_t c_ptr, ukv_size_t c_len) {
    if (!c_ptr || !c_len)
        return;
    allocator_t {}.deallocate(reinterpret_cast<byte_t*>(c_ptr), c_len);
}

void ukv_txn_free(ukv_t const, ukv_txn_t c_txn) {
    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(c_txn);
    delete txn;
}

void ukv_collection_free(ukv_t const db, ukv_collection_t const collection) {
}

void ukv_free(ukv_t c_db) {
    rocks_db_t* db = reinterpret_cast<rocks_db_t*>(c_db);
    delete db;
}

void ukv_error_free(ukv_error_t const error) {
}