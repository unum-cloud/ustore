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

#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction_db.h>

#include "ukv.h"
#include "helpers.hpp"

using namespace unum::ukv;
using namespace unum;

using rocks_db_t = rocksdb::TransactionDB;
using rocks_txn_t = rocksdb::Transaction;
using rocks_col_t = rocksdb::ColumnFamilyHandle;
using rocks_status_t = rocksdb::Status;

ukv_collection_t ukv_default_collection_k = NULL;

struct rocks_db_wrapper_t {
    std::vector<rocks_col_t*> columns;
    std::unique_ptr<rocks_db_t> db;
};

inline rocksdb::Slice to_slice(ukv_key_t const* key) noexcept {
    return {reinterpret_cast<char const*>(key), sizeof(ukv_key_t)};
}

inline rocksdb::Slice to_slice(byte_t const* begin, ukv_val_len_t offset, ukv_val_len_t length) {
    return {reinterpret_cast<const char*>(begin + offset), length};
}

void ukv_open([[maybe_unused]] char const* c_config, ukv_t* c_db, ukv_error_t* c_error) {
    rocks_db_wrapper_t* db_wrapper = new rocks_db_wrapper_t;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_descriptors;
    rocksdb::Options options;
    rocksdb::ConfigOptions config_options;

    rocks_status_t status = rocksdb::LoadLatestOptions(config_options, "./tmp/rocksdb/", &options, &column_descriptors);
    if (column_descriptors.empty())
        column_descriptors.push_back({ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()});

    rocks_db_t* db = nullptr;
    options.create_if_missing = true;
    status = rocks_db_t::Open(options,
                              rocksdb::TransactionDBOptions(),
                              "./tmp/rocksdb/",
                              column_descriptors,
                              &db_wrapper->columns,
                              &db);

    db_wrapper->db = std::unique_ptr<rocks_db_t>(db);

    if (!status.ok())
        *c_error = "Open Error";
    *c_db = db_wrapper;
}

void write_head( //
    rocks_db_wrapper_t* db_wrapper,
    write_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_error_t* c_error) {

    rocks_status_t status;
    rocksdb::WriteBatch batch;
    std::vector<write_task_t> task_arr(n);

    for (ukv_size_t i = 0; i != n; ++i) {
        task_arr[i] = tasks[i];
        rocks_col_t* col = task_arr[i].collection ? reinterpret_cast<rocks_col_t*>(task_arr[i].collection)
                                                  : db_wrapper->db->DefaultColumnFamily();
        status = batch.Put(col,
                           to_slice(&task_arr[i].key),
                           to_slice(task_arr[i].begin, task_arr[i].offset, task_arr[i].length_));
        if (!status.ok()) {
            *c_error = "Write Error";
            return;
        }
    }

    status = db_wrapper->db->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        *c_error = "Write Error";
}

void write_txn( //
    rocks_txn_t* txn,
    write_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_error_t* c_error) {

    rocks_status_t status;
    std::vector<write_task_t> task_arr(n);

    for (ukv_size_t i = 0; i != n; ++i) {
        task_arr[i] = tasks[i];
        status = txn->Put(reinterpret_cast<rocks_col_t*>(task_arr[i].collection),
                          to_slice(&task_arr[i].key),
                          to_slice(task_arr[i].begin, task_arr[i].offset, task_arr[i].length_));
        if (!status.ok()) {
            *c_error = "Transaction Write Error";
            return;
        }
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

    ukv_val_len_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_val_len_t const* c_lens,
    ukv_size_t const c_lens_stride,

    [[maybe_unused]] ukv_options_t const c_options,
    ukv_error_t* c_error) {

    rocks_db_wrapper_t* db = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    rocks_txn_t* txn = (rocks_txn_t*)(c_txn);

    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_ptr_gt<ukv_tape_ptr_t const> vals {c_vals, c_vals_stride};
    strided_ptr_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_ptr_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};

    write_tasks_soa_t tasks {cols, keys, vals, offs, lens};

    return txn ? write_txn(txn, tasks, c_keys_count, c_error) : write_head(db, tasks, c_keys_count, c_error);
}

void read_head( //
    rocks_db_wrapper_t* db_wrapper,
    read_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    std::vector<rocks_col_t*> columns(n);
    std::vector<rocksdb::Slice> keys(n);
    std::vector<std::string> values(n);
    std::vector<read_task_t> task_arr(n);

    for (ukv_size_t i = 0; i != n; ++i) {
        task_arr[i] = tasks[i];
        columns[i] = task_arr[i].collection ? reinterpret_cast<rocks_col_t*>(task_arr[i].collection)
                                            : db_wrapper->db->DefaultColumnFamily();
        keys[i] = to_slice(&task_arr[i].key);
    }

    std::vector<rocks_status_t> statuses = db_wrapper->db->MultiGet(rocksdb::ReadOptions(), columns, keys, &values);
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    for (std::size_t i = 0; i != values.size(); ++i)
        total_bytes += values[i].size();

    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (*c_error)
        return;

    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * n;

    for (std::size_t i = 0; i != values.size(); ++i) {
        if (!values[i].size())
            lens[i] = 0;
        auto len = values[i].size();
        std::memcpy(tape + exported_bytes, values[i].data(), len);
        lens[i] = static_cast<ukv_val_len_t>(len);
        exported_bytes += len;
    };
}

void read_txn( //
    rocks_db_wrapper_t* db_wrapper,
    rocks_txn_t* txn,
    read_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    std::vector<rocks_col_t*> columns(n);
    std::vector<rocksdb::Slice> keys(n);
    std::vector<std::string> values(n);
    std::vector<read_task_t> task_arr(n);

    for (ukv_size_t i = 0; i != n; ++i) {
        task_arr[i] = tasks[i];
        columns[i] = task_arr[i].collection ? reinterpret_cast<rocks_col_t*>(task_arr[i].collection)
                                            : db_wrapper->db->DefaultColumnFamily();
        keys[i] = to_slice(&task_arr[i].key);
    }

    std::vector<rocks_status_t> statuses = txn->MultiGet(rocksdb::ReadOptions(), columns, keys, &values);

    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    for (std::size_t i = 0; i != values.size(); ++i)
        total_bytes += values[i].size();

    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (*c_error)
        return;

    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * n;

    for (std::size_t i = 0; i != values.size(); ++i) {
        if (!values[i].size())
            lens[i] = 0;

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

    [[maybe_unused]] ukv_options_t const c_options,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    if (!c_db) {
        *c_error = "DataBase is NULL!";
        return;
    }

    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(c_txn);

    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {cols, keys};

    return txn ? read_txn(db_wrapper, txn, tasks, c_keys_count, c_tape, c_capacity, c_error)
               : read_head(db_wrapper, tasks, c_keys_count, c_tape, c_capacity, c_error);
}

void ukv_collection_upsert( //
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_collection_t* c_col,
    [[maybe_unused]] ukv_error_t* c_error) {

    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    rocks_col_t* col = nullptr;
    rocks_status_t status;

    if (c_col_name) {
        status = db_wrapper->db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), c_col_name, &col);
        if (status.ok())
            db_wrapper->columns.push_back(col);
        else
            for (auto handle : db_wrapper->columns) {
                if (handle && handle->GetName() == c_col_name) {
                    *c_col = handle;
                    return;
                }
            }
    }
    else
        col = db_wrapper->db->DefaultColumnFamily();

    *c_col = col;
}

void ukv_collection_remove( //
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_error_t* c_error) {

    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    for (auto handle : db_wrapper->columns) {
        if (c_col_name == handle->GetName()) {
            rocks_status_t status = db_wrapper->db->DestroyColumnFamilyHandle(handle);
            if (!status.ok())
                *c_error = "Can't Delete Collection";
            return;
        }
    }
}

void ukv_control( //
    [[maybe_unused]] ukv_t const c_db,
    [[maybe_unused]] ukv_str_view_t c_request,
    ukv_str_view_t* c_response,
    ukv_error_t* c_error) {
    *c_response = NULL;
    *c_error = "Controls aren't supported in this implementation!";
}

void ukv_txn_begin( //
    ukv_t const c_db,
    [[maybe_unused]] ukv_size_t const sequence_number,
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    rocks_db_t* db = reinterpret_cast<rocks_db_wrapper_t*>(c_db)->db.get();
    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(*c_txn);
    txn = db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions(), txn);
    if (!txn)
        *c_error = "Transaction Begin Error";
    *c_txn = txn;
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    [[maybe_unused]] ukv_options_t const c_options,
    ukv_error_t* c_error) {

    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(c_txn);
    rocks_status_t status = txn->Commit();
    if (!status.ok())
        *c_error = "Commit Error";
}

void ukv_tape_free(ukv_t const, ukv_tape_ptr_t c_ptr, ukv_size_t c_len) {
    if (!c_ptr || !c_len)
        return;
    allocator_t {}.deallocate(reinterpret_cast<byte_t*>(c_ptr), c_len);
}

void ukv_txn_free([[maybe_unused]] ukv_t const, [[maybe_unused]] ukv_txn_t c_txn) {
}

void ukv_collection_free([[maybe_unused]] ukv_t const db, [[maybe_unused]] ukv_collection_t const collection) {
}

void ukv_free(ukv_t c_db) {
    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    delete db_wrapper;
}

void ukv_error_free([[maybe_unused]] ukv_error_t const error) {
}