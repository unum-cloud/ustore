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

#include "ukv/ukv.h"
#include "helpers.hpp"

using namespace unum::ukv;
using namespace unum;

using rocks_status_t = rocksdb::Status;
using rocks_db_t = rocksdb::TransactionDB;
using rocks_value_t = rocksdb::PinnableSlice;
using rocks_txn_ptr_t = rocksdb::Transaction*;
using rocks_col_ptr_t = rocksdb::ColumnFamilyHandle*;
using value_uptr_t = std::unique_ptr<rocks_value_t>;

ukv_collection_t ukv_default_collection_k = NULL;
ukv_val_len_t ukv_val_len_missing_k = 0;
ukv_key_t ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();

struct rocks_db_wrapper_t {
    std::vector<rocks_col_ptr_t> columns;
    std::unique_ptr<rocks_db_t> db;
};

inline rocksdb::Slice to_slice(ukv_key_t const& key) noexcept {
    return {reinterpret_cast<char const*>(&key), sizeof(ukv_key_t)};
}

inline rocksdb::Slice to_slice(value_view_t value) noexcept {
    return {reinterpret_cast<const char*>(value.begin()), value.size()};
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

void single_write( //
    rocks_db_wrapper_t* db_wrapper,
    rocks_txn_ptr_t txn,
    write_task_t const& task,
    rocksdb::WriteOptions& options,
    ukv_error_t* c_error) {

    rocks_status_t status;
    auto key = to_slice(task.key);
    rocks_col_ptr_t col =
        task.col ? reinterpret_cast<rocks_col_ptr_t>(task.col) : db_wrapper->db->DefaultColumnFamily();

    if (txn) {
        if (task.is_deleted())
            status = txn->Delete(col, key);
        else
            status = txn->Put(col, key, to_slice(task.view()));
    }
    else {
        if (task.is_deleted())
            status = db_wrapper->db->Delete(options, col, key);
        else
            status = db_wrapper->db->Put(options, col, key, to_slice(task.view()));
    }

    if (!status.ok()) {
        if (status.IsCorruption())
            *c_error = "Write Failure: DB Corrpution";
        else if (status.IsIOError())
            *c_error = "Write Failure: IO  Error";
        else if (status.IsInvalidArgument())
            *c_error = "Write Failure: Invalid Argument";
        else
            *c_error = "Write Failure";
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

    ukv_val_ptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_val_len_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_val_len_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_options_t const c_options,
    ukv_arena_t*,
    ukv_error_t* c_error) {

    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    rocks_txn_ptr_t txn = reinterpret_cast<rocks_txn_ptr_t>(c_txn);
    strided_iterator_gt<ukv_collection_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_soa_t tasks {cols, keys, vals, offs, lens};

    rocksdb::WriteOptions options;
    if (c_options & ukv_option_write_flush_k)
        options.sync = true;

    if (c_keys_count == 1) {
        single_write(db_wrapper, txn, tasks[0], options, c_error);
        return;
    }

    if (txn) {
        for (ukv_size_t i = 0; i != c_keys_count; ++i) {
            write_task_t task = tasks[i];
            txn->Put(reinterpret_cast<rocks_col_ptr_t>(task.col), to_slice(task.key), to_slice(task.view()));
        }
        return;
    }

    rocksdb::WriteBatch batch;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        write_task_t task = tasks[i];
        rocks_col_ptr_t col =
            task.col ? reinterpret_cast<rocks_col_ptr_t>(task.col) : db_wrapper->db->DefaultColumnFamily();
        auto key = to_slice(task.key);
        if (task.is_deleted())
            batch.Delete(col, key);
        else
            batch.Put(col, key, to_slice(task.view()));
    }

    rocks_status_t status = db_wrapper->db->Write(options, &batch);
    if (!status.ok())
        *c_error = "Write Error";
}

void single_read( //
    rocks_db_wrapper_t* db_wrapper,
    rocks_txn_ptr_t txn,
    read_task_t const& task,
    ukv_val_len_t** c_found_lengths,
    ukv_val_ptr_t* c_found_values,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    rocksdb::ReadOptions options;
    rocks_status_t status;
    rocks_col_ptr_t col =
        task.col ? reinterpret_cast<rocks_col_ptr_t>(task.col) : db_wrapper->db->DefaultColumnFamily();

    value_uptr_t value_uptr;
    try {
        value_uptr = std::make_unique<rocks_value_t>();
    }
    catch (...) {
        *c_error = "Fail to allocate value";
    }
    rocks_value_t* value = value_uptr.get();

    auto key = to_slice(task.key);
    try {
        if (txn)
            status = txn->Get(options, col, key, value);
        else
            status = db_wrapper->db->Get(options, col, key, value);
    }
    catch (...) {
        *c_error = "Fail to read";
    }

    if (!status.IsNotFound() && !status.ok()) {
        if (status.IsIOError())
            *c_error = "Read Failure: IO  Error";
        else if (status.IsInvalidArgument())
            *c_error = "Read Failure: Invalid Argument";
        else
            *c_error = "Read Failure";
        return;
    }

    auto len = value->size();
    prepare_memory(arena.output_tape, sizeof(ukv_size_t) + len, c_error);
    if (*c_error)
        return;
    std::memcpy(arena.output_tape.data(), &len, sizeof(ukv_size_t));
    if (len)
        std::memcpy(arena.output_tape.data() + sizeof(ukv_size_t), value->data(), len);

    *c_found_lengths = reinterpret_cast<ukv_val_len_t*>(arena.output_tape.data());
    *c_found_values = reinterpret_cast<ukv_val_ptr_t>(arena.output_tape.data() + sizeof(ukv_size_t));
}

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_options_t const,

    ukv_val_len_t** c_found_lengths,
    ukv_val_ptr_t* c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    rocks_txn_ptr_t txn = reinterpret_cast<rocks_txn_ptr_t>(c_txn);
    strided_iterator_gt<ukv_collection_t const> cols_stride {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys_stride {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {cols_stride, keys_stride};
    stl_arena_t& arena = *cast_arena(c_arena, c_error);

    if (c_keys_count == 1) {
        single_read(db_wrapper, txn, tasks[0], c_found_lengths, c_found_values, arena, c_error);
        return;
    }

    std::vector<rocks_col_ptr_t> cols(c_keys_count);
    std::vector<rocksdb::Slice> keys(c_keys_count);
    std::vector<std::string> vals(c_keys_count);
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        read_task_t task = tasks[i];
        cols[i] = task.col ? reinterpret_cast<rocks_col_ptr_t>(task.col) : db_wrapper->db->DefaultColumnFamily();
        keys[i] = to_slice(task.key);
    }

    if (txn)
        txn->MultiGet(rocksdb::ReadOptions(), cols, keys, &vals);
    else
        db_wrapper->db->MultiGet(rocksdb::ReadOptions(), cols, keys, &vals);

    // 1. Estimate the total size
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    for (std::size_t i = 0; i != c_keys_count; ++i)
        total_bytes += vals[i].size();

    // 2. Allocate a tape for all the values to be fetched
    byte_t* tape = prepare_memory(arena.output_tape, total_bytes, c_error);
    if (*c_error)
        return;

    // 3. Fetch the data
    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    *c_found_lengths = lens;
    *c_found_values = reinterpret_cast<ukv_val_ptr_t>(tape + exported_bytes);

    for (std::size_t i = 0; i != c_keys_count; ++i) {
        auto len = vals[i].size();
        if (len) {
            std::memcpy(tape + exported_bytes, vals[i].data(), len);
            lens[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        else
            lens[i] = ukv_val_len_missing_k;
    }
}

void ukv_collection_open( //
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_str_view_t c_config,
    ukv_collection_t* c_col,
    [[maybe_unused]] ukv_error_t* c_error) {

    rocks_db_wrapper_t* db_wrapper = reinterpret_cast<rocks_db_wrapper_t*>(c_db);
    rocks_col_ptr_t col = nullptr;
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
    ukv_size_t const,
    ukv_options_t const,
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    rocks_db_t* db = reinterpret_cast<rocks_db_wrapper_t*>(c_db)->db.get();
    rocks_txn_ptr_t txn = reinterpret_cast<rocks_txn_ptr_t>(*c_txn);
    txn = db->BeginTransaction(rocksdb::WriteOptions(), rocksdb::TransactionOptions(), txn);
    if (!txn)
        *c_error = "Transaction Begin Error";
    *c_txn = txn;
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    [[maybe_unused]] ukv_options_t const c_options,
    ukv_error_t* c_error) {

    rocks_txn_ptr_t txn = reinterpret_cast<rocks_txn_ptr_t>(c_txn);
    rocks_status_t status = txn->Commit();
    if (!status.ok())
        *c_error = "Commit Error";
}

void ukv_arena_free(ukv_t const, ukv_arena_t c_arena) {
    if (!c_arena)
        return;
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(c_arena);
    delete &arena;
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