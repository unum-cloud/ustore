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

using rocks_db_t = rocksdb::TransactionDB;
using rocks_txn_t = rocksdb::Transaction;
using rocks_col_t = rocksdb::ColumnFamilyHandle;
using rocks_status_t = rocksdb::Status;
using rocks_value_t = rocksdb::PinnableSlice;

ukv_collection_t ukv_default_collection_k = NULL;

struct rocks_db_wrapper_t {
    std::vector<rocks_col_t*> columns;
    std::unique_ptr<rocks_db_t> db;
};

inline rocksdb::Slice to_slice(ukv_key_t const& key) noexcept {
    return {reinterpret_cast<char const*>(&key), sizeof(ukv_key_t)};
}

inline rocksdb::Slice to_slice(value_view_t value) noexcept {
    return {reinterpret_cast<const char*>(value.begin()), value.size()};
}

inline std::unique_ptr<rocks_value_t> get_value(ukv_error_t* c_error) noexcept {
    std::unique_ptr<rocks_value_t> value_uptr;
    try {
        value_uptr = std::make_unique<rocks_value_t>();
    }
    catch (...) {
        *c_error = "Fail to allocate value";
    }
    return value_uptr;
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
    rocks_txn_t* txn,
    ukv_collection_t const* c_col,
    ukv_key_t const* c_key,
    ukv_val_ptr_t const* c_val,
    ukv_val_len_t const* c_len,
    rocksdb::WriteOptions& options,
    ukv_error_t* c_error) {

    rocks_status_t status;
    rocks_col_t* col = reinterpret_cast<rocks_col_t*>(*c_col);
    if (txn) {
        if (*c_len)
            status = db_wrapper->db->Put(options, col, to_slice(*c_key), to_slice(value_view_t(*c_val, *c_len)));
        else
            status = db_wrapper->db->Delete(options, col, to_slice(*c_key));
    }
    else {
        if (*c_len)
            status = txn->Put(col, to_slice(*c_key), to_slice(value_view_t(*c_val, *c_len)));
        else
            status = txn->Delete(col, to_slice(*c_key));
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

void write_head( //
    rocks_db_wrapper_t* db_wrapper,
    write_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_error_t* c_error) {
}

void write_txn( //
    rocks_txn_t* txn,
    write_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_error_t* c_error) {
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
    rocks_txn_t* txn = (rocks_txn_t*)(c_txn);
    rocksdb::WriteOptions options;
    if (c_options & ukv_option_write_flush_k)
        options.sync = true;

    if (c_keys_count == 1) {
        single_write(db_wrapper, txn, c_cols, c_keys, c_vals, c_lens, options, c_error);
        return;
    }
}

void single_read( //
    rocks_db_wrapper_t* db_wrapper,
    rocks_txn_t* txn,
    ukv_collection_t const* c_col,
    ukv_key_t const* c_key,
    ukv_val_len_t** c_found_lengths,
    ukv_val_ptr_t* c_found_values,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    auto value_uptr = get_value(c_error);
    rocks_value_t* value = value_uptr.get();
    rocks_col_t* col = reinterpret_cast<rocks_col_t*>(*c_col);
    rocksdb::ReadOptions options;
    rocks_status_t status;
    if (txn)
        status = db_wrapper->db->Get(options, col, to_slice(*c_key), value);
    else
        status = txn->Get(options, col, to_slice(*c_key), value);

    if (!status.IsNotFound() && !status.ok()) {
        if (status.IsIOError())
            *c_error = "Read Failure: IO  Error";
        else if (status.IsInvalidArgument())
            *c_error = "Read Failure: Invalid Argument";
        else
            *c_error = "Read Failure";
        return;
    }

    stl_arena_t& arena = *cast_arena(c_arena, c_error);
    auto len = value->size();
    prepare_memory(arena.output_tape, sizeof(ukv_size_t) + len, c_error);
    memcpy(arena.output_tape.data(), &len, sizeof(ukv_size_t));
    if (len)
        memcpy(arena.output_tape.data() + sizeof(ukv_size_t), value->data(), len);

    *c_found_lengths = reinterpret_cast<ukv_val_len_t*>(arena.output_tape.data());
    *c_found_values = reinterpret_cast<ukv_val_ptr_t>(arena.output_tape.data() + sizeof(ukv_size_t));
}

void read_head( //
    rocks_db_wrapper_t* db_wrapper,
    read_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_val_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {
}

void read_txn( //
    rocks_db_wrapper_t* db_wrapper,
    rocks_txn_t* txn,
    read_tasks_soa_t tasks,
    ukv_size_t const n,
    ukv_val_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {
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
    rocks_txn_t* txn = reinterpret_cast<rocks_txn_t*>(c_txn);

    if (c_keys_count == 1) {
        single_read(db_wrapper, txn, c_cols, c_keys, c_found_lengths, c_found_values, c_arena, c_error);
        return;
    }
}

void ukv_collection_upsert( //
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_str_view_t c_config,
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

void ukv_arena_free(ukv_t const, ukv_val_ptr_t c_ptr, ukv_size_t c_len) {
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