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

#include <cstring>

#include "ukv.h"
#include <rocksdb/status.h>
#include <rocksdb/cache.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction_db.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/comparator.h>
#include <rocksdb/filter_policy.h>

using db_t = rocksdb::TransactionDB;

std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
rocksdb::Options options = rocksdb::Options();
rocksdb::TransactionDBOptions txn_options = rocksdb::TransactionDBOptions();
rocksdb::Status status;
std::string dir_path = "./tmp";

static rocksdb::Slice to_slice(const ukv_key_t key) {
    // key = __builtin_bswap64(key);
    return {reinterpret_cast<char const*>(&key), sizeof(ukv_key_t)};
}

inline rocksdb::Slice to_slice(ukv_tape_ptr_t value_ptr, ukv_val_len_t length) {
    return {reinterpret_cast<const char*>(value_ptr), length};
}

void ukv_open(char const* config, ukv_t* db, ukv_error_t* error) {
    db_t* raw_db = reinterpret_cast<db_t*>(db);
    status = rocksdb::TransactionDB::Open(options, txn_options, dir_path, cf_descs, &cf_handles, &raw_db);
    if (!status.ok())
        *error = "Open Error";
}

void _ukv_write_head( //
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t const c_values,
    ukv_val_len_t const* c_lengths,
    ukv_error_t* c_error) {

    db_t* db = reinterpret_cast<db_t*>(c_db);
    rocksdb::WriteOptions* opt_ptr_ = reinterpret_cast<rocksdb::WriteOptions*>(c_options);
    size_t value_offset = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        status = db->Put(*opt_ptr_, to_slice(c_keys[i]), to_slice(c_values + value_offset, c_lengths[i]));
        if (!status.ok())
            *c_error = "Write Error";
        value_offset += c_lengths[i];
    }
}

void _ukv_write_txn( //
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t const c_tape,
    ukv_val_len_t const* lengths,
    ukv_error_t* c_error) {

    rocksdb::Transaction* txn = reinterpret_cast<rocksdb::Transaction*>(c_txn);
    size_t value_offset = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        status = txn->Put(to_slice(c_keys[i]), to_slice(c_tape + value_offset, lengths[i]));
        if (!status.ok())
            *c_error = "Write Error";
        value_offset += lengths[i];
    }
}

void _ukv_read_head( //
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    size_t* c_tape_length,
    ukv_error_t* c_error) {

    db_t* db = reinterpret_cast<db_t*>(c_db);
    rocksdb::ReadOptions* opt_ptr = reinterpret_cast<rocksdb::ReadOptions*>(c_options);
    size_t offset = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        rocksdb::PinnableSlice data;
        status = db->Get(*opt_ptr, cf_handles[0], to_slice(c_keys[i]), &data);
        if (status.IsNotFound())
            *c_error = "Key Not Found";
        else if (!status.ok())
            *c_error = "Read Error";
        c_tape_length[i] = data.size();
        memcpy(c_tape[offset], data.data(), c_tape_length[i]);
        offset += c_tape_length[i];
    }
}

void _ukv_read_txn( //
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_tape_length,
    ukv_error_t* c_error) {

    rocksdb::Transaction* txn = reinterpret_cast<rocksdb::Transaction*>(c_txn);
    rocksdb::ReadOptions* opt_ptr = reinterpret_cast<rocksdb::ReadOptions*>(c_options);
    size_t offset = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        rocksdb::PinnableSlice data;
        status = txn->Get(*opt_ptr, to_slice(c_keys[i]), &data);
        if (status.IsNotFound())
            *c_error = "Key Not Found";
        else if (!status.ok())
            *c_error = "Read Error";
        c_tape_length[i] = data.size();
        memcpy(c_tape[offset], data.data(), c_tape_length[i]);
        offset += c_tape_length[i];
    }
}

void ukv_write( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t c_tape,
    ukv_val_len_t const* c_lengths,
    ukv_error_t* c_error) {

    return c_txn ? _ukv_write_txn(c_txn, c_keys, c_keys_count, c_collections, c_options, c_tape, c_lengths, c_error)
                 : _ukv_write_head(c_db, c_keys, c_keys_count, c_collections, c_options, c_tape, c_lengths, c_error);
}

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_tape_length,
    ukv_error_t* c_error) {

    return c_txn ? _ukv_read_txn(c_txn, c_keys, c_keys_count, c_collections, c_options, c_tape, c_tape_length, c_error)
                 : _ukv_read_head(c_db, c_keys, c_keys_count, c_collections, c_options, c_tape, c_tape_length, c_error);
}

void ukv_collection_upsert( //
    ukv_t const db,
    ukv_str_view_t name,
    ukv_collection_t* collection,
    ukv_error_t* error) {
}

void ukv_collection_remove( //
    ukv_t const db,
    ukv_str_view_t name,
    ukv_error_t* error) {
}

void ukv_control( //
    ukv_t const db,
    ukv_str_view_t request,
    ukv_str_view_t* response,
    ukv_error_t* error) {
}

void ukv_txn_begin( //
    ukv_t const db,
    ukv_size_t const sequence_number,
    ukv_txn_t* txn,
    ukv_error_t* error) {
}

void ukv_txn_commit( //
    ukv_txn_t const txn,
    ukv_options_t const options,
    ukv_error_t* error) {
}

void ukv_option_read_colocated(ukv_options_t* options, bool) {
}

void ukv_txn_free(ukv_t const db, ukv_txn_t const txn) {
}

void ukv_collection_free(ukv_t const db, ukv_collection_t const collection) {
}

void ukv_free(ukv_t const db) {
}

void ukv_error_free(ukv_error_t const error) {
}

void ukv_tape_free(ukv_t const db, ukv_tape_ptr_t, ukv_size_t) {
}
