/**
 * @file ukv_leveldb.cpp
 * @author Ashot Vardanian
 *
 * @brief LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string
 * keys to string values
 */

#include <leveldb/db.h>
#include <leveldb/comparator.h>
#include <leveldb/write_batch.h>

#include "ukv.h"
#include "helpers.hpp"

ukv_collection_t ukv_default_collection_k = NULL;

using namespace unum::ukv;
using namespace unum;

using level_db_t = leveldb::DB;
using level_options_t = leveldb::Options;
using level_status_t = leveldb::Status;

struct key_comparator_t final : public leveldb::Comparator {

    int Compare(leveldb::Slice const& a, leveldb::Slice const& b) const override {
        auto ai = *reinterpret_cast<ukv_key_t const*>(a.data());
        auto bi = *reinterpret_cast<ukv_key_t const*>(b.data());
        if (ai == bi)
            return 0;
        return ai < bi ? -1 : 1;
    }

    char const* Name() const override { return "Integral"; }

    void FindShortestSeparator(std::string* start, leveldb::Slice const& limit) const override {}

    void FindShortSuccessor(std::string* key) const override {
        auto& int_key = *reinterpret_cast<ukv_key_t*>(key->data());
        ++int_key;
    }
};

static key_comparator_t key_comparator_k = {};

inline leveldb::Slice to_slice(ukv_key_t const& key) noexcept {
    return {reinterpret_cast<char const*>(&key), sizeof(ukv_key_t)};
}

inline leveldb::Slice to_slice(value_view_t value) noexcept {
    return {reinterpret_cast<const char*>(value.begin()), value.size()};
}

void ukv_open(char const* c_config, ukv_t* c_db, ukv_error_t* c_error) {
    level_db_t* db_ptr = nullptr;
    level_options_t options;
    options.create_if_missing = true;
    options.comparator = &key_comparator_k;
    level_status_t status = level_db_t::Open(options, "./tmp/leveldb/", &db_ptr);
    if (!status.ok()) {
        *c_error = "Couldn't open LevelDB";
        return;
    }
    *c_db = db_ptr;
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

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);

    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_ptr_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_ptr_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_ptr_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_soa_t tasks {cols, keys, vals, offs, lens};

    leveldb::WriteBatch batch;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        auto task = tasks[i];
        auto val = to_slice(task.view());
        auto key = to_slice(task.key);
        if (val.size())
            batch.Put(key, val);
        else
            batch.Delete(key);
    }

    leveldb::WriteOptions options;
    if (c_options & ukv_option_write_flush_k)
        options.sync = true;

    level_status_t status = db.Write(options, &batch);
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

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_val_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {cols, keys};

    leveldb::ReadOptions options;

    // TODO:
    // Read entries one-by-one, exporting onto a tape.
    // On every read, a `malloc` and `memcpy` may accure, if
    // the tape is not long enough, but at least is not determined
    // to happen.
    *c_error = "Not Implemented!";
}

void ukv_collection_upsert( //
    ukv_t const,
    ukv_str_view_t,
    ukv_str_view_t,
    ukv_collection_t*,
    ukv_error_t* c_error) {
    *c_error = "Collections not supported by LevelDB!";
}

void ukv_collection_remove( //
    ukv_t const,
    ukv_str_view_t,
    ukv_error_t* c_error) {
    *c_error = "Collections not supported by LevelDB!";
}

void ukv_control( //
    ukv_t const,
    ukv_str_view_t,
    ukv_str_view_t*,
    ukv_error_t* c_error) {
    *c_error = "Controls not supported by LevelDB!";
}

void ukv_txn_begin( //
    ukv_t const,
    ukv_size_t const,
    ukv_txn_t*,
    ukv_error_t* c_error) {
    *c_error = "Transactions not supported by LevelDB!";
}

void ukv_txn_commit( //
    ukv_txn_t const,
    ukv_options_t const,
    ukv_error_t* c_error) {
    *c_error = "Transactions not supported by LevelDB!";
}

void ukv_arena_free(ukv_t const, ukv_val_ptr_t c_ptr, ukv_size_t c_len) {
    if (!c_ptr || !c_len)
        return;
    allocator_t {}.deallocate(reinterpret_cast<byte_t*>(c_ptr), c_len);
}

void ukv_txn_free(ukv_t const, ukv_txn_t) {
}

void ukv_collection_free(ukv_t const, ukv_collection_t const) {
}

void ukv_free(ukv_t c_db) {
    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);
    delete db;
}

void ukv_error_free(ukv_error_t const) {
}