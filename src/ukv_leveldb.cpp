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

#include "ukv/ukv.h"
#include "helpers.hpp"

ukv_collection_t ukv_default_collection_k = NULL;
ukv_val_len_t ukv_val_len_missing_k = 0;
ukv_key_t ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();

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

    strided_ptr_gt<ukv_collection_t const> cols {c_cols, c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_ptr_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_ptr_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_ptr_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_soa_t tasks {cols, keys, vals, offs, lens};

    leveldb::WriteBatch batch;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        auto task = tasks[i];
        auto key = to_slice(task.key);
        if (task.is_deleted())
            batch.Delete(key);
        else
            batch.Put(key, to_slice(task.view()));
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
    ukv_txn_t const,

    ukv_collection_t const*,
    ukv_size_t const,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_options_t const,

    ukv_val_len_t** c_found_lengths,
    ukv_val_ptr_t* c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    std::string value;
    level_status_t status;
    leveldb::ReadOptions options;
    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    stl_arena_t& arena = *cast_arena(c_arena, c_error);
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {strided_ptr_gt<ukv_collection_t const> {}, keys};

    ukv_size_t lens_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    ukv_size_t exported_bytes = lens_bytes;
    byte_t* tape = prepare_memory(arena.output_tape, lens_bytes, c_error);

    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        auto task = tasks[i];
        status = db.Get(options, to_slice(task.key), &value);
        if (!status.IsNotFound() && !status.ok()) {
            if (status.IsIOError())
                *c_error = "Read Failure: IO  Error";
            else if (status.IsInvalidArgument())
                *c_error = "Read Failure: Invalid Argument";
            else
                *c_error = "Read Failure";
            return;
        }

        auto len = value.size();
        tape = prepare_memory(arena.output_tape, exported_bytes + value.size(), c_error);
        ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(arena.output_tape.data());

        if (len) {
            std::memcpy(tape + exported_bytes, value.data(), len);
            lens[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        else
            lens[i] = ukv_val_len_missing_k;
    }

    *c_found_lengths = reinterpret_cast<ukv_val_len_t*>(arena.output_tape.data());
    *c_found_values = reinterpret_cast<ukv_val_ptr_t>(tape + lens_bytes);
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

void ukv_arena_free(ukv_t const, ukv_arena_t c_arena) {
    if (!c_arena)
        return;
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(c_arena);
    delete &arena;
}

void ukv_txn_free(ukv_t const, ukv_txn_t) {
}

void ukv_collection_free(ukv_t const, ukv_collection_t const) {
}

void ukv_free(ukv_t c_db) {
    if (!c_db)
        return;
    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);
    delete db;
}

void ukv_error_free(ukv_error_t const) {
}