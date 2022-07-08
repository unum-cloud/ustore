/**
 * @file ukv_leveldb.cpp
 * @author Ashot Vardanian
 *
 * @brief LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string
 * keys to string values
 */

#include <leveldb/write_batch.h>
#include <leveldb/db.h>

#include "ukv.h"
#include "helpers.hpp"

ukv_collection_t ukv_default_collection_k = NULL;

using namespace unum::ukv;
using namespace unum;

using level_db_t = leveldb::DB;
using level_options_t = leveldb::Options;
using level_status_t = leveldb::Status;

inline leveldb::Slice to_slice(ukv_key_t const* key) noexcept {
    return {reinterpret_cast<char const*>(key), sizeof(ukv_key_t)};
}

inline leveldb::Slice to_slice(byte_t const* begin, ukv_val_len_t offset, ukv_val_len_t length) {
    return {reinterpret_cast<const char*>(begin + offset), length};
}

void ukv_open([[maybe_unused]] char const* c_config, ukv_t* c_db, ukv_error_t* c_error) {
    level_db_t* db;
    level_options_t options;
    options.create_if_missing = true;
    level_status_t status = level_db_t::Open(options, "./tmp/leveldb/", &db);
    if (!status.ok()) {
        *c_error = "Open Error";
        return;
    }
    *c_db = db;
}

void ukv_write( //
    ukv_t const c_db,
    [[maybe_unused]] ukv_txn_t const c_txn,

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

    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);
    leveldb::WriteBatch batch;

    strided_ptr_gt<ukv_collection_t> colls {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_ptr_gt<ukv_tape_ptr_t const> vals {c_vals, c_vals_stride};
    strided_ptr_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_ptr_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_soa_t tasks {colls, keys, vals, offs, lens};
    std::vector<write_task_t> task_arr(c_keys_count);

    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        task_arr[i] = tasks[i];
        batch.Put(to_slice(&task_arr[i].key), to_slice(task_arr[i].begin, task_arr[i].offset, task_arr[i].length_));
    }

    level_status_t status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok())
        *c_error = "Write Error";
}

void ukv_read( //
    ukv_t const c_db,
    [[maybe_unused]] ukv_txn_t const c_txn,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    [[maybe_unused]] ukv_options_t const c_options,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    level_status_t status;
    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);

    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {cols, keys};
    std::vector<read_task_t> task_arr(c_keys_count);
    std::vector<std::string> values(c_keys_count);
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * c_keys_count;

    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        task_arr[i] = tasks[i];
        status = db->Get(leveldb::ReadOptions(), to_slice(&task_arr[i].key), &values[i]);
        total_bytes += values[i].size();
    }

    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (*c_error)
        return;

    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * c_keys_count;

    for (std::size_t i = 0; i != values.size(); ++i) {
        if (!values[i].size())
            lens[i] = 0;
        auto len = values[i].size();
        std::memcpy(tape + exported_bytes, values[i].data(), len);
        lens[i] = static_cast<ukv_val_len_t>(len);
        exported_bytes += len;
    };
}

void ukv_collection_upsert( //
    [[maybe_unused]] ukv_t const c_db,
    [[maybe_unused]] ukv_str_view_t c_col_name,
    [[maybe_unused]] ukv_collection_t* c_col,
    ukv_error_t* c_error) {
    *c_error = "Collections Not Allowed";
}

void ukv_collection_remove( //
    [[maybe_unused]] ukv_t const c_db,
    [[maybe_unused]] ukv_str_view_t c_col_name,
    [[maybe_unused]] ukv_error_t* c_error) {
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
    [[maybe_unused]] ukv_t const c_db,
    [[maybe_unused]] ukv_size_t const sequence_number,
    [[maybe_unused]] ukv_txn_t* c_txn,
    ukv_error_t* c_error) {
    *c_error = "Transactions Not Allowed";
}

void ukv_txn_commit( //
    [[maybe_unused]] ukv_txn_t const c_txn,
    [[maybe_unused]] ukv_options_t const c_options,
    [[maybe_unused]] ukv_error_t* c_error) {
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
    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);
    delete db;
}

void ukv_error_free([[maybe_unused]] ukv_error_t const error) {
}