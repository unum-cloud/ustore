/**
 * @file backend_leveldb.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded Persistent Key-Value Store on top of @b LevelDB.
 * Has no support for collections, transactions or any non-CRUD jobs.
 */

#include <leveldb/db.h>
#include <leveldb/comparator.h>
#include <leveldb/write_batch.h>

#include "ukv/db.h"
#include "helpers.hpp"

using namespace unum::ukv;
using namespace unum;

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_col_t ukv_col_main_k = 0;
ukv_val_len_t ukv_val_len_missing_k = std::numeric_limits<ukv_val_len_t>::max();
ukv_key_t ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();

using level_db_t = leveldb::DB;
using level_status_t = leveldb::Status;
using level_options_t = leveldb::Options;
using level_iter_uptr_t = std::unique_ptr<leveldb::Iterator>;

struct key_comparator_t final : public leveldb::Comparator {

    inline int Compare(leveldb::Slice const& a, leveldb::Slice const& b) const override {
        auto ai = *reinterpret_cast<ukv_key_t const*>(a.data());
        auto bi = *reinterpret_cast<ukv_key_t const*>(b.data());
        if (ai == bi)
            return 0;
        return ai < bi ? -1 : 1;
    }

    char const* Name() const override { return "Integral"; }

    void FindShortestSeparator(std::string*, leveldb::Slice const&) const override {}

    void FindShortSuccessor(std::string* key) const override {
        auto& int_key = *reinterpret_cast<ukv_key_t*>(key->data());
        ++int_key;
    }
};

static key_comparator_t const key_comparator_k = {};

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

inline leveldb::Slice to_slice(ukv_key_t const& key) noexcept {
    return {reinterpret_cast<char const*>(&key), sizeof(ukv_key_t)};
}

inline leveldb::Slice to_slice(value_view_t value) noexcept {
    return {reinterpret_cast<const char*>(value.begin()), value.size()};
}

inline std::unique_ptr<std::string> make_value(ukv_error_t* c_error) noexcept {
    std::unique_ptr<std::string> value_uptr;
    try {
        value_uptr = std::make_unique<std::string>();
    }
    catch (...) {
        *c_error = "Fail to allocate value";
    }
    return value_uptr;
}

bool export_error(level_status_t const& status, ukv_error_t* c_error) {
    if (status.ok())
        return false;

    if (status.IsCorruption())
        *c_error = "Failure: DB Corruption";
    else if (status.IsIOError())
        *c_error = "Failure: IO  Error";
    else if (status.IsInvalidArgument())
        *c_error = "Failure: Invalid Argument";
    else
        *c_error = "Failure";
    return true;
}

void ukv_db_open(ukv_str_view_t, ukv_t* c_db, ukv_error_t* c_error) {
    try {
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
    catch (...) {
        *c_error = "Open Failure";
    }
}

void write_one( //
    level_db_t& db,
    write_tasks_soa_t const& tasks,
    leveldb::WriteOptions const& options,
    ukv_error_t* c_error) {

    auto task = tasks[0];
    auto key = to_slice(task.key);
    level_status_t status = task.is_deleted() ? db.Delete(options, key) : db.Put(options, key, to_slice(task.view()));
    export_error(status, c_error);
}

void write_many( //
    level_db_t& db,
    write_tasks_soa_t const& tasks,
    leveldb::WriteOptions const& options,
    ukv_error_t* c_error) {

    leveldb::WriteBatch batch;
    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        auto task = tasks[i];
        auto key = to_slice(task.key);
        if (task.is_deleted())
            batch.Delete(key);
        else
            batch.Put(key, to_slice(task.view()));
    }

    level_status_t status = db.Write(options, &batch);
    export_error(status, c_error);
}

void ukv_write( //
    ukv_t const c_db,
    ukv_txn_t const,
    ukv_size_t const c_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
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

    if (!c_db) {
        *c_error = "DataBase is NULL!";
        return;
    }

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_soa_t tasks {cols, keys, vals, offs, lens, c_tasks_count};

    leveldb::WriteOptions options;
    if (c_options & ukv_option_write_flush_k)
        options.sync = true;

    try {
        auto func = c_tasks_count == 1 ? &write_one : &write_many;
        func(db, tasks, options, c_error);
    }
    catch (...) {
        *c_error = "Write Failure";
    }
}

void measure_one( //
    level_db_t& db,
    read_tasks_soa_t const& tasks,
    leveldb::ReadOptions const& options,
    std::string& value,
    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    read_task_t task = tasks[0];
    level_status_t status = db.Get(options, to_slice(task.key), &value);
    if (!status.IsNotFound())
        if (export_error(status, c_error))
            return;

    auto exported_len = status.IsNotFound() ? ukv_val_len_missing_k : static_cast<ukv_size_t>(value.size());
    auto tape = arena.alloc<byte_t>(sizeof(ukv_size_t), c_error);
    if (*c_error)
        return;

    std::memcpy(tape.begin(), &exported_len, sizeof(ukv_size_t));
    *c_found_lengths = reinterpret_cast<ukv_val_len_t*>(tape.begin());
    *c_found_offsets = nullptr;
    *c_found_values = nullptr;
}

void read_one( //
    level_db_t& db,
    read_tasks_soa_t const& tasks,
    leveldb::ReadOptions const& options,
    std::string& value,
    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    read_task_t task = tasks[0];
    level_status_t status = db.Get(options, to_slice(task.key), &value);
    if (!status.IsNotFound())
        if (export_error(status, c_error))
            return;

    auto bytes_in_value = static_cast<ukv_val_len_t>(value.size());
    auto exported_len = status.IsNotFound() ? ukv_val_len_missing_k : bytes_in_value;
    ukv_val_len_t offset = 0;
    auto tape = arena.alloc<byte_t>(sizeof(ukv_val_len_t) * 2 + bytes_in_value, c_error);
    if (*c_error)
        return;

    std::memcpy(tape.begin(), &exported_len, sizeof(ukv_val_len_t));
    std::memcpy(tape.begin() + sizeof(ukv_val_len_t), &offset, sizeof(ukv_val_len_t));
    std::memcpy(tape.begin() + sizeof(ukv_val_len_t) * 2, value.data(), bytes_in_value);

    *c_found_lengths = reinterpret_cast<ukv_val_len_t*>(tape.begin());
    *c_found_offsets = *c_found_lengths + 1;
    *c_found_values = reinterpret_cast<ukv_val_ptr_t>(tape.begin() + sizeof(ukv_val_len_t) * 2);
}

void measure_many( //
    level_db_t& db,
    read_tasks_soa_t const& tasks,
    leveldb::ReadOptions const& options,
    std::string& value,
    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    span_gt<ukv_val_len_t> lens = arena.alloc<ukv_val_len_t>(tasks.count, c_error);
    if (*c_error)
        return;

    std::fill_n(lens.begin(), tasks.count, ukv_val_len_missing_k);
    *c_found_lengths = lens.begin();
    *c_found_offsets = nullptr;
    *c_found_values = nullptr;

    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        read_task_t task = tasks[i];
        level_status_t status = db.Get(options, to_slice(task.key), &value);
        if (status.IsNotFound())
            continue;
        if (export_error(status, c_error))
            return;
        lens[i] = static_cast<ukv_val_len_t>(value.size());
    }
}

void read_many( //
    level_db_t& db,
    read_tasks_soa_t const& tasks,
    leveldb::ReadOptions const& options,
    std::string& value,
    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    ukv_size_t lens_bytes = sizeof(ukv_val_len_t) * tasks.count;
    span_gt<byte_t> tape = arena.alloc<byte_t>(lens_bytes * 2, c_error);
    if (*c_error)
        return;

    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape.begin());
    ukv_val_len_t* offs = lens + tasks.count;
    ukv_val_ptr_t contents = reinterpret_cast<ukv_val_ptr_t>(offs + tasks.count);
    std::fill_n(lens, tasks.count * 2, ukv_val_len_missing_k);

    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        read_task_t task = tasks[i];
        level_status_t status = db.Get(options, to_slice(task.key), &value);
        if (status.IsNotFound())
            continue;
        if (export_error(status, c_error))
            return;

        auto old_tape_len = tape.size();
        auto bytes_in_value = value.size();
        tape = arena.alloc<byte_t>(old_tape_len + bytes_in_value, c_error);
        if (*c_error)
            return;

        lens = reinterpret_cast<ukv_val_len_t*>(tape.begin());
        offs = lens + tasks.count;
        contents = reinterpret_cast<ukv_val_ptr_t>(offs + tasks.count);

        std::memcpy(tape.begin() + old_tape_len, value.data(), bytes_in_value);
        lens[i] = static_cast<ukv_val_len_t>(bytes_in_value);
        offs[i] = reinterpret_cast<ukv_val_ptr_t>(tape.begin() + old_tape_len) - contents;
    }

    *c_found_lengths = lens;
    *c_found_offsets = offs;
    *c_found_values = contents;
}

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const,
    ukv_size_t const c_tasks_count,

    ukv_col_t const*,
    ukv_size_t const,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t& arena = *cast_arena(c_arena, c_error);
    if (*c_error)
        return;

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    leveldb::ReadOptions options;
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {{}, keys, c_tasks_count};

    auto value_uptr = make_value(c_error);
    std::string& value = *value_uptr.get();

    try {
        if (c_tasks_count == 1) {
            auto func = (c_options & ukv_option_read_lengths_k) ? &measure_one : &read_one;
            func(db, tasks, options, value, c_found_values, c_found_offsets, c_found_lengths, arena, c_error);
        }
        else {
            auto func = (c_options & ukv_option_read_lengths_k) ? &measure_many : &read_many;
            func(db, tasks, options, value, c_found_values, c_found_offsets, c_found_lengths, arena, c_error);
        }
    }
    catch (...) {
        *c_error = "Read Failure";
    }
}

void ukv_scan( //
    ukv_t const c_db,
    ukv_txn_t const,
    ukv_size_t const c_min_tasks_count,

    ukv_col_t const*,
    ukv_size_t const,

    ukv_key_t const* c_min_keys,
    ukv_size_t const c_min_keys_stride,

    ukv_size_t const* c_scan_lengths,
    ukv_size_t const c_scan_lengths_stride,

    ukv_options_t const c_options,

    ukv_key_t** c_found_keys,
    ukv_val_len_t** c_found_lengths,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t& arena = *cast_arena(c_arena, c_error);
    if (*c_error)
        return;

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    strided_iterator_gt<ukv_key_t const> keys {c_min_keys, c_min_keys_stride};
    strided_iterator_gt<ukv_size_t const> lens {c_scan_lengths, c_scan_lengths_stride};
    scan_tasks_soa_t tasks {{}, keys, lens, c_min_tasks_count};

    bool export_lengths = (c_options & ukv_option_read_lengths_k);
    leveldb::ReadOptions options;
    options.fill_cache = false;

    ukv_size_t total_lengths = reduce_n(tasks.lengths, tasks.count, 0ul);
    ukv_size_t total_bytes = total_lengths * sizeof(ukv_key_t);
    if (export_lengths)
        total_bytes += total_lengths * sizeof(ukv_val_len_t);

    span_gt<byte_t> tape = arena.alloc<byte_t>(total_bytes, c_error);
    if (*c_error)
        return;

    ukv_key_t* found_keys = reinterpret_cast<ukv_key_t*>(tape.begin());
    ukv_val_len_t* found_lens = reinterpret_cast<ukv_val_len_t*>(found_keys + total_lengths);
    *c_found_keys = found_keys;
    *c_found_lengths = export_lengths ? found_lens : nullptr;

    level_iter_uptr_t it;
    try {
        it = level_iter_uptr_t(db.NewIterator(options));
    }
    catch (...) {
        *c_error = "Fail To Create Iterator";
        return;
    }
    for (ukv_size_t i = 0; i != c_min_tasks_count; ++i) {
        scan_task_t task = tasks[i];
        it->Seek(to_slice(task.min_key));

        ukv_size_t j = 0;
        for (; it->Valid() && j != task.length; j++, it->Next()) {
            std::memcpy(&found_keys[j], it->key().data(), sizeof(ukv_key_t));
            if (export_lengths)
                found_lens[j] = static_cast<ukv_val_len_t>(it->value().size());
        }

        while (j != task.length) {
            found_keys[j] = ukv_key_unknown_k;
            if (export_lengths)
                found_lens[j] = ukv_val_len_missing_k;
            ++j;
        }

        found_keys += task.length;
        found_lens += task.length;
    }
}

void ukv_size( //
    ukv_t const c_db,
    ukv_txn_t const,
    ukv_size_t const n,

    ukv_col_t const*,
    ukv_size_t const,

    ukv_key_t const* c_min_keys,
    ukv_size_t const c_min_keys_stride,

    ukv_key_t const* c_max_keys,
    ukv_size_t const c_max_keys_stride,

    ukv_options_t const,

    ukv_size_t** c_found_estimates,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t& arena = *cast_arena(c_arena, c_error);
    if (*c_error)
        return;

    *c_found_estimates = arena.alloc<ukv_size_t>(6 * n, c_error).begin();
    if (*c_error)
        return;

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    strided_iterator_gt<ukv_key_t const> min_keys {c_min_keys, c_min_keys_stride};
    strided_iterator_gt<ukv_key_t const> max_keys {c_max_keys, c_max_keys_stride};
    uint64_t approximate_size = 0;
    std::optional<std::string> memory_usage;
    level_status_t status;

    for (ukv_size_t i = 0; i != n; ++i) {
        ukv_size_t* estimates = *c_found_estimates + i * 6;
        estimates[0] = static_cast<ukv_size_t>(0);
        estimates[1] = static_cast<ukv_size_t>(0);
        estimates[2] = static_cast<ukv_size_t>(0);
        estimates[3] = static_cast<ukv_size_t>(0);

        ukv_key_t const min_key = min_keys[i];
        ukv_key_t const max_key = max_keys[i];
        leveldb::Range range(to_slice(min_key), to_slice(max_key));
        try {
            db.GetApproximateSizes(&range, 1, &approximate_size);
            memory_usage = "0";
            db.GetProperty("leveldb.approximate-memory-usage", &memory_usage.value());
            estimates[4] = approximate_size;
            estimates[5] = std::stoi(memory_usage.value());
        }
        catch (...) {
            *c_error = "Property Read Failure";
        }
    }
}

void ukv_col_open( //
    ukv_t const,
    ukv_str_view_t c_col_name,
    ukv_str_view_t,
    ukv_col_t*,
    ukv_error_t* c_error) {
    if (c_col_name && std::strlen(c_col_name))
        *c_error = "Collections not supported by LevelDB!";
}

void ukv_col_remove( //
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    if (c_col_name && std::strlen(c_col_name) && (*c_error = "Collections not supported by LevelDB!"))
        return;

    level_db_t& db = *reinterpret_cast<level_db_t*>(c_db);
    leveldb::WriteBatch batch;
    auto it = std::unique_ptr<leveldb::Iterator>(db.NewIterator(leveldb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next())
        batch.Delete(it->key());
    level_status_t status = db.Write(leveldb::WriteOptions(), &batch);
    export_error(status, c_error);
}

void ukv_col_list( //
    ukv_t const,
    ukv_size_t* c_count,
    ukv_str_view_t* c_names,
    ukv_arena_t*,
    ukv_error_t* c_error) {
    *c_count = 0;
    *c_names = nullptr;
}

void ukv_db_control( //
    ukv_t const c_db,
    ukv_str_view_t c_request,
    ukv_str_view_t* c_response,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    if (!c_request && (*c_error = "Request is NULL!"))
        return;

    *c_response = NULL;
    *c_error = "Controls aren't supported in this implementation!";
}

void ukv_txn_begin( //
    ukv_t const,
    ukv_size_t const,
    ukv_options_t const,
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

void ukv_col_free(ukv_t const, ukv_col_t const) {
}

void ukv_db_free(ukv_t c_db) {
    if (!c_db)
        return;
    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);
    delete db;
}

void ukv_error_free(ukv_error_t const) {
}