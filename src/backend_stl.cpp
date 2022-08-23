/**
 * @file backend_stl.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded In-Memory Key-Value Store implementation using only @b STL.
 * This is not the fastest, not the smartest possible solution for @b ACID KVS,
 * but is a good reference design for educational purposes.
 * Deficiencies:
 * > Global Lock.
 * > No support for range queries.
 * > Keeps track of all the deleted keys throughout the history.
 */

#include <vector>
#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <mutex>      // `std::unique_lock`
#include <numeric>    // `std::accumulate`
#include <atomic>     // Thread-safe generation counters
#include <filesystem> // Enumerating the directory
#include <stdio.h>    // Saving/reading from disk

#include "ukv/db.h"
#include "helpers.hpp"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_col_t ukv_col_main_k = 0;
ukv_val_len_t ukv_val_len_missing_k = std::numeric_limits<ukv_val_len_t>::max();
ukv_key_t ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;
namespace fs = std::filesystem;

struct stl_db_t;
struct stl_col_t;
struct stl_txn_t;

struct stl_value_t {
    buffer_t buffer;
    generation_t generation {0};
    bool is_deleted {false};
};

struct stl_col_t {
    std::string name;
    /**
     * @brief Primary data-store.
     * Associative container is used to allow scans.
     */
    std::map<ukv_key_t, stl_value_t> pairs;

    /**
     * @brief Keeps the number of unique elements submitted to the store.
     * It may be different from `pairs.size()`, if some of the entries
     * were deleted.
     */
    std::atomic<std::size_t> unique_elements;

    void reserve_more(std::size_t) {
        //  pairs.reserve(pairs.size() + n);
    }
};

using stl_collection_ptr_t = std::unique_ptr<stl_col_t>;

struct stl_txn_t {
    std::map<col_key_t, buffer_t> upserted;
    std::unordered_map<col_key_t, generation_t, sub_key_hash_t> requested;
    std::unordered_set<col_key_t, sub_key_hash_t> removed;

    stl_db_t* db_ptr {nullptr};
    generation_t generation {0};
};

struct stl_db_t {
    std::shared_mutex mutex;
    stl_col_t main;

    /**
     * @brief A variable-size set of named cols.
     * It's cleaner to implement it with heterogenous lookups as
     * an @c `std::unordered_set`, but it requires GCC11.
     */
    std::unordered_map<std::string_view, stl_collection_ptr_t> named;
    /**
     * @brief The generation/transactions ID of the most recent update.
     * This can be updated even outside of the main @p `mutex` on HEAD state.
     */
    std::atomic<generation_t> youngest_generation {0};
    /**
     * @brief Path on disk, from which the data will be read.
     * When closed, we will try saving the DB on disk.
     */
    std::string persisted_path;
};

stl_col_t& stl_col(stl_db_t& db, ukv_col_t col) {
    return col == ukv_col_main_k ? db.main : *reinterpret_cast<stl_col_t*>(col);
}

void save_to_disk(stl_col_t const& col, std::string const& path, ukv_error_t* c_error) {
    // Using the classical C++ IO mechanisms is a bad tone in the modern world.
    // They are ugly and, more importantly, painfully slow.
    // https://www.reddit.com/r/cpp_questions/comments/e2xia9/performance_comparison_of_various_ways_of_reading/
    //
    // So instead we stick to the LibC way of doing things.
    // POSIX API would have been even better, but LibC will provide
    // higher portability for this reference implementation.
    // https://www.ibm.com/docs/en/i/7.1?topic=functions-fopen-open-files
    file_handle_t handle;
    if ((*c_error = handle.open(path.c_str(), "wb+").release_error()))
        return;

    // Save the collection size
    {
        auto n = static_cast<ukv_size_t>(col.unique_elements.load());
        auto saved_len = std::fwrite(&n, sizeof(ukv_size_t), 1, handle);
        if (saved_len != sizeof(ukv_size_t)) {
            *c_error = "Couldn't write anything to file.";
            return;
        }
    }

    // Save the entries
    for (auto const& [key, seq_val] : col.pairs) {
        if (seq_val.is_deleted)
            continue;

        auto saved_len = std::fwrite(&key, sizeof(ukv_key_t), 1, handle);
        if (saved_len != sizeof(ukv_key_t)) {
            *c_error = "Write partially failed on key.";
            return;
        }

        auto const& buf = seq_val.buffer;
        auto buf_len = static_cast<ukv_val_len_t>(buf.size());
        saved_len = std::fwrite(&buf_len, sizeof(ukv_val_len_t), 1, handle);
        if (saved_len != sizeof(ukv_val_len_t)) {
            *c_error = "Write partially failed on value len.";
            return;
        }

        saved_len = std::fwrite(buf.data(), sizeof(byte_t), buf.size(), handle);
        if (saved_len != buf.size()) {
            *c_error = "Write partially failed on value.";
            return;
        }
    }

    *c_error = handle.close().release_error();
}

void read_from_disk(stl_col_t& col, std::string const& path, ukv_error_t* c_error) {
    // Similar to serialization, we don't use STL here
    file_handle_t handle;
    if ((*c_error = handle.open(path.c_str(), "rb+").release_error()))
        return;

    // Get the col size, to preallocate entries
    auto n = ukv_size_t(0);
    {
        auto read_len = std::fread(&n, sizeof(ukv_size_t), 1, handle);
        if (read_len != sizeof(ukv_size_t)) {
            *c_error = "Couldn't read anything from file.";
            return;
        }
    }

    // Load the entries
    col.pairs.clear();
    col.reserve_more(n);
    col.unique_elements = n;

    for (ukv_size_t i = 0; i != n; ++i) {

        auto key = ukv_key_t {};
        auto read_len = std::fread(&key, sizeof(ukv_key_t), 1, handle);
        if (read_len != sizeof(ukv_key_t)) {
            *c_error = "Read partially failed on key.";
            return;
        }

        auto buf_len = ukv_val_len_t(0);
        read_len = std::fread(&buf_len, sizeof(ukv_val_len_t), 1, handle);
        if (read_len != sizeof(ukv_val_len_t)) {
            *c_error = "Read partially failed on value len.";
            return;
        }

        auto buf = buffer_t(buf_len);
        read_len = std::fread(buf.data(), sizeof(byte_t), buf.size(), handle);
        if (read_len != buf.size()) {
            *c_error = "Read partially failed on value.";
            return;
        }

        col.pairs.emplace(key, stl_value_t {std::move(buf), generation_t {0}, false});
    }

    *c_error = handle.close().release_error();
}

void save_to_disk(stl_db_t const& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    if (!fs::is_directory(dir_path)) {
        *c_error = "Supplied path is not a directory!";
        return;
    }

    save_to_disk(db.main, dir_path / ".stl.ukv", c_error);
    if (*c_error)
        return;

    for (auto const& name_and_col : db.named) {
        auto name_with_ext = std::string(name_and_col.first) + ".stl.ukv";
        save_to_disk(*name_and_col.second, dir_path / name_with_ext, c_error);
        if (*c_error)
            return;
    }
}

void read_from_disk(stl_db_t& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    if (!fs::is_directory(dir_path)) {
        *c_error = "Supplied path is not a directory!";
        return;
    }

    // Parse the main main col
    if (fs::path path = dir_path / ".stl.ukv"; fs::is_regular_file(path)) {
        auto path_str = path.native();
        read_from_disk(db.main, path_str, c_error);
    }

    // Parse all the named cols we can find
    for (auto const& dir_entry : fs::directory_iterator {dir_path}) {
        if (!dir_entry.is_regular_file())
            continue;
        fs::path const& path = dir_entry.path();
        auto path_str = path.native();
        if (path_str.size() <= 8 || path_str.substr(path_str.size() - 8) != ".stl.ukv")
            continue;

        auto filename_w_ext = path.filename().native();
        auto filename = filename_w_ext.substr(0, filename_w_ext.size() - 8);
        auto col = std::make_unique<stl_col_t>();
        col->name = filename;
        read_from_disk(*col, path_str, c_error);
        db.named.emplace(std::string_view(col->name), std::move(col));
    }
}

void write_head( //
    stl_db_t& db,
    write_tasks_soa_t tasks,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    std::unique_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != tasks.count; ++i) {

        write_task_t task = tasks[i];
        stl_col_t& col = stl_col(db, task.col);
        auto key_iterator = col.pairs.find(task.key);

        // We want to insert a new entry, but let's check if we
        // can overwrite the existing value without causing reallocations.
        try {
            if (key_iterator != col.pairs.end()) {
                auto value = task.view();
                key_iterator->second.generation = ++db.youngest_generation;
                key_iterator->second.buffer.assign(value.begin(), value.end());
                key_iterator->second.is_deleted = task.is_deleted();
            }
            else if (!task.is_deleted()) {
                stl_value_t value_w_generation {task.buffer(), ++db.youngest_generation};
                col.pairs.emplace(task.key, std::move(value_w_generation));
                ++col.unique_elements;
            }
        }
        catch (...) {
            *c_error = "Failed to put!";
            break;
        }
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        save_to_disk(db, c_error);
}

void write_txn( //
    stl_txn_t& txn,
    write_tasks_soa_t tasks,
    ukv_options_t const,
    ukv_error_t* c_error) {

    // No need for locking here, until we commit, unless, of course,
    // a col is being deleted.
    stl_db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        write_task_t task = tasks[i];

        try {
            if (task.is_deleted()) {
                txn.upserted.erase(task.location());
                txn.removed.insert(task.location());
            }
            else {
                txn.upserted.insert_or_assign(task.location(), task.buffer());
            }
        }
        catch (...) {
            *c_error = "Failed to put into transaction!";
            break;
        }
    }
}

template <typename value_enumerator_at>
void read_head_under_lock( //
    stl_db_t& db,
    read_tasks_soa_t tasks,
    ukv_options_t const,
    value_enumerator_at enumerator,
    ukv_error_t*) {

    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        read_task_t task = tasks[i];
        stl_col_t const& col = stl_col(db, task.col);
        auto key_iterator = col.pairs.find(task.key);
        bool found = key_iterator != col.pairs.end() && !key_iterator->second.is_deleted;
        auto value = found ? value_view(key_iterator->second.buffer) : value_view_t {};
        enumerator(i, value);
    }
}

template <typename value_enumerator_at>
void read_txn_under_lock( //
    stl_txn_t& txn,
    read_tasks_soa_t tasks,
    ukv_options_t const c_options,
    value_enumerator_at enumerator,
    ukv_error_t* c_error) {

    stl_db_t& db = *txn.db_ptr;
    generation_t const youngest_generation = db.youngest_generation.load();
    bool const should_track_requests = c_options & ukv_option_read_track_k;

    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        read_task_t task = tasks[i];
        stl_col_t const& col = stl_col(db, task.col);

        // Some keys may already be overwritten inside of transaction
        if (auto inner_iterator = txn.upserted.find(task.location()); inner_iterator != txn.upserted.end())
            enumerator(i, value_view(inner_iterator->second));

        // Some may have been deleted inside the transaction
        else if (auto inner_iterator = txn.removed.find(task.location()); inner_iterator != txn.removed.end())
            enumerator(i, value_view_t {});

        // Others should be pulled from the main store
        else if (auto key_iterator = col.pairs.find(task.key); key_iterator != col.pairs.end()) {

            if (entry_was_overwritten(key_iterator->second.generation, txn.generation, youngest_generation) &&
                (*c_error = "Requested key was already overwritten since the start of the transaction!"))
                return;

            bool found = !key_iterator->second.is_deleted;
            auto value = found ? value_view(key_iterator->second.buffer) : value_view_t {};
            enumerator(i, value);

            if (should_track_requests)
                txn.requested.emplace(task.location(), key_iterator->second.generation);
        }

        // But some will be missing
        else {
            enumerator(i, value_view_t {});

            if (should_track_requests)
                txn.requested.emplace(task.location(), generation_t {});
        }
    }
}

void scan_head( //
    stl_db_t& db,
    scan_tasks_soa_t tasks,
    ukv_options_t const options,
    ukv_size_t** c_found_counts,
    ukv_key_t*** c_found_keys,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    std::shared_lock _ {db.mutex};

    // 1. Allocate a tape for all the values to be fetched
    auto counts = arena.alloc_or_dummy<ukv_size_t>(tasks.count, c_error, c_found_counts);
    if (*c_error)
        return;
    auto total_keys = reduce_n(tasks.lengths, tasks.count, 0ul);
    auto keys_columns = arena.alloc_or_dummy<ukv_key_t*>(tasks.count, c_error, c_found_keys);
    auto keys = arena.alloc<ukv_key_t>(total_keys, c_error);
    if (*c_error)
        return;

    // 2. Fetch the data
    ukv_size_t keys_fill_progress = 0;
    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        scan_task_t task = tasks[i];
        stl_col_t const& col = stl_col(db, task.col);
        auto key_iterator = col.pairs.lower_bound(task.min_key);
        auto keys_column = keys_columns[i] = keys.begin() + keys_fill_progress;

        ukv_size_t j = 0;
        for (; j != task.length && key_iterator != col.pairs.end(); ++key_iterator) {
            if (key_iterator->second.is_deleted)
                continue;
            keys_column[j] = key_iterator->first;
            ++j;
        }

        counts[i] = j;
        keys_fill_progress += task.length;
    }
}

void scan_txn( //
    stl_txn_t& txn,
    scan_tasks_soa_t tasks,
    ukv_options_t const options,
    ukv_size_t** c_found_counts,
    ukv_key_t*** c_found_keys,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    stl_db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    // 1. Allocate a tape for all the values to be fetched
    auto counts = arena.alloc_or_dummy<ukv_size_t>(tasks.count, c_error, c_found_counts);
    if (*c_error)
        return;
    auto total_keys = reduce_n(tasks.lengths, tasks.count, 0ul);
    auto keys_columns = arena.alloc_or_dummy<ukv_key_t*>(tasks.count, c_error, c_found_keys);
    auto keys = arena.alloc<ukv_key_t>(total_keys, c_error);
    if (*c_error)
        return;

    // 2. Fetch the data
    ukv_size_t keys_fill_progress = 0;
    for (ukv_size_t i = 0; i != tasks.count; ++i) {
        scan_task_t task = tasks[i];
        stl_col_t const& col = stl_col(db, task.col);
        auto key_iterator = col.pairs.lower_bound(task.min_key);
        auto txn_iterator = txn.upserted.lower_bound(task.min_key);
        auto keys_column = keys_columns[i] = keys.begin() + keys_fill_progress;

        ukv_size_t j = 0;
        for (; j != task.length && key_iterator != col.pairs.end();) {
            // Check if the key was already removed:
            if (key_iterator->second.is_deleted || txn.removed.find(key_iterator->first) != txn.removed.end()) {
                ++key_iterator;
                continue;
            }

            // Compare against the incoming inserted keys:
            bool check_in_txn = txn_iterator != txn.upserted.end() && txn_iterator->first.col == task.col;
            if (check_in_txn && txn_iterator->first.key <= key_iterator->first) {
                keys_column[j] = txn_iterator->first.key;
                ++txn_iterator;
                ++j;
                continue;
            }

            // Export from the main store:
            keys_column[j] = key_iterator->first;
            ++key_iterator;
            ++j;
        }

        // As in any `set_union`, don't forget the tail :)
        while (j != task.length && txn_iterator != txn.upserted.end() && txn_iterator->first.col == task.col) {
            keys_column[j] = txn_iterator->first.key;
            ++txn_iterator;
            ++j;
        }

        counts[i] = j;
        keys_fill_progress += task.length;
    }
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_db_open( //
    ukv_str_view_t c_config,
    ukv_t* c_db,
    ukv_error_t* c_error) {

    try {
        auto db_ptr = new stl_db_t {};
        auto len = std::strlen(c_config);
        if (len) {
            db_ptr->persisted_path = std::string(c_config, len);
            read_from_disk(*db_ptr, c_error);
        }
        *c_db = db_ptr;
    }
    catch (...) {
        *c_error = "Failed to initialize the database";
    }
}

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    ukv_1x8_t** c_found_nulls,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {cols, keys, c_tasks_count};
    bool const needs_export = c_found_values != nullptr;

    // 1. Allocate a tape for all the values to be pulled
    auto offs = arena.alloc_or_dummy<ukv_val_len_t>(tasks.count + 1, c_error, c_found_offsets);
    if (*c_error)
        return;
    auto lens = arena.alloc_or_dummy<ukv_val_len_t>(tasks.count, c_error, c_found_lengths);
    if (*c_error)
        return;
    auto nulls = arena.alloc_or_dummy<ukv_1x8_t>(tasks.count, c_error, c_found_nulls);
    if (*c_error)
        return;

    // 2. Pull metadata
    std::size_t total_length = 0;
    auto meta_enumerator = [&](std::size_t i, value_view_t value) {
        nulls[i] = value;
        lens[i] = value ? value.size() : ukv_val_len_missing_k;
        total_length += value.size();
    };

    std::shared_lock _ {db.mutex};
    c_txn ? read_txn_under_lock(txn, tasks, c_options, meta_enumerator, c_error)
          : read_head_under_lock(db, tasks, c_options, meta_enumerator, c_error);
    if (!needs_export)
        return;

    // 3. Pull the data, once we know the total length
    ukv_val_len_t progress_in_tape = 0;
    ukv_val_len_t last_value_length = 0;
    auto tape = arena.alloc<byte_t>(total_length, c_error);
    auto data_enumerator = [&](std::size_t i, value_view_t value) {
        offs[i] = progress_in_tape;
        std::memcpy(tape.begin() + progress_in_tape, value.begin(), value.size());
        progress_in_tape += value.size();
    };

    c_txn ? read_txn_under_lock(txn, tasks, c_options, data_enumerator, c_error)
          : read_head_under_lock(db, tasks, c_options, data_enumerator, c_error);

    *c_found_values = reinterpret_cast<ukv_val_ptr_t>(tape.begin());
    if (needs_export)
        offs[tasks.count] = progress_in_tape;
}

void ukv_write( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
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

    ukv_1x8_t const* c_nulls,

    ukv_options_t const c_options,
    ukv_arena_t*,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    strided_range_gt<ukv_1x8_t const> nulls {c_nulls};
    write_tasks_soa_t tasks {cols, keys, vals, offs, lens, nulls, c_tasks_count};

    return c_txn ? write_txn(txn, tasks, c_options, c_error) : write_head(db, tasks, c_options, c_error);
}

void ukv_scan( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const c_min_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_min_keys,
    ukv_size_t const c_min_keys_stride,

    ukv_size_t const* c_scan_lengths,
    ukv_size_t const c_scan_lengths_stride,

    ukv_options_t const c_options,

    ukv_size_t** c_found_counts,
    ukv_key_t*** c_found_keys,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_min_keys, c_min_keys_stride};
    strided_iterator_gt<ukv_size_t const> lens {c_scan_lengths, c_scan_lengths_stride};
    scan_tasks_soa_t tasks {cols, keys, lens, c_min_tasks_count};

    return c_txn ? scan_txn(txn, tasks, c_options, c_found_counts, c_found_keys, arena, c_error)
                 : scan_head(db, tasks, c_options, c_found_counts, c_found_keys, arena, c_error);
}

void ukv_size( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const n,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

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
    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;
    auto estimates = *c_found_estimates = arena.alloc<ukv_size_t>(6 * n, c_error).begin();
    if (*c_error)
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> min_keys {c_min_keys, c_min_keys_stride};
    strided_iterator_gt<ukv_key_t const> max_keys {c_max_keys, c_max_keys_stride};

    std::shared_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != n; ++i) {
        stl_col_t const& col = stl_col(db, cols[i]);
        ukv_key_t const min_key = min_keys[i];
        ukv_key_t const max_key = max_keys[i];
        std::size_t deleted_count = 0;

        // Estimate the presence in the main store
        std::size_t main_count = 0;
        std::size_t main_bytes = 0;
        auto min_iterator = col.pairs.lower_bound(min_key);
        auto max_iterator = col.pairs.lower_bound(max_key);
        for (; min_iterator != max_iterator; ++min_iterator) {
            if (min_iterator->second.is_deleted) {
                ++deleted_count;
                continue;
            }
            ++main_count;
            main_bytes += min_iterator->second.buffer.size();
        }

        // Estimate the metrics from within a transaction
        std::size_t txn_count = 0;
        std::size_t txn_bytes = 0;
        if (c_txn) {
            auto min_iterator = txn.upserted.lower_bound(min_key);
            auto max_iterator = txn.upserted.lower_bound(max_key);
            for (; min_iterator != max_iterator; ++min_iterator, ++txn_count)
                txn_bytes += min_iterator->second.size();
            deleted_count += txn.removed.size();
        }

        //
        auto estimate = estimates + i * 6;
        estimate[0] = static_cast<ukv_size_t>(main_count);
        estimate[1] = static_cast<ukv_size_t>(main_count + txn_count);
        estimate[2] = static_cast<ukv_size_t>(main_bytes);
        estimate[3] = static_cast<ukv_size_t>(main_bytes + txn_bytes);
        estimate[4] = estimate[0] * (sizeof(ukv_key_t) + sizeof(ukv_val_len_t)) + estimate[2];
        estimate[5] = (estimate[1] + deleted_count) * (sizeof(ukv_key_t) + sizeof(ukv_val_len_t)) + estimate[3];
    }
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ukv_col_open(
    // Inputs:
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_str_view_t,
    // Outputs:
    ukv_col_t* c_col,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    auto name_len = std::strlen(c_col_name);
    if (!name_len) {
        *c_col = ukv_col_main_k;
        return;
    }

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    std::unique_lock _ {db.mutex};

    auto const col_name = std::string_view(c_col_name, name_len);
    auto col_it = db.named.find(col_name);
    if (col_it == db.named.end()) {
        try {
            auto new_col = std::make_unique<stl_col_t>();
            new_col->name = col_name;
            *c_col = reinterpret_cast<ukv_col_t>(new_col.get());
            db.named.emplace(new_col->name, std::move(new_col));
        }
        catch (...) {
            *c_error = "Failed to create a new col!";
        }
    }
    else {
        *c_col = reinterpret_cast<ukv_col_t>(col_it->second.get());
    }
}

void ukv_col_remove(
    // Inputs:
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    // Outputs:
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_col_name);
    if (!name_len) {
        db.main.pairs.clear();
        db.main.unique_elements = 0;
    }
    else {
        auto col_name = std::string_view(c_col_name, name_len);
        auto col_it = db.named.find(col_name);
        if (col_it != db.named.end()) {
            db.named.erase(col_it);
        }
    }
}

void ukv_col_list( //
    ukv_t const c_db,
    ukv_size_t* c_count,
    ukv_col_t** c_ids,
    ukv_val_len_t** c_offs,
    ukv_str_view_t* c_names,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    if (!c_count || !c_names) {
        *c_error = "Names and count outputs can't be NULL";
        return;
    }

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    std::shared_lock _ {db.mutex};
    std::size_t cols_count = db.named.size();
    *c_count = static_cast<ukv_size_t>(cols_count);

    // Every string will be null-terminated
    std::size_t strings_length = 0;
    for (auto const& name_and_contents : db.named)
        strings_length += name_and_contents.first.size() + 1;
    auto names = arena.alloc<char>(strings_length, c_error).begin();
    *c_names = names;
    if (*c_error)
        return;

    // For every collection we also need to export IDs and offsets
    auto ids = arena.alloc_or_dummy<ukv_col_t>(cols_count, c_error, c_ids);
    if (*c_error)
        return;
    auto offs = arena.alloc_or_dummy<ukv_val_len_t>(cols_count + 1, c_error, c_offs);
    if (*c_error)
        return;

    std::size_t i = 0;
    for (auto const& name_and_contents : db.named) {
        auto len = name_and_contents.first.size();
        std::memcpy(names, name_and_contents.first.data(), len);
        names[len] = '\0';
        ids[i] = reinterpret_cast<ukv_col_t>(name_and_contents.second.get());
        offs[i] = static_cast<ukv_val_len_t>(names - *c_names);
        names += len + 1;
        ++i;
    }
    offs[i] = static_cast<ukv_val_len_t>(names - *c_names);
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

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_txn_begin(
    // Inputs:
    ukv_t const c_db,
    ukv_size_t const c_generation,
    ukv_options_t const,
    // Outputs:
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    if (!*c_txn) {
        try {
            *c_txn = new stl_txn_t();
        }
        catch (...) {
            *c_error = "Failed to initialize the transaction";
        }
    }

    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(*c_txn);
    txn.db_ptr = &db;
    txn.generation = c_generation ? c_generation : ++db.youngest_generation;
    txn.requested.clear();
    txn.upserted.clear();
    txn.removed.clear();
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    if (!c_txn && (*c_error = "Transaction is NULL!"))
        return;

    // This write may fail with out-of-memory errors, if Hash-Tables
    // bucket allocation fails, but no values will be copied, only moved.
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    stl_db_t& db = *txn.db_ptr;
    std::unique_lock _ {db.mutex};
    generation_t const youngest_generation = db.youngest_generation.load();

    // 1. Check for refreshes among fetched keys
    for (auto const& [col_key, sub_generation] : txn.requested) {
        stl_col_t& col = stl_col(db, col_key.col);
        auto key_iterator = col.pairs.find(col_key.key);
        if (key_iterator == col.pairs.end())
            continue;
        if (key_iterator->second.generation != sub_generation &&
            (*c_error = "Requested key was already overwritten since the start of the transaction!"))
            return;
    }

    // 2. Check for collisions among incoming values
    for (auto const& [col_key, value] : txn.upserted) {
        stl_col_t& col = stl_col(db, col_key.col);
        auto key_iterator = col.pairs.find(col_key.key);
        if (key_iterator == col.pairs.end())
            continue;

        if (key_iterator->second.generation == txn.generation && (*c_error = "Can't commit same entry more than once!"))
            return;

        if (entry_was_overwritten(key_iterator->second.generation, txn.generation, youngest_generation) &&
            (*c_error = "Incoming key collides with newer entry!"))
            return;
    }

    // 3. Check for collisions among deleted values
    for (auto const& col_key : txn.removed) {
        stl_col_t& col = stl_col(db, col_key.col);
        auto key_iterator = col.pairs.find(col_key.key);
        if (key_iterator == col.pairs.end())
            continue;

        if (key_iterator->second.generation == txn.generation && (*c_error = "Can't commit same entry more than once!"))
            return;

        if (entry_was_overwritten(key_iterator->second.generation, txn.generation, youngest_generation) &&
            (*c_error = "Removed key collides with newer entry!"))
            return;
    }

    // 4. Allocate space for more nodes across different cols
    try {
        db.main.reserve_more(txn.upserted.size());
        for (auto& name_and_col : db.named)
            name_and_col.second->reserve_more(txn.upserted.size());
    }
    catch (...) {
        *c_error = "Not enough memory!";
        return;
    }

    // 5. Import the data, as no collisions were detected
    for (auto& sub_key_and_value : txn.upserted) {
        stl_col_t& col = stl_col(db, sub_key_and_value.first.col);
        auto key_iterator = col.pairs.find(sub_key_and_value.first.key);
        // A key was deleted:
        // if (sub_key_and_value.second.empty()) {
        //     if (key_iterator != col.pairs.end())
        //         col.pairs.erase(key_iterator);
        // }
        // A keys was updated:
        // else
        if (key_iterator != col.pairs.end()) {
            key_iterator->second.generation = txn.generation;
            std::swap(key_iterator->second.buffer, sub_key_and_value.second);
        }
        // A key was inserted:
        else {
            stl_value_t value_w_generation {std::move(sub_key_and_value.second), txn.generation};
            col.pairs.emplace(sub_key_and_value.first.key, std::move(value_w_generation));
            ++col.unique_elements;
        }
    }

    // 6. Remove the requested entries
    for (auto const& col_key : txn.removed) {
        stl_col_t& col = stl_col(db, col_key.col);
        auto key_iterator = col.pairs.find(col_key.key);
        if (key_iterator == col.pairs.end())
            continue;

        key_iterator->second.is_deleted = true;
        key_iterator->second.generation = txn.generation;
        key_iterator->second.buffer.clear();
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        save_to_disk(db, c_error);
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_t const, ukv_arena_t c_arena) {
    if (!c_arena)
        return;
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(c_arena);
    delete &arena;
}

void ukv_txn_free(ukv_t const, ukv_txn_t const c_txn) {
    if (!c_txn)
        return;
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    delete &txn;
}

void ukv_db_free(ukv_t c_db) {
    if (!c_db)
        return;
    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    delete &db;
}

void ukv_col_free(ukv_t const, ukv_col_t const) {
    // In this in-memory freeing the col handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
