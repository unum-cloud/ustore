/**
 * @file ukv_stl.cpp
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
#include <unordered_map>
#include <shared_mutex>
#include <atomic>     // Thread-safe sequence counters
#include <filesystem> // Enumerating the directory
#include <stdio.h>    // Saving/reading from disk

#include "ukv.h"
#include "helpers.hpp"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_collection_t ukv_default_collection_k = NULL;

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;
namespace fs = std::filesystem;

struct txn_t;
struct db_t;

struct sequenced_value_t {
    value_t data;
    sequence_t sequence_number {0};
};

struct col_t {
    std::string name;
    std::unordered_map<ukv_key_t, sequenced_value_t> pairs;

    void reserve_more(std::size_t n) { pairs.reserve(pairs.size() + n); }
};

using col_ptr_t = std::unique_ptr<col_t>;

struct located_key_t {
    col_t* col_ptr = nullptr;
    ukv_key_t key {0};

    inline bool operator==(located_key_t const& other) const noexcept {
        return (col_ptr == other.col_ptr) & (key == other.key);
    }
    inline bool operator!=(located_key_t const& other) const noexcept {
        return (col_ptr != other.col_ptr) | (key != other.key);
    }
};

struct located_key_hash_t {
    inline std::size_t operator()(located_key_t const& located) const noexcept {
        return std::hash<ukv_key_t> {}(located.key);
    }
};

struct txn_t {
    std::unordered_map<located_key_t, sequence_t, located_key_hash_t> requested_keys;
    std::unordered_map<located_key_t, value_t, located_key_hash_t> new_values;
    db_t* db_ptr {nullptr};
    sequence_t sequence_number {0};
};

struct db_t {
    std::shared_mutex mutex;
    col_t unnamed;

    /**
     * @brief A variable-size set of named cols.
     * It's cleaner to implement it with heterogenous lookups as
     * an @c `std::unordered_set`, but it requires GCC11.
     */
    std::unordered_map<std::string_view, col_ptr_t> named;
    /**
     * @brief The sequence/transactions ID of the most recent update.
     * This can be updated even outside of the main @p `mutex` on HEAD state.
     */
    std::atomic<sequence_t> youngest_sequence {0};
    /**
     * @brief Path on disk, from which the data will be read.
     * When closed, we will try saving the DB on disk.
     */
    std::string persisted_path;
};

struct read_task_t {
    col_t& collection;
    ukv_key_t key;

    inline located_key_t location() const noexcept { return located_key_t {&collection, key}; }
};

struct read_tasks_t {
    db_t& db;
    strided_ptr_gt<ukv_collection_t> cols;
    strided_ptr_gt<ukv_key_t const> keys;

    inline read_task_t operator[](ukv_size_t i) const noexcept {
        col_t& col = cols && cols[i] ? *reinterpret_cast<col_t*>(cols[i]) : db.unnamed;
        ukv_key_t key = keys[i];
        return {col, key};
    }
};

struct write_task_t {
    col_t& collection;
    ukv_key_t key;
    byte_t const* begin;
    ukv_val_len_t length;

    inline located_key_t location() const noexcept { return located_key_t {&collection, key}; }
    value_t value() const { return {begin, begin + length}; }
};

struct write_tasks_t {
    db_t& db;
    strided_ptr_gt<ukv_collection_t> cols;
    strided_ptr_gt<ukv_key_t const> keys;
    strided_ptr_gt<ukv_tape_ptr_t const> vals;
    strided_ptr_gt<ukv_val_len_t const> lens;

    inline write_task_t operator[](ukv_size_t i) const noexcept {
        col_t& col = cols && cols[i] ? *reinterpret_cast<col_t*>(cols[i]) : db.unnamed;
        ukv_key_t key = keys[i];
        byte_t const* begin;
        ukv_val_len_t len;
        if (vals) {
            begin = reinterpret_cast<byte_t const*>(vals[i]);
            len = lens ? lens[i] : std::strlen(reinterpret_cast<char const*>(vals[i]));
        }
        else {
            begin = nullptr;
            len = 0;
        }
        return {col, key, begin, len};
    }
};

void save_to_disk(col_t const& col, std::string const& path, ukv_error_t* c_error) {
    // Using the classical C++ IO mechanisms is a bad tone in the modern world.
    // They are ugly and, more importantly, painly slow.
    // https://www.reddit.com/r/cpp_questions/comments/e2xia9/performance_comparison_of_various_ways_of_reading/
    //
    // So instead we stick to the LibC way of doing things.
    // POSIX API would have been even better, but LibC will provide
    // higher portability for this reference implementation.
    // https://www.ibm.com/docs/en/i/7.1?topic=functions-fopen-open-files
    FILE* handle = fopen(path.c_str(), "wb+");

    // Save the collection size
    {
        auto n = static_cast<ukv_size_t>(col.pairs.size());
        auto saved_len = fwrite(&n, sizeof(ukv_size_t), 1, handle);
        if (saved_len != sizeof(ukv_size_t)) {
            *c_error = "Couldn't write anything to file.";
            goto cleanup;
        }
    }

    // Save the entries
    for (auto const& [key, seq_val] : col.pairs) {
        auto saved_len = fwrite(&key, sizeof(ukv_key_t), 1, handle);
        if (saved_len != sizeof(ukv_key_t)) {
            *c_error = "Write partially failed on key.";
            break;
        }

        auto const& val = seq_val.data;
        auto val_len = static_cast<ukv_val_len_t>(val.size());
        saved_len = fwrite(&val_len, sizeof(ukv_val_len_t), 1, handle);
        if (saved_len != sizeof(ukv_val_len_t)) {
            *c_error = "Write partially failed on value len.";
            break;
        }

        saved_len = fwrite(val.data(), sizeof(byte_t), val.size(), handle);
        if (saved_len != val.size()) {
            *c_error = "Write partially failed on value.";
            break;
        }
    }

cleanup:
    if (fclose(handle) == EOF)
        *c_error = "Couldn't close the file after write.";
}

void read_from_disk(col_t& col, std::string const& path, ukv_error_t* c_error) {
    // Similar to serialization, we don't use STL here
    FILE* handle = fopen(path.c_str(), "rb+");

    // Get the col size, to preallocate entries
    auto n = ukv_size_t(0);
    {
        auto read_len = fread(&n, sizeof(ukv_size_t), 1, handle);
        if (read_len != sizeof(ukv_size_t)) {
            *c_error = "Couldn't read anything from file.";
            goto cleanup;
        }
    }

    // Save the entries
    col.pairs.reserve(n);
    for (ukv_size_t i = 0; i != n; ++i) {

        auto key = ukv_key_t {};
        auto read_len = fread(&key, sizeof(ukv_key_t), 1, handle);
        if (read_len != sizeof(ukv_key_t)) {
            *c_error = "Read partially failed on key.";
            break;
        }

        auto val_len = ukv_val_len_t(0);
        read_len = fread(&val_len, sizeof(ukv_val_len_t), 1, handle);
        if (read_len != sizeof(ukv_val_len_t)) {
            *c_error = "Read partially failed on value len.";
            break;
        }

        auto val = value_t(val_len);
        read_len = fread(val.data(), sizeof(byte_t), val.size(), handle);
        if (read_len != val.size()) {
            *c_error = "Read partially failed on value.";
            break;
        }

        col.pairs.emplace(key, sequenced_value_t {std::move(val), sequence_t {0}});
    }

cleanup:
    if (fclose(handle) == EOF)
        *c_error = "Couldn't close the file after reading.";
}

void save_to_disk(db_t const& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    if (!fs::is_directory(dir_path)) {
        *c_error = "Supplied path is not a directory!";
        return;
    }

    save_to_disk(db.unnamed, dir_path / ".stl.ukv", c_error);
    if (*c_error)
        return;

    for (auto const& name_and_col : db.named) {
        auto name_with_ext = std::string(name_and_col.first) + ".stl.ukv";
        save_to_disk(*name_and_col.second, dir_path / name_with_ext, c_error);
        if (*c_error)
            return;
    }
}

void read_from_disk(db_t& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    if (!fs::is_directory(dir_path)) {
        *c_error = "Supplied path is not a directory!";
        return;
    }

    // Parse the main unnamed col
    if (fs::path path = dir_path / ".stl.ukv"; fs::is_regular_file(path)) {
        auto path_str = path.native();
        read_from_disk(db.unnamed, path_str, c_error);
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
        auto col = std::make_unique<col_t>();
        col->name = filename;
        read_from_disk(*col, path_str, c_error);
        db.named.emplace(std::string_view(col->name), std::move(col));
    }
}

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

void write_head( //
    db_t& db,
    write_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    std::unique_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != n; ++i) {

        write_task_t task = tasks[i];
        auto key_iterator = task.collection.pairs.find(task.key);

        // We want to insert a new entry, but let's check if we
        // can overwrite the existig value without causing reallocations.
        try {
            if (key_iterator != task.collection.pairs.end()) {
                key_iterator->second.sequence_number = ++db.youngest_sequence;
                key_iterator->second.data.assign(task.begin, task.begin + task.length);
            }
            else {
                sequenced_value_t sequenced_value = {task.value(), ++db.youngest_sequence};
                task.collection.pairs.insert_or_assign(task.key, std::move(sequenced_value));
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

void measure_head( //
    db_t& db,
    read_tasks_t tasks,
    ukv_size_t const n,
    [[maybe_unused]] ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    // 1. Allocate a tape for all the values to be pulled
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (!tape)
        return;

    std::shared_lock _ {db.mutex};

    // 2. Pull the data
    auto lens = reinterpret_cast<ukv_val_len_t*>(tape);
    for (ukv_size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];
        auto key_iterator = task.collection.pairs.find(task.key);
        lens[i] = key_iterator != task.collection.pairs.end() ? key_iterator->second.data.size() : 0;
    }
}

void read_head( //
    db_t& db,
    read_tasks_t tasks,
    ukv_size_t const n,
    [[maybe_unused]] ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    std::shared_lock _ {db.mutex};

    // 1. Estimate the total size
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    for (ukv_size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];
        auto key_iterator = task.collection.pairs.find(task.key);
        if (key_iterator != task.collection.pairs.end())
            total_bytes += key_iterator->second.data.size();
    }

    // 2. Allocate a tape for all the values to be fetched
    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (!tape)
        return;

    // 3. Fetch the data
    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * n;
    for (ukv_size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];
        auto key_iterator = task.collection.pairs.find(task.key);
        if (key_iterator != task.collection.pairs.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(tape + exported_bytes, key_iterator->second.data.data(), len);
            lens[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        else {
            lens[i] = 0;
        }
    }
}

void write_txn( //
    txn_t& txn,
    write_tasks_t tasks,
    ukv_size_t const n,
    [[maybe_unused]] ukv_options_t const c_options,
    ukv_error_t* c_error) {

    // No need for locking here, until we commit, unless, of course,
    // a col is being deleted.
    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != n; ++i) {
        write_task_t task = tasks[i];

        try {
            txn.new_values.insert_or_assign(task.location(), task.value());
        }
        catch (...) {
            *c_error = "Failed to put into transaction!";
            break;
        }
    }
}

void measure_txn( //
    txn_t& txn,
    read_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    // 1. Allocate a tape for all the values to be pulled
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (!tape)
        return;

    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();
    bool should_track_requests = !(c_options & ukv_option_read_transparent_k);

    // 2. Pull the data
    auto lens = reinterpret_cast<ukv_val_len_t*>(tape);
    for (ukv_size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];

        // Some keys may already be overwritten inside of transaction
        if (auto inner_iterator = txn.new_values.find(task.location()); inner_iterator != txn.new_values.end()) {
            lens[i] = inner_iterator->second.size();
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = task.collection.pairs.find(task.key);
                 key_iterator != task.collection.pairs.end()) {
            if (entry_was_overwritten(key_iterator->second.sequence_number,
                                      txn.sequence_number,
                                      youngest_sequence_number)) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
            lens[i] = key_iterator->second.data.size();

            if (should_track_requests)
                txn.requested_keys.emplace(task.location(), key_iterator->second.sequence_number);
        }
        // But some will be missing
        else {
            lens[i] = 0;

            if (should_track_requests)
                txn.requested_keys.emplace(task.location(), sequence_t {});
        }
    }
}

void read_txn( //
    txn_t& txn,
    read_tasks_t tasks,
    ukv_size_t const n,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();
    bool should_track_requests = !(c_options & ukv_option_read_transparent_k);

    // 1. Estimate the total size of keys
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * n;
    for (ukv_size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];

        // Some keys may already be overwritten inside of transaction
        if (auto inner_iterator = txn.new_values.find(task.location()); inner_iterator != txn.new_values.end()) {
            total_bytes += inner_iterator->second.size();
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = task.collection.pairs.find(task.key);
                 key_iterator != task.collection.pairs.end()) {
            if (entry_was_overwritten(key_iterator->second.sequence_number,
                                      txn.sequence_number,
                                      youngest_sequence_number)) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
            total_bytes += key_iterator->second.data.size();
        }
    }

    // 2. Allocate a tape for all the values to be pulled
    byte_t* tape = reserve_tape(c_tape, c_capacity, total_bytes, c_error);
    if (!tape)
        return;

    // 3. Pull the data
    ukv_val_len_t* lens = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * n;
    for (ukv_size_t i = 0; i != n; ++i) {
        read_task_t task = tasks[i];

        // Some keys may already be overwritten inside of transaction
        if (auto inner_iterator = txn.new_values.find(task.location()); inner_iterator != txn.new_values.end()) {
            auto len = inner_iterator->second.size();
            std::memcpy(tape + exported_bytes, inner_iterator->second.data(), len);
            lens[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = task.collection.pairs.find(task.key);
                 key_iterator != task.collection.pairs.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(tape + exported_bytes, key_iterator->second.data.data(), len);
            lens[i] = static_cast<ukv_val_len_t>(len);

            if (should_track_requests)
                txn.requested_keys.emplace(task.location(), key_iterator->second.sequence_number);

            exported_bytes += len;
        }
        // But some will be missing
        else {
            lens[i] = 0;

            if (should_track_requests)
                txn.requested_keys.emplace(task.location(), sequence_t {});
        }
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

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_t tasks {db, cols, keys};

    if (c_txn) {
        auto func = (c_options & ukv_option_read_lengths_k) ? &measure_txn : &read_txn;
        return func(txn, tasks, c_keys_count, c_options, c_tape, c_capacity, c_error);
    }
    else {
        auto func = (c_options & ukv_option_read_lengths_k) ? &measure_head : &read_head;
        return func(db, tasks, c_keys_count, c_options, c_tape, c_capacity, c_error);
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

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    strided_ptr_gt<ukv_collection_t> cols {const_cast<ukv_collection_t*>(c_cols), c_cols_stride};
    strided_ptr_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_ptr_gt<ukv_tape_ptr_t const> vals {c_vals, c_vals_stride};
    strided_ptr_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_t tasks {db, cols, keys, vals, lens};

    return c_txn ? write_txn(txn, tasks, c_keys_count, c_options, c_error)
                 : write_head(db, tasks, c_keys_count, c_options, c_error);
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_open( //
    char const* c_config,
    ukv_t* c_db,
    ukv_error_t* c_error) {

    try {
        auto db_ptr = new db_t {};
        auto len = std::strlen(c_config);
        if (len) {
            db_ptr->persisted_path = std::string(c_config, len);
            read_from_disk(*db_ptr, c_error);
        }
        *c_db = db_ptr;
    }
    catch (...) {
        *c_error = "Failed to initizalize the database";
    }
}

/*********************************************************/
/*****************	cols Management	  ****************/
/*********************************************************/

void ukv_collection_upsert(
    // Inputs:
    ukv_t const c_db,
    char const* c_col_name,
    // Outputs:
    ukv_collection_t* c_col,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_col_name);
    auto const col_name = std::string_view(c_col_name, name_len);

    auto col_it = db.named.find(col_name);
    if (col_it == db.named.end()) {
        try {
            auto new_col = std::make_unique<col_t>();
            new_col->name = col_name;
            *c_col = new_col.get();
            db.named.insert_or_assign(new_col->name, std::move(new_col));
        }
        catch (...) {
            *c_error = "Failed to create a new col!";
        }
    }
    else {
        *c_col = col_it->second.get();
    }
}

void ukv_collection_remove(
    // Inputs:
    ukv_t const c_db,
    char const* c_col_name,
    // Outputs:
    [[maybe_unused]] ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_col_name);
    auto col_name = std::string_view(c_col_name, name_len);

    auto col_it = db.named.find(col_name);
    if (col_it != db.named.end()) {
        db.named.erase(col_it);
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

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_txn_begin(
    // Inputs:
    ukv_t const c_db,
    ukv_size_t const c_sequence_number,
    // Outputs:
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    if (!*c_txn) {
        try {
            *c_txn = new txn_t();
        }
        catch (...) {
            *c_error = "Failed to initizalize the transaction";
        }
    }

    txn_t& txn = *reinterpret_cast<txn_t*>(*c_txn);
    txn.db_ptr = &db;
    txn.sequence_number = c_sequence_number ? c_sequence_number : ++db.youngest_sequence;
    txn.requested_keys.clear();
    txn.new_values.clear();
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    // This write may fail with out-of-memory errors, if Hash-Tables
    // bucket allocation fails, but no values will be copied, only moved.
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::unique_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();

    // 1. Check for refreshes among fetched keys
    for (auto const& [located_key, located_sequence] : txn.requested_keys) {
        col_t& col = *located_key.col_ptr;
        auto key_iterator = col.pairs.find(located_key.key);
        if (key_iterator != col.pairs.end()) {
            if (key_iterator->second.sequence_number != located_sequence) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
        }
    }

    // 2. Check for collisions among incoming values
    for (auto const& [located_key, value] : txn.new_values) {
        col_t& col = *located_key.col_ptr;
        auto key_iterator = col.pairs.find(located_key.key);
        if (key_iterator != col.pairs.end()) {
            if (key_iterator->second.sequence_number == txn.sequence_number) {
                *c_error = "Can't commit same entry more than once!";
                return;
            }
            if (entry_was_overwritten(key_iterator->second.sequence_number,
                                      txn.sequence_number,
                                      youngest_sequence_number)) {
                *c_error = "Incoming key collides with newer entry!";
                return;
            }
        }
    }

    // 3. Allocate space for more nodes across different cols
    try {
        db.unnamed.reserve_more(txn.new_values.size());
        for (auto& name_and_col : db.named)
            name_and_col.second->reserve_more(txn.new_values.size());
    }
    catch (...) {
        *c_error = "Not enough memory!";
        return;
    }

    // 4. Import the data, as no collisions were detected
    for (auto& located_key_and_value : txn.new_values) {
        col_t& col = *located_key_and_value.first.col_ptr;
        auto key_iterator = col.pairs.find(located_key_and_value.first.key);
        // A key was deleted:
        // if (located_key_and_value.second.empty()) {
        //     if (key_iterator != col.pairs.end())
        //         col.pairs.erase(key_iterator);
        // }
        // A keys was updated:
        // else
        if (key_iterator != col.pairs.end()) {
            key_iterator->second.sequence_number = txn.sequence_number;
            std::swap(key_iterator->second.data, located_key_and_value.second);
        }
        // A key was inserted:
        else {
            sequenced_value_t sequenced_value {std::move(located_key_and_value.second), txn.sequence_number};
            col.pairs.insert_or_assign(located_key_and_value.first.key, std::move(sequenced_value));
        }
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        save_to_disk(db, c_error);
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_tape_free(ukv_t const, ukv_tape_ptr_t c_ptr, ukv_size_t c_len) {
    if (!c_ptr || !c_len)
        return;
    allocator_t {}.deallocate(reinterpret_cast<byte_t*>(c_ptr), c_len);
}

void ukv_txn_free(ukv_t const, ukv_txn_t const c_txn) {
    if (!c_txn)
        return;
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    delete &txn;
}

void ukv_free(ukv_t c_db) {
    if (!c_db)
        return;
    db_t& db = *reinterpret_cast<db_t*>(c_db);
    delete &db;
}

void ukv_collection_free(ukv_t const, ukv_collection_t const) {
    // In this in-memory freeing the col handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
