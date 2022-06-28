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
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
namespace fs = std::filesystem;

namespace {

struct txn_t;
struct db_t;

struct sequenced_value_t {
    value_t data;
    sequence_t sequence_number {0};
};

struct collection_t {
    std::string name;
    std::unordered_map<key_t, sequenced_value_t> pairs;

    void reserve_more(std::size_t n) { pairs.reserve(pairs.size() + n); }
};

using collection_ptr_t = std::unique_ptr<collection_t>;

struct located_key_t {
    collection_t* collection_ptr = nullptr;
    key_t key {0};

    inline bool operator==(located_key_t const& other) const noexcept {
        return (collection_ptr == other.collection_ptr) & (key == other.key);
    }
    inline bool operator!=(located_key_t const& other) const noexcept {
        return (collection_ptr != other.collection_ptr) | (key != other.key);
    }
};

struct located_key_hash_t {
    inline std::size_t operator()(located_key_t const& located) const noexcept {
        return std::hash<key_t> {}(located.key);
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
    collection_t unnamed;

    /**
     * @brief A variable-size set of named collections.
     * It's cleaner to implement it with heterogenous lookups as
     * an @c `std::unordered_set`, but it requires GCC11.
     */
    std::unordered_map<std::string_view, collection_ptr_t> named;
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

} // namespace

void save_to_disk(collection_t const& col, std::string const& path, ukv_error_t* c_error) {
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
        auto saved_length = fwrite(&n, sizeof(ukv_size_t), 1, handle);
        if (saved_length != sizeof(ukv_size_t)) {
            *c_error = "Couldn't write anything to file.";
            break;
        }
    }

    // Save the entries
    for (auto const& [key, val] : col.pairs) {
        auto saved_length = fwrite(&key, sizeof(key_t), 1, handle);
        if (saved_length != sizeof(key_t)) {
            *c_error = "Write partially failed on key.";
            break;
        }

        auto val_len = static_cast<ukv_val_len_t>(val.size());
        auto saved_length = fwrite(&val_len, sizeof(ukv_val_len_t), 1, handle);
        if (saved_length != sizeof(ukv_val_len_t)) {
            *c_error = "Write partially failed on value length.";
            break;
        }

        auto saved_length = fwrite(val.data(), sizeof(byte_t), val.size(), handle);
        if (saved_length != val.size()) {
            *c_error = "Write partially failed on value.";
            break;
        }
    }

    if (fclose(handle) == EOF)
        *c_error = "Couldn't close the file after write.";
}

void read_from_disk(collection_t& col, std::string const& path, ukv_error_t* c_error) {
    // Similar to serialization, we don't use STL here
    FILE* handle = fopen(path.c_str(), "rb+");

    // Get the collection size, to preallocate entries
    auto n = ukv_size_t(0);
    {
        auto read_length = fread(&n, sizeof(ukv_size_t), 1, handle);
        if (read_length != sizeof(ukv_size_t)) {
            *c_error = "Couldn't read anything from file.";
            break;
        }
    }

    // Save the entries
    col.pairs.reserve(n);
    for (ukv_size_t i = 0; i != n; ++i) {

        auto key = key_t {};
        auto read_length = fread(&key, sizeof(key_t), 1, handle);
        if (read_length != sizeof(key_t)) {
            *c_error = "Read partially failed on key.";
            break;
        }

        auto val_len = ukv_val_len_t(0);
        auto read_length = fread(&val_len, sizeof(ukv_val_len_t), 1, handle);
        if (read_length != sizeof(ukv_val_len_t)) {
            *c_error = "Read partially failed on value length.";
            break;
        }

        auto val = value_t(val_len);
        auto read_length = fread(val.data(), sizeof(byte_t), val.size(), handle);
        if (read_length != val.size()) {
            *c_error = "Read partially failed on value.";
            break;
        }

        col.pairs.emplace(key, std::move(value));
    }

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

    for (auto const& [name, col] : db.named) {
        save_to_disk(col, dir_path / (name + ".stl.ukv"), c_error);
        if (*c_error)
            return;
    }
}

void read_from_disk(db_t const& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    if (!fs::is_directory(dir_path)) {
        *c_error = "Supplied path is not a directory!";
        return;
    }

    // Parse the main unnamed collection
    if (fs::path path = dir_path / ".stl.ukv"; fs::is_regular_file(path)) {
        auto path_str = path.native();
        read_from_disk(*col, path_str, c_error);
        db.named.emplace(std::string_view(col->name), std::move(col));
    }

    // Parse all the named collections we can find
    for (auto const& dir_entry : fs::directory_iterator {dir_path}) {
        if (!dir_entry.is_regular_file())
            continue;
        fs::path const& path = dir_entry.path();
        auto path_str = path.native();
        if (!path_str.ends_with(".stl.ukv"))
            continue;

        auto filename_w_ext = path.filename().native();
        auto filename = filename_w_ext.substr(0, filename_w_ext.size() - 8);
        auto col = std::make_unique<collection_t>();
        col->name = filename;
        read_from_disk(*col, path_str, c_error);
        db.named.emplace(std::string_view(col->name), std::move(col));
    }
}

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

void _ukv_write_head( //
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t const c_values,
    ukv_val_len_t const* c_lengths,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};

    ukv_size_t exported_bytes = 0;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {

        collection_t& collection = collection_at(db, c_collections, i, c_options);
        auto key = c_keys[i];
        auto length = c_lengths[i];
        auto begin = reinterpret_cast<byte_t const*>(c_values) + exported_bytes;
        auto key_iterator = collection.pairs.find(key);

        // We want to insert a new entry, but let's check if we
        // can overwrite the existig value without causing reallocations.
        try {
            if (key_iterator != collection.pairs.end()) {
                key_iterator->second.sequence_number = ++db.youngest_sequence;
                key_iterator->second.data.assign(begin, begin + length);
            }
            else {
                sequenced_value_t sequenced_value {
                    value_t(begin, begin + length),
                    ++db.youngest_sequence,
                };
                collection.pairs.insert_or_assign(key, std::move(sequenced_value));
            }
        }
        catch (...) {
            *c_error = "Failed to put!";
            break;
        }
        exported_bytes += length;
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (std::uintptr_t(c_options) & write_flush_k)
        save_to_disk(db, c_error);
}

void _ukv_measure_head( //
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_tape_length,
    ukv_error_t* c_error) {

    // 1. Allocate a tape for all the values to be pulled
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    byte_t* tape = reserve_tape(c_tape, c_tape_length, total_bytes, c_error);
    if (!tape)
        return;

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::shared_lock _ {db.mutex};

    // 2. Pull the data
    auto lengths = reinterpret_cast<ukv_val_len_t*>(tape);
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);
        auto key_iterator = collection.pairs.find(c_keys[i]);
        lengths[i] = key_iterator != collection.pairs.end() ? key_iterator->second.data.size() : 0;
    }
}

void _ukv_read_head( //
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_tape_length,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::shared_lock _ {db.mutex};

    // 1. Estimate the total size
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);
        auto key_iterator = collection.pairs.find(c_keys[i]);
        if (key_iterator != collection.pairs.end())
            total_bytes += key_iterator->second.data.size();
    }

    // 2. Allocate a tape for all the values to be fetched
    byte_t* tape = reserve_tape(c_tape, c_tape_length, total_bytes, c_error);
    if (!tape)
        return;

    // 3. Fetch the data
    ukv_val_len_t* lengths = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);
        auto key_iterator = collection.pairs.find(c_keys[i]);
        if (key_iterator != collection.pairs.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(tape + exported_bytes, key_iterator->second.data.data(), len);
            lengths[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        else {
            lengths[i] = 0;
        }
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

    // No need for locking here, until we commit, unless, of course,
    // a collection is being deleted.
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    ukv_size_t exported_bytes = 0;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);
        auto key = c_keys[i];
        auto length = lengths[i];
        auto begin = reinterpret_cast<byte_t const*>(c_tape) + exported_bytes;

        try {
            located_key_t located_key {&collection, key};
            value_t value {begin, begin + length};
            txn.new_values.insert_or_assign(std::move(located_key), std::move(value));
        }
        catch (...) {
            *c_error = "Failed to put into transaction!";
            break;
        }
        exported_bytes += length;
    }
}

void _ukv_measure_txn( //
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_collection_t const* c_collections,
    ukv_options_t const c_options,
    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_tape_length,
    ukv_error_t* c_error) {

    // 1. Allocate a tape for all the values to be pulled
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    byte_t* tape = reserve_tape(c_tape, c_tape_length, total_bytes, c_error);
    if (!tape)
        return;

    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();

    // 2. Pull the data
    auto lengths = reinterpret_cast<ukv_val_len_t*>(tape);
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);

        // Some keys may already be overwritten inside of transaction
        if (auto overwrite_iterator = txn.new_values.find(located_key_t {&collection, c_keys[i]});
            overwrite_iterator != txn.new_values.end()) {
            lengths[i] = overwrite_iterator->second.size();
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = collection.pairs.find(c_keys[i]); key_iterator != collection.pairs.end()) {
            if (entry_was_overwritten(key_iterator->second.sequence_number,
                                      txn.sequence_number,
                                      youngest_sequence_number)) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
            lengths[i] = key_iterator->second.data.size();
        }
        // But some will be missing
        else {
            lengths[i] = 0;
        }
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

    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();

    // 1. Estimate the total size of keys
    ukv_size_t total_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);

        // Some keys may already be overwritten inside of transaction
        if (auto overwrite_iterator = txn.new_values.find(located_key_t {&collection, c_keys[i]});
            overwrite_iterator != txn.new_values.end()) {
            total_bytes += overwrite_iterator->second.size();
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = collection.pairs.find(c_keys[i]); key_iterator != collection.pairs.end()) {
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
    byte_t* tape = reserve_tape(c_tape, c_tape_length, total_bytes, c_error);
    if (!tape)
        return;

    // 3. Pull the data
    ukv_val_len_t* lengths = reinterpret_cast<ukv_val_len_t*>(tape);
    ukv_size_t exported_bytes = sizeof(ukv_val_len_t) * c_keys_count;
    for (ukv_size_t i = 0; i != c_keys_count; ++i) {
        collection_t& collection = collection_at(db, c_collections, i, c_options);

        // Some keys may already be overwritten inside of transaction
        if (auto overwrite_iterator = txn.new_values.find(located_key_t {&collection, c_keys[i]});
            overwrite_iterator != txn.new_values.end()) {
            auto len = overwrite_iterator->second.size();
            std::memcpy(tape + exported_bytes, overwrite_iterator->second.data(), len);
            lengths[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = collection.pairs.find(c_keys[i]); key_iterator != collection.pairs.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(tape + exported_bytes, key_iterator->second.data.data(), len);
            lengths[i] = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        // But some will be missing
        else {
            lengths[i] = 0;
        }
    }
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

    if (std::uintptr_t(c_options) & read_lengths_k)
        return c_txn ? _ukv_measure_txn(c_txn,
                                        c_keys,
                                        c_keys_count,
                                        c_collections,
                                        c_options,
                                        c_tape,
                                        c_tape_length,
                                        c_error)
                     : _ukv_measure_head(c_db,
                                         c_keys,
                                         c_keys_count,
                                         c_collections,
                                         c_options,
                                         c_tape,
                                         c_tape_length,
                                         c_error);

    return c_txn ? _ukv_read_txn(c_txn, c_keys, c_keys_count, c_collections, c_options, c_tape, c_tape_length, c_error)
                 : _ukv_read_head(c_db, c_keys, c_keys_count, c_collections, c_options, c_tape, c_tape_length, c_error);
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
/*****************	collections Management	  ****************/
/*********************************************************/

void ukv_collection_upsert(
    // Inputs:
    ukv_t const c_db,
    char const* c_collection_name,
    // Outputs:
    ukv_collection_t* c_collection,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_collection_name);
    auto const collection_name = std::string_view(c_collection_name, name_len);

    auto collection_it = db.named.find(collection_name);
    if (collection_it == db.named.end()) {
        try {
            auto new_collection = std::make_unique<collection_t>();
            new_collection->name = collection_name;
            *c_collection = new_collection.get();
            db.named.insert_or_assign(new_collection->name, std::move(new_collection));
        }
        catch (...) {
            *c_error = "Failed to create a new collection!";
        }
    }
    else {
        *c_collection = collection_it->second.get();
    }
}

void ukv_collection_remove(
    // Inputs:
    ukv_t const c_db,
    char const* c_collection_name,
    // Outputs:
    [[maybe_unused]] ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_collection_name);
    auto collection_name = std::string_view(c_collection_name, name_len);

    auto collection_it = db.named.find(collection_name);
    if (collection_it != db.named.end()) {
        db.named.erase(collection_it);
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
        collection_t& collection = *located_key.collection_ptr;
        auto key_iterator = collection.pairs.find(located_key.key);
        if (key_iterator != collection.pairs.end()) {
            if (key_iterator->second.sequence_number != located_sequence) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
        }
    }

    // 2. Check for collisions among incoming values
    for (auto const& [located_key, value] : txn.new_values) {
        collection_t& collection = *located_key.collection_ptr;
        auto key_iterator = collection.pairs.find(located_key.key);
        if (key_iterator != collection.pairs.end()) {
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

    // 3. Allocate space for more nodes across different collections
    try {
        db.unnamed.reserve_more(txn.new_values.size());
        for (auto& name_and_collection : db.named)
            name_and_collection.second->reserve_more(txn.new_values.size());
    }
    catch (...) {
        *c_error = "Not enough memory!";
        return;
    }

    // 4. Import the data, as no collisions were detected
    for (auto& located_key_and_value : txn.new_values) {
        collection_t& collection = *located_key_and_value.first.collection_ptr;
        auto key_iterator = collection.pairs.find(located_key_and_value.first.key);
        // A key was deleted:
        // if (located_key_and_value.second.empty()) {
        //     if (key_iterator != collection.pairs.end())
        //         collection.pairs.erase(key_iterator);
        // }
        // A keys was updated:
        // else
        if (key_iterator != collection.pairs.end()) {
            key_iterator->second.sequence_number = txn.sequence_number;
            std::swap(key_iterator->second.data, located_key_and_value.second);
        }
        // A key was inserted:
        else {
            sequenced_value_t sequenced_value {
                std::move(located_key_and_value.second),
                txn.sequence_number,
            };
            collection.pairs.insert_or_assign(located_key_and_value.first.key, std::move(sequenced_value));
        }
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (std::uintptr_t(c_options) & write_flush_k)
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
    // In this in-memory freeing the collection handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
