/**
 * @file engine_stl.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded In-Memory Key-Value Store implementation using only @b STL.
 * This is not the fastest, not the smartest possible solution for @b ACID KVS,
 * but is a good reference design for educational purposes.
 * Deficiencies:
 * > Global Lock.
 * > No support for range queries.
 * > Keeps watch of all the deleted keys throughout the history.
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

ukv_collection_t const ukv_collection_main_k = 0;
ukv_length_t const ukv_length_missing_k = std::numeric_limits<ukv_length_t>::max();
ukv_key_t const ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();
bool const ukv_supports_transactions_k = true;
bool const ukv_supports_named_collections_k = true;
bool const ukv_supports_snapshots_k = false;

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;
namespace fs = std::filesystem;

struct stl_db_t;
struct stl_collection_t;
struct stl_txn_t;

struct stl_value_t {
    buffer_t buffer;
    generation_t generation {0};
    bool is_deleted {false};

    void reset(generation_t gen) {
        is_deleted = true;
        generation = gen;
        buffer.clear();
    }
};

struct stl_collection_t {
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

using stl_collection_ptr_t = std::unique_ptr<stl_collection_t>;

struct stl_txn_t {
    std::map<collection_key_t, buffer_t> upserted;
    std::unordered_map<collection_key_t, generation_t, sub_key_hash_t> requested;
    std::unordered_set<collection_key_t, sub_key_hash_t> removed;

    stl_db_t* db_ptr {nullptr};
    generation_t generation {0};
};

struct stl_db_t {
    std::shared_mutex mutex;
    stl_collection_t main;

    /**
     * @brief A variable-size set of named collections.
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

stl_collection_t& stl_collection(stl_db_t& db, ukv_collection_t collection) {
    return collection == ukv_collection_main_k ? db.main : *reinterpret_cast<stl_collection_t*>(collection);
}

void save_to_disk(stl_collection_t const& collection, std::string const& path, ukv_error_t* c_error) {
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
        auto n = static_cast<ukv_size_t>(collection.unique_elements.load());
        auto saved_len = std::fwrite(&n, sizeof(ukv_size_t), 1, handle);
        return_if_error(saved_len == 1, c_error, 0, "Couldn't write anything to file.");
    }

    // Save the entries
    for (auto const& [key, seq_val] : collection.pairs) {
        if (seq_val.is_deleted)
            continue;

        auto saved_len = std::fwrite(&key, sizeof(ukv_key_t), 1, handle);
        return_if_error(saved_len == 1, c_error, 0, "Write partially failed on key.");

        auto const& buf = seq_val.buffer;
        auto buf_len = static_cast<ukv_length_t>(buf.size());
        saved_len = std::fwrite(&buf_len, sizeof(ukv_length_t), 1, handle);
        return_if_error(saved_len == 1, c_error, 0, "Write partially failed on value len.");

        saved_len = std::fwrite(buf.data(), sizeof(byte_t), buf.size(), handle);
        return_if_error(saved_len == buf.size(), c_error, 0, "Write partially failed on value.");
    }

    log_error(c_error, 0, handle.close().release_error());
}

void read_from_disk(stl_collection_t& collection, std::string const& path, ukv_error_t* c_error) {
    // Similar to serialization, we don't use STL here
    file_handle_t handle;
    if ((*c_error = handle.open(path.c_str(), "rb+").release_error()))
        return;

    // Get the collection size, to preallocate entries
    auto n = ukv_size_t(0);
    {
        auto read_len = std::fread(&n, sizeof(ukv_size_t), 1, handle);
        return_if_error(read_len == 1, c_error, 0, "Couldn't read anything from file.");
    }

    // Load the entries
    collection.pairs.clear();
    collection.reserve_more(n);
    collection.unique_elements = n;

    for (ukv_size_t i = 0; i != n; ++i) {

        auto key = ukv_key_t {};
        auto read_len = std::fread(&key, sizeof(ukv_key_t), 1, handle);
        return_if_error(read_len == 1, c_error, 0, "Read partially failed on key.");

        auto buf_len = ukv_length_t(0);
        read_len = std::fread(&buf_len, sizeof(ukv_length_t), 1, handle);
        return_if_error(read_len == 1, c_error, 0, "Read partially failed on value len.");

        auto buf = buffer_t(buf_len);
        read_len = std::fread(buf.data(), sizeof(byte_t), buf.size(), handle);
        return_if_error(read_len == buf.size(), c_error, 0, "Read partially failed on value.");

        collection.pairs.emplace(key, stl_value_t {std::move(buf), generation_t {0}, false});
    }

    log_error(c_error, 0, handle.close().release_error());
}

void save_to_disk(stl_db_t const& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    return_if_error(fs::is_directory(dir_path), c_error, args_wrong_k, "Supplied path is not a directory!");

    save_to_disk(db.main, dir_path / ".stl.ukv", c_error);
    return_on_error(c_error);

    for (auto const& name_and_collection : db.named) {
        auto name_with_ext = std::string(name_and_collection.first) + ".stl.ukv";
        save_to_disk(*name_and_collection.second, dir_path / name_with_ext, c_error);
        return_on_error(c_error);
    }
}

void read_from_disk(stl_db_t& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    return_if_error(fs::is_directory(dir_path), c_error, args_wrong_k, "Supplied path is not a directory!");

    // Parse the main main col
    if (fs::path path = dir_path / ".stl.ukv"; fs::is_regular_file(path)) {
        auto path_str = path.native();
        read_from_disk(db.main, path_str, c_error);
    }

    // Parse all the named collections we can find
    for (auto const& dir_entry : fs::directory_iterator {dir_path}) {
        if (!dir_entry.is_regular_file())
            continue;
        fs::path const& path = dir_entry.path();
        auto path_str = path.native();
        if (path_str.size() <= 8 || path_str.substr(path_str.size() - 8) != ".stl.ukv")
            continue;

        auto filename_w_ext = path.filename().native();
        auto filename = filename_w_ext.substr(0, filename_w_ext.size() - 8);
        auto collection = std::make_unique<stl_collection_t>();
        collection->name = filename;
        read_from_disk(*collection, path_str, c_error);
        db.named.emplace(std::string_view(collection->name), std::move(collection));
    }
}

void write_head( //
    stl_db_t& db,
    places_arg_t places,
    contents_arg_t contents,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    std::unique_lock _ {db.mutex};

    for (std::size_t i = 0; i != places.size(); ++i) {

        auto place = places[i];
        auto content = contents[i];
        stl_collection_t& collection = stl_collection(db, place.collection);
        auto key_iterator = collection.pairs.find(place.key);

        // We want to insert a new entry, but let's check if we
        // can overwrite the existing value without causing reallocations.
        safe_section("Copying new value", c_error, [&] {
            if (key_iterator != collection.pairs.end()) {
                key_iterator->second.generation = ++db.youngest_generation;
                key_iterator->second.buffer.assign(content.begin(), content.end());
                key_iterator->second.is_deleted = !content;
            }
            else if (content) {
                buffer_t buffer {content.begin(), content.end()};
                stl_value_t value_w_generation {std::move(buffer), ++db.youngest_generation};
                collection.pairs.emplace(place.key, std::move(value_w_generation));
                ++collection.unique_elements;
            }
        });
        return_on_error(c_error);
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        save_to_disk(db, c_error);
}

void write_txn( //
    stl_txn_t& txn,
    places_arg_t places,
    contents_arg_t contents,
    ukv_options_t const,
    ukv_error_t* c_error) {

    // No need for locking here, until we commit, unless, of course,
    // a collection is being deleted.
    stl_db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    for (std::size_t i = 0; i != places.size(); ++i) {

        auto place = places[i];
        auto content = contents[i];
        safe_section("Copying new value", c_error, [&] {
            if (!content) {
                txn.upserted.erase(place.collection_key());
                txn.removed.insert(place.collection_key());
            }
            else {
                txn.upserted.insert_or_assign(place.collection_key(), buffer_t {content.begin(), content.end()});
            }
        });
        return_on_error(c_error);
    }
}

template <typename value_enumerator_at>
void read_head_under_lock( //
    stl_db_t& db,
    places_arg_t tasks,
    ukv_options_t const,
    value_enumerator_at enumerator,
    ukv_error_t*) {

    for (std::size_t i = 0; i != tasks.size(); ++i) {
        place_t place = tasks[i];
        stl_collection_t const& collection = stl_collection(db, place.collection);
        auto key_iterator = collection.pairs.find(place.key);
        bool found = key_iterator != collection.pairs.end() && !key_iterator->second.is_deleted;
        auto value = found ? value_view(key_iterator->second.buffer) : value_view_t {};
        enumerator(i, value);
    }
}

template <typename value_enumerator_at>
void read_txn_under_lock( //
    stl_txn_t& txn,
    places_arg_t tasks,
    ukv_options_t const c_options,
    value_enumerator_at enumerator,
    ukv_error_t* c_error) {

    stl_db_t& db = *txn.db_ptr;
    generation_t const youngest_generation = db.youngest_generation.load();
    bool const watch = c_options & ukv_option_watch_k;

    for (std::size_t i = 0; i != tasks.size(); ++i) {
        place_t place = tasks[i];
        stl_collection_t const& collection = stl_collection(db, place.collection);

        // Some keys may already be overwritten inside of transaction
        if (auto inner_iterator = txn.upserted.find(place.collection_key()); inner_iterator != txn.upserted.end())
            enumerator(i, value_view(inner_iterator->second));

        // Some may have been deleted inside the transaction
        else if (auto inner_iterator = txn.removed.find(place.collection_key()); inner_iterator != txn.removed.end())
            enumerator(i, value_view_t {});

        // Others should be pulled from the main store
        else if (auto key_iterator = collection.pairs.find(place.key); key_iterator != collection.pairs.end()) {

            if (entry_was_overwritten(key_iterator->second.generation, txn.generation, youngest_generation) &&
                (*c_error = "Requested key was already overwritten since the start of the transaction!"))
                return;

            bool found = !key_iterator->second.is_deleted;
            auto value = found ? value_view(key_iterator->second.buffer) : value_view_t {};
            enumerator(i, value);

            if (watch)
                txn.requested.emplace(place.collection_key(), key_iterator->second.generation);
        }

        // But some will be missing
        else {
            enumerator(i, value_view_t {});

            if (watch)
                txn.requested.emplace(place.collection_key(), generation_t {});
        }
    }
}

void scan_head( //
    stl_db_t& db,
    scans_arg_t tasks,
    ukv_options_t const options,
    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_counts,
    ukv_key_t** c_found_keys,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    std::shared_lock _ {db.mutex};

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy<ukv_length_t>(tasks.count + 1, c_error, c_found_offsets);
    return_on_error(c_error);
    auto counts = arena.alloc_or_dummy<ukv_length_t>(tasks.count, c_error, c_found_counts);
    return_on_error(c_error);

    auto total_keys = reduce_n(tasks.limits, tasks.count, 0ul);
    auto keys_output = *c_found_keys = arena.alloc<ukv_key_t>(total_keys, c_error).begin();
    return_on_error(c_error);

    // 2. Fetch the data
    for (std::size_t i = 0; i != tasks.size(); ++i) {
        scan_t scan = tasks[i];
        stl_collection_t const& collection = stl_collection(db, scan.collection);
        offsets[i] = keys_output - *c_found_keys;

        ukv_length_t j = 0;
        auto key_iterator = collection.pairs.lower_bound(scan.min_key);
        for (; j != scan.limit && key_iterator != collection.pairs.end() && key_iterator->first < scan.max_key;
             ++key_iterator) {
            if (key_iterator->second.is_deleted)
                continue;
            *keys_output = key_iterator->first;
            ++keys_output;
            ++j;
        }

        counts[i] = j;
    }
    offsets[tasks.size()] = keys_output - *c_found_keys;
}

void scan_txn( //
    stl_txn_t& txn,
    scans_arg_t tasks,
    ukv_options_t const options,
    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_counts,
    ukv_key_t** c_found_keys,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    stl_db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy<ukv_length_t>(tasks.count + 1, c_error, c_found_offsets);
    return_on_error(c_error);
    auto counts = arena.alloc_or_dummy<ukv_length_t>(tasks.count, c_error, c_found_counts);
    return_on_error(c_error);

    auto total_keys = reduce_n(tasks.limits, tasks.count, 0ul);
    auto keys_output = *c_found_keys = arena.alloc<ukv_key_t>(total_keys, c_error).begin();
    return_on_error(c_error);

    // 2. Fetch the data
    for (std::size_t i = 0; i != tasks.size(); ++i) {
        scan_t scan = tasks[i];
        stl_collection_t const& collection = stl_collection(db, scan.collection);
        offsets[i] = keys_output - *c_found_keys;

        ukv_length_t j = 0;
        auto key_iterator = collection.pairs.lower_bound(scan.min_key);
        auto txn_iterator = txn.upserted.lower_bound(scan.min_key);
        for (; j != scan.limit && key_iterator != collection.pairs.end();) {
            // Check if the key was already removed:
            if (key_iterator->second.is_deleted || txn.removed.find(key_iterator->first) != txn.removed.end()) {
                ++key_iterator;
                continue;
            }

            // Compare against the incoming inserted keys:
            bool check_in_txn = txn_iterator != txn.upserted.end() && txn_iterator->first.collection == scan.collection;
            if (check_in_txn && txn_iterator->first.key <= key_iterator->first) {
                *keys_output = txn_iterator->first.key;
                ++keys_output;
                ++txn_iterator;
                ++j;
                continue;
            }

            // Make sure we haven't reached the end keys
            if (key_iterator->first >= scan.max_key)
                break;

            // Export from the main store:
            *keys_output = key_iterator->first;
            ++keys_output;
            ++key_iterator;
            ++j;
        }

        // As in any `set_union`, don't forget the tail :)
        while (j != scan.limit && txn_iterator != txn.upserted.end() &&
               txn_iterator->first.collection == scan.collection && txn_iterator->first.key < scan.max_key) {
            *keys_output = txn_iterator->first.key;
            ++keys_output;
            ++txn_iterator;
            ++j;
        }

        counts[i] = j;
    }
    offsets[tasks.size()] = keys_output - *c_found_keys;
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_database_init( //
    ukv_str_view_t c_config,
    ukv_database_t* c_db,
    ukv_error_t* c_error) {

    safe_section("Initializing DBMS", c_error, [&] {
        auto db_ptr = new stl_db_t {};
        auto len = c_config ? std::strlen(c_config) : 0;
        if (len) {
            db_ptr->persisted_path = std::string(c_config, len);
            read_from_disk(*db_ptr, c_error);
        }
        *c_db = db_ptr;
    });
}

void ukv_read( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_octet_t** c_found_presences,

    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_lengths,
    ukv_bytes_ptr_t* c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {collections, keys, {}, c_tasks_count};
    validate_read(c_txn, places, c_options, c_error);
    return_on_error(c_error);

    bool const needs_export = c_found_values != nullptr;

    // 1. Allocate a tape for all the values to be pulled
    auto offs = arena.alloc_or_dummy<ukv_length_t>(places.count + 1, c_error, c_found_offsets);
    return_on_error(c_error);
    auto lens = arena.alloc_or_dummy<ukv_length_t>(places.count, c_error, c_found_lengths);
    return_on_error(c_error);
    auto presences = arena.alloc_or_dummy<ukv_octet_t>(places.count, c_error, c_found_presences);
    return_on_error(c_error);

    // 2. Pull metadata
    ukv_length_t total_length = 0;
    auto meta_enumerator = [&](std::size_t i, value_view_t value) {
        presences[i] = bool(value);
        offs[i] = total_length;
        lens[i] = value ? static_cast<ukv_length_t>(value.size()) : ukv_length_missing_k;
        total_length += static_cast<ukv_length_t>(value.size());
    };

    std::shared_lock _ {db.mutex};
    c_txn ? read_txn_under_lock(txn, places, c_options, meta_enumerator, c_error)
          : read_head_under_lock(db, places, c_options, meta_enumerator, c_error);
    offs[places.count] = total_length;
    if (!needs_export)
        return;

    // 3. Pull the data, once we know the total length
    auto tape = arena.alloc<byte_t>(total_length, c_error).begin();
    auto data_enumerator = [&](std::size_t i, value_view_t value) {
        std::memcpy(std::exchange(tape, tape + value.size()), value.begin(), value.size());
    };

    *c_found_values = reinterpret_cast<ukv_byte_t*>(tape);
    c_txn ? read_txn_under_lock(txn, places, c_options, data_enumerator, c_error)
          : read_head_under_lock(db, places, c_options, data_enumerator, c_error);
}

void ukv_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_octet_t const* c_presences,

    ukv_length_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_length_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_bytes_cptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_options_t const c_options,

    ukv_arena_t*,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_length_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_lens, c_lens_stride};
    strided_iterator_gt<ukv_octet_t const> presences {c_presences, sizeof(ukv_octet_t)};

    places_arg_t places {collections, keys, {}, c_tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c_tasks_count};

    validate_write(c_txn, places, contents, c_options, c_error);
    return_on_error(c_error);

    return c_txn ? write_txn(txn, places, contents, c_options, c_error)
                 : write_head(db, places, contents, c_options, c_error);
}

void ukv_scan( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_min_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_start_keys,
    ukv_size_t const c_start_keys_stride,

    ukv_key_t const* c_end_keys,
    ukv_size_t const c_end_keys_stride,

    ukv_length_t const* c_scan_limits,
    ukv_size_t const c_scan_limits_stride,

    ukv_options_t const c_options,

    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_counts,
    ukv_key_t** c_found_keys,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_scan_limits, c_scan_limits_stride};
    scans_arg_t scans {collections, start_keys, end_keys, lens, c_min_tasks_count};

    validate_scan(c_txn, scans, c_options, c_error);
    return_on_error(c_error);

    return c_txn ? scan_txn(txn, scans, c_options, c_found_offsets, c_found_counts, c_found_keys, arena, c_error)
                 : scan_head(db, scans, c_options, c_found_offsets, c_found_counts, c_found_keys, arena, c_error);
}

void ukv_size( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const n,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_start_keys,
    ukv_size_t const c_start_keys_stride,

    ukv_key_t const* c_end_keys,
    ukv_size_t const c_end_keys_stride,

    ukv_options_t const,

    ukv_size_t** c_min_cardinalities,
    ukv_size_t** c_max_cardinalities,
    ukv_size_t** c_min_value_bytes,
    ukv_size_t** c_max_value_bytes,
    ukv_size_t** c_min_space_usages,
    ukv_size_t** c_max_space_usages,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    auto min_cardinalities = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_min_cardinalities);
    auto max_cardinalities = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_max_cardinalities);
    auto min_value_bytes = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_min_value_bytes);
    auto max_value_bytes = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_max_value_bytes);
    auto min_space_usages = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_min_space_usages);
    auto max_space_usages = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_max_space_usages);
    return_on_error(c_error);

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};

    std::shared_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != n; ++i) {
        stl_collection_t const& collection = stl_collection(db, collections[i]);
        ukv_key_t const min_key = start_keys[i];
        ukv_key_t const max_key = end_keys[i];
        std::size_t deleted_count = 0;

        // Estimate the presence in the main store
        std::size_t main_count = 0;
        std::size_t main_bytes = 0;
        auto min_iterator = collection.pairs.lower_bound(min_key);
        auto max_iterator = collection.pairs.lower_bound(max_key);
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
        ukv_size_t estimate[6];
        min_cardinalities[i] = estimate[0] = static_cast<ukv_size_t>(main_count);
        max_cardinalities[i] = estimate[1] = static_cast<ukv_size_t>(main_count + txn_count);
        min_value_bytes[i] = estimate[2] = static_cast<ukv_size_t>(main_bytes);
        max_value_bytes[i] = estimate[3] = static_cast<ukv_size_t>(main_bytes + txn_bytes);
        min_space_usages[i] = estimate[4] = estimate[0] * (sizeof(ukv_key_t) + sizeof(ukv_length_t)) + estimate[2];
        max_space_usages[i] = estimate[5] =
            (estimate[1] + deleted_count) * (sizeof(ukv_key_t) + sizeof(ukv_length_t)) + estimate[3];
    }
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ukv_collection_init(
    // Inputs:
    ukv_database_t const c_db,
    ukv_str_view_t c_collection_name,
    ukv_str_view_t,
    // Outputs:
    ukv_collection_t* c_collection,
    ukv_error_t* c_error) {

    auto name_len = c_collection_name ? std::strlen(c_collection_name) : 0;
    if (!name_len) {
        *c_collection = ukv_collection_main_k;
        return;
    }

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    std::unique_lock _ {db.mutex};

    auto const collection_name = std::string_view(c_collection_name, name_len);
    auto collection_it = db.named.find(collection_name);
    if (collection_it == db.named.end()) {
        safe_section("Inserting new collection", c_error, [&] {
            auto new_collection = std::make_unique<stl_collection_t>();
            new_collection->name = collection_name;
            *c_collection = reinterpret_cast<ukv_collection_t>(new_collection.get());
            db.named.emplace(new_collection->name, std::move(new_collection));
        });
    }
    else {
        *c_collection = reinterpret_cast<ukv_collection_t>(collection_it->second.get());
    }
}

void ukv_collection_drop(
    // Inputs:
    ukv_database_t const c_db,
    ukv_collection_t c_collection_id,
    ukv_drop_mode_t c_mode,
    // Outputs:
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    bool invalidate = c_mode == ukv_drop_keys_vals_handle_k;
    return_if_error(c_collection_id == ukv_collection_main_k || !invalidate,
                    c_error,
                    args_combo_k,
                    "Default collection can't be invalidated.");

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    stl_collection_t const& collection = stl_collection(db, c_collection_id);
    stl_collection_t* collection_ptr_to_clear = nullptr;
    auto collection_it_to_remove = db.named.end();

    if (c_collection_id == ukv_collection_main_k)
        collection_ptr_to_clear = &db.main;
    else {
        for (auto it = db.named.begin(); it != db.named.end() && collection_it_to_remove == db.named.end(); ++it) {
            if (it->second.get() == &collection) {
                collection_it_to_remove = it;
                collection_ptr_to_clear = collection_it_to_remove->second.get();
            }
        }
    }

    if (c_mode == ukv_drop_keys_vals_handle_k) {
        if (collection_it_to_remove == db.named.end())
            return;
        db.named.erase(collection_it_to_remove);
    }
    else if (c_mode == ukv_drop_keys_vals_k) {
        if (!collection_ptr_to_clear)
            return;
        collection_ptr_to_clear->pairs.clear();
        collection_ptr_to_clear->unique_elements = 0;
    }

    else if (c_mode == ukv_drop_vals_k) {
        if (!collection_ptr_to_clear)
            return;
        generation_t gen = ++db.youngest_generation;
        for (auto& kv : collection_ptr_to_clear->pairs)
            kv.second.reset(gen);
    }
}

void ukv_collection_list( //
    ukv_database_t const c_db,
    ukv_transaction_t const,
    ukv_size_t* c_count,
    ukv_collection_t** c_ids,
    ukv_length_t** c_offsets,
    ukv_char_t** c_names,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c_count && c_names, c_error, args_combo_k, "Need names and outputs!");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    std::shared_lock _ {db.mutex};
    std::size_t collections_count = db.named.size();
    *c_count = static_cast<ukv_size_t>(collections_count);

    // Every string will be null-terminated
    std::size_t strings_length = 0;
    for (auto const& name_and_contents : db.named)
        strings_length += name_and_contents.first.size() + 1;
    auto names = arena.alloc<char>(strings_length, c_error).begin();
    *c_names = names;
    return_on_error(c_error);

    // For every collection we also need to export IDs and offsets
    auto ids = arena.alloc_or_dummy<ukv_collection_t>(collections_count, c_error, c_ids);
    return_on_error(c_error);
    auto offs = arena.alloc_or_dummy<ukv_length_t>(collections_count + 1, c_error, c_offsets);
    return_on_error(c_error);

    std::size_t i = 0;
    for (auto const& name_and_contents : db.named) {
        auto len = name_and_contents.first.size();
        std::memcpy(names, name_and_contents.first.data(), len);
        names[len] = '\0';
        ids[i] = reinterpret_cast<ukv_collection_t>(name_and_contents.second.get());
        offs[i] = static_cast<ukv_length_t>(names - *c_names);
        names += len + 1;
        ++i;
    }
    offs[i] = static_cast<ukv_length_t>(names - *c_names);
}

void ukv_database_control( //
    ukv_database_t const c_db,
    ukv_str_view_t c_request,
    ukv_char_t** c_response,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c_request, c_error, uninitialized_state_k, "Request is uninitialized");

    *c_response = NULL;
    log_error(c_error, missing_feature_k, "Controls aren't supported in this implementation!");
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_transaction_init(
    // Inputs:
    ukv_database_t const c_db,
    ukv_options_t const,
    // Outputs:
    ukv_transaction_t* c_txn,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    validate_transaction_begin(c_txn, c_options, c_error);
    return_on_error(c_error);

    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);

    safe_section("Initializing transaction state", c_error, [&] {
        if (!*c_txn)
            *c_txn = new stl_txn_t();
    });
    return_on_error(c_error);

    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(*c_txn);
    txn.db_ptr = &db;
    txn.generation = ++db.youngest_generation;
    txn.requested.clear();
    txn.upserted.clear();
    txn.removed.clear();
}

void ukv_transaction_commit( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);

    validate_transaction_commit(c_txn, c_options, c_error);
    return_on_error(c_error);
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);

    // This write may fail with out-of-memory errors, if Hash-Tables
    // bucket allocation fails, but no values will be copied, only moved.
    std::unique_lock _ {db.mutex};
    generation_t const youngest_generation = db.youngest_generation.load();

    // 1. Check for refreshes among fetched keys
    for (auto const& [collection_key, sub_generation] : txn.requested) {
        stl_collection_t& collection = stl_collection(db, collection_key.collection);
        auto key_iterator = collection.pairs.find(collection_key.key);
        if (key_iterator == collection.pairs.end())
            continue;
        if (key_iterator->second.generation != sub_generation &&
            (*c_error = "Requested key was already overwritten since the start of the transaction!"))
            return;
    }

    // 2. Check for collisions among incoming values
    for (auto const& [collection_key, value] : txn.upserted) {
        stl_collection_t& collection = stl_collection(db, collection_key.collection);
        auto key_iterator = collection.pairs.find(collection_key.key);
        if (key_iterator == collection.pairs.end())
            continue;

        if (key_iterator->second.generation == txn.generation && (*c_error = "Can't commit same entry more than once!"))
            return;

        if (entry_was_overwritten(key_iterator->second.generation, txn.generation, youngest_generation) &&
            (*c_error = "Incoming key collides with newer entry!"))
            return;
    }

    // 3. Check for collisions among deleted values
    for (auto const& collection_key : txn.removed) {
        stl_collection_t& collection = stl_collection(db, collection_key.collection);
        auto key_iterator = collection.pairs.find(collection_key.key);
        if (key_iterator == collection.pairs.end())
            continue;

        if (key_iterator->second.generation == txn.generation && (*c_error = "Can't commit same entry more than once!"))
            return;

        if (entry_was_overwritten(key_iterator->second.generation, txn.generation, youngest_generation) &&
            (*c_error = "Removed key collides with newer entry!"))
            return;
    }

    // 4. Allocate space for more nodes across different collections
    safe_section("Inserting new collections", c_error, [&] {
        db.main.reserve_more(txn.upserted.size());
        for (auto& name_and_collection : db.named)
            name_and_collection.second->reserve_more(txn.upserted.size());
    });
    return_on_error(c_error);

    // 5. Import the data, as no collisions were detected
    for (auto& sub_key_and_value : txn.upserted) {
        stl_collection_t& collection = stl_collection(db, sub_key_and_value.first.collection);
        auto key_iterator = collection.pairs.find(sub_key_and_value.first.key);
        // A key was deleted:
        // if (sub_key_and_value.second.empty()) {
        //     if (key_iterator != collection.pairs.end())
        //         collection.pairs.erase(key_iterator);
        // }
        // A keys was updated:
        // else
        if (key_iterator != collection.pairs.end()) {
            key_iterator->second.generation = txn.generation;
            std::swap(key_iterator->second.buffer, sub_key_and_value.second);
        }
        // A key was inserted:
        else {
            stl_value_t value_w_generation {std::move(sub_key_and_value.second), txn.generation};
            collection.pairs.emplace(sub_key_and_value.first.key, std::move(value_w_generation));
            ++collection.unique_elements;
        }
    }

    // 6. Remove the requested entries
    for (auto const& collection_key : txn.removed) {
        stl_collection_t& collection = stl_collection(db, collection_key.collection);
        auto key_iterator = collection.pairs.find(collection_key.key);
        if (key_iterator == collection.pairs.end())
            continue;

        key_iterator->second.reset(txn.generation);
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        save_to_disk(db, c_error);
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_database_t const, ukv_arena_t c_arena) {
    if (!c_arena)
        return;
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(c_arena);
    delete &arena;
}

void ukv_transaction_free(ukv_database_t const, ukv_transaction_t const c_txn) {
    if (!c_txn)
        return;
    stl_txn_t& txn = *reinterpret_cast<stl_txn_t*>(c_txn);
    delete &txn;
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;
    stl_db_t& db = *reinterpret_cast<stl_db_t*>(c_db);
    delete &db;
}

void ukv_collection_free(ukv_database_t const, ukv_collection_t const) {
    // In this in-memory freeing the collection handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
