/**
 * @file engine_ram.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded In-Memory Key-Value Store built on @b AVL trees.
 * This implementation uses straightforward approach to implement concurrency.
 * It keeps all the entries sorted and is pretty fast for a BST-based container.
 * It supports:
 * > random sampling.
 * >
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
#include "helpers/pmr.hpp"
#include "helpers/file.hpp"
#include "helpers/avl.hpp"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_collection_t const ukv_collection_main_k = 0;
ukv_length_t const ukv_length_missing_k = std::numeric_limits<ukv_length_t>::max();
ukv_key_t const ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();
bool const ukv_supports_transactions_k = true;
bool const ukv_supports_named_collections_k = true;
bool const ukv_supports_snapshots_k = true;

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;
namespace fs = std::filesystem;

using blob_allocator_t = std::allocator<byte_t>;
struct blob_t {
    value_view_t range;

    blob_t() = default;
    blob_t(blob_t const&) = delete;
    blob_t& operator=(blob_t const&) = delete;

    blob_t(value_view_t other, ukv_error_t* c_error) {
        auto begin = blob_allocator_t {}.allocate(other.size());
        return_if_error(begin != nullptr, c_error, "Failed to copy a blob");
        range = {begin, other.size()};
    }

    ~blob_t() {
        if (range)
            blob_allocator_t {}.deallocate((byte_t*)range.data(), range.size());
    }
};

using acid_t = acid_gt<collection_key_t, blob_t>;
using txn_t = typename acid_t::transaction_t;
using generation_t = typename acid_t::generation_t;

struct db_t {
    std::mutex mutex;
    std::unordered_map<std::string, ukv_collection_t> names;

    /**
     * @brief Primary database state.
     */
    acid_t acid;
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

void save_to_disk(db_t const& db, ukv_error_t* c_error) {
}

void read_from_disk(db_t& db, ukv_error_t* c_error) {
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_database_init( //
    ukv_str_view_t c_config,
    ukv_database_t* c_db,
    ukv_error_t* c_error) {

    safe_section("Initializing DBMS", c_error, [&] {
        auto db_ptr = new db_t {};
        auto len = c_config ? std::strlen(c_config) : 0;
        if (len) {
            db_ptr->persisted_path = std::string(c_config, len);
            read_from_disk(*db_ptr, c_error);
        }
        *c_db = db_ptr;
    });
}

template <typename enumerator_at>
void read(db_t& db, places_arg_t places, ukv_options_t const, enumerator_at enumerator, ukv_error_t*) {
    for (std::size_t i = 0; i != places.size(); ++i) {
        place_t place = places[i];
        collection_key_t key = place.collection_key();
        db.acid.for_one(
            key,
            [&](blob_t const& value) { enumerator(i, value); },
            [&]() { enumerator(i, value_view_t {}); });
    }
}

template <typename enumerator_at>
void read(txn_t& txn, places_arg_t places, ukv_options_t const options, enumerator_at enumerator, ukv_error_t*) {

    bool const watch = options & ukv_option_txn_watch_k;
    for (std::size_t i = 0; i != places.size(); ++i) {
        place_t place = places[i];
        collection_key_t key = place.collection_key();
        txn.for_one(
            key,
            watch,
            [&](blob_t const& value) { enumerator(i, value); },
            [&]() { enumerator(i, value_view_t {}); });
    }
}

template <typename enumerator_at>
void write(db_t& db, places_arg_t places, contents_arg_t contents, ukv_options_t const, ukv_error_t* c_error) {

    safe_vector_gt<collection_key_t> keys(places.count, c_error);
    safe_vector_gt<blob_t> copies(places.count, c_error);
    return_on_error(c_error);

    for (std::size_t i = 0; i != places.size(); ++i) {
        place_t place = places[i];
        value_view_t content = contents[i];
        collection_key_t key = place.collection_key();
        keys[i] = key;
        copies[i] = blob_t {content, c_error};
        return_on_error(c_error);
    }

    db.acid.set_many(key, copies.begin());
}

template <typename enumerator_at>
void write(txn_t& txn, places_arg_t places, ukv_options_t const options, enumerator_at enumerator, ukv_error_t*) {

    bool const watch = options & ukv_option_txn_watch_k;
    for (std::size_t i = 0; i != places.size(); ++i) {
        place_t place = places[i];
        value_view_t content = contents[i];
        collection_key_t key = place.collection_key();
        value_t blob {content, c_error};
        return_on_error(c_error);
        txn.set(key, std::move(blob), watch);
    }
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
    if (!c_tasks_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
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

    c_txn //
        ? read(txn, places, c_options, meta_enumerator, c_error)
        : read(db, places, c_options, meta_enumerator, c_error);
    offs[places.count] = total_length;
    if (!needs_export)
        return;

    // 3. Pull the data, once we know the total length
    auto tape = arena.alloc<byte_t>(total_length, c_error).begin();
    auto data_enumerator = [&](std::size_t i, value_view_t value) {
        std::memcpy(std::exchange(tape, tape + value.size()), value.begin(), value.size());
    };

    *c_found_values = reinterpret_cast<ukv_byte_t*>(tape);
    c_txn //
        ? read(txn, places, c_options, data_enumerator, c_error)
        : read(db, places, c_options, data_enumerator, c_error);
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
    if (!c_tasks_count)
        return;

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
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

    return c_txn //
               ? write(txn, places, contents, c_options, c_error)
               : write(db, places, contents, c_options, c_error);
}

void ukv_scan( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

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
    if (!c_tasks_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_scan_limits, c_scan_limits_stride};
    scans_arg_t scans {collections, start_keys, end_keys, lens, c_tasks_count};

    validate_scan(c_txn, scans, c_options, c_error);
    return_on_error(c_error);

    return c_txn ? scan(txn, scans, c_options, c_found_offsets, c_found_counts, c_found_keys, arena, c_error)
                 : scan(db, scans, c_options, c_found_offsets, c_found_counts, c_found_keys, arena, c_error);
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

    ukv_options_t const c_options,

    ukv_size_t** c_min_cardinalities,
    ukv_size_t** c_max_cardinalities,
    ukv_size_t** c_min_value_bytes,
    ukv_size_t** c_max_value_bytes,
    ukv_size_t** c_min_space_usages,
    ukv_size_t** c_max_space_usages,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    if (!n)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    auto min_cardinalities = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_min_cardinalities);
    auto max_cardinalities = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_max_cardinalities);
    auto min_value_bytes = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_min_value_bytes);
    auto max_value_bytes = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_max_value_bytes);
    auto min_space_usages = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_min_space_usages);
    auto max_space_usages = arena.alloc_or_dummy<ukv_size_t>(n, c_error, c_max_space_usages);
    return_on_error(c_error);

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};

    *c_error = "Not implemented";
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
    db_t& db = *reinterpret_cast<db_t*>(c_db);
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
    return_if_error(c_collection_id != ukv_collection_main_k || !invalidate,
                    c_error,
                    args_combo_k,
                    "Default collection can't be invalidated.");

    db_t& db = *reinterpret_cast<db_t*>(c_db);
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
    ukv_options_t const c_options,
    ukv_size_t* c_count,
    ukv_collection_t** c_ids,
    ukv_length_t** c_offsets,
    ukv_char_t** c_names,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c_count && c_names, c_error, args_combo_k, "Need names and outputs!");

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    db_t& db = *reinterpret_cast<db_t*>(c_db);
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
    ukv_options_t const c_options,
    // Outputs:
    ukv_transaction_t* c_txn,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    validate_transaction_begin(c_txn, c_options, c_error);
    return_on_error(c_error);

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    safe_section("Initializing transaction state", c_error, [&] {
        if (!*c_txn)
            *c_txn = new txn_t(db.transaction());
    });
    return_on_error(c_error);

    txn_t& txn = *reinterpret_cast<txn_t*>(*c_txn);
}

void ukv_transaction_commit( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    db_t& db = *reinterpret_cast<db_t*>(c_db);

    validate_transaction_commit(c_txn, c_options, c_error);
    return_on_error(c_error);
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    txn.commit();

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
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    delete &txn;
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;
    db_t& db = *reinterpret_cast<db_t*>(c_db);
    delete &db;
}

void ukv_collection_free(ukv_database_t const, ukv_collection_t const) {
    // In this in-memory freeing the collection handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
