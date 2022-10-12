/**
 * @file engine_ram.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded In-Memory Key-Value Store built on @b AVL trees or STL.
 * This implementation uses straightforward approach to implement concurrency.
 * It keeps all the pairs sorted and is pretty fast for a BST-based container.
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

#include <consistent_set.hpp> // `av::consistent_set_gt`

#include "ukv/db.h"
#include "helpers/pmr.hpp"
#include "helpers/file.hpp"
#include "helpers/avl.hpp"
#include "helpers/vector.hpp"      // `unintialized_vector_gt`
#include "ukv/cpp/ranges_args.hpp" // `places_arg_t`

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
using namespace av;
namespace fs = std::filesystem;

using blob_allocator_t = std::allocator<byte_t>;

struct pair_t {
    collection_key_t collection_key;
    value_view_t range;

    pair_t() = default;
    pair_t(pair_t const&) = delete;
    pair_t& operator=(pair_t const&) = delete;

    pair_t(collection_key_t collection_key) noexcept : collection_key(collection_key) {}

    pair_t(collection_key_t collection_key, value_view_t other, ukv_error_t* c_error) noexcept
        : collection_key(collection_key) {
        if (other.size()) {
            auto begin = blob_allocator_t {}.allocate(other.size());
            return_if_error(begin != nullptr, c_error, out_of_memory_k, "Failed to copy a blob");
            range = {begin, other.size()};
            std::memcpy(begin, other.begin(), other.size());
        }
        else
            range = other;
    }

    ~pair_t() noexcept {
        if (range.size())
            blob_allocator_t {}.deallocate((byte_t*)range.data(), range.size());
        range = {};
    }

    pair_t(pair_t&& other) noexcept
        : collection_key(other.collection_key), range(std::exchange(other.range, value_view_t {})) {}

    pair_t& operator=(pair_t&& other) noexcept {
        std::swap(collection_key, other.collection_key);
        std::swap(range, other.range);
        return *this;
    }

    operator collection_key_t() const noexcept { return collection_key; }
};

struct pair_compare_t {
    using value_type = collection_key_t;
    bool operator()(collection_key_t const& a, collection_key_t const& b) const noexcept { return a < b; }
};

/*********************************************************/
/*****************  Using Consistent Sets ****************/
/*********************************************************/

using consistent_set_t = consistent_set_gt<pair_t, pair_compare_t>;
using transaction_t = typename consistent_set_t::transaction_t;
using generation_t = typename consistent_set_t::generation_t;

template <typename set_or_transaction_at, typename callback_at>
consistent_set_status_t find_and_watch(set_or_transaction_at& set_or_transaction,
                                       collection_key_t collection_key,
                                       ukv_options_t options,
                                       callback_at&& callback) noexcept {

    if constexpr (!std::is_same<set_or_transaction_at, consistent_set_t>()) {
        bool dont_watch = options & ukv_option_transaction_dont_watch_k;
        if (!dont_watch)
            if (auto watch_status = set_or_transaction.watch(collection_key); !watch_status)
                return watch_status;
    }

    auto find_status = set_or_transaction.find(
        collection_key,
        [&](pair_t const& pair) noexcept { callback(pair.range); },
        [&]() noexcept { callback(value_view_t {}); });
    return find_status;
}

template <typename set_or_transaction_at, typename callback_at>
consistent_set_status_t scan_and_watch(set_or_transaction_at& set_or_transaction,
                                       collection_key_t start,
                                       std::size_t range_limit,
                                       ukv_options_t options,
                                       callback_at&& callback) noexcept {

    std::size_t match_idx = 0;
    collection_key_t previous = start;
    bool reached_end = false;
    auto watch_status = consistent_set_status_t();
    auto callback_pair = [&](pair_t const& pair) {
        reached_end = pair.collection_key.collection != previous.collection;
        if (reached_end)
            return;

        if constexpr (!std::is_same<set_or_transaction_at, consistent_set_t>()) {
            bool dont_watch = options & ukv_option_transaction_dont_watch_k;
            if (!dont_watch)
                if (watch_status = set_or_transaction.watch(pair); !watch_status)
                    return;
        }

        callback(pair);
        previous.key = pair.collection_key.key;
        ++match_idx;
    };

    auto find_status = set_or_transaction.find(start, callback_pair, [] {});
    if (!find_status)
        return find_status;
    if (!watch_status)
        return watch_status;

    while (match_idx != range_limit && !reached_end) {
        find_status = set_or_transaction.find_next(previous, callback_pair, [&] { reached_end = true; });
        if (!find_status)
            return find_status;
        if (!watch_status)
            return watch_status;
    }

    return {};
}

template <typename set_or_transaction_at, typename callback_at>
consistent_set_status_t scan_full(set_or_transaction_at& set_or_transaction, callback_at&& callback) noexcept {

    collection_key_t previous {ukv_collection_main_k, ukv_key_unknown_k};
    while (true) {
        auto callback_pair = [&](pair_t const& pair) {
            callback(pair);
            previous.key = pair.collection_key.key;
        };

        auto reached_end = false;
        auto callback_nothing = [&] {
            reached_end = true;
        };

        auto status = set_or_transaction.find_next(previous, callback_pair, callback_nothing);
        if (reached_end)
            break;
        if (!status)
            return status;
    }

    return {};
}

/*********************************************************/
/***************** Collections Management ****************/
/*********************************************************/

struct string_hash_t {
    using stl_t = std::hash<std::string_view>;
    using is_transparent = void;

    auto operator()(const char* str) const { return stl_t {}(str); }
    auto operator()(std::string_view str) const { return stl_t {}(str); }
    auto operator()(std::string const& str) const { return stl_t {}(str); }
};

struct string_eq_t : public std::equal_to<std::string_view> {
    using is_transparent = void;
};

struct string_less_t : public std::less<std::string_view> {
    using is_transparent = void;
};

struct database_t {
    /**
     * @brief Rarely-used mutex for global reorganizations, like:
     * > Removing existing collections or adding new ones.
     * > Listing present collections.
     */
    std::shared_mutex restructuring_mutex;

    /**
     * @brief Primary database state.
     */
    consistent_set_t pairs;

    /**
     * @brief A variable-size set of named collections.
     * It's cleaner to implement it with heterogenous lookups as
     * an @c `std::unordered_map`, but it requires GCC11 and C++20.
     */
    std::map<std::string, ukv_collection_t, string_less_t> names;

    /**
     * @brief Path on disk, from which the data will be read.
     * When closed, we will try saving the DB on disk.
     */
    std::string persisted_path;

    database_t(consistent_set_t&& set) noexcept(false) : pairs(std::move(set)) {}

    database_t(database_t&& other) noexcept
        : pairs(std::move(other.pairs)), names(std::move(other.names)),
          persisted_path(std::move(other.persisted_path)) {}
};

ukv_collection_t new_collection(database_t& db) noexcept {
    bool is_new = false;
    ukv_collection_t new_handle = ukv_collection_main_k;
    while (!is_new) {
        auto top = static_cast<std::uint64_t>(std::rand());
        auto bottom = static_cast<std::uint64_t>(std::rand());
        new_handle = static_cast<ukv_collection_t>((top << 32) | bottom);
        is_new = new_handle != ukv_collection_main_k;
        for (auto const& [name, existing_handle] : db.names)
            is_new &= new_handle != existing_handle;
    }
    return new_handle;
}

void export_error_code(consistent_set_status_t code, ukv_error_t* c_error) noexcept {
    if (!code)
        *c_error = "Faced error!";
}

/*********************************************************/
/*****************	 Writing to Disk	  ****************/
/*********************************************************/

void write_pair(file_handle_t const& handle, pair_t const& pair, ukv_error_t* c_error) {

    if (!pair.range)
        return;

    auto saved_len = std::fwrite(&pair.collection_key.collection, sizeof(ukv_collection_t), 1, handle);
    return_if_error(saved_len == 1, c_error, 0, "Write partially failed on collection.");

    saved_len = std::fwrite(&pair.collection_key.key, sizeof(ukv_key_t), 1, handle);
    return_if_error(saved_len == 1, c_error, 0, "Write partially failed on key.");

    auto buf = value_view_t(pair.range);
    auto buf_len = static_cast<ukv_length_t>(buf.size());
    saved_len = std::fwrite(&buf_len, sizeof(ukv_length_t), 1, handle);
    return_if_error(saved_len == 1, c_error, 0, "Write partially failed on value len.");

    saved_len = std::fwrite(buf.data(), sizeof(byte_t), buf.size(), handle);
    return_if_error(saved_len == buf.size(), c_error, 0, "Write partially failed on value.");
}

void read_pair(file_handle_t const& handle, pair_t& pair, bool should_continue, ukv_error_t* c_error) {

    // An empty row may contain no content
    auto read_len = std::fread(&pair.collection_key.collection, sizeof(ukv_collection_t), 1, handle);
    should_continue &= read_len == 1;
    if (!should_continue)
        return;
    return_if_error(read_len <= 1, c_error, 0, "Read yielded unexpected result on key.");

    // .. but if the row exists, it shouldn't be partial
    read_len = std::fread(&pair.collection_key.key, sizeof(ukv_key_t), 1, handle);
    return_if_error(read_len == 1, c_error, 0, "Read partially failed on key.");

    auto buf_len = ukv_length_t(0);
    read_len = std::fread(&buf_len, sizeof(ukv_length_t), 1, handle);
    return_if_error(read_len == 1, c_error, 0, "Read partially failed on value len.");

    auto buf_ptr = blob_allocator_t {}.allocate(buf_len);
    return_if_error(buf_ptr != nullptr, c_error, out_of_memory_k, "Failed to allocate a blob");
    pair.range = value_view_t {buf_ptr, buf_len};
    read_len = std::fread(buf_ptr, sizeof(byte_t), buf_len, handle);
    return_if_error(read_len == buf_len, c_error, 0, "Read partially failed on value.");
}

void write(database_t const& db, std::string const& path, ukv_error_t* c_error) {
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

    // Print stats about the overall dataset:
    // https://fmt.dev/latest/api.html#_CPPv4IDpEN3fmt5printEvPNSt4FILEE13format_stringIDp1TEDpRR1T
    std::fprintf(handle, "Total Items: %zu\n", db.pairs.size());
    std::fprintf(handle, "Named Collections: %zu\n", db.names.size());
    for (auto const& name_and_handle : db.names)
        std::fprintf(handle,
                     "-%s: 0x%016zx\n",
                     name_and_handle.first.c_str(),
                     static_cast<std::size_t>(name_and_handle.second));
    std::fprintf(handle, "\n");

    // Save the pairs
    scan_full(db.pairs, [&](pair_t const& pair) { write_pair(handle, pair, c_error); });
    return_on_error(c_error);

    // Close the file
    log_error(c_error, 0, handle.close().release_error());
}

void read(database_t& db, std::string const& path, ukv_error_t* c_error) {
    db.names.clear();
    auto status = db.pairs.clear();
    if (!status)
        return export_error_code(status, c_error);

    // Check if file even exists
    if (!std::filesystem::exists(path))
        return;

    // Similar to serialization, we don't use STL here
    file_handle_t handle;
    if ((*c_error = handle.open(path.c_str(), "rb+").release_error()))
        return;

    // Get the collection size, to preallocate pairs
    char line_buffer[256];
    while (std::fgets(line_buffer, sizeof(line_buffer), handle) != NULL) {
        // Check if it is a collection description
        auto line_length = std::find(line_buffer, line_buffer + sizeof(line_buffer), '\n') - line_buffer;
        if (line_length == 0)
            break;
        if (line_buffer[0] == '-') {
            auto name_length = std::find(line_buffer + 1, line_buffer + sizeof(line_buffer), ':') - line_buffer;
            auto name = std::string_view(line_buffer + 1, name_length);
            auto id_str_begin = line_buffer + line_length - 16;
            auto id_str_end = line_buffer + line_length;
            auto id = std::strtoull(id_str_begin, &id_str_end, 16);
            db.names.emplace(name, id);
        }
        else
            // Skip metadata rows
            continue;
    }

    // Load the pairs
    bool should_continue = true;
    while (std::feof(handle) == 0) {
        pair_t pair;
        read_pair(handle, pair, should_continue, c_error);
        if (!should_continue)
            break;
        return_on_error(c_error);
        auto status = db.pairs.upsert(std::move(pair));
        if (!status)
            return export_error_code(status, c_error);
    }

    // Close the file
    log_error(c_error, 0, handle.close().release_error());
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_database_init( //
    ukv_str_view_t c_config,
    ukv_database_t* c_db,
    ukv_error_t* c_error) {

    safe_section("Initializing DBMS", c_error, [&] {
        auto maybe_pairs = consistent_set_t::make();
        return_if_error(maybe_pairs, c_error, error_unknown_k, "Couldn't build consistent set");
        auto db = database_t(std::move(maybe_pairs.value()));
        auto db_ptr = std::make_unique<database_t>(std::move(db)).release();
        auto len = c_config ? std::strlen(c_config) : 0;
        if (len) {
            db_ptr->persisted_path = std::string(c_config, len);
            read(*db_ptr, db_ptr->persisted_path, c_error);
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
    if (!c_tasks_count)
        return;

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {collections, keys, {}, c_tasks_count};
    validate_read(c_txn, places, c_options, c_error);
    return_on_error(c_error);

    // 1. Allocate a tape for all the values to be pulled
    growing_tape_t tape(arena);
    tape.reserve(places.size(), c_error);
    return_on_error(c_error);
    auto back_inserter = [&](value_view_t value) noexcept {
        tape.push_back(value, c_error);
    };

    // 2. Pull the data
    for (std::size_t task_idx = 0; task_idx != places.size(); ++task_idx) {
        place_t place = places[task_idx];
        collection_key_t key = place.collection_key();
        auto status = c_txn //
                          ? find_and_watch(txn, key, c_options, back_inserter)
                          : find_and_watch(db.pairs, key, c_options, back_inserter);
        if (!status)
            return export_error_code(status, c_error);
    }

    // 3. Export the results
    if (c_found_presences)
        *c_found_presences = tape.presences().get();
    if (c_found_offsets)
        *c_found_offsets = tape.offsets().begin().get();
    if (c_found_lengths)
        *c_found_lengths = tape.lengths().begin().get();
    if (c_found_values)
        *c_found_values = (ukv_bytes_ptr_t)tape.contents().begin().get();
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

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    if (!c_tasks_count)
        return;

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_length_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_lens, c_lens_stride};
    bits_view_t presences {c_presences};

    places_arg_t places {collections, keys, {}, c_tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c_tasks_count};

    validate_write(c_txn, places, contents, c_options, c_error);
    return_on_error(c_error);

    // Writes are the only operations that significantly differ
    // in terms of transactional and batch operations.
    // The latter will also differ depending on the number
    // pairs you are working with - one or more.
    if (c_txn) {
        bool dont_watch = c_options & ukv_option_transaction_dont_watch_k;
        for (std::size_t i = 0; i != places.size(); ++i) {
            place_t place = places[i];
            value_view_t content = contents[i];
            collection_key_t key = place.collection_key();
            if (!dont_watch)
                if (auto watch_status = txn.watch(key); !watch_status)
                    return export_error_code(watch_status, c_error);

            pair_t pair {key, content, c_error};
            return_on_error(c_error);
            auto status = txn.upsert(std::move(pair));
            if (!status)
                return export_error_code(status, c_error);
        }
        return;
    }

    // Non-transactional but atomic batch-write operation.
    // It requires producing a copy of input data.
    else if (c_tasks_count > 1) {
        uninitialized_vector_gt<pair_t> copies(places.count, arena, c_error);
        return_on_error(c_error);
        initialized_range_gt<pair_t> copies_constructed(copies);

        for (std::size_t i = 0; i != places.size(); ++i) {
            place_t place = places[i];
            value_view_t content = contents[i];
            collection_key_t key = place.collection_key();

            pair_t pair {key, content, c_error};
            return_on_error(c_error);
            copies[i] = std::move(pair);
        }

        auto status = db.pairs.upsert(std::make_move_iterator(copies.begin()), std::make_move_iterator(copies.end()));
        return export_error_code(status, c_error);
    }

    // Just a single non-batch write
    else {
        place_t place = places[0];
        value_view_t content = contents[0];
        collection_key_t key = place.collection_key();

        pair_t pair {key, content, c_error};
        return_on_error(c_error);
        auto status = db.pairs.upsert(std::move(pair));
        return export_error_code(status, c_error);
    }
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

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_scan_limits, c_scan_limits_stride};
    scans_arg_t scans {collections, start_keys, lens, c_tasks_count};

    validate_scan(c_txn, scans, c_options, c_error);
    return_on_error(c_error);

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy(scans.count + 1, c_error, c_found_offsets);
    return_on_error(c_error);
    auto counts = arena.alloc_or_dummy(scans.count, c_error, c_found_counts);
    return_on_error(c_error);

    auto total_keys = reduce_n(scans.limits, scans.count, 0ul);
    auto keys_output = *c_found_keys = arena.alloc<ukv_key_t>(total_keys, c_error).begin();
    return_on_error(c_error);

    // 2. Fetch the data
    for (std::size_t task_idx = 0; task_idx != scans.count; ++task_idx) {
        scan_t scan = scans[task_idx];
        offsets[task_idx] = keys_output - *c_found_keys;

        ukv_length_t matched_pairs_count = 0;
        auto found_pair = [&](pair_t const& pair) noexcept {
            *keys_output = pair.collection_key.key;
            ++keys_output;
            ++matched_pairs_count;
        };

        auto previous_key = collection_key_t {scan.collection, scan.min_key};
        auto status = c_txn //
                          ? scan_and_watch(txn, previous_key, scan.limit, c_options, found_pair)
                          : scan_and_watch(db.pairs, previous_key, scan.limit, c_options, found_pair);
        if (!status)
            return export_error_code(status, c_error);

        counts[task_idx] = matched_pairs_count;
    }
    offsets[scans.count] = keys_output - *c_found_keys;
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

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    auto min_cardinalities = arena.alloc_or_dummy(n, c_error, c_min_cardinalities);
    auto max_cardinalities = arena.alloc_or_dummy(n, c_error, c_max_cardinalities);
    auto min_value_bytes = arena.alloc_or_dummy(n, c_error, c_min_value_bytes);
    auto max_value_bytes = arena.alloc_or_dummy(n, c_error, c_max_value_bytes);
    auto min_space_usages = arena.alloc_or_dummy(n, c_error, c_min_space_usages);
    auto max_space_usages = arena.alloc_or_dummy(n, c_error, c_max_space_usages);
    return_on_error(c_error);

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
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
    database_t& db = *reinterpret_cast<database_t*>(c_db);
    std::unique_lock _ {db.restructuring_mutex};

    std::string_view collection_name {c_collection_name, name_len};
    auto collection_it = db.names.find(collection_name);
    return_if_error(collection_it == db.names.end(), c_error, args_wrong_k, "Such collection already exists!");

    auto new_collection_id = new_collection(db);
    safe_section("Inserting new collection", c_error, [&] { db.names.emplace(collection_name, new_collection_id); });
    *c_collection = new_collection_id;
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

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    std::unique_lock _ {db.restructuring_mutex};

    if (c_mode == ukv_drop_keys_vals_handle_k) {
        auto status = db.pairs.erase_all(c_collection_id);
        if (!status)
            return export_error_code(status, c_error);

        for (auto it = db.names.begin(); it != db.names.end(); ++it) {
            if (c_collection_id != it->second)
                continue;
            db.names.erase(it);
            break;
        }
    }

    else if (c_mode == ukv_drop_keys_vals_k) {
        auto status = db.pairs.erase_all(c_collection_id);
        return export_error_code(status, c_error);
    }

    else if (c_mode == ukv_drop_vals_k) {
        auto status = db.pairs.find_all(c_collection_id, [&](pair_t& pair) {
            pair = pair_t {pair.collection_key, {}, nullptr};
        });
        return export_error_code(status, c_error);
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

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    std::shared_lock _ {db.restructuring_mutex};
    std::size_t collections_count = db.names.size();
    *c_count = static_cast<ukv_size_t>(collections_count);

    // Every string will be null-terminated
    std::size_t strings_length = 0;
    for (auto const& name_and_handle : db.names)
        strings_length += name_and_handle.first.size() + 1;
    auto names = arena.alloc<char>(strings_length, c_error).begin();
    *c_names = names;
    return_on_error(c_error);

    // For every collection we also need to export IDs and offsets
    auto ids = arena.alloc_or_dummy(collections_count, c_error, c_ids);
    return_on_error(c_error);
    auto offs = arena.alloc_or_dummy(collections_count + 1, c_error, c_offsets);
    return_on_error(c_error);

    std::size_t i = 0;
    for (auto const& name_and_handle : db.names) {
        auto len = name_and_handle.first.size();
        std::memcpy(names, name_and_handle.first.data(), len);
        names[len] = '\0';
        ids[i] = name_and_handle.second;
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

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    safe_section("Initializing transaction state", c_error, [&] {
        if (*c_txn)
            return;

        auto maybe_txn = db.pairs.transaction();
        return_if_error(maybe_txn, c_error, error_unknown_k, "Couldn't start a transaction");
        *c_txn = std::make_unique<transaction_t>(std::move(maybe_txn.value())).release();
    });
    return_on_error(c_error);

    transaction_t& txn = *reinterpret_cast<transaction_t*>(*c_txn);
    auto status = txn.reset();
    return export_error_code(status, c_error);
}

void ukv_transaction_commit( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    database_t& db = *reinterpret_cast<database_t*>(c_db);

    validate_transaction_commit(c_txn, c_options, c_error);
    return_on_error(c_error);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    auto status = txn.commit();
    if (!status)
        return export_error_code(status, c_error);

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        write(db, db.persisted_path, c_error);
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
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    delete &txn;
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    if (!db.persisted_path.empty()) {
        ukv_error_t c_error = nullptr;
        write(db, db.persisted_path, &c_error);
    }

    delete &db;
}

void ukv_collection_free(ukv_database_t const, ukv_collection_t const) {
    // In this in-memory freeing the collection handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
