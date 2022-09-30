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
#include <map>           // Collection names
#include <unordered_map> // Watched keys generations
#include <set>           // Primary entries container
#include <shared_mutex>  // Syncing access to entries container
#include <mutex>         // `std::unique_lock`
#include <numeric>       // `std::accumulate`
#include <atomic>        // Thread-safe generation counters
#include <stdio.h>       // Saving/reading from disk
#include <filesystem>    // Enumerating the directory

#include "ukv/db.h"
#include "ukv/cpp/ranges_args.hpp" // `places_arg_t`
#include "helpers/pmr.hpp"         // `stl_arena_t`
#include "helpers/file.hpp"        // `file_handle_t`

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

using allocator_t = std::allocator<byte_t>;
using byte_ptr_t = byte_t*;
using generation_t = std::int64_t;

constexpr generation_t missing_data_generation_k = std::numeric_limits<generation_t>::min();

struct database_t;
struct collection_t;
struct transaction_t;

struct entry_t {
    ukv_collection_t collection {ukv_collection_main_k};
    ukv_key_t key {ukv_key_unknown_k};
    mutable generation_t generation {missing_data_generation_k};
    mutable byte_ptr_t value_begin {nullptr};
    mutable byte_ptr_t value_end {nullptr};

    entry_t() noexcept = default;
    entry_t(collection_key_t col_key) noexcept : collection(col_key.collection), key(col_key.key) {}
    entry_t(entry_t const&) = delete;
    entry_t& operator=(entry_t const&) = delete;

    entry_t(entry_t&& other) noexcept
        : collection(other.collection), key(other.key), generation(other.generation),
          value_begin(std::exchange(other.value_begin, nullptr)), value_end(std::exchange(other.value_end, nullptr)) {}

    entry_t& operator=(entry_t&& other) noexcept {
        std::swap(collection, other.collection);
        std::swap(key, other.key);
        std::swap(generation, other.generation);
        std::swap(value_begin, other.value_begin);
        std::swap(value_end, other.value_end);
        return *this;
    }

    ~entry_t() noexcept { release_blob(); }

    collection_key_t collection_key() const noexcept { return {collection, key}; }
    bool is_deleted() const noexcept { return !value_begin && !value_end; }
    bool is_empty() const noexcept { return value_begin == value_end; }
    explicit operator bool() const noexcept { return !is_deleted(); }
    operator value_view_t() const noexcept {
        return !is_deleted() ? value_view_t {value_begin, value_end} : value_view_t {};
    }

    void release_blob() const noexcept {
        if (value_end != value_begin)
            allocator_t {}.deallocate(value_begin, value_end - value_begin);
        value_begin = value_end = nullptr;
        generation = missing_data_generation_k;
    }

    bool assign_empty(generation_t generation) const noexcept {
        return assign_blob(value_view_t {(byte_ptr_t)(this), (byte_ptr_t)(this)}, generation);
    }
    bool assign_null(generation_t generation) const noexcept { return assign_blob(value_view_t {}, generation); }

    bool assign_blob(value_view_t value, generation_t g) const noexcept {
        release_blob();
        if (value.size()) {
            auto begin = allocator_t {}.allocate(value.size());
            if (!begin)
                return false;
            value_begin = begin;
            value_end = begin + value.size();
            generation = g;
            std::memcpy(value_begin, value.data(), value.size());
            return true;
        }
        else {
            // Dirty-hack - using self-reference to distinguish empty (zero
            // length) value from deleted one.
            value_begin = value_end = value ? (byte_ptr_t)(this) : nullptr;
            generation = g;
            return true;
        }
    }

    void swap_blob(entry_t const& other) const noexcept {
        std::swap(generation, other.generation);
        std::swap(value_begin, other.value_begin);
        std::swap(value_end, other.value_end);
    }
};

struct entry_compare_t {
    using is_transparent = void;
    bool operator()(entry_t const& a, entry_t const& b) const noexcept {
        return a.collection == b.collection ? a.key < b.key : a.collection < b.collection;
    }
    bool operator()(entry_t const& a, collection_key_t b) const noexcept {
        return a.collection == b.collection ? a.key < b.key : a.collection < b.collection;
    }
    bool operator()(collection_key_t a, entry_t const& b) const noexcept {
        return a.collection == b.collection ? a.key < b.key : a.collection < b.collection;
    }
    bool operator()(entry_t const& a, ukv_collection_t b) const noexcept { return a.collection < b; }
    bool operator()(ukv_collection_t a, entry_t const& b) const noexcept { return a < b.collection; }
};

using collection_ptr_t = std::unique_ptr<collection_t>;

struct transaction_t {
    std::set<entry_t, entry_compare_t> changes;
    std::unordered_map<collection_key_t, generation_t> watched;

    database_t* db_ptr {nullptr};
    generation_t generation {0};
};

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
    std::shared_mutex mutex;
    std::set<entry_t, entry_compare_t> entries;

    /**
     * @brief A variable-size set of named collections.
     * It's cleaner to implement it with heterogenous lookups as
     * an @c `std::unordered_mao`, but it requires GCC11 and C++20.
     */
    std::map<std::string, ukv_collection_t, string_less_t> names;
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

    void reserve_entry_nodes(std::size_t) {
        //  pairs.reserve(pairs.size() + n);
    }

    ukv_collection_t new_collection() const noexcept {
        bool is_new = false;
        ukv_collection_t new_handle = ukv_collection_main_k;
        while (!is_new) {
            auto top = static_cast<std::uint64_t>(std::rand());
            auto bottom = static_cast<std::uint64_t>(std::rand());
            new_handle = static_cast<ukv_collection_t>((top << 32) | bottom);
            is_new = new_handle != ukv_collection_main_k;
            for (auto const& [name, existing_handle] : names)
                is_new &= new_handle != existing_handle;
        }
        return new_handle;
    }
};

/**
 * @brief Solves the problem of modulo arithmetic and `generation_t` overflow.
 * Still works correctly, when `max` has overflown, but `min` hasn't yet,
 * so `min` can be bigger than `max`.
 */
inline bool entry_was_overwritten(generation_t entry_generation,
                                  generation_t transaction_generation,
                                  generation_t youngest_generation) noexcept {

    return transaction_generation <= youngest_generation
               ? ((entry_generation >= transaction_generation) & (entry_generation <= youngest_generation))
               : ((entry_generation >= transaction_generation) | (entry_generation <= youngest_generation));
}
#if 0
void save_to_disk(collection_t const& collection, std::string const& path, ukv_error_t* c_error) {
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
        auto n = static_cast<ukv_size_t>(collection.unique_elements);
        auto saved_len = std::fwrite(&n, sizeof(ukv_size_t), 1, handle);
        return_if_error(saved_len == 1, c_error, 0, "Couldn't write anything to file.");
    }

    // Save the entries
    for (auto const& [key, seq_val] : db.entries) {
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

void read_from_disk(collection_t& collection, std::string const& path, ukv_error_t* c_error) {
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
    db.entries.clear();
    collection.reserve_entry_nodes(n);
    collection.unique_elements = n;

    for (ukv_size_t i = 0; i != n; ++i) {

        auto key = ukv_key_t {};
        auto read_len = std::fread(&key, sizeof(ukv_key_t), 1, handle);
        return_if_error(read_len == 1, c_error, 0, "Read partially failed on key.");

        auto buf_len = ukv_length_t(0);
        read_len = std::fread(&buf_len, sizeof(ukv_length_t), 1, handle);
        return_if_error(read_len == 1, c_error, 0, "Read partially failed on value len.");

        auto buf = blob_t(buf_len);
        read_len = std::fread(buf.data(), sizeof(byte_t), buf.size(), handle);
        return_if_error(read_len == buf.size(), c_error, 0, "Read partially failed on value.");

        db.entries.emplace(key, entry_t {std::move(buf), generation_t {0}, false});
    }

    log_error(c_error, 0, handle.close().release_error());
}

void save_to_disk(database_t const& db, ukv_error_t* c_error) {
    auto dir_path = fs::path(db.persisted_path);
    return_if_error(fs::is_directory(dir_path), c_error, args_wrong_k, "Supplied path is not a directory!");

    save_to_disk(db.main, dir_path / ".stl.ukv", c_error);
    return_on_error(c_error);

    for (auto const& name_and_collection : db.names) {
        auto name_with_ext = std::string(name_and_collection.first) + ".stl.ukv";
        save_to_disk(*name_and_collection.second, dir_path / name_with_ext, c_error);
        return_on_error(c_error);
    }
}

void read_from_disk(database_t& db, ukv_error_t* c_error) {
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
        auto collection = std::make_unique<collection_t>();
        collection->name = filename;
        read_from_disk(*collection, path_str, c_error);
        db.names.emplace(std::string_view(collection->name), std::move(collection));
    }
}
#endif
void save_to_disk(database_t const& db, ukv_error_t* c_error) {
}
void read_from_disk(database_t& db, ukv_error_t* c_error) {
}

void write_head( //
    database_t& db,
    places_arg_t places,
    contents_arg_t contents,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    std::unique_lock _ {db.mutex};
    auto new_generation = ++db.youngest_generation;

    for (std::size_t i = 0; i != places.size(); ++i) {

        auto place = places[i];
        auto content = contents[i];
        auto db_iterator = db.entries.find(place.collection_key());

        // We want to insert a new entry, but let's check if we
        // can overwrite the existing value without causing reallocations.
        safe_section("Copying new value", c_error, [&] {
            if (db_iterator == db.entries.end())
                db_iterator = db.entries.emplace(place.collection_key()).first;

            entry_t const& entry = *db_iterator;
            entry.assign_blob(content, new_generation);
        });
        return_on_error(c_error);
    }

    // TODO: Degrade the lock to "shared" state before starting expensive IO
    if (c_options & ukv_option_write_flush_k)
        save_to_disk(db, c_error);
}

void write_txn( //
    transaction_t& txn,
    places_arg_t places,
    contents_arg_t contents,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    // No need for locking here, until we commit, unless, of course,
    // a collection is being deleted.
    database_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    bool dont_watch = c_options & ukv_option_transaction_dont_watch_k;

    for (std::size_t i = 0; i != places.size(); ++i) {

        auto place = places[i];
        auto content = contents[i];

        safe_section("Copying new value", c_error, [&] {
            // Track potential future changes
            if (!dont_watch) {
                auto db_iterator = db.entries.find(place.collection_key());
                auto last_generation =
                    db_iterator != db.entries.end() ? db_iterator->generation : missing_data_generation_k;
                txn.watched.insert_or_assign(place.collection_key(), last_generation);
            }

            // Update transaction state
            entry_t entry {place.collection_key()};
            entry.assign_blob(content, txn.generation);
            txn.changes.insert(std::move(entry));
        });
        return_on_error(c_error);
    }
}

template <typename value_enumerator_at>
void read_head_under_lock( //
    database_t& db,
    places_arg_t tasks,
    ukv_options_t const,
    value_enumerator_at enumerator,
    ukv_error_t*) {

    for (std::size_t i = 0; i != tasks.size(); ++i) {
        place_t place = tasks[i];
        auto db_iterator = db.entries.find(place.collection_key());
        auto value = db_iterator != db.entries.end() ? value_view_t(*db_iterator) : value_view_t {};
        enumerator(i, value);
    }
}

template <typename value_enumerator_at>
void read_txn_under_lock( //
    transaction_t& txn,
    places_arg_t tasks,
    ukv_options_t const c_options,
    value_enumerator_at enumerator,
    ukv_error_t* c_error) {

    database_t& db = *txn.db_ptr;
    generation_t const youngest_generation = db.youngest_generation.load();
    bool const dont_watch = c_options & ukv_option_transaction_dont_watch_k;

    for (std::size_t i = 0; i != tasks.size(); ++i) {
        place_t place = tasks[i];

        // Some keys may already be overwritten inside of transaction
        if (auto txn_iterator = txn.changes.find(place.collection_key()); txn_iterator != txn.changes.end())
            enumerator(i, value_view_t(*txn_iterator));

        // Others should be pulled from the main store
        else if (auto db_iterator = db.entries.find(place.collection_key()); db_iterator != db.entries.end()) {

            if (entry_was_overwritten(db_iterator->generation, txn.generation, youngest_generation) &&
                (*c_error = "Requested key was already overwritten since the start of the transaction!"))
                return;

            enumerator(i, value_view_t(*db_iterator));
            if (!dont_watch)
                txn.watched.emplace(place.collection_key(), db_iterator->generation);
        }

        // But some will be missing
        else {
            enumerator(i, value_view_t {});

            if (!dont_watch)
                txn.watched.emplace(place.collection_key(), missing_data_generation_k);
        }
    }
}

void scan_head( //
    database_t& db,
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
        offsets[i] = keys_output - *c_found_keys;

        ukv_length_t j = 0;
        auto db_iterator = db.entries.lower_bound(collection_key_t {scan.collection, scan.min_key});
        while (j != scan.limit &&                            //
               db_iterator != db.entries.end() &&            //
               db_iterator->collection == scan.collection && //
               db_iterator->key < scan.max_key) {

            if (db_iterator->is_deleted()) {
                ++db_iterator;
                continue;
            }

            *keys_output = db_iterator->key;
            ++db_iterator;
            ++keys_output;
            ++j;
        }

        counts[i] = j;
    }
    offsets[tasks.size()] = keys_output - *c_found_keys;
}

void scan_txn( //
    transaction_t& txn,
    scans_arg_t tasks,
    ukv_options_t const c_options,
    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_counts,
    ukv_key_t** c_found_keys,
    stl_arena_t& arena,
    ukv_error_t* c_error) {

    database_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    bool const dont_watch = c_options & ukv_option_transaction_dont_watch_k;

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
        offsets[i] = keys_output - *c_found_keys;

        ukv_length_t j = 0;
        auto db_iterator = db.entries.lower_bound(collection_key_t {scan.collection, scan.min_key});
        auto txn_iterator = txn.changes.lower_bound(collection_key_t {scan.collection, scan.min_key});
        while (j != scan.limit && db_iterator != db.entries.end() && db_iterator->collection == scan.collection) {

            // Check if the key was already removed:
            auto join_iterator = txn.changes.find(db_iterator->collection_key());
            if (db_iterator->is_deleted() || (join_iterator != txn.changes.end() && join_iterator->is_deleted())) {
                ++db_iterator;
                continue;
            }

            // Compare against the incoming inserted keys:
            bool check_in_txn = txn_iterator != txn.changes.end() && txn_iterator->collection == scan.collection;
            if (check_in_txn && txn_iterator->key <= db_iterator->key) {
                *keys_output = txn_iterator->key;
                ++keys_output;
                ++txn_iterator;
                ++j;
                continue;
            }

            // Make sure we haven't reached the end keys
            if (db_iterator->key >= scan.max_key)
                break;

            // Export from the main store:
            *keys_output = db_iterator->key;
            ++keys_output;
            ++db_iterator;
            ++j;
        }

        // As in any `set_union`, don't forget the tail :)
        while (j != scan.limit &&                             //
               txn_iterator != txn.changes.end() &&           //
               txn_iterator->collection == scan.collection && //
               txn_iterator->key < scan.max_key) {
            *keys_output = txn_iterator->key;
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
        auto db_ptr = new database_t {};
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
    if (!c_tasks_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
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
    if (!c_tasks_count)
        return;

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
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

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_scan_limits, c_scan_limits_stride};
    scans_arg_t scans {collections, start_keys, end_keys, lens, c_tasks_count};

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

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};

    std::shared_lock _ {db.mutex};

    for (ukv_size_t i = 0; i != n; ++i) {
        ukv_collection_t const collection = collections[i];
        ukv_key_t const min_key = start_keys[i];
        ukv_key_t const max_key = end_keys[i];
        std::size_t deleted_count = 0;

        // Estimate the presence in the main store
        std::size_t main_count = 0;
        std::size_t main_bytes = 0;
        auto min_iterator = db.entries.lower_bound(collection_key_t {collection, min_key});
        auto max_iterator = db.entries.upper_bound(collection_key_t {collection, max_key});
        for (; min_iterator != max_iterator; ++min_iterator) {
            if (min_iterator->is_deleted()) {
                ++deleted_count;
                continue;
            }
            ++main_count;
            main_bytes += value_view_t(*min_iterator).size();
        }

        // Estimate the metrics from within a transaction
        std::size_t txn_count = 0;
        std::size_t txn_bytes = 0;
        if (c_txn) {
            auto min_iterator = txn.changes.lower_bound(collection_key_t {collection, min_key});
            auto max_iterator = txn.changes.upper_bound(collection_key_t {collection, max_key});
            for (; min_iterator != max_iterator; ++min_iterator, ++txn_count)
                txn_bytes += value_view_t(*min_iterator).size();
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
    ukv_str_view_t const c_collection_name,
    ukv_str_view_t const,
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
    std::unique_lock _ {db.mutex};

    std::string_view collection_name {c_collection_name, name_len};
    auto collection_it = db.names.find(collection_name);
    return_if_error(collection_it == db.names.end(), c_error, args_wrong_k, "Such collection already exists!");

    auto new_collection = db.new_collection();
    safe_section("Inserting new collection", c_error, [&] { db.names.emplace(collection_name, new_collection); });
    *c_collection = new_collection;
}

void ukv_collection_drop(
    // Inputs:
    ukv_database_t const c_db,
    ukv_collection_t const c_collection,
    ukv_drop_mode_t const c_mode,
    // Outputs:
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    bool invalidate = c_mode == ukv_drop_keys_vals_handle_k;
    return_if_error(c_collection != ukv_collection_main_k || !invalidate,
                    c_error,
                    args_combo_k,
                    "Default collection can't be invalidated.");

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    std::unique_lock _ {db.mutex};

    auto [begin, end] = db.entries.equal_range(c_collection);
    if (c_mode == ukv_drop_keys_vals_handle_k) {
        db.entries.erase(begin, end);
        for (auto it = db.names.begin(); it != db.names.end(); ++it) {
            if (c_collection != it->second)
                continue;
            db.names.erase(it);
            break;
        }
    }

    else if (c_mode == ukv_drop_keys_vals_k)
        db.entries.erase(begin, end);

    else if (c_mode == ukv_drop_vals_k) {
        generation_t gen = ++db.youngest_generation;
        for (; begin != end; ++begin)
            begin->assign_empty(gen);
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

    database_t& db = *reinterpret_cast<database_t*>(c_db);
    std::shared_lock _ {db.mutex};
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
    auto ids = arena.alloc_or_dummy<ukv_collection_t>(collections_count, c_error, c_ids);
    return_on_error(c_error);
    auto offs = arena.alloc_or_dummy<ukv_length_t>(collections_count + 1, c_error, c_offsets);
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
        if (!*c_txn)
            *c_txn = new transaction_t();
    });
    return_on_error(c_error);

    transaction_t& txn = *reinterpret_cast<transaction_t*>(*c_txn);
    txn.db_ptr = &db;
    txn.generation = ++db.youngest_generation;
    txn.watched.clear();
    txn.changes.clear();
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

    // This write may fail with out-of-memory errors, if Hash-Tables
    // bucket allocation fails, but no values will be copied, only moved.
    std::unique_lock _ {db.mutex};
    generation_t const youngest_generation = db.youngest_generation.load();

    // 1. Check for refreshes among fetched keys
    for (auto const& [collection_key, watched_generation] : txn.watched) {
        auto db_iterator = db.entries.find(collection_key);
        if (db_iterator == db.entries.end())
            continue;
        if (db_iterator->generation != watched_generation &&
            (*c_error = "Requested key was already overwritten since the start of the transaction!"))
            return;
    }

    // 2. Check for collisions among incoming values
    for (auto const& changed_entry : txn.changes) {
        auto db_iterator = db.entries.find(changed_entry.collection_key());
        if (db_iterator == db.entries.end())
            continue;

        if (db_iterator->generation == txn.generation && (*c_error = "Can't commit same entry more than once!"))
            return;

        if (entry_was_overwritten(db_iterator->generation, txn.generation, youngest_generation) &&
            (*c_error = "Incoming key collides with newer entry!"))
            return;
    }

    // 3. Allocate space for more nodes across different collections
    db.reserve_entry_nodes(txn.changes.size());
    return_on_error(c_error);

    // 4. Import the data, as no collisions were detected
    for (entry_t const& changed_entry : txn.changes) {
        auto db_iterator = db.entries.find(changed_entry.collection_key());
        // A key was deleted:
        // if (changed_entry.empty()) {
        //     if (db_iterator != db.entries.end())
        //         db.entries.erase(db_iterator);
        // }
        // A keys was updated:
        // else
        if (db_iterator != db.entries.end())
            db_iterator->swap_blob(changed_entry);

        // A key was inserted:
        else {
            entry_t new_entry {changed_entry.collection_key()};
            new_entry.generation = missing_data_generation_k;
            new_entry.swap_blob(changed_entry);
            db.entries.insert(std::move(new_entry));
        }
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
    transaction_t& txn = *reinterpret_cast<transaction_t*>(c_txn);
    delete &txn;
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;
    database_t& db = *reinterpret_cast<database_t*>(c_db);
    delete &db;
}

void ukv_collection_free(ukv_database_t const, ukv_collection_t const) {
    // In this in-memory freeing the collection handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
