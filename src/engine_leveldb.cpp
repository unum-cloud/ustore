/**
 * @file engine_leveldb.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded Persistent Key-Value Store on top of @b LevelDB.
 * Has no support for collections, transactions or any non-CRUD jobs.
 */

#include <leveldb/db.h>
#include <leveldb/comparator.h>
#include <leveldb/write_batch.h>

#include "ukv/db.h"
#include "ukv/cpp/ranges_args.hpp" // `places_arg_t`
#include "helpers/vector.hpp"      // `uninitialized_vector_gt`

using namespace unum::ukv;
using namespace unum;

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_collection_t const ukv_collection_main_k = 0;
ukv_length_t const ukv_length_missing_k = std::numeric_limits<ukv_length_t>::max();
ukv_key_t const ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();
bool const ukv_supports_transactions_k = false;
bool const ukv_supports_named_collections_k = false;
bool const ukv_supports_snapshots_k = false;

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

struct level_txn_t {
    leveldb::DB* db = nullptr;
    leveldb::Snapshot const* snapshot = nullptr;
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

void ukv_database_init(ukv_database_init_t* c_ptr) {

    ukv_database_init_t& c = *c_ptr;
    if (!c.config || !std::strlen(c.config)) {
        *c.error = "LevelDB requires a configuration file or a path!";
        return;
    }

    try {
        level_db_t* db_ptr = nullptr;
        level_options_t options;
        options.compression = leveldb::kNoCompression;
        options.create_if_missing = true;
        options.comparator = &key_comparator_k;
        level_status_t status = level_db_t::Open(options, c.config, &db_ptr);
        if (!status.ok()) {
            *c.error = "Couldn't open LevelDB";
            return;
        }
        *c.db = db_ptr;
    }
    catch (...) {
        *c.error = "Open Failure";
    }
}

void write_one( //
    level_db_t& db,
    places_arg_t const& places,
    contents_arg_t const& contents,
    leveldb::WriteOptions const& options,
    ukv_error_t* c_error) {

    auto place = places[0];
    auto content = contents[0];
    auto key = to_slice(place.key);
    level_status_t status = !content ? db.Delete(options, key) : db.Put(options, key, to_slice(content));
    export_error(status, c_error);
}

void write_many( //
    level_db_t& db,
    places_arg_t const& places,
    contents_arg_t const& contents,
    leveldb::WriteOptions const& options,
    ukv_error_t* c_error) {

    leveldb::WriteBatch batch;
    for (std::size_t i = 0; i != places.size(); ++i) {
        auto place = places[i];
        auto content = contents[i];

        auto key = to_slice(place.key);
        if (!content)
            batch.Delete(key);
        else
            batch.Put(key, to_slice(content));
    }

    level_status_t status = db.Write(options, &batch);
    export_error(status, c_error);
}

void ukv_write(ukv_write_t* c_ptr) {

    ukv_write_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    level_db_t& db = *reinterpret_cast<level_db_t*>(c.db);
    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c.values, c.values_stride};
    strided_iterator_gt<ukv_length_t const> offs {c.offsets, c.offsets_stride};
    strided_iterator_gt<ukv_length_t const> lens {c.lengths, c.lengths_stride};
    bits_view_t presences {c.presences};

    places_arg_t places {collections, keys, {}, c.tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c.tasks_count};

    leveldb::WriteOptions options;
    if (c.options & ukv_option_write_flush_k)
        options.sync = true;

    try {
        auto func = c.tasks_count == 1 ? &write_one : &write_many;
        func(db, places, contents, options, c.error);
    }
    catch (...) {
        *c.error = "Write Failure";
    }
}

template <typename value_enumerator_at>
void read_enumerate( //
    level_db_t& db,
    places_arg_t tasks,
    leveldb::ReadOptions const& options,
    std::string& value,
    value_enumerator_at enumerator,
    ukv_error_t* c_error) {

    for (std::size_t i = 0; i != tasks.size(); ++i) {
        place_t place = tasks[i];
        level_status_t status = db.Get(options, to_slice(place.key), &value);
        if (!status.IsNotFound()) {
            if (export_error(status, c_error))
                return;
            auto begin = reinterpret_cast<ukv_bytes_cptr_t>(value.data());
            auto length = static_cast<ukv_length_t>(value.size());
            enumerator(i, value_view_t {begin, length});
        }
        else
            enumerator(i, value_view_t {});
    }
}

void ukv_read(ukv_read_t* c_ptr) {

    ukv_read_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    level_db_t& db = *reinterpret_cast<level_db_t*>(c.db);
    level_txn_t& txn = *reinterpret_cast<level_txn_t*>(c.transaction);
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    places_arg_t places {{}, keys, {}, c.tasks_count};

    // 1. Allocate a tape for all the values to be pulled
    auto offs = arena.alloc_or_dummy(places.count + 1, c.error, c.offsets);
    return_on_error(c.error);
    auto lens = arena.alloc_or_dummy(places.count, c.error, c.lengths);
    return_on_error(c.error);
    auto presences = arena.alloc_or_dummy(places.count, c.error, c.presences);
    return_on_error(c.error);
    bool const needs_export = c.values != nullptr;

    uninitialized_vector_gt<byte_t> contents(arena);

    // 2. Pull metadata & data in one run, as reading from disk is expensive
    try {
        leveldb::ReadOptions options;
        if (c.transaction)
            options.snapshot = txn.snapshot;

        std::string value_buffer;
        ukv_length_t progress_in_tape = 0;
        auto data_enumerator = [&](std::size_t i, value_view_t value) {
            presences[i] = bool(value);
            lens[i] = value ? value.size() : ukv_length_missing_k;
            offs[i] = contents.size();
            if (needs_export)
                contents.insert(contents.size(), value.begin(), value.end(), c.error);
        };
        read_enumerate(db, places, options, value_buffer, data_enumerator, c.error);
        offs[places.count] = contents.size();
        if (needs_export)
            *c.values = reinterpret_cast<ukv_bytes_ptr_t>(contents.begin());
    }
    catch (...) {
        *c.error = "Read Failure";
    }
}

void ukv_scan(ukv_scan_t* c_ptr) {

    ukv_scan_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    level_db_t& db = *reinterpret_cast<level_db_t*>(c.db);
    level_txn_t& txn = *reinterpret_cast<level_txn_t*>(c.transaction);
    strided_iterator_gt<ukv_key_t const> start_keys {c.start_keys, c.start_keys_stride};
    strided_iterator_gt<ukv_length_t const> limits {c.count_limits, c.count_limits_stride};
    scans_arg_t tasks {{}, start_keys, limits, c.tasks_count};

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy(tasks.count + 1, c.error, c.offsets);
    return_on_error(c.error);
    auto counts = arena.alloc_or_dummy(tasks.count, c.error, c.counts);
    return_on_error(c.error);

    auto total_keys = reduce_n(tasks.limits, tasks.count, 0ul);
    auto keys_output = *c.keys = arena.alloc<ukv_key_t>(total_keys, c.error).begin();
    return_on_error(c.error);

    // 2. Fetch the data
    leveldb::ReadOptions options;
    options.fill_cache = false;

    if (c.transaction)
        options.snapshot = txn.snapshot;

    level_iter_uptr_t it;
    try {
        it = level_iter_uptr_t(db.NewIterator(options));
    }
    catch (...) {
        *c.error = "Fail To Create Iterator";
        return;
    }
    for (ukv_size_t i = 0; i != c.tasks_count; ++i) {
        scan_t task = tasks[i];
        it->Seek(to_slice(task.min_key));
        offsets[i] = keys_output - *c.keys;

        ukv_size_t j = 0;
        while (it->Valid() && j != task.limit) {
            std::memcpy(keys_output, it->key().data(), sizeof(ukv_key_t));
            ++keys_output;
            ++j;
            it->Next();
        }

        counts[i] = j;
    }

    offsets[tasks.size()] = keys_output - *c.keys;
}

void ukv_measure(ukv_measure_t* c_ptr) {

    ukv_measure_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    auto min_cardinalities = arena.alloc_or_dummy(c.tasks_count, c.error, c.min_cardinalities);
    auto max_cardinalities = arena.alloc_or_dummy(c.tasks_count, c.error, c.max_cardinalities);
    auto min_value_bytes = arena.alloc_or_dummy(c.tasks_count, c.error, c.min_value_bytes);
    auto max_value_bytes = arena.alloc_or_dummy(c.tasks_count, c.error, c.max_value_bytes);
    auto min_space_usages = arena.alloc_or_dummy(c.tasks_count, c.error, c.min_space_usages);
    auto max_space_usages = arena.alloc_or_dummy(c.tasks_count, c.error, c.max_space_usages);
    return_on_error(c.error);

    level_db_t& db = *reinterpret_cast<level_db_t*>(c.db);
    strided_iterator_gt<ukv_key_t const> start_keys {c.start_keys, c.start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c.end_keys, c.end_keys_stride};
    uint64_t approximate_size = 0;
    std::optional<std::string> memory_usage;
    level_status_t status;

    for (ukv_size_t i = 0; i != c.tasks_count; ++i) {
        min_cardinalities[i] = static_cast<ukv_size_t>(0);
        max_cardinalities[i] = static_cast<ukv_size_t>(0);
        min_value_bytes[i] = static_cast<ukv_size_t>(0);
        max_value_bytes[i] = static_cast<ukv_size_t>(0);

        ukv_key_t const min_key = start_keys[i];
        ukv_key_t const max_key = end_keys[i];
        leveldb::Range range(to_slice(min_key), to_slice(max_key));
        try {
            db.GetApproximateSizes(&range, 1, &approximate_size);
            min_space_usages[i] = approximate_size;

            memory_usage = "0";
            db.GetProperty("leveldb.approximate-memory-usage", &memory_usage.value());
            max_space_usages[i] = std::stoi(memory_usage.value());
        }
        catch (...) {
            *c.error = "Property Read Failure";
        }
    }
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ukv_collection_create(ukv_collection_create_t* c_ptr) {

    ukv_collection_create_t& c = *c_ptr;
    if (c.name && std::strlen(c.name))
        *c.error = "Collections not supported by LevelDB!";
}

void ukv_collection_drop(ukv_collection_drop_t* c_ptr) {

    ukv_collection_drop_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    bool invalidate = c.mode == ukv_drop_keys_vals_handle_k;
    return_if_error(c.id == ukv_collection_main_k && !invalidate,
                    c.error,
                    args_combo_k,
                    "Collections not supported by LevelDB!");

    level_db_t& db = *reinterpret_cast<level_db_t*>(c.db);

    leveldb::WriteBatch batch;
    auto it = std::unique_ptr<leveldb::Iterator>(db.NewIterator(leveldb::ReadOptions()));

    if (c.mode == ukv_drop_keys_vals_k) {
        for (it->SeekToFirst(); it->Valid(); it->Next())
            batch.Delete(it->key());
    }

    else if (c.mode == ukv_drop_vals_k) {
        for (it->SeekToFirst(); it->Valid(); it->Next())
            batch.Put(it->key(), leveldb::Slice());
    }

    leveldb::WriteOptions options;
    options.sync = true;
    level_status_t status = db.Write(options, &batch);
    export_error(status, c.error);
}

void ukv_collection_list(ukv_collection_list_t* c_ptr) {

    ukv_collection_list_t& c = *c_ptr;
    *c.count = 0;
    if (c.ids)
        *c.ids = nullptr;
    if (c.offsets)
        *c.offsets = nullptr;
    if (c.names)
        *c.names = nullptr;
}

void ukv_database_control(ukv_database_control_t* c_ptr) {

    ukv_database_control_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    if (!c.request && (*c.error = "Request is NULL!"))
        return;

    *c.response = NULL;
    *c.error = "Controls aren't supported in this implementation!";
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_transaction_init(ukv_transaction_init_t* c_ptr) {

    ukv_transaction_init_t& c = *c_ptr;
    if (!c.transaction)
        safe_section("Allocating transaction handle", c.error, [&] { *c.transaction = new level_txn_t(); });
    return_on_error(c.error);

    level_db_t& db = *reinterpret_cast<level_db_t*>(c.db);
    level_txn_t& txn = *reinterpret_cast<level_txn_t*>(c.transaction);

    auto wants_just_snapshot = !(c.options & ukv_option_transaction_dont_watch_k);
    if (wants_just_snapshot) {
        txn.snapshot = db.GetSnapshot();
        txn.db = &db;
        if (!txn.snapshot)
            *c.error = "Couldn't start a transaction!";
    }
    else
        *c.error = "Transactions not supported by LevelDB!";
}

void ukv_transaction_commit(ukv_transaction_commit_t* c_ptr) {

    ukv_transaction_commit_t& c = *c_ptr;
    *c.error = "Transactions not supported by LevelDB!";
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_arena_t c_arena) {
    clear_linked_memory(c_arena);
}

void ukv_transaction_free(ukv_transaction_t c_txn) {
    if (!c_txn)
        return;
    level_txn_t& txn = *reinterpret_cast<level_txn_t*>(c_txn);
    level_db_t& db = *txn.db;
    if (txn.snapshot)
        db.ReleaseSnapshot(txn.snapshot);
    txn.db = nullptr;
    txn.snapshot = nullptr;
    delete &txn;
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;
    level_db_t* db = reinterpret_cast<level_db_t*>(c_db);
    delete db;
}

void ukv_error_free(ukv_error_t const) {
}