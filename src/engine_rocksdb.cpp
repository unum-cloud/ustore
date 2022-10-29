/**
 * @file engine_rocksdb.cpp
 * @author Ashot Vardanian
 *
 * @brief Embedded Persistent Key-Value Store on top of @b RocksDB.
 * It natively supports ACID transactions and iterators (range queries)
 * and is implemented via @b Log-Structured-Merge-Tree. This makes RocksDB
 * great for write-intensive operations. It's already a common engine
 * choice for various Relational Database, built on top of it.
 * Examples: Yugabyte, TiDB, and, optionally: Mongo, MySQL, Cassandra, MariaDB.
 *
 * ## @b PlainTable vs `BlockBasedTable` Format
 * We use fixed-length integer keys, which are natively supported by `PlainTable`.
 * It, however, doesn't support @b non-prefix-based-`Seek()` in scans.
 * Moreover, not being the default variant, its significantly less optimized,
 * so after numerous tests we decided to stick to `BlockBasedTable`.
 * https://github.com/facebook/rocksdb/wiki/PlainTable-Format
 */

#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>

#include "ukv/db.h"
#include "ukv/cpp/ranges_args.hpp" // `places_arg_t`
#include "helpers/vector.hpp"      // `uninitialized_vector_gt`

using namespace unum::ukv;
using namespace unum;

using rocks_native_t = rocksdb::OptimisticTransactionDB;
using rocks_status_t = rocksdb::Status;
using rocks_value_t = rocksdb::PinnableSlice;
using rocks_txn_t = rocksdb::Transaction;
using rocks_collection_t = rocksdb::ColumnFamilyHandle;

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_collection_t const ukv_collection_main_k = 0;
ukv_length_t const ukv_length_missing_k = std::numeric_limits<ukv_length_t>::max();
ukv_key_t const ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();
bool const ukv_supports_transactions_k = true;
bool const ukv_supports_named_collections_k = true;
bool const ukv_supports_snapshots_k = true;

struct key_comparator_t final : public rocksdb::Comparator {
    inline int Compare(rocksdb::Slice const& a, rocksdb::Slice const& b) const override {
        auto ai = *reinterpret_cast<ukv_key_t const*>(a.data());
        auto bi = *reinterpret_cast<ukv_key_t const*>(b.data());
        if (ai == bi)
            return 0;
        return ai < bi ? -1 : 1;
    }
    const char* Name() const override { return "i64"; }
    void FindShortestSeparator(std::string*, rocksdb::Slice const&) const override {}
    void FindShortSuccessor(std::string*) const override {}
    bool CanKeysWithDifferentByteContentsBeEqual() const override { return false; }
    bool IsSameLengthImmediateSuccessor(rocksdb::Slice const& s, rocksdb::Slice const& t) const override {
        auto si = *reinterpret_cast<ukv_key_t const*>(s.data());
        auto ti = *reinterpret_cast<ukv_key_t const*>(t.data());
        return si + 1 == ti;
    }
};

static key_comparator_t key_comparator_k = {};

struct rocks_db_t {
    std::vector<rocks_collection_t*> columns;
    std::unique_ptr<rocks_native_t> native;
};

inline rocksdb::Slice to_slice(ukv_key_t const& key) noexcept {
    return {reinterpret_cast<char const*>(&key), sizeof(ukv_key_t)};
}

inline rocksdb::Slice to_slice(value_view_t value) noexcept {
    return {reinterpret_cast<const char*>(value.begin()), value.size()};
}

inline std::unique_ptr<rocks_value_t> make_value(ukv_error_t* c_error) noexcept {
    std::unique_ptr<rocks_value_t> value_uptr;
    safe_section("Allocating RocksDB-compatible value buffer", c_error, [&] {
        value_uptr = std::make_unique<rocks_value_t>();
    });
    return value_uptr;
}

bool export_error(rocks_status_t const& status, ukv_error_t* c_error) {
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

rocks_collection_t* rocks_collection(rocks_db_t& db, ukv_collection_t collection) {
    return collection == ukv_collection_main_k ? db.native->DefaultColumnFamily()
                                               : reinterpret_cast<rocks_collection_t*>(collection);
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_database_init(ukv_database_init_t* c_ptr) {

    ukv_database_init_t& c = *c_ptr;
    if (!c.config || !std::strlen(c.config)) {
        *c.error = "RocksDB requires a configuration file or a path!";
        return;
    }

    safe_section("Opening RocksDB", c.error, [&] {
        rocks_db_t* db_ptr = new rocks_db_t;
        std::vector<rocksdb::ColumnFamilyDescriptor> column_descriptors;
        rocksdb::Options options;
        rocksdb::ConfigOptions config_options;

        std::string path = c.config; // TODO: take the path from config!
        rocks_status_t status = rocksdb::LoadLatestOptions(config_options, path, &options, &column_descriptors);
        if (column_descriptors.empty())
            column_descriptors.push_back({rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()});

        return_if_error(status.ok() || status.IsNotFound(), c.error, error_unknown_k, "Recovering RocksDB state");
        if (status.IsNotFound())
            options.compression = rocksdb::kNoCompression;
        options.create_if_missing = true;
        options.comparator = &key_comparator_k;

        rocks_native_t* native_db = nullptr;
        rocksdb::OptimisticTransactionDBOptions txn_options;
        status = rocks_native_t::Open(options, txn_options, path, column_descriptors, &db_ptr->columns, &native_db);
        return_if_error(status.ok(), c.error, error_unknown_k, "Opening RocksDB with options");

        db_ptr->native = std::unique_ptr<rocks_native_t>(native_db);
        *c.db = db_ptr;
    });
}

void write_one( //
    rocks_db_t& db,
    rocks_txn_t& txn,
    places_arg_t const& places,
    contents_arg_t const& contents,
    ukv_options_t const c_options,
    ukv_error_t* c_error) noexcept(false) {

    bool const safe = c_options & ukv_option_write_flush_k;
    bool const watch = !(c_options & ukv_option_transaction_dont_watch_k);

    rocksdb::WriteOptions options;
    options.sync = safe;
    options.disableWAL = !safe;

    auto place = places[0];
    auto content = contents[0];
    auto collection = rocks_collection(db, place.collection);
    auto key = to_slice(place.key);
    rocks_status_t status;

    if (&txn)
        status =        //
            !content    //
                ? watch //
                      ? txn.SingleDelete(collection, key)
                      : txn.DeleteUntracked(collection, key)
                : watch //
                      ? txn.Put(collection, key, to_slice(content))
                      : txn.PutUntracked(collection, key, to_slice(content));
    else
        status =     //
            !content //
                ? db.native->SingleDelete(options, collection, key)
                : db.native->Put(options, collection, key, to_slice(content));

    export_error(status, c_error);
}

void write_many( //
    rocks_db_t& db,
    rocks_txn_t& txn,
    places_arg_t const& places,
    contents_arg_t const& contents,
    ukv_options_t const c_options,
    ukv_error_t* c_error) noexcept(false) {

    bool const safe = c_options & ukv_option_write_flush_k;
    bool const watch = !(c_options & ukv_option_transaction_dont_watch_k);

    rocksdb::WriteOptions options;
    options.sync = safe;
    options.disableWAL = !safe;

    if (&txn) {
        for (std::size_t i = 0; i != places.size(); ++i) {
            auto place = places[i];
            auto content = contents[i];
            auto collection = rocks_collection(db, place.collection);
            auto key = to_slice(place.key);
            auto status =   //
                !content    //
                    ? watch //
                          ? txn.SingleDelete(collection, key)
                          : txn.DeleteUntracked(collection, key)
                    : watch //
                          ? txn.Put(collection, key, to_slice(content))
                          : txn.PutUntracked(collection, key, to_slice(content));
            export_error(status, c_error);
            return_on_error(c_error);
        }
    }
    else {
        rocksdb::WriteBatch batch;
        for (std::size_t i = 0; i != places.size(); ++i) {
            auto place = places[i];
            auto content = contents[i];
            auto collection = rocks_collection(db, place.collection);
            auto key = to_slice(place.key);
            auto status = !content //
                              ? batch.Delete(collection, key)
                              : batch.Put(collection, key, to_slice(content));
            export_error(status, c_error);
        }

        rocks_status_t status = db.native->Write(options, &batch);
        export_error(status, c_error);
    }
}

void ukv_write(ukv_write_t* c_ptr) {

    ukv_write_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    rocks_txn_t& txn = *reinterpret_cast<rocks_txn_t*>(c.transaction);
    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c.values, c.values_stride};
    strided_iterator_gt<ukv_length_t const> offs {c.offsets, c.offsets_stride};
    strided_iterator_gt<ukv_length_t const> lens {c.lengths, c.lengths_stride};
    bits_view_t presences {c.presences};

    places_arg_t places {collections, keys, {}, c.tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c.tasks_count};

    safe_section("Writing into RocksDB", c.error, [&] {
        auto func = c.tasks_count == 1 ? &write_one : &write_many;
        func(db, txn, places, contents, c.options, c.error);
    });
}

template <typename value_enumerator_at>
void read_one( //
    rocks_db_t& db,
    rocks_txn_t& txn,
    places_arg_t places,
    ukv_options_t const c_options,
    value_enumerator_at enumerator,
    ukv_error_t* c_error) noexcept(false) {

    rocksdb::ReadOptions options;
    if (&txn)
        options.snapshot = txn.GetSnapshot();

    bool watch = !(c_options & ukv_option_transaction_dont_watch_k);

    place_t place = places[0];
    auto col = rocks_collection(db, place.collection);
    auto key = to_slice(place.key);
    auto value_uptr = make_value(c_error);
    return_on_error(c_error);

    rocks_value_t& value = *value_uptr.get();
    rocks_status_t status = //
        &txn                //
            ? watch         //
                  ? txn.GetForUpdate(options, col, key, &value)
                  : txn.Get(options, col, key, &value)
            : db.native->Get(options, col, key, &value);
    if (!status.IsNotFound()) {
        if (export_error(status, c_error))
            return;
        auto begin = reinterpret_cast<ukv_bytes_cptr_t>(value.data());
        auto length = static_cast<ukv_length_t>(value.size());
        enumerator(0, value_view_t {begin, length});
    }
    else
        enumerator(0, value_view_t {});
}

template <typename value_enumerator_at>
void read_many( //
    rocks_db_t& db,
    rocks_txn_t& txn,
    places_arg_t places,
    ukv_options_t const c_options,
    value_enumerator_at enumerator,
    ukv_error_t* c_error) noexcept(false) {

    rocksdb::ReadOptions options;
    if (&txn)
        options.snapshot = txn.GetSnapshot();

    bool watch = !(c_options & ukv_option_transaction_dont_watch_k);
    std::vector<rocks_collection_t*> cols(places.count);
    std::vector<rocksdb::Slice> keys(places.count);
    std::vector<std::string> vals(places.count);
    for (std::size_t i = 0; i != places.size(); ++i) {
        place_t place = places[i];
        cols[i] = rocks_collection(db, place.collection);
        keys[i] = to_slice(place.key);
    }

    std::vector<rocks_status_t> statuses = //
        &txn                               //
            ? watch                        //
                  ? txn.MultiGetForUpdate(options, cols, keys, &vals)
                  : txn.MultiGet(options, cols, keys, &vals)
            : db.native->MultiGet(options, cols, keys, &vals);
    for (std::size_t i = 0; i != places.size(); ++i) {
        if (!statuses[i].IsNotFound()) {
            if (export_error(statuses[i], c_error))
                return;
            auto begin = reinterpret_cast<ukv_bytes_cptr_t>(vals[i].data());
            auto length = static_cast<ukv_length_t>(vals[i].size());
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

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    rocks_txn_t& txn = *reinterpret_cast<rocks_txn_t*>(c.transaction);

    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    places_arg_t places {collections, keys, {}, c.tasks_count};

    // 1. Allocate a tape for all the values to be pulled
    auto offs = arena.alloc_or_dummy(places.count + 1, c.error, c.offsets);
    return_on_error(c.error);
    auto lens = arena.alloc_or_dummy(places.count, c.error, c.lengths);
    return_on_error(c.error);
    auto presences = arena.alloc_or_dummy(places.count, c.error, c.presences);
    return_on_error(c.error);
    uninitialized_vector_gt<byte_t> contents(arena);

    // 2. Pull metadata & data in one run, as reading from disk is expensive
    bool const needs_export = c.values != nullptr;
    auto data_enumerator = [&](std::size_t i, value_view_t value) {
        presences[i] = bool(value);
        lens[i] = value ? value.size() : ukv_length_missing_k;
        if (needs_export) {
            offs[i] = contents.size();
            contents.insert(contents.size(), value.begin(), value.end(), c.error);
        }
    };

    safe_section("Reading from RocksDB", c.error, [&] {
        c.tasks_count == 1 //
            ? read_one(db, txn, places, c.options, data_enumerator, c.error)
            : read_many(db, txn, places, c.options, data_enumerator, c.error);
        offs[places.count] = contents.size();

        if (needs_export)
            *c.values = reinterpret_cast<ukv_bytes_ptr_t>(contents.begin());
    });
}

void ukv_scan(ukv_scan_t* c_ptr) {

    ukv_scan_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    rocks_txn_t& txn = *reinterpret_cast<rocks_txn_t*>(c.transaction);
    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c.start_keys, c.start_keys_stride};
    strided_iterator_gt<ukv_length_t const> limits {c.count_limits, c.count_limits_stride};
    scans_arg_t tasks {collections, start_keys, limits, c.tasks_count};

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy(tasks.count + 1, c.error, c.offsets);
    return_on_error(c.error);
    auto counts = arena.alloc_or_dummy(tasks.count, c.error, c.counts);
    return_on_error(c.error);

    auto total_keys = reduce_n(tasks.limits, tasks.count, 0ul);
    auto keys_output = *c.keys = arena.alloc<ukv_key_t>(total_keys, c.error).begin();
    return_on_error(c.error);

    // 2. Fetch the data
    rocksdb::ReadOptions options;
    options.fill_cache = false;

    if (&txn)
        options.snapshot = txn.GetSnapshot();

    for (ukv_size_t i = 0; i != c.tasks_count; ++i) {
        scan_t task = tasks[i];
        auto collection = rocks_collection(db, task.collection);

        std::unique_ptr<rocksdb::Iterator> it;
        safe_section("Creating a RocksDB iterator", c.error, [&] {
            it = &txn //
                     ? std::unique_ptr<rocksdb::Iterator>(txn.GetIterator(options, collection))
                     : std::unique_ptr<rocksdb::Iterator>(db.native->NewIterator(options, collection));
        });
        return_on_error(c.error);

        ukv_size_t j = 0;
        it->Seek(to_slice(task.min_key));
        offsets[i] = keys_output - *c.keys;

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

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c.start_keys, c.start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c.end_keys, c.end_keys_stride};
    rocksdb::SizeApproximationOptions options;

    rocksdb::Range range;
    uint64_t approximate_size = 0;
    uint64_t keys_size = 0;
    uint64_t sst_files_size = 0;
    rocks_status_t status;

    for (ukv_size_t i = 0; i != c.tasks_count; ++i) {
        auto collection = rocks_collection(db, collections[i]);
        ukv_key_t const min_key = start_keys[i];
        ukv_key_t const max_key = end_keys[i];
        range = rocksdb::Range(to_slice(min_key), to_slice(max_key));
        safe_section("Retrieving properties from RocksDB", c.error, [&] {
            status = db.native->GetApproximateSizes(options, collection, &range, 1, &approximate_size);
            if (export_error(status, c.error))
                return;
            db.native->GetIntProperty(collection, "rocksdb.estimate-num-keys", &keys_size);
            db.native->GetIntProperty(collection, "rocksdb.total-sst-files-size", &sst_files_size);
        });
        return_on_error(c.error);

        ukv_size_t estimate[6];
        min_cardinalities[i] = estimate[0] = static_cast<ukv_size_t>(0);
        max_cardinalities[i] = estimate[1] = static_cast<ukv_size_t>(keys_size);
        min_value_bytes[i] = estimate[2] = static_cast<ukv_size_t>(0);
        max_value_bytes[i] = estimate[3] = static_cast<ukv_size_t>(0);
        min_space_usages[i] = estimate[4] = approximate_size;
        max_space_usages[i] = estimate[5] = sst_files_size;
    }
}

void ukv_collection_create(ukv_collection_create_t* c_ptr) {

    ukv_collection_create_t& c = *c_ptr;

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    if (!c.name || (c.name && !std::strlen(c.name))) {
        *c.id = reinterpret_cast<ukv_collection_t>(db.native->DefaultColumnFamily());
        return;
    }

    for (auto handle : db.columns) {
        if (handle && handle->GetName() == c.name) {
            *c.id = reinterpret_cast<ukv_collection_t>(handle);
            return;
        }
    }

    rocks_collection_t* collection = nullptr;
    rocks_status_t status = db.native->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), c.name, &collection);
    if (!export_error(status, c.error)) {
        db.columns.push_back(collection);
        *c.id = reinterpret_cast<ukv_collection_t>(collection);
    }
}

void ukv_collection_drop(ukv_collection_drop_t* c_ptr) {

    ukv_collection_drop_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    bool invalidate = c.mode == ukv_drop_keys_vals_handle_k;
    return_if_error(c.id != ukv_collection_main_k || !invalidate,
                    c.error,
                    args_combo_k,
                    "Default collection can't be invalidated.");

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    rocks_collection_t* collection_ptr = reinterpret_cast<rocks_collection_t*>(c.id);
    rocks_collection_t* collection_ptr_to_clear = nullptr;

    if (c.id == ukv_collection_main_k)
        collection_ptr_to_clear = db.native->DefaultColumnFamily();
    else {
        for (auto it = db.columns.begin(); it != db.columns.end(); it++) {
            collection_ptr_to_clear = reinterpret_cast<rocks_collection_t*>(*it);
            if (collection_ptr_to_clear == collection_ptr)
                break;
        }
    }

    rocksdb::WriteOptions options;
    options.sync = true;

    if (c.mode == ukv_drop_keys_vals_handle_k) {
        for (auto it = db.columns.begin(); it != db.columns.end(); it++) {
            if (collection_ptr_to_clear == *it) {
                rocks_status_t status = db.native->DropColumnFamily(collection_ptr_to_clear);
                if (export_error(status, c.error))
                    return;
                db.columns.erase(it);
                break;
            }
        }
        return;
    }
    else if (c.mode == ukv_drop_keys_vals_k) {
        rocksdb::WriteBatch batch;
        auto it =
            std::unique_ptr<rocksdb::Iterator>(db.native->NewIterator(rocksdb::ReadOptions(), collection_ptr_to_clear));
        for (it->SeekToFirst(); it->Valid(); it->Next())
            batch.Delete(collection_ptr_to_clear, it->key());
        rocks_status_t status = db.native->Write(options, &batch);
        export_error(status, c.error);
        return;
    }

    else if (c.mode == ukv_drop_vals_k) {
        rocksdb::WriteBatch batch;
        auto it =
            std::unique_ptr<rocksdb::Iterator>(db.native->NewIterator(rocksdb::ReadOptions(), collection_ptr_to_clear));
        for (it->SeekToFirst(); it->Valid(); it->Next())
            batch.Put(collection_ptr_to_clear, it->key(), rocksdb::Slice());
        rocks_status_t status = db.native->Write(options, &batch);
        export_error(status, c.error);
        return;
    }
}

void ukv_collection_list(ukv_collection_list_t* c_ptr) {

    ukv_collection_list_t& c = *c_ptr;
    return_if_error(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c.count && c.names, c.error, args_combo_k, "Need names and outputs!");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    std::size_t collections_count = db.columns.size() - 1;
    *c.count = static_cast<ukv_size_t>(collections_count);

    // Every string will be null-terminated
    std::size_t strings_length = 0;
    for (auto const& column : db.columns)
        strings_length += column->GetName().size() + 1;

    auto names = arena.alloc<char>(strings_length, c.error).begin();
    *c.names = names;
    return_on_error(c.error);

    // For every collection we also need to export IDs and offsets
    auto ids = arena.alloc_or_dummy(collections_count, c.error, c.ids);
    return_on_error(c.error);
    auto offs = arena.alloc_or_dummy(collections_count + 1, c.error, c.offsets);
    return_on_error(c.error);

    std::size_t i = 0;
    for (auto const& column : db.columns) {
        if (column->GetName() == rocksdb::kDefaultColumnFamilyName)
            continue;

        auto len = column->GetName().size();
        std::memcpy(names, column->GetName().data(), len);
        names[len] = '\0';
        ids[i] = reinterpret_cast<ukv_collection_t>(column);
        offs[i] = static_cast<ukv_length_t>(names - *c.names);
        names += len + 1;
        ++i;
    }
    offs[i] = static_cast<ukv_length_t>(names - *c.names);
}

void ukv_database_control(ukv_database_control_t* c_ptr) {

    ukv_database_control_t& c = *c_ptr;
    *c.response = NULL;
    *c.error = "Controls aren't supported in this implementation!";
}

void ukv_transaction_init(ukv_transaction_init_t* c_ptr) {

    ukv_transaction_init_t& c = *c_ptr;
    bool const safe = c.options & ukv_option_write_flush_k;
    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c.db);
    rocks_txn_t& txn = **reinterpret_cast<rocks_txn_t**>(c.transaction);
    rocksdb::OptimisticTransactionOptions txn_options;
    txn_options.set_snapshot = !(c.options & ukv_option_transaction_dont_watch_k);
    rocksdb::WriteOptions options;
    options.sync = safe;
    options.disableWAL = !safe;
    auto new_txn = db.native->BeginTransaction(options, txn_options, &txn);
    if (!new_txn)
        *c.error = "Couldn't start a transaction!";
    else
        *c.transaction = new_txn;
}

void ukv_transaction_commit(ukv_transaction_commit_t* c_ptr) {

    ukv_transaction_commit_t& c = *c_ptr;
    if (!c.transaction)
        return;
    rocks_txn_t& txn = *reinterpret_cast<rocks_txn_t*>(c.transaction);
    rocks_status_t status = txn.Commit();
    export_error(status, c.error);
}

void ukv_arena_free(ukv_arena_t c_arena) {
    clear_linked_memory(c_arena);
}

void ukv_transaction_free(ukv_transaction_t c_transaction) {
    if (!c_transaction)
        return;
    delete reinterpret_cast<rocks_txn_t*>(c_transaction);
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;
    rocks_db_t& db = *reinterpret_cast<rocks_db_t*>(c_db);
    for (rocks_collection_t* cf : db.columns)
        db.native->DestroyColumnFamilyHandle(cf);
    db.native.reset();
    delete &db;
}

void ukv_error_free(ukv_error_t const) {
}