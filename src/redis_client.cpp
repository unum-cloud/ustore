#include <charconv>

#include <sw/redis++/redis++.h>

#include "ustore/db.h"
#include "ustore/cpp/types.hpp"
#include "helpers/linked_memory.hpp"
#include "helpers/linked_array.hpp"
#include "ustore/cpp/ranges_args.hpp" // `places_arg_t`

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ustore_collection_t const ustore_collection_main_k = 0;
ustore_length_t const ustore_length_missing_k = std::numeric_limits<ustore_length_t>::max();
ustore_key_t const ustore_key_unknown_k = std::numeric_limits<ustore_key_t>::max();
bool const ustore_supports_transactions_k = true;
bool const ustore_supports_named_collections_k = true;
bool const ustore_supports_snapshots_k = false;
static const char kDefaultCollectionName[] = "default";

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ustore;
using namespace unum;
namespace redis = sw::redis;

inline redis::StringView to_string_view(byte_t const* p, size_t size_bytes) noexcept {
    return {reinterpret_cast<const char*>(p), size_bytes};
}

inline redis::StringView to_string_view(ustore_key_t const& k) noexcept {
    return {reinterpret_cast<const char*>(&k), sizeof(ustore_key_t)};
}

inline redis::StringView redis_collection(ustore_collection_t collection) {
    return collection == ustore_collection_main_k ? kDefaultCollectionName : reinterpret_cast<const char*>(collection);
}

struct redis_txn_t {
    std::unique_ptr<redis::Transaction> native;
    std::unordered_map<collection_key_t, redis::OptionalString> uncommited;

    redis::OptionalString get_uncommited(ustore_collection_t collection, ustore_key_t key) {
        native->hget(redis_collection(collection), to_string_view(key));
        auto it = uncommited.find({collection, key});
        if (it != uncommited.end())
            return it->second;
        return {};
    }

    void set(ustore_collection_t collection, ustore_key_t key, value_view_t value) {
        native->hset(redis_collection(collection), to_string_view(key), to_string_view(value.data(), value.size()));
        uncommited[{collection, key}] = std::string((const char*)value.data(), value.size());
    }

    void del(ustore_collection_t collection, ustore_key_t key) {
        native->hdel(redis_collection(collection), to_string_view(key));
        uncommited.erase({collection, key});
    }

    void exec() {
        native->exec();
        uncommited.clear();
    }
};

struct redis_client_t {
    std::unique_ptr<redis::Redis> native;
    std::vector<std::string> collections;
};

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ustore_database_init(ustore_database_init_t* c_ptr) {
    ustore_database_init_t& c = *c_ptr;

    safe_section("Starting client", c.error, [&] {
        redis::ConnectionOptions connection_options;
        connection_options.host = "127.0.0.1";
        connection_options.port = 6379;

        redis_client_t* db_ptr = new redis_client_t;
        db_ptr->native = std::make_unique<redis::Redis>(connection_options);

        db_ptr->native->keys("*", std::back_inserter(db_ptr->collections));
        auto it = std::find(db_ptr->collections.begin(), db_ptr->collections.end(), kDefaultCollectionName);
        if (it != db_ptr->collections.end())
            db_ptr->collections.erase(it);
        *c.db = db_ptr;
    });
}

void ustore_read(ustore_read_t* c_ptr) {

    ustore_read_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    redis_txn_t& txn = *reinterpret_cast<redis_txn_t*>(c.transaction);

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_if_error_m(c.error);

    strided_iterator_gt<ustore_key_t const> keys {c.keys, c.keys_stride};
    strided_iterator_gt<ustore_collection_t const> collections {c.collections, c.collections_stride};
    places_arg_t places {collections, keys, {}, c.tasks_count};
    validate_read(c.transaction, places, c.options, c.error);
    return_if_error_m(c.error);

    // 1. Allocate a tape for all the values to be pulled
    auto offs = arena.alloc_or_dummy(places.count + 1, c.error, c.offsets);
    return_if_error_m(c.error);
    auto lens = arena.alloc_or_dummy(places.count, c.error, c.lengths);
    return_if_error_m(c.error);
    auto presences = arena.alloc_or_dummy(places.count, c.error, c.presences);
    return_if_error_m(c.error);
    bool const needs_export = c.values != nullptr;

    safe_section("Reading values", c.error, [&] {
        uninitialized_array_gt<byte_t> contents(arena);
        for (std::size_t i = 0; i != places.size(); ++i) {
            place_t place = places[i];
            redis::OptionalString value;

            if (c.transaction) {
                value = txn.get_uncommited(place.collection, place.key);
                if (!value)
                    value = db.native->hget(redis_collection(place.collection), to_string_view(place.key));
            }
            else
                value = db.native->hget(redis_collection(place.collection), to_string_view(place.key));

            offs[i] = contents.size();
            presences[i] = bool(value);
            lens[i] = value ? value->size() : ustore_length_missing_k;
            if (needs_export && value) {
                contents.insert(contents.size(),
                                (byte_t*)value->data(),
                                (byte_t*)(value->data() + value->size()),
                                c.error);
                return_if_error_m(c.error);
            }
        }
        offs[places.count] = contents.size();
        if (needs_export)
            *c.values = reinterpret_cast<ustore_bytes_ptr_t>(contents.begin());
    });
}

void ustore_write(ustore_write_t* c_ptr) {
    ustore_write_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    redis_txn_t& txn = *reinterpret_cast<redis_txn_t*>(c.transaction);
    strided_iterator_gt<ustore_key_t const> keys {c.keys, c.keys_stride};
    strided_iterator_gt<ustore_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ustore_bytes_cptr_t const> vals {c.values, c.values_stride};
    strided_iterator_gt<ustore_length_t const> offs {c.offsets, c.offsets_stride};
    strided_iterator_gt<ustore_length_t const> lens {c.lengths, c.lengths_stride};
    bits_view_t presences {c.presences};

    places_arg_t places {collections, keys, {}, c.tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c.tasks_count};

    validate_write(c.transaction, places, contents, c.options, c.error);
    return_if_error_m(c.error);

    safe_section("Writing values", c.error, [&] {
        for (std::size_t i = 0; i != places.size(); ++i) {
            auto place = places[i];
            auto content = contents[i];
            if (content) {
                if (c.transaction)
                    txn.set(place.collection, place.key, content);
                else
                    db.native->hset(redis_collection(place.collection),
                                    to_string_view(place.key),
                                    to_string_view(content.data(), content.size()));
            }
            else {
                if (c.transaction)
                    txn.del(place.collection, place.key);
                else
                    db.native->hdel(redis_collection(place.collection), to_string_view(place.key));
            }
        }
    });
}

void ustore_scan(ustore_scan_t* c_ptr) {
    ustore_scan_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_if_error_m(c.error);

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    strided_iterator_gt<ustore_key_t const> start_keys {c.start_keys, c.start_keys_stride};
    strided_iterator_gt<ustore_length_t const> limits {c.count_limits, c.count_limits_stride};
    strided_iterator_gt<ustore_collection_t const> collections {c.collections, c.collections_stride};
    scans_arg_t scans {collections, start_keys, limits, c.tasks_count};

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy(scans.count + 1, c.error, c.offsets);
    return_if_error_m(c.error);
    auto counts = arena.alloc_or_dummy(scans.count, c.error, c.counts);
    return_if_error_m(c.error);

    auto total_keys = reduce_n(scans.limits, scans.count, 0ul);
    auto keys_output = *c.keys = arena.alloc<ustore_key_t>(total_keys, c.error).begin();
    return_if_error_m(c.error);

    safe_section("Scanning keys", c.error, [&] {
        std::vector<std::string> keys;
        for (ustore_size_t i = 0; i != c.tasks_count; ++i) {
            auto scan = scans[i];
            offsets[i] = keys_output - *c.keys;
            db.native->hkeys(redis_collection(scan.collection), std::inserter(keys, keys.begin()));

            for (std::size_t i = 0; i != keys.size(); ++i) {
                *keys_output = *reinterpret_cast<ustore_key_t*>(keys[i].data());
                ++keys_output;
            }
            counts[i] = keys.size();
            keys.clear();
        }
    });

    offsets[scans.size()] = keys_output - *c.keys;
}

void ustore_sample(ustore_sample_t* c_ptr) {
    ustore_sample_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    if (!c.tasks_count)
        return;

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_if_error_m(c.error);

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    redis_txn_t& txn = *reinterpret_cast<redis_txn_t*>(c.transaction);
    strided_iterator_gt<ustore_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ustore_length_t const> lens {c.count_limits, c.count_limits_stride};
    sample_args_t samples {collections, lens, c.tasks_count};

    // 1. Allocate a tape for all the values to be fetched
    auto offsets = arena.alloc_or_dummy(samples.count + 1, c.error, c.offsets);
    return_if_error_m(c.error);
    auto counts = arena.alloc_or_dummy(samples.count, c.error, c.counts);
    return_if_error_m(c.error);

    auto total_keys = reduce_n(samples.limits, samples.count, 0ul);
    auto keys_output = *c.keys = arena.alloc<ustore_key_t>(total_keys, c.error).begin();
    return_if_error_m(c.error);
    std::vector<std::string> keys;

    for (std::size_t task_idx = 0; task_idx != samples.count; ++task_idx) {
        sample_arg_t task = samples[task_idx];
        auto collection = redis_collection(task.collection);
        offsets[task_idx] = keys_output - *c.keys;

        safe_section("Sampling", c.error, [&] {
            db.native->command("HRANDFIELD", collection, task.limit, std::back_inserter(keys));
        });

        for (std::size_t i = 0; i != keys.size(); ++i) {
            *keys_output = *reinterpret_cast<ustore_key_t*>(keys[i].data());
            ++keys_output;
        }
        counts[task_idx] = keys.size();
        keys.clear();
    }
    offsets[samples.count] = keys_output - *c.keys;
}

void ustore_measure(ustore_measure_t* c_ptr) {
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ustore_collection_create(ustore_collection_create_t* c_ptr) {
    ustore_collection_create_t& c = *c_ptr;
    auto name_len = c.name ? std::strlen(c.name) : 0;
    return_error_if_m(name_len, c.error, args_wrong_k, "Default collection is always present");
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    for (auto& collection : db.collections)
        return_error_if_m(collection != c.name, c.error, args_wrong_k, "Such collection already exists!");

    db.collections.push_back(c.name);
    *c.id = reinterpret_cast<ustore_collection_t>(db.collections.back().data());
}

void ustore_collection_drop(ustore_collection_drop_t* c_ptr) {
    ustore_collection_drop_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    bool invalidate = c.mode == ustore_drop_keys_vals_handle_k;
    return_error_if_m(c.id != ustore_collection_main_k || !invalidate,
                      c.error,
                      args_combo_k,
                      "Default collection can't be invalidated.");

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    auto collection = redis_collection(c.id);

    safe_section("Dropping collection", c.error, [&] {
        if (c.mode == ustore_drop_keys_vals_handle_k) {
            db.native->del(collection);
            auto it = std::find(db.collections.begin(), db.collections.end(), collection);
            if (it != db.collections.end())
                db.collections.erase(it);
        }
        else {
            std::vector<std::string> keys;
            db.native->hkeys(collection, std::back_inserter(keys));
            for (const auto& key : keys)
                c.mode == ustore_drop_keys_vals_k ? db.native->hdel(collection, key)
                                                  : db.native->hset(collection, key, "");
        }
    });
}

void ustore_collection_list(ustore_collection_list_t* c_ptr) {
    ustore_collection_list_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    return_error_if_m(c.count && c.names, c.error, args_combo_k, "Need names and outputs!");

    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_if_error_m(c.error);

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    std::size_t collections_count = db.collections.size();
    *c.count = static_cast<ustore_size_t>(collections_count);

    // Every string will be null-terminated
    std::size_t strings_length = 0;
    for (auto const& collection : db.collections)
        strings_length += collection.size() + 1;

    auto names = arena.alloc<char>(strings_length, c.error).begin();
    return_if_error_m(c.error);
    *c.names = names;

    // For every collection we also need to export IDs and offsets
    auto ids = arena.alloc_or_dummy(collections_count, c.error, c.ids);
    return_if_error_m(c.error);
    auto offs = arena.alloc_or_dummy(collections_count + 1, c.error, c.offsets);
    return_if_error_m(c.error);

    std::size_t i = 0;
    for (auto const& collection : db.collections) {
        auto len = collection.size();
        std::memcpy(names, collection.data(), len);
        names[len] = '\0';
        ids[i] = reinterpret_cast<ustore_collection_t>(collection.c_str());
        offs[i] = static_cast<ustore_length_t>(names - *c.names);
        names += len + 1;
        ++i;
    }
    offs[i] = static_cast<ustore_length_t>(names - *c.names);
}

void ustore_database_control(ustore_database_control_t* c_ptr) {
}

/*********************************************************/
/*****************		Snapshots	  ****************/
/*********************************************************/
void ustore_snapshot_list(ustore_snapshot_list_t* c_ptr) {
    ustore_snapshot_list_t& c = *c_ptr;
    *c.error = "Snapshots not supported by Redis!";
}

void ustore_snapshot_create(ustore_snapshot_create_t* c_ptr) {
    ustore_snapshot_create_t& c = *c_ptr;
    *c.error = "Snapshots not supported by Redis!";
}

void ustore_snapshot_drop(ustore_snapshot_drop_t* c_ptr) {
    ustore_snapshot_drop_t& c = *c_ptr;
    *c.error = "Snapshots not supported by Redis!";
}

void ustore_snapshot_export(ustore_snapshot_export_t* c_ptr) {
    ustore_snapshot_export_t& c = *c_ptr;
    *c.error = "Snapshots not supported by Redis!";
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ustore_transaction_init(ustore_transaction_init_t* c_ptr) {
    ustore_transaction_init_t& c = *c_ptr;
    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    validate_transaction_begin(c.transaction, c.options, c.error);
    return_if_error_m(c.error);

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    safe_section("Initializing Transaction", c.error, [&] {
        auto txn_ptr = new redis_txn_t;
        txn_ptr->native = std::make_unique<redis::Transaction>(db.native->transaction());
        *c.transaction = txn_ptr;
    });
}

void ustore_transaction_commit(ustore_transaction_commit_t* c_ptr) {
    ustore_transaction_commit_t& c = *c_ptr;
    if (!c.transaction)
        return;

    validate_transaction_commit(c.transaction, c.options, c.error);
    return_if_error_m(c.error);

    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c.db);
    redis_txn_t& txn = *reinterpret_cast<redis_txn_t*>(c.transaction);
    safe_section("Commiting Transaction", c.error, [&] { txn.exec(); });
    return_if_error_m(c.error);
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ustore_arena_free(ustore_arena_t c_arena) {
    clear_linked_memory(c_arena);
}

void ustore_transaction_free(ustore_transaction_t const c_transaction) {
    if (!c_transaction)
        return;
    delete reinterpret_cast<redis_txn_t*>(c_transaction);
}

void ustore_database_free(ustore_database_t c_db) {
    if (!c_db)
        return;
    redis_client_t& db = *reinterpret_cast<redis_client_t*>(c_db);
    delete &db;
}

void ustore_error_free(ustore_error_t) {
}
