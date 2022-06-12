#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>
#include <cstring> // `std::memcpy`

#include "ukv.h"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using allocator_t = std::allocator<uint8_t>;
using key_t = ukv_key_t;
using value_t = std::vector<uint8_t, allocator_t>;
using sequence_t = size_t;

struct txn_t;
struct db_t;

struct sequenced_value_t {
    value_t data;
    sequence_t sequence_number {0};
};

struct collection_t {
    std::unordered_map<key_t, sequenced_value_t> content;
};

struct located_key_t {
    collection_t *collection_ptr = nullptr;
    key_t key {0};

    bool operator==(located_key_t const &other) const noexcept {
        return (collection_ptr == other.collection_ptr) & (key == other.key);
    }
    bool operator!=(located_key_t const &other) const noexcept {
        return (collection_ptr == other.collection_ptr) & (key == other.key);
    }
};

struct located_key_hash_t {
    size_t operator()(located_key_t const &located) const noexcept {
        return std::hash<key_t> {}(located.key);
    }
};

struct txn_t {
    std::unordered_map<located_key_t, sequence_t, located_key_hash_t> requested_keys;
    std::unordered_map<located_key_t, value_t, located_key_hash_t> new_values;
    db_t *db_ptr {nullptr};
    sequence_t sequence_number {0};
};

struct db_t {
    std::shared_mutex mutex;
    collection_t main_collection;
    std::unordered_map<std::string, collection_t> named_collections;
    /**
     * @brief The sequence/transactions ID of the most recent update.
     * This can be updated even outside of the main @p `mutex` on HEAD state.
     */
    std::atomic<sequence_t> youngest_sequence {0};
    std::atomic<sequence_t> oldest_running_transaction {0};
};

bool key_was_changed(db_t &db, sequence_t reference_sequence_number);

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

void ukv_open(
    // Inputs:
    [[maybe_unused]] char const *config,
    // Outputs:
    ukv_t *db,
    [[maybe_unused]] ukv_error_t *c_error) {

    *db = new db_t {};
}

void ukv_put(
    // Inputs:
    ukv_t const c_db,
    key_t const *c_keys,
    size_t const c_keys_count,
    ukv_column_t const *c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_write_t const c_options,
    //
    ukv_val_ptr_t const *c_values,
    ukv_val_len_t const *c_values_lengths,
    // Outputs:
    ukv_error_t *c_error) {

    db_t &db = *reinterpret_cast<db_t *>(c_db);
    for (size_t i = 0; i != c_keys_count; ++i) {
        auto len = c_values_lengths[i];
        auto begin = reinterpret_cast<uint8_t const *>(c_values);
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main_collection;

        try {
            sequenced_value_t sequenced_value {
                value_t(begin, begin + len),
                ++db.youngest_sequence,
            };
            collection.content.emplace(*c_keys, std::move(sequenced_value));
        }
        catch (...) {
            *c_error = "Failed to put!";
            break;
        }

        ++c_keys;
        ++c_values_lengths;
        c_values += len;
        c_columns += c_columns_count > 1;
    }
}

void ukv_contains(
    // Inputs:
    ukv_t const c_db,
    key_t const *c_keys,
    size_t const c_keys_count,
    ukv_column_t const *c_columns,
    size_t const c_columns_count,

    // In-outs:
    [[maybe_unused]] void **c_arena,
    [[maybe_unused]] size_t *c_arena_length,

    // Outputs:
    bool *c_values,
    [[maybe_unused]] ukv_error_t *c_error) {

    db_t &db = *reinterpret_cast<db_t *>(c_db);
    for (size_t i = 0; i != c_keys_count; ++i) {
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main_collection;

        *c_values = collection.content.find(*c_keys) != collection.content.end();

        ++c_keys;
        c_columns += c_columns_count > 1;
    }
}

void ukv_get(
    // Inputs:
    ukv_t const c_db,
    key_t const *c_keys,
    size_t const c_keys_count,
    ukv_column_t const *c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_read_t const c_options,

    // In-outs:
    void **c_arena,
    size_t *c_arena_length,

    // Outputs:
    ukv_val_ptr_t *c_values,
    ukv_val_len_t *c_values_lengths,
    [[maybe_unused]] ukv_error_t *c_error) {

    db_t &db = *reinterpret_cast<db_t *>(c_db);

    // 1. Estimate the total size
    size_t total_bytes = 0;

    for (size_t i = 0; i != c_keys_count; ++i) {
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main_collection;
        auto key_iterator = collection.content.find(*c_keys);
        if (key_iterator != collection.content.end())
            total_bytes += key_iterator->second.data.size();

        ++c_keys;
        c_columns += c_columns_count > 1;
    }

    // 2. Fetch all the data into a single continuous memory arena
    auto arena = allocator_t {}.allocate(total_bytes);
    size_t exported_bytes = 0;

    for (size_t i = 0; i != c_keys_count; ++i) {
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main_collection;
        auto key_iterator = collection.content.find(*c_keys);
        if (key_iterator != collection.content.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(arena + exported_bytes, key_iterator->second.data.data(), len);
            *c_values = reinterpret_cast<ukv_val_ptr_t>(arena + exported_bytes);
            *c_values_lengths = static_cast<ukv_val_len_t>(len);
            exported_bytes += len;
        }
        else {
            *c_values = NULL;
            *c_values_lengths = 0;
        }

        ++c_keys;
        c_columns += c_columns_count > 1;
        ++c_values;
        ++c_values_lengths;
    }

    *c_arena = arena;
    *c_arena_length = total_bytes;
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_txn_begin(
    // Inputs:
    ukv_t const c_db,
    size_t const c_sequence_number,
    // Outputs:
    ukv_txn_t *c_txn,
    ukv_error_t *c_error) {

    db_t &db = *reinterpret_cast<db_t *>(c_db);
    if (!*c_txn) {
        try {
            *c_txn = new txn_t();
        }
        catch (...) {
            *c_error = "Failed to initizalize the transaction";
        }
    }

    txn_t &txn = *reinterpret_cast<txn_t *>(*c_txn);
    txn.db_ptr = &db;
    txn.sequence_number = c_sequence_number;
    txn.requested_keys.clear();
    txn.new_values.clear();
}

void ukv_txn_put(
    // Inputs:
    ukv_txn_t const c_txn,
    key_t const *c_keys,
    size_t const c_keys_count,
    ukv_column_t const *c_columns,
    size_t const c_columns_count,
    //
    ukv_val_ptr_t const *c_values,
    ukv_val_len_t const *c_values_lengths,
    // Outputs:
    ukv_error_t *c_error) {

    txn_t &txn = *reinterpret_cast<txn_t *>(c_txn);
    db_t &db = *txn.db_ptr;
    for (size_t i = 0; i != c_keys_count; ++i) {
        auto len = c_values_lengths[i];
        auto begin = reinterpret_cast<uint8_t const *>(c_values);
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main_collection;

        try {
            located_key_t located_key {&collection, *c_keys};
            value_t value {begin, begin + len};
            txn.new_values.emplace(std::move(located_key), std::move(value));
        }
        catch (...) {
            *c_error = "Failed to put into transaction!";
            break;
        }

        ++c_keys;
        ++c_values_lengths;
        c_values += len;
        c_columns += c_columns_count > 1;
    }
}

void ukv_txn_get(
    // Inputs:
    ukv_txn_t const c_txn,
    key_t const *c_keys,
    size_t const c_keys_count,
    ukv_column_t const *c_columns,
    size_t const c_columns_count,

    // In-outs:
    void **c_arena,
    size_t *c_arena_length,

    // Outputs:
    ukv_val_ptr_t *c_values,
    ukv_val_len_t *c_values_lengths,
    ukv_error_t *c_error) {

    // This read can fail, if the values to be read have already
    // changed since the beginning of the transaction!

    txn_t &txn = *reinterpret_cast<txn_t *>(c_txn);
    db_t &db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    for (size_t i = 0; i != c_keys_count; ++i) {
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main_collection;
        auto key_iterator = collection.content.find(*c_keys);

        if (key_was_changed(db, txn.sequence_number)) {
        }

        ++c_keys;
        c_columns += c_columns_count > 1;
    }
}

void ukv_txn_commit(
    // Inputs:
    ukv_txn_t const c_txn,
    // Outputs:
    ukv_error_t *c_error) {

    // This write may fail with out-of-memory errors, if Hash-Tables
    // bucket allocation fails, but no values will be copied, only moved.

    txn_t &txn = *reinterpret_cast<txn_t *>(c_txn);
    db_t &db = *txn.db_ptr;
    std::unique_lock _ {db.mutex};
}

/*********************************************************/
/*****************		  Iterators	      ****************/
/*********************************************************/

void ukv_iter_make(ukv_column_t const, ukv_iter_t *, ukv_error_t *error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_seek(ukv_iter_t const, ukv_key_t, ukv_error_t *error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_advance(ukv_iter_t const, size_t const, ukv_error_t *error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_get_key(ukv_iter_t const, ukv_key_t *, ukv_error_t *error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_get_value_size(ukv_iter_t const, size_t *, size_t *, ukv_error_t *error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_get_value(ukv_iter_t const, void **, size_t *, ukv_val_ptr_t *, ukv_val_len_t *, ukv_error_t *error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_get_free(ukv_t const, void *c_ptr, size_t c_len) {
    allocator_t {}.deallocate(reinterpret_cast<uint8_t *>(c_ptr), c_len);
}

void ukv_txn_free(ukv_t const c_db, ukv_txn_t const c_txn);

void ukv_free(ukv_t c_db) {
    db_t &db = *reinterpret_cast<db_t *>(c_db);
    delete &db;
}

void ukv_column_free(ukv_t const db, ukv_column_t const column);

void ukv_iter_free(ukv_t const, ukv_iter_t const) {}

void ukv_error_free(ukv_t, ukv_error_t) {}
