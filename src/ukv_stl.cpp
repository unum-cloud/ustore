#include <unordered_map>
#include <vector>
#include <string>
#include <span>
#include <cstring> // `std::memcpy`

#include "ukv.h"

using allocator_t = std::allocator<uint8_t>;
using key_t = ukv_key_t;
using value_t = std::vector<uint8_t, allocator_t>;

struct sequenced_value_t {
    value_t data;
    size_t sequence_number = 0;
};

struct txn_t {
    size_t sequence_number = 0;
    std::unordered_map<key_t, size_t> requested_keys;
    std::unordered_map<key_t, value_t> new_values;
};

struct collection_t {
    std::unordered_map<key_t, sequenced_value_t> content;
};

struct db_t {
    collection_t main;
    std::unordered_map<std::string, collection_t> named;
    size_t youngest_sequence = 0;
};

void ukv_open(
    // Inputs:
    [[maybe_unused]] char const *config,
    // Outputs:
    ukv_t *db,
    [[maybe_unused]] ukv_error_t *error) {

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
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main;

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
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main;

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
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main;
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
        collection_t &collection = c_columns_count ? *reinterpret_cast<collection_t *>(*c_columns) : db.main;
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
