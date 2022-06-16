#include <vector>
#include <string>
#include <string_view>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>
#include <cstring> // `std::memcpy`

#include "ukv.h"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

namespace {

enum class byte_t : uint8_t {};
using allocator_t = std::allocator<byte_t>;
using key_t = ukv_key_t;
using value_t = std::vector<byte_t, allocator_t>;
using sequence_t = size_t;

struct txn_t;
struct db_t;

struct sequenced_value_t {
    value_t data;
    sequence_t sequence_number {0};
};

struct column_t {
    std::string name;
    std::unordered_map<key_t, sequenced_value_t> content;

    void reserve_more(size_t n) { content.reserve(content.size() + n); }
};

using column_ptr_t = std::unique_ptr<column_t>;

struct located_key_t {
    column_t* column_ptr = nullptr;
    key_t key {0};

    bool operator==(located_key_t const& other) const noexcept {
        return (column_ptr == other.column_ptr) & (key == other.key);
    }
    bool operator!=(located_key_t const& other) const noexcept {
        return (column_ptr != other.column_ptr) | (key != other.key);
    }
};

struct located_key_hash_t {
    size_t operator()(located_key_t const& located) const noexcept { return std::hash<key_t> {}(located.key); }
};

struct txn_t {
    std::unordered_map<located_key_t, sequence_t, located_key_hash_t> requested_keys;
    std::unordered_map<located_key_t, value_t, located_key_hash_t> new_values;
    db_t* db_ptr {nullptr};
    sequence_t sequence_number {0};
};

struct db_t {
    std::shared_mutex mutex;
    column_t main_column;
    /**
     * @brief A variable-size set of named columns.
     * It's cleaner to implement it with heterogenous lookups as
     * an @c `std::unordered_set`, but it requires GCC11.
     */
    std::unordered_map<std::string_view, column_ptr_t> named_columns;
    /**
     * @brief The sequence/transactions ID of the most recent update.
     * This can be updated even outside of the main @p `mutex` on HEAD state.
     */
    std::atomic<sequence_t> youngest_sequence {0};
};

/**
 * @brief Solves the problem of modulo arithmetic and `sequence_t` overflow.
 * Still works correctly, when `max` has overflown, but `min` hasn't yet,
 * so `min` can be bigger than `max`.
 */
bool belongs_to_gap(sequence_t sequence_number, sequence_t min, sequence_t max) noexcept {
    return min < max ? ((sequence_number > min) & (sequence_number <= max))
                     : ((sequence_number > min) | (sequence_number <= max));
}

} // namespace

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

void ukv_open(
    // Inputs:
    [[maybe_unused]] char const* config,
    // Outputs:
    ukv_t* db,
    [[maybe_unused]] ukv_error_t* c_error) {

    *db = new db_t {};
}

void ukv_write(
    // Inputs:
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_write_t const c_options,
    //
    ukv_val_ptr_t const* c_values,
    ukv_val_len_t const* c_values_lengths,
    // Outputs:
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};

    for (size_t i = 0; i != c_keys_count; ++i) {
        auto len = *c_values_lengths;
        auto begin = reinterpret_cast<byte_t const*>(*c_values);
        column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(*c_columns) : db.main_column;
        auto key_iterator = column.content.find(*c_keys);

        if (begin) {
            // We want to insert a new entry, but let's check if we
            // can overwrite the existig value without causing reallocations.
            try {
                if (key_iterator != column.content.end()) {
                    key_iterator->second.sequence_number = ++db.youngest_sequence;
                    key_iterator->second.data.assign(begin, begin + len);
                }
                else {
                    sequenced_value_t sequenced_value {
                        value_t(begin, begin + len),
                        ++db.youngest_sequence,
                    };
                    column.content.insert_or_assign(*c_keys, std::move(sequenced_value));
                }
            }
            catch (...) {
                *c_error = "Failed to put!";
                break;
            }
        }
        else {
            // We should delete the value
            if (key_iterator != column.content.end())
                column.content.erase(key_iterator);
        }

        ++c_keys;
        ++c_values_lengths;
        ++c_values;
        c_columns += c_columns_count > 1;
    }
}

void ukv_read(
    // Inputs:
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_read_t const c_options,

    // In-outs:
    void** c_arena,
    size_t* c_arena_length,

    // Outputs:
    ukv_val_ptr_t* c_values,
    ukv_val_len_t* c_values_lengths,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::shared_lock _ {db.mutex};

    // 0. Check if the user is only interested in existance checks
    if (!c_values) {
        for (size_t i = 0; i != c_keys_count; ++i) {
            column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(c_columns[i]) : db.main_column;
            auto key_iterator = column.content.find(c_keys[i]);
            c_values_lengths[i] = key_iterator != column.content.end() ? key_iterator->second.data.size() : 0;
        }
        return;
    }

    // 1. Estimate the total size
    size_t total_bytes = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(c_columns[i]) : db.main_column;
        auto key_iterator = column.content.find(c_keys[i]);
        if (key_iterator != column.content.end())
            total_bytes += key_iterator->second.data.size();
    }

    // 2. Allocate a tape for all the values to be fetched
    byte_t* arena = *reinterpret_cast<byte_t**>(c_arena);
    if (total_bytes > *c_arena_length) {
        try {
            allocator_t {}.deallocate(arena, *c_arena_length);
            arena = allocator_t {}.allocate(total_bytes);
            *c_arena = arena;
            *c_arena_length = total_bytes;
        }
        catch (...) {
            *c_error = "Failed to allocate memory for exports!";
            return;
        }
    }

    // 3. Fetch the data
    size_t exported_into_arena = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(*c_columns) : db.main_column;
        auto key_iterator = column.content.find(*c_keys);
        if (key_iterator != column.content.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(arena + exported_into_arena, key_iterator->second.data.data(), len);
            *c_values = reinterpret_cast<ukv_val_ptr_t>(arena + exported_into_arena);
            *c_values_lengths = static_cast<ukv_val_len_t>(len);
            exported_into_arena += len;
        }
        else {
            *c_values = NULL;
            *c_values_lengths = 0;
        }

        ++c_keys;
        ++c_values;
        ++c_values_lengths;
        c_columns += c_columns_count > 1;
    }
}

/*********************************************************/
/*****************	Columns Management	  ****************/
/*********************************************************/

void ukv_column_upsert(
    // Inputs:
    ukv_t const c_db,
    char const* c_column_name,
    // Outputs:
    ukv_column_t* c_column,
    ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_column_name);
    auto const column_name = std::string_view(c_column_name, name_len);

    auto column_it = db.named_columns.find(column_name);
    if (column_it == db.named_columns.end()) {
        try {
            auto new_column = std::make_unique<column_t>();
            new_column->name = column_name;
            *c_column = new_column.get();
            db.named_columns.insert_or_assign(new_column->name, std::move(new_column));
        }
        catch (...) {
            *c_error = "Failed to create a new column!";
        }
    }
    else {
        *c_column = column_it->second.get();
    }
}

void ukv_column_remove(
    // Inputs:
    ukv_t const c_db,
    char const* c_column_name,
    // Outputs:
    [[maybe_unused]] ukv_error_t* c_error) {

    db_t& db = *reinterpret_cast<db_t*>(c_db);
    std::unique_lock _ {db.mutex};
    auto name_len = std::strlen(c_column_name);
    auto column_name = std::string_view(c_column_name, name_len);

    auto column_it = db.named_columns.find(column_name);
    if (column_it != db.named_columns.end()) {
        db.named_columns.erase(column_it);
    }
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_txn_begin(
    // Inputs:
    ukv_t const c_db,
    size_t const c_sequence_number,
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

void ukv_txn_write(
    // Inputs:
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    //
    ukv_val_ptr_t const* c_values,
    ukv_val_len_t const* c_values_lengths,
    // Outputs:
    ukv_error_t* c_error) {

    // We need a `shared_lock` here just to avoid any changes to
    // the underlying addresses of columns.
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};

    for (size_t i = 0; i != c_keys_count; ++i) {
        auto len = c_values_lengths[i];
        auto begin = reinterpret_cast<byte_t const*>(*c_values);
        column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(*c_columns) : db.main_column;

        try {
            located_key_t located_key {&column, *c_keys};
            value_t value {begin, begin + len};
            txn.new_values.insert_or_assign(std::move(located_key), std::move(value));
        }
        catch (...) {
            *c_error = "Failed to put into transaction!";
            break;
        }

        ++c_keys;
        ++c_values_lengths;
        ++c_values;
        c_columns += c_columns_count > 1;
    }
}

void ukv_txn_read(
    // Inputs:
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_read_t const options,

    // In-outs:
    void** c_arena,
    size_t* c_arena_length,

    // Outputs:
    ukv_val_ptr_t* c_values,
    ukv_val_len_t* c_values_lengths,
    ukv_error_t* c_error) {

    // This read can fail, if the values to be read have already
    // changed since the beginning of the transaction!
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::shared_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();

    // 0. Check if the user is only interested in existance checks
    if (!c_values) {
        for (size_t i = 0; i != c_keys_count; ++i) {
            column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(c_columns[i]) : db.main_column;

            // Some keys may already be overwritten inside of transaction
            if (auto overwrite_iterator = txn.new_values.find(located_key_t {&column, c_keys[i]});
                overwrite_iterator != txn.new_values.end()) {
                c_values_lengths[i] = overwrite_iterator->second.size();
            }
            // Others should be pulled from the main store
            else if (auto key_iterator = column.content.find(c_keys[i]); key_iterator != column.content.end()) {
                if (belongs_to_gap(key_iterator->second.sequence_number,
                                   txn.sequence_number,
                                   youngest_sequence_number)) {
                    *c_error = "Requested key was already overwritten since the start of the transaction!";
                    return;
                }
                c_values_lengths[i] = key_iterator->second.data.size();
            }
            // But some will be missing
            else {
                c_values_lengths[i] = 0;
            }
        }
        return;
    }

    // 1. Estimate the total size of keys outside of the transaction
    size_t total_bytes = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(c_columns[i]) : db.main_column;

        // Some keys may already be overwritten inside of transaction
        if (auto overwrite_iterator = txn.new_values.find(located_key_t {&column, c_keys[i]});
            overwrite_iterator != txn.new_values.end()) {
            // We don't need extra memory for those, as transactions state can't be changed concurrently.
            // We can simply return pointers to the inserted values.
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = column.content.find(c_keys[i]); key_iterator != column.content.end()) {
            if (belongs_to_gap(key_iterator->second.sequence_number, txn.sequence_number, youngest_sequence_number)) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
            total_bytes += key_iterator->second.data.size();
        }
    }

    // 2. Allocate a tape for all the values to be pulled
    byte_t* arena = *reinterpret_cast<byte_t**>(c_arena);
    if (total_bytes <= *c_arena_length) {
        try {
            allocator_t {}.deallocate(arena, *c_arena_length);
            arena = allocator_t {}.allocate(total_bytes);
            *c_arena = arena;
            *c_arena_length = total_bytes;
        }
        catch (...) {
            *c_error = "Failed to allocate memory for exports!";
            return;
        }
    }

    // 3. Pull the data from the main store
    size_t exported_into_arena = 0;
    for (size_t i = 0; i != c_keys_count; ++i) {
        column_t& column = c_columns_count ? *reinterpret_cast<column_t*>(*c_columns) : db.main_column;

        // Some keys may already be overwritten inside of transaction
        if (auto overwrite_iterator = txn.new_values.find(located_key_t {&column, c_keys[i]});
            overwrite_iterator != txn.new_values.end()) {
            *c_values = reinterpret_cast<ukv_val_ptr_t>(overwrite_iterator->second.data());
            *c_values_lengths = static_cast<ukv_val_len_t>(overwrite_iterator->second.size());
        }
        // Others should be pulled from the main store
        else if (auto key_iterator = column.content.find(*c_keys); key_iterator != column.content.end()) {
            auto len = key_iterator->second.data.size();
            std::memcpy(arena + exported_into_arena, key_iterator->second.data.data(), len);
            *c_values = reinterpret_cast<ukv_val_ptr_t>(arena + exported_into_arena);
            *c_values_lengths = static_cast<ukv_val_len_t>(len);
            exported_into_arena += len;
        }
        // But some will be missing
        else {
            *c_values = NULL;
            *c_values_lengths = 0;
        }

        ++c_keys;
        ++c_values;
        ++c_values_lengths;
        c_columns += c_columns_count > 1;
    }
}

void ukv_txn_commit(
    // Inputs:
    ukv_txn_t const c_txn,
    [[maybe_unused]] ukv_options_write_t const options,
    // Outputs:
    ukv_error_t* c_error) {

    // This write may fail with out-of-memory errors, if Hash-Tables
    // bucket allocation fails, but no values will be copied, only moved.
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    db_t& db = *txn.db_ptr;
    std::unique_lock _ {db.mutex};
    sequence_t const youngest_sequence_number = db.youngest_sequence.load();

    // 1. Check for refreshes among fetched keys
    for (auto const& [located_key, located_sequence] : txn.requested_keys) {
        column_t& column = *located_key.column_ptr;
        auto key_iterator = column.content.find(located_key.key);
        if (key_iterator != column.content.end()) {
            if (key_iterator->second.sequence_number != located_sequence) {
                *c_error = "Requested key was already overwritten since the start of the transaction!";
                return;
            }
        }
    }

    // 2. Check for collisions among incoming values
    for (auto const& [located_key, value] : txn.new_values) {
        column_t& column = *located_key.column_ptr;
        auto key_iterator = column.content.find(located_key.key);
        if (key_iterator != column.content.end()) {
            if (belongs_to_gap(key_iterator->second.sequence_number, txn.sequence_number, youngest_sequence_number)) {
                *c_error = "Incoming key collides with newer entry!";
                return;
            }
        }
    }

    // 3. Allocate space for more nodes across different collections
    try {
        db.main_column.reserve_more(txn.new_values.size());
        for (auto& name_and_column : db.named_columns)
            name_and_column.second->reserve_more(txn.new_values.size());
    }
    catch (...) {
        *c_error = "Not enough memory!";
        return;
    }

    // 4. Import the data, as no collisions were detected
    for (auto& located_key_and_value : txn.new_values) {
        column_t& column = *located_key_and_value.first.column_ptr;
        auto key_iterator = column.content.find(located_key_and_value.first.key);
        // A key was deleted:
        if (located_key_and_value.second.empty()) {
            if (key_iterator != column.content.end())
                column.content.erase(key_iterator);
        }
        // A keys was updated:
        else if (key_iterator != column.content.end()) {
            key_iterator->second.sequence_number = txn.sequence_number;
            std::swap(key_iterator->second.data, located_key_and_value.second);
        }
        // A key was inserted:
        else {
            sequenced_value_t sequenced_value {
                std::move(located_key_and_value.second),
                txn.sequence_number,
            };
            column.content.insert_or_assign(located_key_and_value.first.key, std::move(sequenced_value));
        }
    }

    // 5. Commit the newest transaction ID
    db.youngest_sequence = txn.sequence_number;
}

/*********************************************************/
/*****************		  Iterators	      ****************/
/*********************************************************/

void ukv_iter_make(ukv_column_t const, ukv_iter_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_seek(ukv_iter_t const, ukv_key_t, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_advance(ukv_iter_t const, size_t const, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_read_key(ukv_iter_t const, ukv_key_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_read_value_size(ukv_iter_t const, size_t*, size_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_read_value(ukv_iter_t const, void**, size_t*, ukv_val_ptr_t*, ukv_val_len_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_t const, void* c_ptr, size_t c_len) {
    allocator_t {}.deallocate(reinterpret_cast<byte_t*>(c_ptr), c_len);
}

void ukv_txn_free(ukv_t const, ukv_txn_t const c_txn) {
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    delete &txn;
}

void ukv_free(ukv_t c_db) {
    db_t& db = *reinterpret_cast<db_t*>(c_db);
    delete &db;
}

void ukv_column_free(ukv_t const, ukv_column_t const) {
    // In this in-memory freeing the column handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_iter_free(ukv_t const, ukv_iter_t const) {
}

void ukv_error_free(ukv_error_t) {
}
