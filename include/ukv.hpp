/**
 * @file ukv.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings built on top of @see "ukv.h" with
 * two primary purposes:
 * > @b RAII controls for non-trivial & potentially heavy objects.
 * > syntactic @b sugar, iterators, containers and other C++  stuff.
 */

#pragma once
#include <string_view>

#include "ukv.h"
#include "utility.hpp"

namespace unum::ukv {

/**
 * @brief Append-only datastructure for variable length blobs.
 * Owns the underlying memory and is external to the underlying DB.
 * Is suited for data preparation before passing to the C API.
 */
class appendable_tape_t {
    std::vector<ukv_val_len_t> lengths_;
    std::vector<byte_t> data_;

  public:
    void push_back(value_view_t value) {
        lengths_.push_back(static_cast<ukv_val_len_t>(value.size()));
        data_.insert(data_.end(), value.begin(), value.end());
    }

    void clear() {
        lengths_.clear();
        data_.clear();
    }

    operator taped_values_t() const noexcept {
        taped_values_t view;
        view.lengths = lengths_.data();
        view.values = ukv_tape_ptr_t(data_.data());
        view.count = data_.size();
        return view;
    }
};

/**
 * @brief A proxy object, that allows both lookups and writes
 * with `[]` and assignment operators for a batch of keys
 * simultaneously.
 */
struct sample_handle_t {

    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_tape_ptr_t* memory = nullptr;
    ukv_size_t* capacity = 0;
    ukv_options_t options = ukv_options_default_k;

    collections_t cols;
    keys_t keys;

    [[nodiscard]] expected_gt<taped_values_t> get() const noexcept {
        error_t error;
        ukv_read(db,
                 txn,
                 cols.range.raw,
                 cols.range.stride,
                 keys.range.raw,
                 keys.range.count,
                 keys.range.stride,
                 options,
                 memory,
                 capacity,
                 &error.raw);
        if (error)
            return {std::move(error)};

        return {taped_values_t {*memory, *capacity}};
    }

    [[nodiscard]] error_t set(disjoint_values_t vals) noexcept {
        error_t error;
        ukv_write(db,
                  txn,
                  cols.range.raw,
                  cols.range.stride,
                  keys.range.raw,
                  keys.range.count,
                  keys.range.stride,
                  vals.values_range.raw,
                  vals.values_range.stride,
                  vals.lengths_range.raw,
                  vals.lengths_range.stride,
                  options,
                  &error.raw);
        return error;
    }

    operator expected_gt<taped_values_t>() const noexcept { return get(); }
    sample_handle_t& operator=(disjoint_values_t vals) noexcept(false) {
        auto error = set(vals);
        if (error)
            throw error;
        return *this;
    }
};

struct db_t;
struct session_t;
struct collection_handle_t;

struct collection_handle_t {
    ukv_t db = nullptr;
    ukv_collection_t raw = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_tape_ptr_t* memory = nullptr;
    ukv_size_t* capacity = 0;
    ukv_options_t options = ukv_options_default_k;

    collection_handle_t(ukv_t db_ptr) noexcept : db(db_ptr) {}
    ~collection_handle_t() noexcept { ukv_collection_free(db, raw); }

    void drop();
};

struct txn_t {

    collection_handle_t operator[](std::string const& name);

    sample_handle_t operator[](located_keys_t) noexcept;
    expected_gt<taped_values_t> operator[](located_keys_t) const noexcept;

    sample_handle_t operator[](keys_t) noexcept;
    expected_gt<taped_values_t> operator[](keys_t) const noexcept;

    error_t rollback();
    error_t commit();
};
struct db_t;

struct session_t {
    db_t& db;
    managed_tape_t read_tape;
    std::vector<located_key_t> planned_lookups;

    session_t(db_t& d) noexcept : db(d) {}
    expected_gt<txn_t> transact();

    collection_handle_t operator[](std::string const& name);

    sample_handle_t operator[](located_keys_t) noexcept;
    expected_gt<taped_values_t> operator[](located_keys_t) const noexcept;

    sample_handle_t operator[](keys_t) noexcept;
    expected_gt<taped_values_t> operator[](keys_t) const noexcept;
};

struct db_t {
    ukv_t raw = nullptr;

    error_t open(std::string const& config) {
        error_t error;
        ukv_open(config.c_str(), &raw, &error.raw);
        return error;
    }

    void close() {
        ukv_free(raw);
        raw = nullptr;
    }

    ~db_t() {
        if (raw)
            close();
    }
};

} // namespace unum::ukv

// auto db = db_t {};
// auto txn =
// auto col = db["bank"];
// auto val = col[alice_id];
//