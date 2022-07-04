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
#include <string_view> // `std::string_view`
#include <memory>      // `std::enable_shared_from_this`

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

    operator taped_values_view_t() const noexcept {
        taped_values_view_t view;
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
struct sample_proxy_t {

    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_tape_ptr_t* memory = nullptr;
    ukv_size_t* capacity = 0;
    ukv_options_t options = ukv_options_default_k;

    collections_view_t cols;
    keys_view_t keys;

    [[nodiscard]] expected_gt<taped_values_view_t> get() const noexcept {
        error_t error;
        ukv_read(db,
                 txn,
                 cols.range.begin().get(),
                 cols.range.stride(),
                 keys.range.begin().get(),
                 keys.range.count(),
                 keys.range.stride(),
                 options,
                 memory,
                 capacity,
                 error.internal_cptr());
        if (error)
            return {std::move(error)};

        return {taped_values_view_t {*memory, *capacity}};
    }

    [[nodiscard]] error_t set(disjoint_values_view_t vals) noexcept {
        error_t error;
        ukv_write(db,
                  txn,
                  cols.range.begin().get(),
                  cols.range.stride(),
                  keys.range.begin().get(),
                  keys.range.count(),
                  keys.range.stride(),
                  vals.values_range.begin().get(),
                  vals.values_range.stride(),
                  vals.offsets_range.begin().get(),
                  vals.offsets_range.stride(),
                  vals.lengths_range.begin().get(),
                  vals.lengths_range.stride(),
                  options,
                  error.internal_cptr());
        return error;
    }

    operator expected_gt<taped_values_view_t>() const noexcept { return get(); }
    sample_proxy_t& operator=(disjoint_values_view_t vals) noexcept(false) {
        auto error = set(vals);
        error.throw_unhandled();
        return *this;
    }
};

class db_t;
class session_t;
class collection_t;

/**
 * @brief RAII abstraction wrapping a collection handle.
 * Generally cheap to construct.
 */
class collection_t {
    ukv_t db_ = nullptr;
    ukv_collection_t col_ = ukv_default_collection_k;

  public:
    inline collection_t() = default;
    inline collection_t(ukv_t db_ptr, ukv_collection_t col_ptr) noexcept : db_(db_ptr), col_(col_ptr) {}
    inline collection_t(collection_t&& other) noexcept
        : db_(other.db_), col_(std::exchange(other.col_, ukv_default_collection_k)) {}
    inline ~collection_t() noexcept { ukv_collection_free(db_, col_); }
    inline collection_t& operator=(collection_t&& other) noexcept {
        if (col_ != ukv_default_collection_k)
            ukv_collection_free(db_, col_);
        db_ = other.db_;
        col_ = std::exchange(other.col_, ukv_default_collection_k);
        return *this;
    }
    inline operator ukv_collection_t() const noexcept { return col_; }
    inline ukv_t db() const noexcept { return db_; }
};

/**
 * @brief Unlike `session_t`, not only allows planning and batching read
 * requests together, but also stores all the writes in it's internal state
 * until being `commit()`-ed.
 */
class txn_t {
    ukv_t db_ = nullptr;
    ukv_txn_t txn_ = nullptr;
    managed_tape_t read_tape_;

  public:
    txn_t(ukv_t db, ukv_txn_t txn) : db_(db), txn_(txn), read_tape_(db) {}
    inline ukv_t db() const noexcept { return db_; }
    inline managed_tape_t& tape() noexcept { return read_tape_; }
    inline operator ukv_txn_t() const noexcept { return txn_; }

    ~txn_t() {
        if (txn_)
            ukv_txn_free(db_, txn_);
        txn_ = nullptr;
    }

    inline sample_proxy_t operator[](located_keys_view_t located) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = txn_,
            .memory = read_tape_.internal_memory(),
            .capacity = read_tape_.internal_capacity(),
            .cols = located.collections(),
            .keys = located.keys(),
        };
    }

    inline sample_proxy_t operator[](keys_view_t keys) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = txn_,
            .memory = read_tape_.internal_memory(),
            .capacity = read_tape_.internal_capacity(),
            .keys = keys,
        };
    }

    error_t reset() {
        error_t error;
        ukv_txn_begin(db_, 0, &txn_, error.internal_cptr());
        return error;
    }

    error_t commit(ukv_options_t options = ukv_options_default_k) {
        error_t error;
        ukv_txn_commit(txn_, options, error.internal_cptr());
        return error;
    }
};

/**
 * @brief A RAII abstraction to handle temporary aligned memory
 * for requests coming from a single user thread and planning the
 * lazy lookups.
 */
class session_t {
    ukv_t db_ = nullptr;
    managed_tape_t read_tape_;
    std::vector<located_key_t> lazy_lookups_;

  public:
    session_t(ukv_t db) : db_(db), read_tape_(db) {}
    inline ukv_t db() const noexcept { return db_; }
    inline managed_tape_t& tape() noexcept { return read_tape_; }

    inline sample_proxy_t operator[](located_keys_view_t located) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = nullptr,
            .memory = read_tape_.internal_memory(),
            .capacity = read_tape_.internal_capacity(),
            .cols = located.collections(),
            .keys = located.keys(),
        };
    }

    inline sample_proxy_t operator[](keys_view_t keys) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = nullptr,
            .memory = read_tape_.internal_memory(),
            .capacity = read_tape_.internal_capacity(),
            .keys = keys,
        };
    }

    inline session_t& new_plan() noexcept {
        lazy_lookups_.clear();
        return *this;
    }

    inline session_t& plan(located_key_t located) {
        lazy_lookups_.push_back(located);
        return *this;
    }

    inline sample_proxy_t sample() noexcept { return operator[](located_keys_view_t {lazy_lookups_}); }

    expected_gt<txn_t> transact() {
        error_t error;
        ukv_txn_t raw = nullptr;
        ukv_txn_begin(db_, 0, &raw, error.internal_cptr());
        if (error)
            return {std::move(error), txn_t {db_, nullptr}};
        else
            return txn_t {db_, raw};
    }
};

/**
 * @brief DataBase instance encapsulator, which is responsible for
 * > session-allocation for fine-grained operations,
 * > globally blocking operations, like restructuring.
 * This object must leave at least as long, as the last session using it.
 */
class db_t : public std::enable_shared_from_this<db_t> {
    ukv_t db_ = nullptr;

  public:
    error_t open(std::string const& config) {
        error_t error;
        ukv_open(config.c_str(), &db_, error.internal_cptr());
        return error;
    }

    void close() {
        ukv_free(db_);
        db_ = nullptr;
    }

    ~db_t() {
        if (db_)
            close();
    }

    inline operator ukv_t() const noexcept { return db_; }
    session_t session() { return {db_}; }
    expected_gt<collection_t> operator[](std::string const& name) { return collection(name); }

    expected_gt<collection_t> collection(std::string const& name) {
        error_t error;
        ukv_collection_t col = nullptr;
        ukv_collection_upsert(db_, name.c_str(), &col, error.internal_cptr());
        if (error)
            return error;
        else
            return collection_t {db_, col};
    }

    error_t remove(std::string const& name) {
        error_t error;
        ukv_collection_remove(db_, name.c_str(), error.internal_cptr());
        return error;
    }
};

} // namespace unum::ukv

// auto db = db_t {};
// auto txn =
// auto col = db["bank"];
// auto val = col[alice_id];
// auto val2 = col[42_key.in(col)]