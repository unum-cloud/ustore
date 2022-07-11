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
#include <string> // NULL-terminated named
#include <memory> // `std::enable_shared_from_this`

#include "ukv.h"
#include "utility.hpp"

namespace unum::ukv {

/**
 * @brief Append-only datastructure for variable length blobs.
 * Owns the underlying arena and is external to the underlying DB.
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
        return {lengths_.data(), ukv_val_ptr_t(data_.data()), data_.size()};
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
    ukv_arena_t* arena = nullptr;

    collections_view_t cols;
    keys_view_t keys;

    [[nodiscard]] expected_gt<taped_values_view_t> get(bool transparent = false) const noexcept {

        error_t error;
        ukv_val_len_t* found_lengths = nullptr;
        ukv_val_ptr_t found_values = nullptr;

        ukv_read(db,
                 txn,
                 cols.begin().get(),
                 cols.stride(),
                 keys.begin().get(),
                 keys.count(),
                 keys.stride(),
                 transparent ? ukv_option_read_transparent_k : ukv_options_default_k,
                 &found_lengths,
                 &found_values,
                 arena,
                 error.internal_cptr());
        if (error)
            return {std::move(error)};

        return taped_values_view_t {found_lengths, found_values, static_cast<ukv_size_t>(keys.size())};
    }

    [[nodiscard]] expected_gt<range_gt<ukv_val_len_t const*>> lengths(bool transparent = false) const noexcept {

        error_t error;
        ukv_val_len_t* found_lengths = nullptr;
        ukv_val_ptr_t found_values = nullptr;

        ukv_read(db,
                 txn,
                 cols.begin().get(),
                 cols.stride(),
                 keys.begin().get(),
                 keys.count(),
                 keys.stride(),
                 transparent ? ukv_option_read_transparent_k : ukv_options_default_k,
                 &found_lengths,
                 &found_values,
                 arena,
                 error.internal_cptr());
        if (error)
            return {std::move(error)};

        return range_gt<ukv_val_len_t const*> {found_lengths, found_lengths + keys.count()};
    }

    /**
     * @brief Checks if certain vertices are present in the graph.
     * They maybe disconnected from everything else.
     */
    [[nodiscard]] expected_gt<strided_range_gt<bool const>> contains(bool transparent = false) const noexcept {

        error_t error;
        ukv_val_len_t* found_lengths = nullptr;
        ukv_val_ptr_t found_values = nullptr;

        ukv_read(db,
                 txn,
                 cols.begin().get(),
                 cols.stride(),
                 keys.begin().get(),
                 keys.count(),
                 keys.stride(),
                 transparent ? ukv_option_read_transparent_k : ukv_options_default_k,
                 &found_lengths,
                 &found_values,
                 arena,
                 error.internal_cptr());
        if (error)
            return error;

        // Transaform the `found_lengths` into booleans.
        std::transform(found_lengths, found_lengths + keys.size(), found_lengths, [](ukv_val_len_t len) {
            return len != ukv_val_len_missing_k;
        });

        // Cast assuming "Little-Endian" architecture
        auto last_byte_offset = sizeof(ukv_val_len_t) - sizeof(bool);
        auto booleans = reinterpret_cast<bool const*>(found_lengths);
        return strided_range_gt<bool const> {booleans + last_byte_offset, sizeof(ukv_val_len_t), keys.size()};
    }

    [[nodiscard]] error_t set(disjoint_values_view_t vals, bool flush = false) noexcept {
        error_t error;
        ukv_write(db,
                  txn,
                  cols.begin().get(),
                  cols.stride(),
                  keys.begin().get(),
                  keys.count(),
                  keys.stride(),
                  vals.values_range.begin().get(),
                  vals.values_range.stride(),
                  vals.offsets_range.begin().get(),
                  vals.offsets_range.stride(),
                  vals.lengths_range.begin().get(),
                  vals.lengths_range.stride(),
                  flush ? ukv_option_write_flush_k : ukv_options_default_k,
                  arena,
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
    inline collection_t(ukv_t db_ptr, ukv_collection_t col_ptr = ukv_default_collection_k) noexcept
        : db_(db_ptr), col_(col_ptr) {}
    inline collection_t(collection_t&& other) noexcept
        : db_(other.db_), col_(std::exchange(other.col_, ukv_default_collection_k)) {}
    inline ~collection_t() noexcept {
        if (col_)
            ukv_collection_free(db_, col_);
        col_ = nullptr;
    }
    inline collection_t& operator=(collection_t&& other) noexcept {
        if (col_)
            ukv_collection_free(db_, col_);
        db_ = other.db_;
        col_ = std::exchange(other.col_, ukv_default_collection_k);
        return *this;
    }
    inline operator ukv_collection_t() const noexcept { return col_; }
    inline ukv_collection_t* internal_cptr() noexcept { return &col_; }
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
    managed_arena_t arena_;

  public:
    txn_t(ukv_t db, ukv_txn_t txn) : db_(db), txn_(txn), arena_(db) {}
    txn_t(txn_t const&) = delete;
    txn_t(txn_t&& other) noexcept
        : db_(other.db_), txn_(std::exchange(other.txn_, nullptr)), arena_(std::move(other.arena_)) {}

    inline ukv_t db() const noexcept { return db_; }
    inline managed_arena_t& arena() noexcept { return arena_; }
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
            .arena = arena_.internal_cptr(),
            .cols = located.members(&located_key_t::collection),
            .keys = located.members(&located_key_t::key),
        };
    }

    inline sample_proxy_t operator[](keys_view_t keys) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = txn_,
            .arena = arena_.internal_cptr(),
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
 * @brief A RAII abstraction to handle temporary aligned arena
 * for requests coming from a single user thread and planning the
 * lazy lookups.
 */
class session_t {
    ukv_t db_ = nullptr;
    managed_arena_t arena_;
    std::vector<located_key_t> lazy_lookups_;

  public:
    session_t(ukv_t db) : db_(db), arena_(db) {}
    session_t(session_t const&) = delete;
    session_t(session_t&& other) noexcept
        : db_(other.db_), arena_(std::move(other.arena_)), lazy_lookups_(std::move(other.lazy_lookups_)) {}

    inline ukv_t db() const noexcept { return db_; }
    inline managed_arena_t& arena() noexcept { return arena_; }

    inline sample_proxy_t operator[](located_keys_view_t located) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = nullptr,
            .arena = arena_.internal_cptr(),
            .cols = located.members(&located_key_t::collection),
            .keys = located.members(&located_key_t::key),
        };
    }

    inline sample_proxy_t operator[](keys_view_t keys) noexcept {
        return sample_proxy_t {
            .db = db_,
            .txn = nullptr,
            .arena = arena_.internal_cptr(),
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
    db_t() = default;
    db_t(db_t const&) = delete;
    db_t(db_t&& other) noexcept : db_(std::exchange(other.db_, nullptr)) {}

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
        ukv_collection_upsert(db_, name.c_str(), nullptr, &col, error.internal_cptr());
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
