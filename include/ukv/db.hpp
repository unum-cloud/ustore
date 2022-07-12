/**
 * @file db.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings for @see "db.h".
 */

#pragma once
#include <string> // NULL-terminated named
#include <memory> // `std::enable_shared_from_this`

#include "ukv/ukv.h"
#include "utility.hpp"

namespace unum::ukv {

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

    [[nodiscard]] expected_gt<range_gt<ukv_val_len_t*>> lengths(bool transparent = false) const noexcept {

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

        return range_gt<ukv_val_len_t*> {found_lengths, found_lengths + keys.count()};
    }

    /**
     * @brief Checks if certain vertices are present in the graph.
     * They maybe disconnected from everything else.
     */
    [[nodiscard]] expected_gt<strided_range_gt<bool>> contains(bool transparent = false) const noexcept {

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
        auto last_byte_offset = 0; // sizeof(ukv_val_len_t) - sizeof(bool);
        auto booleans = reinterpret_cast<bool*>(found_lengths);
        return strided_range_gt<bool> {booleans + last_byte_offset, sizeof(ukv_val_len_t), keys.size()};
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
                  vals.contents.begin().get(),
                  vals.contents.stride(),
                  vals.offsets.begin().get(),
                  vals.offsets.stride(),
                  vals.lengths.begin().get(),
                  vals.lengths.stride(),
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

class collection_keys_iterator_t {

    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_arena_t* arena = nullptr;

    ukv_collection_t col = ukv_default_collection_k;
    ukv_size_t read_ahead = 0;

    ukv_key_t next_min_key_ = 0;
    range_gt<ukv_key_t*> prefetched_keys_;
    std::size_t prefetched_offset_;

    expected_gt<range_gt<ukv_key_t*>> prefetch_starting_with(ukv_key_t next_min_key_) {
        ukv_key_t* found_keys = nullptr;
        ukv_val_len_t* found_lens = nullptr;
        error_t error;
        ukv_scan(db,
                 txn,
                 &col,
                 0,
                 &next_min_key_,
                 1,
                 0,
                 &read_ahead,
                 0,
                 ukv_options_default_k,
                 &found_keys,
                 &found_lens,
                 arena,
                 error.internal_cptr());
        if (error)
            return std::move(error);

        auto present_end = std::find(found_keys, found_keys + read_ahead, ukv_key_unknown_k);
        return range_gt<ukv_key_t*> {found_keys, present_end};
    }

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = ukv_key_t;
    using pointer = ukv_key_t*;
    using reference = ukv_key_t&;

    error_t seek_to_first() {

        prefetched_keys_ = {};
        prefetched_offset_ = 0;
        next_min_key_ = std::numeric_limits<ukv_key_t>::min();
        auto batch = prefetch_starting_with(next_min_key_);
        if (!batch)
            return batch.release_error();

        prefetched_keys_ = *batch;
        if (prefetched_keys_.size() == 0) {
            next_min_key_ = ukv_key_unknown_k;
            return {};
        }

        ukv_key_t result = prefetched_keys_[0];
        prefetched_offset_ = 0;
        next_min_key_ = prefetched_keys_[prefetched_keys_.size() - 1] + 1;
        return {};
    }

    error_t advance() {

        if (prefetched_offset_ < prefetched_keys_.size()) {
            ++prefetched_offset_;
            return {};
        }

        auto batch = prefetch_starting_with(next_min_key_);
        if (!batch)
            return batch.release_error();

        prefetched_keys_ = *batch;
        if (prefetched_keys_.size() == 0) {
            next_min_key_ = ukv_key_unknown_k;
            return {};
        }

        prefetched_offset_ = 1;
        next_min_key_ = prefetched_keys_[prefetched_keys_.size() - 1] + 1;
        return {};
    }

    ukv_key_t key() const noexcept { return prefetched_keys_[prefetched_offset_]; }
    ukv_key_t operator*() const noexcept { return key(); }
    bool is_end() const noexcept {
        return next_min_key_ == ukv_key_unknown_k && prefetched_offset_ >= prefetched_keys_.size();
    }
    bool operator==(collection_keys_iterator_t const& other) const noexcept {
        if (col != other.col)
            return false;
        if (is_end() || other.is_end())
            return is_end() == other.is_end();
        return key() == other.key();
    }
    bool operator!=(collection_keys_iterator_t const& other) const noexcept {
        if (col == other.col)
            return true;
        if (is_end() || other.is_end())
            return is_end() != other.is_end();
        return key() != other.key();
    }
};

/**
 * @brief Implements multi-way set intersection to join entities
 * from different collections, that have matching identifiers.
 */
struct collections_join_t {

    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_arena_t* arena = nullptr;

    collections_view_t cols;
    ukv_key_t next_min_key_ = 0;
    ukv_size_t window_size = 0;

    strided_range_gt<ukv_key_t*> prefetched_keys_;
    strided_range_gt<ukv_val_len_t> prefetched_lengths;
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

    inline expected_gt<std::size_t> size() const noexcept { return 0; }
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

    error_t clear(collection_t&) { return {}; }
};

} // namespace unum::ukv
