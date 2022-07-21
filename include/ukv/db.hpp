/**
 * @file db.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings for @see "db.h".
 */

#pragma once
#include <string>  // NULL-terminated names
#include <cstring> // `std::strlen`
#include <memory>  // `std::enable_shared_from_this`

#include "ukv/ukv.h"
#include "utility.hpp"

namespace unum::ukv {

/**
 * @brief A proxy object, that allows both lookups and writes
 * with `[]` and assignment operators for a batch of keys
 * simultaneously.
 */
class entries_ref_t {

    ukv_t db_ = nullptr;
    ukv_txn_t txn_ = nullptr;
    ukv_arena_t* arena_ = nullptr;

    collections_view_t cols_;
    keys_view_t keys_;
    fields_view_t fields_;

    expected_gt<taped_values_view_t> any_get(ukv_format_t format, ukv_options_t options) const noexcept {

        status_t status;
        ukv_val_len_t* found_lengths = nullptr;
        ukv_val_ptr_t found_values = nullptr;

        if (fields_ || format != ukv_format_binary_k)
            ukv_docs_read(db_,
                          txn_,
                          keys_.count(),
                          cols_.begin().get(),
                          cols_.stride(),
                          keys_.begin().get(),
                          keys_.stride(),
                          fields_.begin().get(),
                          fields_.stride(),
                          options,
                          format,
                          &found_lengths,
                          &found_values,
                          arena_,
                          status.internal_cptr());
        else
            ukv_read(db_,
                     txn_,
                     keys_.count(),
                     cols_.begin().get(),
                     cols_.stride(),
                     keys_.begin().get(),
                     keys_.stride(),
                     options,
                     &found_lengths,
                     &found_values,
                     arena_,
                     status.internal_cptr());
        if (!status)
            return status;

        return taped_values_view_t {found_lengths, found_values, keys_.count()};
    }

    /**
     * @brief Pair-wise assigns values to keys_ located in this proxy objects.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t any_set(disjoint_values_view_t vals, ukv_format_t format, ukv_options_t options) noexcept {
        status_t status;
        if (fields_ || format != ukv_format_binary_k)
            ukv_docs_write(db_,
                           txn_,
                           keys_.count(),
                           cols_.begin().get(),
                           cols_.stride(),
                           keys_.begin().get(),
                           keys_.stride(),
                           fields_.begin().get(),
                           fields_.stride(),
                           options,
                           format,
                           vals.contents.begin().get(),
                           vals.contents.stride(),
                           vals.offsets.begin().get(),
                           vals.offsets.stride(),
                           vals.lengths.begin().get(),
                           vals.lengths.stride(),
                           arena_,
                           status.internal_cptr());
        else
            ukv_write(db_,
                      txn_,
                      keys_.count(),
                      cols_.begin().get(),
                      cols_.stride(),
                      keys_.begin().get(),
                      keys_.stride(),
                      vals.contents.begin().get(),
                      vals.contents.stride(),
                      vals.offsets.begin().get(),
                      vals.offsets.stride(),
                      vals.lengths.begin().get(),
                      vals.lengths.stride(),
                      options,
                      arena_,
                      status.internal_cptr());
        return status;
    }

  public:
    entries_ref_t(ukv_t db,
                  ukv_txn_t txn,
                  ukv_arena_t* arena,
                  collections_view_t cols,
                  keys_view_t keys,
                  fields_view_t fields = {}) noexcept
        : db_(db), txn_(txn), arena_(arena), cols_(cols), keys_(keys), fields_(fields) {}

    expected_gt<taped_values_view_t> get(ukv_format_t format = ukv_format_binary_k,
                                         bool transparent = false) const noexcept {
        auto options = static_cast<ukv_options_t>(
            (transparent ? ukv_option_read_transparent_k : ukv_options_default_k) | ukv_option_read_lengths_k);
        return any_get(format, options);
    }

    expected_gt<indexed_range_gt<ukv_val_len_t*>> lengths(ukv_format_t format = ukv_format_binary_k,
                                                          bool transparent = false) const noexcept {

        auto options = static_cast<ukv_options_t>(
            (transparent ? ukv_option_read_transparent_k : ukv_options_default_k) | ukv_option_read_lengths_k);
        auto maybe = any_get(format, options);
        if (!maybe)
            return maybe.release_status();

        auto found_lengths = maybe->lengths();
        return indexed_range_gt<ukv_val_len_t*> {found_lengths, found_lengths + keys_.count()};
    }

    /**
     * @brief Checks if requested keys_ are present in the store.
     * ! Related values may be empty strings.
     */
    expected_gt<strided_range_gt<bool>> contains(ukv_format_t format = ukv_format_binary_k,
                                                 bool transparent = false) const noexcept {

        auto maybe = lengths(format, transparent);
        if (!maybe)
            return maybe.release_status();

        // Transform the `found_lengths` into booleans.
        auto found_lengths = maybe->begin();
        std::transform(found_lengths, found_lengths + keys_.size(), found_lengths, [](ukv_val_len_t len) {
            return len != ukv_val_len_missing_k;
        });

        // Cast assuming "Little-Endian" architecture
        auto last_byte_offset = 0; // sizeof(ukv_val_len_t) - sizeof(bool);
        auto booleans = reinterpret_cast<bool*>(found_lengths);
        return strided_range_gt<bool> {booleans + last_byte_offset, sizeof(ukv_val_len_t), keys_.size()};
    }

    /**
     * @brief Pair-wise assigns values to keys_ located in this proxy objects.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t set(disjoint_values_view_t vals, ukv_format_t format = ukv_format_binary_k, bool flush = false) noexcept {
        return any_set(vals, format, flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Removes both the keys_ and the associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t erase(bool flush = false) noexcept { return set(disjoint_values_view_t {}, ukv_format_binary_k, flush); }

    /**
     * @brief Keeps the keys_, but clears the contents of associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t clear(bool flush = false) noexcept {
        ukv_val_ptr_t any = reinterpret_cast<ukv_val_ptr_t>(this);
        ukv_val_len_t len = 0;
        return set(disjoint_values_view_t {.contents = {any}, .offsets = {}, .lengths = {len}},
                   ukv_format_binary_k,
                   flush);
    }

    operator expected_gt<taped_values_view_t>() const noexcept { return get(); }

    entries_ref_t& operator=(disjoint_values_view_t vals) noexcept(false) {
        auto status = set(vals);
        status.throw_unhandled();
        return *this;
    }

    entries_ref_t& operator=(nullptr_t) noexcept(false) {
        auto status = erase();
        status.throw_unhandled();
        return *this;
    }

    collections_view_t cols() const noexcept { return cols_; }
    keys_view_t keys() const noexcept { return keys_; }
};

/**
 * @brief Iterator (almost) over the keys in a single collection.
 * Manages it's own memory and may be expressive to construct.
 * Prefer to `seek`, instead of re-creating such a stream.
 * Unlike classical iterators, keeps an internal state,
 * which makes it @b non copy-constructible!
 */
class keys_stream_t {

    ukv_t db_ = nullptr;
    ukv_collection_t col_ = ukv_default_collection_k;
    ukv_txn_t txn_ = nullptr;

    managed_arena_t arena_;
    ukv_size_t read_ahead_ = 0;

    ukv_key_t next_min_key_ = std::numeric_limits<ukv_key_t>::min();
    indexed_range_gt<ukv_key_t*> fetched_keys_;
    std::size_t fetched_offset_ = 0;

    status_t prefetch() noexcept {

        if (next_min_key_ == ukv_key_unknown_k)
            return {};

        ukv_key_t* found_keys = nullptr;
        ukv_val_len_t* found_lens = nullptr;
        status_t status;
        ukv_scan(db_,
                 txn_,
                 1,
                 &col_,
                 0,
                 &next_min_key_,
                 0,
                 &read_ahead_,
                 0,
                 ukv_options_default_k,
                 &found_keys,
                 &found_lens,
                 arena_.internal_cptr(),
                 status.internal_cptr());
        if (!status)
            return status;

        auto present_end = std::find(found_keys, found_keys + read_ahead_, ukv_key_unknown_k);
        fetched_keys_ = indexed_range_gt<ukv_key_t*> {found_keys, present_end};
        fetched_offset_ = 0;

        auto count = static_cast<ukv_size_t>(fetched_keys_.size());
        next_min_key_ = count < read_ahead_ ? ukv_key_unknown_k : fetched_keys_[count - 1] + 1;
        return {};
    }

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = ukv_key_t;
    using pointer = ukv_key_t*;
    using reference = ukv_key_t&;

    static constexpr std::size_t default_read_ahead_k = 256;

    keys_stream_t(ukv_t db,
                  ukv_collection_t col = ukv_default_collection_k,
                  std::size_t read_ahead = keys_stream_t::default_read_ahead_k,
                  ukv_txn_t txn = nullptr)
        : db_(db), col_(col), txn_(txn), arena_(db), read_ahead_(static_cast<ukv_size_t>(read_ahead)) {}

    keys_stream_t(keys_stream_t&&) = default;
    keys_stream_t& operator=(keys_stream_t&&) = default;

    keys_stream_t(keys_stream_t const&) = delete;
    keys_stream_t& operator=(keys_stream_t const&) = delete;

    status_t seek(ukv_key_t key) noexcept {
        fetched_keys_ = {};
        fetched_offset_ = 0;
        next_min_key_ = key;
        return prefetch();
    }

    status_t advance() noexcept {

        if (fetched_offset_ >= fetched_keys_.size())
            return prefetch();

        ++fetched_offset_;
        return {};
    }

    /**
     * ! Unlike the `advance()`, canonically returns a self-reference,
     * ! meaning that the error must be propagated in a different way.
     * ! So we promote this iterator to `end()`, once an error occurs.
     */
    inline keys_stream_t& operator++() noexcept {
        status_t status = advance();
        if (status)
            return *this;

        fetched_keys_ = {};
        fetched_offset_ = 0;
        next_min_key_ = ukv_key_unknown_k;
        return *this;
    }

    ukv_key_t key() const noexcept { return fetched_keys_[fetched_offset_]; }
    ukv_key_t operator*() const noexcept { return key(); }
    status_t seek_to_first() noexcept { return seek(std::numeric_limits<ukv_key_t>::min()); }
    status_t seek_to_next_batch() noexcept { return seek(next_min_key_); }

    /**
     * @brief Exposes all the fetched keys at once, including the passed ones.
     * Should be used with `seek_to_next_batch`. Next `advance` will do the same.
     */
    indexed_range_gt<ukv_key_t const*> keys_batch() noexcept {
        fetched_offset_ = fetched_keys_.size();
        return {fetched_keys_.begin(), fetched_keys_.end()};
    }

    bool is_end() const noexcept {
        return next_min_key_ == ukv_key_unknown_k && fetched_offset_ >= fetched_keys_.size();
    }

    bool operator==(keys_stream_t const& other) const noexcept {
        if (col_ != other.col_)
            return false;
        if (is_end() || other.is_end())
            return is_end() == other.is_end();
        return key() == other.key();
    }

    bool operator!=(keys_stream_t const& other) const noexcept {
        if (col_ == other.col_)
            return true;
        if (is_end() || other.is_end())
            return is_end() != other.is_end();
        return key() != other.key();
    }
};

using keys_range_t = range_gt<keys_stream_t>;

inline expected_gt<keys_range_t> keys_range(ukv_t db,
                                            ukv_collection_t col = ukv_default_collection_k,
                                            ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                                            ukv_key_t max_key = ukv_key_unknown_k,
                                            std::size_t read_ahead = keys_stream_t::default_read_ahead_k,
                                            ukv_txn_t txn = nullptr) {

    keys_stream_t b {db, col, read_ahead, txn};
    keys_stream_t e {db, col, read_ahead, txn};
    status_t status = b.seek(min_key);
    if (!status)
        return status;
    status = e.seek(max_key);
    if (!status)
        return status;

    keys_range_t result {std::move(b), std::move(e)};
    return result;
}

/**
 * @brief Implements multi-way set intersection to join entities
 * from different collections, that have matching identifiers.
 *
 * Implementation-wise, scans the smallest collection and batch-selects
 * in others.
 */
struct collections_join_t {

    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_arena_t* arena = nullptr;

    collections_view_t cols;
    ukv_key_t next_min_key_ = 0;
    ukv_size_t window_size = 0;

    strided_range_gt<ukv_key_t*> fetched_keys_;
    strided_range_gt<ukv_val_len_t> fetched_lengths;
};

class db_t;
class db_session_t;
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
        std::swap(db_, other.db_);
        std::swap(col_, other.col_);
        return *this;
    }
    inline operator ukv_collection_t() const noexcept { return col_; }
    inline ukv_collection_t* internal_cptr() noexcept { return &col_; }
    inline ukv_t db() const noexcept { return db_; }

    inline expected_gt<std::size_t> size() const noexcept { return 0; }

    auto keys(ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
              ukv_key_t max_key = ukv_key_unknown_k,
              std::size_t read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {
        return keys_range(db_, col_, min_key, max_key, read_ahead, nullptr);
    }
};

/**
 * @brief Unlike `db_session_t`, not only allows planning and batching read
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

    inline entries_ref_t operator[](located_keys_view_t located) noexcept {
        return {
            db_,
            txn_,
            arena_.internal_cptr(),
            located.members(&located_key_t::collection),
            located.members(&located_key_t::key),
        };
    }

    inline entries_ref_t operator[](keys_view_t keys) noexcept { return {db_, txn_, arena_.internal_cptr(), {}, keys}; }

    status_t reset() {
        status_t status;
        ukv_txn_begin(db_, 0, ukv_options_default_k, &txn_, status.internal_cptr());
        return status;
    }

    status_t commit(ukv_options_t options = ukv_options_default_k) {
        status_t status;
        ukv_txn_commit(txn_, options, status.internal_cptr());
        return status;
    }

    auto keys(ukv_collection_t col = ukv_default_collection_k,
              ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
              ukv_key_t max_key = ukv_key_unknown_k,
              std::size_t read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {
        return keys_range(db_, col, min_key, max_key, read_ahead, txn_);
    }
};

/**
 * @brief A RAII abstraction to handle temporary aligned arena
 * for requests coming from a single user thread and planning the
 * lazy lookups.
 */
class db_session_t {
    ukv_t db_ = nullptr;
    managed_arena_t arena_;
    std::vector<located_key_t> lazy_lookups_;

  public:
    db_session_t(ukv_t db) : db_(db), arena_(db) {}
    db_session_t(db_session_t const&) = delete;
    db_session_t(db_session_t&& other) noexcept
        : db_(other.db_), arena_(std::move(other.arena_)), lazy_lookups_(std::move(other.lazy_lookups_)) {}

    inline ukv_t db() const noexcept { return db_; }
    inline managed_arena_t& arena() noexcept { return arena_; }

    inline entries_ref_t operator[](located_keys_view_t located) noexcept {
        return entries_ref_t {
            db_,
            nullptr,
            arena_.internal_cptr(),
            located.members(&located_key_t::collection),
            located.members(&located_key_t::key),
        };
    }

    inline entries_ref_t operator[](keys_view_t keys) noexcept {
        return entries_ref_t {db_, nullptr, arena_.internal_cptr(), {}, keys};
    }

    inline db_session_t& new_plan() noexcept {
        lazy_lookups_.clear();
        return *this;
    }

    inline db_session_t& plan(located_key_t located) {
        lazy_lookups_.push_back(located);
        return *this;
    }

    inline entries_ref_t sample() noexcept { return operator[](located_keys_view_t {lazy_lookups_}); }

    expected_gt<txn_t> transact() {
        status_t status;
        ukv_txn_t raw = nullptr;
        ukv_txn_begin(db_, 0, ukv_options_default_k, &raw, status.internal_cptr());
        if (!status)
            return {std::move(status), txn_t {db_, nullptr}};
        else
            return txn_t {db_, raw};
    }

    auto keys(ukv_collection_t col = ukv_default_collection_k,
              ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
              ukv_key_t max_key = ukv_key_unknown_k,
              std::size_t read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {
        return keys_range(db_, col, min_key, max_key, read_ahead, nullptr);
    }

    expected_gt<bool> contains(std::string_view name) noexcept {
        status_t status;
        ukv_size_t count = 0;
        ukv_str_view_t names = nullptr;
        ukv_collection_list(db_, &count, &names, arena_.internal_cptr(), status.internal_cptr());
        if (!status)
            return status;

        while (count) {
            auto len = std::strlen(names);
            auto found = std::string_view(names, len);
            if (found == name)
                return true;

            names += len + 1;
            --count;
        }
        return false;
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

    status_t open(std::string const& config) {
        status_t status;
        ukv_open(config.c_str(), &db_, status.internal_cptr());
        return status;
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
    db_session_t session() { return {db_}; }

    expected_gt<collection_t> operator[](std::string const& name) noexcept { return collection(name); }

    expected_gt<collection_t> collection(std::string const& name) noexcept {
        status_t status;
        ukv_collection_t col = nullptr;
        ukv_collection_open(db_, name.c_str(), nullptr, &col, status.internal_cptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col};
    }

    status_t remove(std::string const& name) noexcept {
        status_t status;
        ukv_collection_remove(db_, name.c_str(), status.internal_cptr());
        return status;
    }
};

} // namespace unum::ukv
