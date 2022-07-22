/**
 * @file db.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings for @see "ukv/db.h".
 */

#pragma once
#include <string>  // NULL-terminated names
#include <cstring> // `std::strlen`
#include <memory>  // `std::enable_shared_from_this`
#include <variant> // `std::variant`
#include <mutex>   // `std::mutex` for `default_arena_`

#include "ukv/ukv.h"
#include "ukv/utility.hpp"

namespace unum::ukv {

class value_refs_t;
class collection_t;
class keys_stream_t;
class keys_range_t;
class txn_t;
class db_t;

/**
 * @brief A proxy object, that allows both lookups and writes
 * with `[]` and assignment operators for a batch of keys
 * simultaneously.
 */
class value_refs_t {
  protected:
    ukv_t db_ = nullptr;
    ukv_txn_t txn_ = nullptr;

    collections_view_t cols_;
    keys_view_t keys_;
    fields_view_t fields_;

    any_arena_t arena_;

    inline expected_gt<taped_values_view_t> any_get(ukv_doc_format_t format, ukv_options_t options) noexcept;
    inline status_t any_set(disjoint_values_view_t vals, ukv_doc_format_t format, ukv_options_t options) noexcept;

  public:
    inline value_refs_t(
        ukv_t db, ukv_txn_t txn, collections_view_t cols, keys_view_t keys, fields_view_t fields = {}) noexcept
        : db_(db), txn_(txn), cols_(cols), keys_(keys), fields_(fields), arena_(db) {}

    inline value_refs_t(value_refs_t const&) = delete;
    inline value_refs_t& operator=(value_refs_t const&) = delete;

    inline value_refs_t(value_refs_t&& other) noexcept
        : db_(std::exchange(other.db_, nullptr)), txn_(std::exchange(other.txn_, nullptr)),
          cols_(std::exchange(other.cols_, {})), keys_(std::exchange(other.keys_, {})),
          fields_(std::exchange(other.fields_, {})), arena_(std::move(other.arena_)) {}

    inline value_refs_t& operator=(value_refs_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(txn_, other.txn_);
        std::swap(cols_, other.cols_);
        std::swap(keys_, other.keys_);
        std::swap(fields_, other.fields_);
        std::swap(arena_, other.arena_);
        return *this;
    }

    inline value_refs_t& on(managed_arena_t& arena) noexcept {
        arena_ = arena;
        return *this;
    }

    inline value_refs_t& from(ukv_txn_t txn) noexcept {
        txn_ = txn;
        return *this;
    }

    inline expected_gt<taped_values_view_t> get( //
        ukv_doc_format_t format = ukv_doc_format_binary_k,
        bool track = false) & noexcept {
        auto options = track ? ukv_option_read_track_k : ukv_options_default_k;
        return any_get(format, options);
    }

    inline expected_gt<indexed_range_gt<ukv_val_len_t*>> lengths( //
        ukv_doc_format_t format = ukv_doc_format_binary_k,
        bool track = false) & noexcept {

        auto options = static_cast<ukv_options_t>((track ? ukv_option_read_track_k : ukv_options_default_k) |
                                                  ukv_option_read_lengths_k);
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
    inline expected_gt<strided_range_gt<bool>> contains( //
        ukv_doc_format_t format = ukv_doc_format_binary_k,
        bool track = false) & noexcept {

        auto maybe = lengths(format, track);
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
    inline status_t set( //
        disjoint_values_view_t vals,
        ukv_doc_format_t format = ukv_doc_format_binary_k,
        bool flush = false) noexcept {
        return any_set(vals, format, flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Removes both the keys_ and the associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    inline status_t erase(bool flush = false) noexcept { //
        return set(disjoint_values_view_t {}, ukv_doc_format_binary_k, flush);
    }

    /**
     * @brief Keeps the keys_, but clears the contents of associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    inline status_t clear(bool flush = false) noexcept {
        ukv_val_ptr_t any = reinterpret_cast<ukv_val_ptr_t>(this);
        ukv_val_len_t len = 0;
        return set(disjoint_values_view_t {.contents = {any}, .offsets = {}, .lengths = {len}},
                   ukv_doc_format_binary_k,
                   flush);
    }

    inline operator expected_gt<taped_values_view_t>() & noexcept { return get(); }

    inline value_refs_t& operator=(disjoint_values_view_t vals) noexcept(false) {
        auto status = set(vals);
        status.throw_unhandled();
        return *this;
    }

    inline value_refs_t& operator=(nullptr_t) noexcept(false) {
        auto status = erase();
        status.throw_unhandled();
        return *this;
    }

    inline collections_view_t cols() const noexcept { return cols_; }
    inline keys_view_t keys() const noexcept { return keys_; }
    inline fields_view_t fields() const noexcept { return fields_; }

    inline value_refs_t& cols(collections_view_t cols) noexcept {
        cols_ = cols;
        return *this;
    }
    inline value_refs_t& keys(keys_view_t keys) noexcept {
        keys_ = keys;
        return *this;
    }
    inline value_refs_t& fields(fields_view_t fields) noexcept {
        fields_ = fields;
        return *this;
    }
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
    keys_stream_t& operator++() noexcept {
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

struct size_range_t {
    std::size_t min = 0;
    std::size_t max = 0;
};

struct size_estimates_t {
    size_range_t cardinality;
    size_range_t bytes_in_values;
    size_range_t bytes_on_disk;
};

class keys_range_t {

    ukv_t db_;
    ukv_txn_t txn_;
    ukv_collection_t col_;
    ukv_key_t min_key_;
    ukv_key_t max_key_;
    std::size_t read_ahead_;

  public:
    keys_range_t(ukv_t db,
                 ukv_txn_t txn = nullptr,
                 ukv_collection_t col = ukv_default_collection_k,
                 ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                 ukv_key_t max_key = ukv_key_unknown_k,
                 std::size_t read_ahead = keys_stream_t::default_read_ahead_k) noexcept
        : db_(db), txn_(txn), col_(col), min_key_(min_key), max_key_(max_key), read_ahead_(read_ahead) {}

    keys_range_t(keys_range_t const&) = default;
    keys_range_t& operator=(keys_range_t const&) = default;

    expected_gt<keys_stream_t> find_begin() noexcept {
        keys_stream_t stream {db_, col_, read_ahead_, txn_};
        status_t status = stream.seek(min_key_);
        return {std::move(status), std::move(stream)};
    }

    expected_gt<keys_stream_t> find_end() noexcept {
        keys_stream_t stream {db_, col_, read_ahead_, txn_};
        status_t status = stream.seek(max_key_);
        return {std::move(status), std::move(stream)};
    }

    expected_gt<size_estimates_t> find_size() noexcept;

    keys_stream_t begin() noexcept(false) {
        auto maybe = find_begin();
        maybe.throw_unhandled();
        return *std::move(maybe);
    }

    keys_stream_t end() noexcept(false) {
        auto maybe = find_end();
        maybe.throw_unhandled();
        return *std::move(maybe);
    }
};

/**
 * @brief RAII abstraction wrapping a collection handle.
 * Generally cheap to construct. Can address both collections
 * "HEAD" state, as well as some "snapshot"/"transaction" view.
 */
class collection_t {
    ukv_t db_ = nullptr;
    ukv_collection_t col_ = ukv_default_collection_k;
    ukv_txn_t txn_ = nullptr;

  public:
    inline collection_t() = default;
    inline collection_t(ukv_t db_ptr,
                        ukv_collection_t col_ptr = ukv_default_collection_k,
                        ukv_txn_t txn = nullptr) noexcept
        : db_(db_ptr), col_(col_ptr), txn_(txn) {}

    inline collection_t(collection_t&& other) noexcept
        : db_(other.db_), col_(std::exchange(other.col_, ukv_default_collection_k)),
          txn_(std::exchange(other.txn_, nullptr)) {}
    inline ~collection_t() noexcept {
        if (col_)
            ukv_collection_free(db_, col_);
    }
    inline collection_t& operator=(collection_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(col_, other.col_);
        std::swap(txn_, other.txn_);
        return *this;
    }
    inline operator ukv_collection_t() const noexcept { return col_; }
    inline ukv_collection_t* internal_cptr() noexcept { return &col_; }
    inline ukv_t db() const noexcept { return db_; }
    inline ukv_txn_t txn() const noexcept { return txn_; }

    inline expected_gt<std::size_t> size() const noexcept { return 0; }

    inline keys_range_t keys(ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                             ukv_key_t max_key = ukv_key_unknown_k,
                             std::size_t read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {
        return {db_, txn_, col_, min_key, max_key, read_ahead};
    }

    inline value_refs_t operator[](keys_view_t keys) const noexcept { //
        return {db_, txn_, strided_range_gt<ukv_collection_t const> {&col_, 0, keys.size()}, keys, fields_view_t {}};
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

  public:
    txn_t(ukv_t db, ukv_txn_t txn) noexcept : db_(db), txn_(txn) {}
    txn_t(txn_t const&) = delete;
    txn_t(txn_t&& other) noexcept : db_(other.db_), txn_(std::exchange(other.txn_, nullptr)) {}

    inline ukv_t db() const noexcept { return db_; }
    inline operator ukv_txn_t() const noexcept { return txn_; }

    ~txn_t() noexcept {
        if (txn_)
            ukv_txn_free(db_, txn_);
    }

    value_refs_t operator[](located_keys_view_t located) noexcept {
        return {db_, txn_, located.members(&located_key_t::collection), located.members(&located_key_t::key)};
    }

    value_refs_t operator[](keys_view_t keys) noexcept { //
        return {db_, txn_, {}, keys};
    }

    status_t reset(bool snapshot = false) noexcept {
        status_t status;
        auto options = snapshot ? ukv_option_txn_snapshot_k : ukv_options_default_k;
        ukv_txn_begin(db_, 0, options, &txn_, status.internal_cptr());
        return status;
    }

    status_t commit(bool flush = false) noexcept {
        status_t status;
        auto options = flush ? ukv_option_write_flush_k : ukv_options_default_k;
        ukv_txn_commit(txn_, options, status.internal_cptr());
        return status;
    }

    expected_gt<collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<collection_t>() noexcept { return collection(""); }

    expected_gt<collection_t> collection(ukv_str_view_t name = "") noexcept {
        status_t status;
        ukv_collection_t col = nullptr;
        ukv_collection_open(db_, name, nullptr, &col, status.internal_cptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col, txn_};
    }
};

/**
 * @brief Thread-Safe DataBase instance encapsulator, which is responsible for
 * > session-allocation for fine-grained operations,
 * > globally blocking operations, like restructuring.
 * This object must leave at least as long, as the last session using it.
 *
 * @section Thread Safety
 * Matches the C implementation. Everything except `open`/`close` can be called
 * from any thread.
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

    /**
     * @brief Checks if a collection with requested `name` is present in the DB.
     * @param memory Temporary memory required for storing the execution results.
     */
    expected_gt<bool> contains(std::string_view name, managed_arena_t& memory) noexcept {
        if (name.empty())
            return true;

        status_t status;
        ukv_size_t count = 0;
        ukv_str_view_t names = nullptr;
        ukv_collection_list(db_, &count, &names, memory.internal_cptr(), status.internal_cptr());
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

    /**
     * @brief Checks if a collection with requested `name` is present in the DB.
     */
    expected_gt<bool> contains(std::string_view name) noexcept {
        managed_arena_t arena(db_);
        return contains(name, arena);
    }

    expected_gt<collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<collection_t>() noexcept { return collection(""); }

    expected_gt<collection_t> collection(ukv_str_view_t name = "") noexcept {
        status_t status;
        ukv_collection_t col = nullptr;
        ukv_collection_open(db_, name, nullptr, &col, status.internal_cptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col};
    }

    status_t remove(ukv_str_view_t name) noexcept {
        status_t status;
        ukv_collection_remove(db_, name, status.internal_cptr());
        return status;
    }

    expected_gt<txn_t> transact() {
        status_t status;
        ukv_txn_t raw = nullptr;
        ukv_txn_begin(db_, 0, ukv_options_default_k, &raw, status.internal_cptr());
        if (!status)
            return {std::move(status), txn_t {db_, nullptr}};
        else
            return txn_t {db_, raw};
    }
};

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

inline expected_gt<taped_values_view_t> value_refs_t::any_get(ukv_doc_format_t format, ukv_options_t options) noexcept {

    status_t status;
    ukv_val_len_t* found_lengths = nullptr;
    ukv_val_ptr_t found_values = nullptr;

    if (fields_ || format != ukv_doc_format_binary_k)
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
                      arena_.internal_cptr(),
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
                 arena_.internal_cptr(),
                 status.internal_cptr());
    if (!status)
        return status;

    return taped_values_view_t {found_lengths, found_values, keys_.count()};
}

inline status_t value_refs_t::any_set(disjoint_values_view_t vals,
                                      ukv_doc_format_t format,
                                      ukv_options_t options) noexcept {
    status_t status;
    if (fields_ || format != ukv_doc_format_binary_k)
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
                       arena_.internal_cptr(),
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
                  arena_.internal_cptr(),
                  status.internal_cptr());
    return status;
}

} // namespace unum::ukv
