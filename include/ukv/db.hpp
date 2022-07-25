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

class collection_t;
class keys_stream_t;
class keys_range_t;
class txn_t;
class db_t;

using doc_fmt_t = ukv_doc_format_t;

template <typename locations_at>
class member_refs_gt;

/**
 * @brief Unlike `member_refs_gt`, yields results on a temporary arena,
 * which is less efficient, but requires less code. To reuse a memory
 * buffer, just call `.on(arena)` to convert to `member_refs_gt`.
 */
template <typename locations_at>
class managed_refs_gt {
  protected:
    ukv_t db_ = nullptr;
    ukv_txn_t txn_ = nullptr;
    ukv_arena_t* arena_ = nullptr;

    locations_at locations_;

    template <typename values_arg_at>
    status_t any_set(values_arg_at&&, doc_fmt_t, ukv_options_t) noexcept;
    expected_gt<taped_values_view_t> any_get(doc_fmt_t, ukv_options_t) noexcept;

  public:
    using contains_t = strided_range_gt<bool>;
    using lengths_t = indexed_range_gt<ukv_val_len_t*>;
    operator expected_gt<taped_values_view_t>() noexcept { return get(); }
    locations_at& locations() noexcept { return locations_; }
    locations_at const& locations() const noexcept { return locations_; }

    managed_refs_gt(ukv_t db, ukv_txn_t txn, managed_arena_t& arena, locations_at&& locations) noexcept
        : db_(db), txn_(txn), arena_(arena.member_ptr()), locations_(std::forward<locations_at>(locations)) {}

    managed_refs_gt(managed_refs_gt const&) = delete;
    managed_refs_gt& operator=(managed_refs_gt const&) = delete;

    managed_refs_gt(managed_refs_gt&& other) noexcept
        : db_(std::exchange(other.db_, nullptr)), txn_(std::exchange(other.txn_, nullptr)),
          txn_(std::exchange(other.arena_, nullptr)), locations_(std::move(other.locations_)) {}

    managed_refs_gt& operator=(managed_refs_gt&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(txn_, other.txn_);
        std::swap(arena_, other.arena_);
        std::swap(locations_, other.locations_);
        return *this;
    }

    expected_gt<taped_values_view_t> get(doc_fmt_t format = ukv_doc_format_binary_k, bool track = false) noexcept {
        auto options = track ? ukv_option_read_track_k : ukv_options_default_k;
        return any_get(format, options);
    }

    expected_gt<lengths_t> lengths(doc_fmt_t format = ukv_doc_format_binary_k, bool track = false) noexcept {
        auto options = (track ? ukv_option_read_track_k : ukv_options_default_k) | ukv_option_read_lengths_k;
        auto maybe = any_get(format, static_cast<ukv_options_t>(options));
        if (!maybe)
            return maybe.release_status();

        auto found_lengths = maybe->lengths();
        return lengths_t {found_lengths, found_lengths + location_get_count(locations_)};
    }

    /**
     * @brief Checks if requested keys are present in the store.
     * ! Related values may be empty strings.
     */
    expected_gt<contains_t> contains(doc_fmt_t format = ukv_doc_format_binary_k, bool track = false) noexcept {

        auto maybe = lengths(format, track);
        if (!maybe)
            return maybe.release_status();

        // Transform the `found_lengths` into booleans.
        auto found_lengths = maybe->begin();
        std::transform(found_lengths,
                       found_lengths + location_get_count(locations_),
                       found_lengths,
                       [](ukv_val_len_t len) { return len != ukv_val_len_missing_k; });

        // Cast assuming "Little-Endian" architecture
        auto last_byte_offset = 0; // sizeof(ukv_val_len_t) - sizeof(bool);
        auto booleans = reinterpret_cast<bool*>(found_lengths);
        return contains_t {booleans + last_byte_offset, sizeof(ukv_val_len_t), location_get_count(locations_)};
    }

    /**
     * @brief Pair-wise assigns values to keys located in this proxy objects.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    template <typename values_arg_at>
    status_t set(values_arg_at&& vals, doc_fmt_t format = ukv_doc_format_binary_k, bool flush = false) noexcept {
        return any_set(std::forward<values_arg_at>(vals),
                       format,
                       flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Removes both the keys and the associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t erase(bool flush = false) noexcept { //
        return set(nullptr, ukv_doc_format_binary_k, flush);
    }

    /**
     * @brief Keeps the locations_, but clears the contents of associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t clear(bool flush = false) noexcept {
        ukv_val_ptr_t any = reinterpret_cast<ukv_val_ptr_t>(this);
        ukv_val_len_t len = 0;
        return set(
            values_arg_t {
                .contents_begin = {&any},
                .offsets_begin = {},
                .lengths_begin = {&len},
            },
            ukv_doc_format_binary_k,
            flush);
    }

    template <typename values_arg_at>
    managed_refs_gt& operator=(values_arg_at&& vals) noexcept(false) {
        auto status = set(std::forward<values_arg_at>(vals));
        status.throw_unhandled();
        return *this;
    }

    managed_refs_gt& operator=(nullptr_t) noexcept(false) {
        auto status = erase();
        status.throw_unhandled();
        return *this;
    }
};

/**
 * @brief A proxy object, that allows both lookups and writes
 * with `[]` and assignment operators for a batch of keys
 * simultaneously.
 * Following assignment combinations are possible:
 * > one value to many keys
 * > many values to many keys
 * > one value to one key
 * The only impossible combination is assigning many values to one key.
 *
 * @tparam locations_at Type describing the address of a value in DBMS.
 * > (ukv_collection_t?, ukv_key_t, ukv_field_t?): Single KV-pair location.
 * > (ukv_collection_t*, ukv_key_t*, ukv_field_t*): Externally owned range of keys.
 * > (ukv_collection_t[x], ukv_key_t[x], ukv_field_t[x]): On-stack array of addresses.
 *
 * @tparam arena_at Either a
 */
template <typename locations_at>
class member_refs_gt {
  protected:
    ukv_t db_ = nullptr;
    ukv_txn_t txn_ = nullptr;

    locations_at locations_;

  public:
    using contains_t = strided_range_gt<bool>;
    using lengths_t = indexed_range_gt<ukv_val_len_t*>;
    operator expected_gt<std::pair<taped_values_view_t, managed_arena_t>>() noexcept { return get(); }
    locations_at& locations() noexcept { return locations_; }
    locations_at const& locations() const noexcept { return locations_; }

    member_refs_gt(ukv_t db, ukv_txn_t txn, locations_at&& location) noexcept
        : db_(db), txn_(txn), locations_(std::forward<locations_at>(location)) {}

    member_refs_gt(member_refs_gt const&) = delete;
    member_refs_gt& operator=(member_refs_gt const&) = delete;

    member_refs_gt(member_refs_gt&& other) noexcept
        : db_(std::exchange(other.db_, nullptr)), txn_(std::exchange(other.txn_, nullptr)),
          locations_(std::move(other.locations_)) {}

    member_refs_gt& operator=(member_refs_gt&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(txn_, other.txn_);
        std::swap(locations_, other.locations_);
        return *this;
    }

    member_refs_gt& from(ukv_txn_t txn) noexcept {
        txn_ = txn;
        return *this;
    }

    managed_refs_gt<locations_at> on(managed_arena_t& arena) && noexcept {
        return {db_, txn_, arena, std::forward<locations_at>(locations_)};
    }

    managed_refs_gt<locations_at const&> on(managed_arena_t& arena) const& noexcept {
        return {db_, txn_, arena, locations_};
    }

    expected_gt<std::pair<taped_values_view_t, managed_arena_t>> //
    get(doc_fmt_t format = ukv_doc_format_binary_k, bool track = false) noexcept {
        managed_arena_t arena(db_);
        auto maybe = on(arena).get(format, track);
        if (!maybe)
            return {maybe.release_status(), {{}, std::move(arena)}};
        return std::make_pair(*std::move(maybe), std::move(arena));
    }

    expected_gt<std::pair<lengths_t, managed_arena_t>> //
    lengths(doc_fmt_t format = ukv_doc_format_binary_k, bool track = false) noexcept {
        managed_arena_t arena(db_);
        auto maybe = on(arena).lengths(format, track);
        if (!maybe)
            return {maybe.release_status(), {{}, std::move(arena)}};
        return std::make_pair(*std::move(maybe), std::move(arena));
    }

    expected_gt<std::pair<contains_t, managed_arena_t>> //
    contains(doc_fmt_t format = ukv_doc_format_binary_k, bool track = false) noexcept {
        managed_arena_t arena(db_);
        auto maybe = on(arena).contains(format, track);
        if (!maybe)
            return {maybe.release_status(), {{}, std::move(arena)}};
        return std::make_pair(*std::move(maybe), std::move(arena));
    }

    template <typename values_arg_at>
    expected_gt<managed_arena_t> //
    set(values_arg_at&& vals, doc_fmt_t format = ukv_doc_format_binary_k, bool flush = false) noexcept {
        managed_arena_t arena(db_);
        auto maybe = on(arena).set(std::forward<values_arg_at>(vals), format, flush);
        return {std::move(maybe), std::move(arena)};
    }

    expected_gt<managed_arena_t> erase(bool flush = false) noexcept { //
        managed_arena_t arena(db_);
        auto maybe = on(arena).erase(flush);
        return {std::move(maybe), std::move(arena)};
    }

    expected_gt<managed_arena_t> clear(bool flush = false) noexcept {
        managed_arena_t arena(db_);
        auto maybe = on(arena).clear(flush);
        return {std::move(maybe), std::move(arena)};
    }

    template <typename values_arg_at>
    member_refs_gt& operator=(values_arg_at&& vals) noexcept(false) {
        auto status = set(std::forward<values_arg_at>(vals));
        status.throw_unhandled();
        return *this;
    }

    member_refs_gt& operator=(nullptr_t) noexcept(false) {
        auto status = erase();
        status.throw_unhandled();
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
                 arena_.member_ptr(),
                 status.member_ptr());
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

    expected_gt<size_estimates_t> find_size() noexcept {
        status_t status;
        managed_arena_t arena(db_);
        size_estimates_t result;
        ukv_size(db_,
                 txn_,
                 1,
                 &col_,
                 0,
                 &min_key_,
                 0,
                 &max_key_,
                 0,
                 ukv_options_default_k,
                 &result.cardinality.min,
                 arena.member_ptr(),
                 status.member_ptr());
        if (!status)
            return status;
        return result;
    }

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
    inline ukv_collection_t* member_ptr() noexcept { return &col_; }
    inline ukv_t db() const noexcept { return db_; }
    inline ukv_txn_t txn() const noexcept { return txn_; }

    inline expected_gt<std::size_t> size() const noexcept { return 0; }

    inline keys_range_t keys(ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                             ukv_key_t max_key = ukv_key_unknown_k,
                             std::size_t read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {
        return {db_, txn_, col_, min_key, max_key, read_ahead};
    }

    inline member_refs_gt<keys_arg_t> operator[](keys_view_t keys) noexcept {
        keys_arg_t located;
        located.collections_begin = &col_;
        located.keys_begin = keys.begin();
        located.count = keys.size();
        return at(std::move(located));
    }

    template <typename keys_arg_at>
    member_refs_gt<keys_arg_at> operator[](keys_arg_at&& keys) noexcept { //
        return at(std::forward<keys_arg_at>(keys));
    }

    template <typename keys_arg_at>
    member_refs_gt<keys_arg_at> at(keys_arg_at&& keys) noexcept { //
        return {db_, txn_, std::forward<keys_arg_at>(keys)};
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

    member_refs_gt<keys_arg_t> operator[](located_keys_view_t cols_and_keys) noexcept {
        keys_arg_t located;
        located.collections_begin = cols_and_keys.members(&located_key_t::collection).begin();
        located.keys_begin = cols_and_keys.members(&located_key_t::key).begin();
        located.count = cols_and_keys.size();
        return {db_, txn_, std::move(located)};
    }

    member_refs_gt<keys_arg_t> operator[](keys_view_t keys) noexcept { //
        keys_arg_t located;
        located.keys_begin = keys.begin();
        located.count = keys.size();
        return {db_, txn_, std::move(located)};
    }

    template <typename keys_arg_at>
    member_refs_gt<keys_arg_at> operator[](keys_arg_at keys) noexcept { //
        return {db_, txn_, std::move(keys)};
    }

    status_t reset(bool snapshot = false) noexcept {
        status_t status;
        auto options = snapshot ? ukv_option_txn_snapshot_k : ukv_options_default_k;
        ukv_txn_begin(db_, 0, options, &txn_, status.member_ptr());
        return status;
    }

    status_t commit(bool flush = false) noexcept {
        status_t status;
        auto options = flush ? ukv_option_write_flush_k : ukv_options_default_k;
        ukv_txn_commit(txn_, options, status.member_ptr());
        return status;
    }

    expected_gt<collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<collection_t>() noexcept { return collection(""); }

    expected_gt<collection_t> collection(ukv_str_view_t name = "") noexcept {
        status_t status;
        ukv_collection_t col = nullptr;
        ukv_collection_open(db_, name, nullptr, &col, status.member_ptr());
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

    status_t open(std::string const& config = "") {
        status_t status;
        ukv_open(config.c_str(), &db_, status.member_ptr());
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
        ukv_collection_list(db_, &count, &names, memory.member_ptr(), status.member_ptr());
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
        ukv_collection_open(db_, name, nullptr, &col, status.member_ptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col};
    }

    status_t remove(ukv_str_view_t name) noexcept {
        status_t status;
        ukv_collection_remove(db_, name, status.member_ptr());
        return status;
    }

    expected_gt<txn_t> transact() {
        status_t status;
        ukv_txn_t raw = nullptr;
        ukv_txn_begin(db_, 0, ukv_options_default_k, &raw, status.member_ptr());
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

template <typename locations_at>
expected_gt<taped_values_view_t> managed_refs_gt<locations_at>::any_get(doc_fmt_t format,
                                                                        ukv_options_t options) noexcept {
    status_t status;
    ukv_val_len_t* found_lengths = nullptr;
    ukv_val_ptr_t found_values = nullptr;

    auto count = location_get_count(locations_);
    auto keys = location_get_keys(locations_);
    auto cols = location_get_cols(locations_);
    auto fields = location_get_fields(locations_);

    if (fields || format != ukv_doc_format_binary_k)
        ukv_docs_read( //
            db_,
            txn_,
            count,
            cols.get(),
            cols.stride(),
            keys.get(),
            keys.stride(),
            fields.get(),
            fields.stride(),
            options,
            format,
            &found_lengths,
            &found_values,
            arena_,
            status.member_ptr());
    else
        ukv_read( //
            db_,
            txn_,
            count,
            cols.get(),
            cols.stride(),
            keys.get(),
            keys.stride(),
            options,
            &found_lengths,
            &found_values,
            arena_,
            status.member_ptr());

    if (!status)
        return status;

    return taped_values_view_t {found_lengths, found_values, count};
}

template <typename locations_at>
template <typename values_arg_at>
status_t managed_refs_gt<locations_at>::any_set(values_arg_at&& vals_ref,
                                                doc_fmt_t format,
                                                ukv_options_t options) noexcept {
    status_t status;

    auto count = location_get_count(locations_);
    auto keys = location_get_keys(locations_);
    auto cols = location_get_cols(locations_);
    auto fields = location_get_fields(locations_);

    auto vals = vals_ref;
    auto contents = value_get_contents(vals);
    auto offsets = value_get_offsets(vals);
    auto lengths = value_get_lengths(vals);

    if (fields || format != ukv_doc_format_binary_k)
        ukv_docs_write( //
            db_,
            txn_,
            count,
            cols.get(),
            cols.stride(),
            keys.get(),
            keys.stride(),
            fields.get(),
            fields.stride(),
            options,
            format,
            contents.get(),
            contents.stride(),
            offsets.get(),
            offsets.stride(),
            lengths.get(),
            lengths.stride(),
            arena_,
            status.member_ptr());
    else
        ukv_write( //
            db_,
            txn_,
            count,
            cols.get(),
            cols.stride(),
            keys.get(),
            keys.stride(),
            contents.get(),
            contents.stride(),
            offsets.get(),
            offsets.stride(),
            lengths.get(),
            lengths.stride(),
            options,
            arena_,
            status.member_ptr());
    return status;
}

} // namespace unum::ukv
