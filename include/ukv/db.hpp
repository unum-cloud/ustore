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

template <typename locations_store_t>
class member_refs_gt;

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
 * @tparam locations_store_t Type describing the address of a value in DBMS.
 * > (ukv_collection_t?, ukv_key_t, ukv_field_t?): Single KV-pair location.
 * > (ukv_collection_t*, ukv_key_t*, ukv_field_t*): Externally owned range of keys.
 * > (ukv_collection_t[x], ukv_key_t[x], ukv_field_t[x]): On-stack array of addresses.
 */
template <typename locations_at>
class member_refs_gt {
  public:
    static_assert(!std::is_rvalue_reference_v<locations_at>, //
                  "The internal object can't be an R-value Reference");

    using locations_store_t = location_store_gt<locations_at>;
    using locations_plain_t = typename locations_store_t::plain_t;
    using keys_extractor_t = keys_arg_extractor_gt<locations_plain_t>;
    static constexpr bool is_one_k = keys_extractor_t::is_one_k;

    using value_t = std::conditional_t<is_one_k, value_view_t, taped_values_view_t>;
    using present_t = std::conditional_t<is_one_k, bool, strided_range_gt<bool>>;
    using length_t = std::conditional_t<is_one_k, ukv_val_len_t, indexed_range_gt<ukv_val_len_t*>>;

  protected:
    ukv_t db_ = nullptr;
    ukv_txn_t txn_ = nullptr;
    any_arena_t arena_;
    locations_store_t locations_;
    ukv_doc_format_t format_ = ukv_doc_format_binary_k;

    template <typename values_arg_at>
    status_t any_assign(values_arg_at&&, ukv_options_t) noexcept;
    expected_gt<value_t> any_get(ukv_options_t) noexcept;

  public:
    member_refs_gt(ukv_t db,
                   ukv_txn_t txn,
                   locations_store_t locations,
                   ukv_doc_format_t format = ukv_doc_format_binary_k) noexcept
        : db_(db), txn_(txn), locations_(locations), format_(format) {}

    member_refs_gt(member_refs_gt const&) = delete;
    member_refs_gt& operator=(member_refs_gt const&) = delete;

    member_refs_gt(member_refs_gt&& other) noexcept
        : db_(std::exchange(other.db_, nullptr)), txn_(std::exchange(other.txn_, nullptr)),
          arena_(std::exchange(other.arena_, any_arena_t {db_})), locations_(std::move(other.locations_)),
          format_(other.format_) {}

    member_refs_gt& operator=(member_refs_gt&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(txn_, other.txn_);
        std::swap(arena_, other.arena_);
        std::swap(locations_, other.locations_);
        std::swap(format_, other.format_);
        return *this;
    }

    member_refs_gt& on(arena_t& arena) noexcept {
        arena_ = arena;
        return *this;
    }

    member_refs_gt& as(ukv_doc_format_t format) noexcept {
        format_ = format;
        return *this;
    }

    given_gt<value_t> value(bool track = false) noexcept {
        return any_get(track ? ukv_option_read_track_k : ukv_options_default_k);
    }

    operator given_gt<value_t>() noexcept { return value(); }

    given_gt<length_t> length(bool track = false) noexcept {
        auto options = (track ? ukv_option_read_track_k : ukv_options_default_k) | ukv_option_read_lengths_k;
        auto maybe = any_get(static_cast<ukv_options_t>(options));
        if (!maybe)
            return maybe.release_status();

        if constexpr (is_one_k) {
            length_t result = *maybe ? maybe->size() : ukv_val_len_missing_k;
            return {result, maybe.release_arena()};
        }
        else {
            auto found_lengths = maybe->lengths();
            auto count = keys_extractor_t {}.count(locations_.ref());
            length_t result {found_lengths, found_lengths + count};
            return {result, maybe.release_arena()};
        }
    }

    /**
     * @brief Checks if requested keys are present in the store.
     * ! Related values may be empty strings.
     */
    given_gt<present_t> present(bool track = false) noexcept {

        auto maybe = length(track);
        if (!maybe)
            return maybe.release_status();

        if constexpr (is_one_k) {
            present_t result = *maybe != ukv_val_len_missing_k;
            return {result, maybe.release_arena()};
        }
        else {
            // Transform the `found_lengths` into booleans.
            auto found_lengths = maybe->begin();
            auto count = keys_extractor_t {}.count(locations_.ref());
            std::transform(found_lengths, found_lengths + count, found_lengths, [](ukv_val_len_t len) {
                return len != ukv_val_len_missing_k;
            });

            // Cast assuming "Little-Endian" architecture
            auto last_byte_offset = 0; // sizeof(ukv_val_len_t) - sizeof(bool);
            auto booleans = reinterpret_cast<bool*>(found_lengths);
            present_t result {booleans + last_byte_offset, sizeof(ukv_val_len_t), count};
            return {result, maybe.release_arena()};
        }
    }

    /**
     * @brief Pair-wise assigns values to keys located in this proxy objects.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    template <typename values_arg_at>
    status_t assign(values_arg_at&& vals, bool flush = false) noexcept {
        return any_assign(std::forward<values_arg_at>(vals), flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Removes both the keys and the associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t erase(bool flush = false) noexcept { //
        return assign(nullptr, flush);
    }

    /**
     * @brief Keeps the keys, but clears the contents of associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t clear(bool flush = false) noexcept {
        ukv_val_ptr_t any = reinterpret_cast<ukv_val_ptr_t>(this);
        ukv_val_len_t len = 0;
        values_arg_t arg {
            .contents_begin = {&any},
            .offsets_begin = {},
            .lengths_begin = {&len},
        };
        return assign(arg, flush);
    }

    template <typename values_arg_at>
    member_refs_gt& operator=(values_arg_at&& vals) noexcept(false) {
        auto status = assign(std::forward<values_arg_at>(vals));
        status.throw_unhandled();
        return *this;
    }

    member_refs_gt& operator=(nullptr_t) noexcept(false) {
        auto status = erase();
        status.throw_unhandled();
        return *this;
    }

    locations_plain_t& locations() noexcept { return locations_.ref(); }
    locations_plain_t& locations() const noexcept { return locations_.ref(); }
};

static_assert(member_refs_gt<ukv_key_t>::is_one_k);
static_assert(std::is_same_v<member_refs_gt<ukv_key_t>::value_t, value_view_t>);
static_assert(member_refs_gt<ukv_key_t>::is_one_k);
static_assert(!member_refs_gt<keys_arg_t>::is_one_k);

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

    arena_t arena_;
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
        if (col_ != other.col_)
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
        arena_t arena(db_);
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
                 reinterpret_cast<ukv_size_t*>(&result.cardinality.min),
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
    ukv_doc_format_t format_ = ukv_doc_format_binary_k;

  public:
    inline collection_t() = default;
    inline collection_t(ukv_t db_ptr,
                        ukv_collection_t col_ptr = ukv_default_collection_k,
                        ukv_txn_t txn = nullptr,
                        ukv_doc_format_t format = ukv_doc_format_binary_k) noexcept
        : db_(db_ptr), col_(col_ptr), txn_(txn), format_(format) {}

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
    collection_t& as(ukv_doc_format_t format) noexcept {
        format_ = format;
        return *this;
    }

    inline keys_range_t keys(ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                             ukv_key_t max_key = ukv_key_unknown_k,
                             std::size_t read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {
        return {db_, txn_, col_, min_key, max_key, read_ahead};
    }

    inline member_refs_gt<keys_arg_t> operator[](keys_view_t const& keys) noexcept { return at(keys); }
    inline member_refs_gt<keys_arg_t> at(keys_view_t const& keys) noexcept {
        keys_arg_t arg;
        arg.collections_begin = &col_;
        arg.keys_begin = keys.begin();
        arg.count = keys.size();
        return {db_, txn_, {std::move(arg)}, format_};
    }

    template <typename keys_arg_at>
    auto operator[](keys_arg_at&& keys) noexcept { //
        return at(std::forward<keys_arg_at>(keys));
    }

    template <typename keys_arg_at>
    auto at(keys_arg_at&& keys) noexcept { //
        constexpr bool is_one_k = is_one<keys_arg_at>();
        if constexpr (is_one_k) {
            using result_t = member_refs_gt<col_key_field_t>;
            using plain_t = std::remove_reference_t<keys_arg_at>;
            static_assert(!sfinae_has_collection_gt<plain_t>::value, "Overwriting existing collection!");
            col_key_field_t arg;
            arg.collection = col_;
            if constexpr (std::is_integral_v<plain_t>)
                arg.key = keys;
            else
                arg.key = keys.key;

            if constexpr (sfinae_has_field_gt<plain_t>::value)
                arg.field = keys.field;
            return result_t {db_, txn_, arg, format_};
        }
        else {
            using locations_t = locations_in_collection_gt<keys_arg_at>;
            using result_t = member_refs_gt<locations_t>;
            return result_t {db_, txn_, locations_t {std::forward<keys_arg_at>(keys), col_}, format_};
        }
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

    member_refs_gt<keys_arg_t> operator[](strided_range_gt<col_key_t const> cols_and_keys) noexcept {
        keys_arg_t arg;
        arg.collections_begin = cols_and_keys.members(&col_key_t::collection).begin();
        arg.keys_begin = cols_and_keys.members(&col_key_t::key).begin();
        arg.count = cols_and_keys.size();
        return {db_, txn_, std::move(arg)};
    }

    member_refs_gt<keys_arg_t> operator[](strided_range_gt<col_key_field_t const> cols_and_keys) noexcept {
        keys_arg_t arg;
        arg.collections_begin = cols_and_keys.members(&col_key_field_t::collection).begin();
        arg.keys_begin = cols_and_keys.members(&col_key_field_t::key).begin();
        arg.fields_begin = cols_and_keys.members(&col_key_field_t::field).begin();
        arg.count = cols_and_keys.size();
        return {db_, txn_, std::move(arg)};
    }

    member_refs_gt<keys_arg_t> operator[](keys_view_t keys) noexcept { //
        keys_arg_t arg;
        arg.keys_begin = keys.begin();
        arg.count = keys.size();
        return {db_, txn_, std::move(arg)};
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
    expected_gt<bool> contains(std::string_view name, arena_t& memory) noexcept {
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
        arena_t arena(db_);
        return contains(name, arena);
    }

    expected_gt<collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<collection_t>() noexcept { return collection(""); }
    expected_gt<collection_t> operator*() noexcept { return collection(""); }

    expected_gt<collection_t> collection(ukv_str_view_t name = "",
                                         ukv_doc_format_t format = ukv_doc_format_binary_k) noexcept {
        status_t status;
        ukv_collection_t col = nullptr;
        ukv_collection_open(db_, name, nullptr, &col, status.member_ptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col, format};
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

    strided_range_gt<ukv_collection_t const> cols;
    ukv_key_t next_min_key_ = 0;
    ukv_size_t window_size = 0;

    strided_range_gt<ukv_key_t*> fetched_keys_;
    strided_range_gt<ukv_val_len_t> fetched_lengths;
};

template <typename locations_store_t>
expected_gt<typename member_refs_gt<locations_store_t>::value_t> //
member_refs_gt<locations_store_t>::any_get(ukv_options_t options) noexcept {
    status_t status;
    ukv_val_len_t* found_lengths = nullptr;
    ukv_val_ptr_t found_values = nullptr;

    decltype(auto) locs = locations_.ref();
    auto count = keys_extractor_t {}.count(locs);
    auto keys = keys_extractor_t {}.keys(locs);
    auto cols = keys_extractor_t {}.cols(locs);
    auto fields = keys_extractor_t {}.fields(locs);
    auto has_fields = fields && (!fields.repeats() || *fields);

    if (has_fields || format_ != ukv_doc_format_binary_k)
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
            format_,
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

    if constexpr (is_one_k)
        return value_view_t {found_values, *found_lengths};
    else
        return taped_values_view_t {found_lengths, found_values, count};
}

template <typename locations_store_t>
template <typename values_arg_at>
status_t member_refs_gt<locations_store_t>::any_assign(values_arg_at&& vals_ref, ukv_options_t options) noexcept {
    status_t status;
    using value_extractor_t = values_arg_extractor_gt<std::remove_reference_t<values_arg_at>>;

    decltype(auto) locs = locations_.ref();
    auto count = keys_extractor_t {}.count(locs);
    auto keys = keys_extractor_t {}.keys(locs);
    auto cols = keys_extractor_t {}.cols(locs);
    auto fields = keys_extractor_t {}.fields(locs);
    auto has_fields = fields && (!fields.repeats() || *fields);

    auto vals = vals_ref;
    auto contents = value_extractor_t {}.contents(vals);
    auto offsets = value_extractor_t {}.offsets(vals);
    auto lengths = value_extractor_t {}.lengths(vals);

    if (has_fields || format_ != ukv_doc_format_binary_k)
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
            format_,
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
