/**
 * @file members_range.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings for @see "ukv/db.h".
 */

#pragma once
#include "ukv/ukv.h"
#include "ukv/cpp/ranges.hpp" // `indexed_range_gt`

namespace unum::ukv {

class keys_stream_t;
class pairs_stream_t;
struct members_range_t;
struct pairs_range_t;
struct size_range_t;
struct size_estimates_t;

/**
 * @brief Iterator (almost) over the keys in a single collection.
 * Manages it's own memory and may be expressive to construct.
 * Prefer to `seek`, instead of re-creating such a stream.
 * Unlike classical iterators, keeps an internal state,
 * which makes it @b non copy-constructible!
 *
 * @section Class Specs
 * > Concurrency: Must be used from a single thread!
 * > Lifetime: @b Must live shorter then the collection it belongs to.
 * > Copyable: No.
 * > Exceptions: Never.
 */

class keys_stream_t {

    ukv_t db_ = nullptr;
    ukv_col_t col_ = ukv_col_main_k;
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
        ukv_scan( //
            db_,
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
        next_min_key_ = count <= read_ahead_ ? ukv_key_unknown_k : fetched_keys_[count - 1] + 1;
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
                  ukv_col_t col = ukv_col_main_k,
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

class pairs_stream_t {

    ukv_t db_ = nullptr;
    ukv_col_t col_ = ukv_col_main_k;
    ukv_txn_t txn_ = nullptr;

    arena_t arena_scan_;
    arena_t arena_read_;
    ukv_size_t read_ahead_ = 0;

    ukv_key_t next_min_key_ = std::numeric_limits<ukv_key_t>::min();
    indexed_range_gt<ukv_key_t*> fetched_keys_;
    tape_view_t values_view;
    std::size_t fetched_offset_ = 0;

    status_t prefetch() noexcept {

        if (next_min_key_ == ukv_key_unknown_k)
            return {};

        ukv_key_t* found_keys = nullptr;
        ukv_val_len_t* found_lens = nullptr;
        ukv_val_ptr_t found_vals = nullptr;
        status_t status;
        ukv_scan( //
            db_,
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
            arena_scan_.member_ptr(),
            status.member_ptr());
        if (!status)
            return status;

        auto present_end = std::find(found_keys, found_keys + read_ahead_, ukv_key_unknown_k);
        fetched_keys_ = indexed_range_gt<ukv_key_t*> {found_keys, present_end};
        fetched_offset_ = 0;
        auto count = static_cast<ukv_size_t>(fetched_keys_.size());

        ukv_read( //
            db_,
            txn_,
            count,
            &col_,
            0,
            found_keys,
            sizeof(ukv_key_t),
            ukv_options_default_k,
            &found_lens,
            &found_vals,
            arena_read_.member_ptr(),
            status.member_ptr());
        if (!status)
            return status;

        values_view = tape_view_t {found_lens, found_vals, count};
        next_min_key_ = count <= read_ahead_ ? ukv_key_unknown_k : fetched_keys_[count - 1] + 1;
        return {};
    }

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<ukv_key_t, value_view_t>;

    static constexpr std::size_t default_read_ahead_k = 256;

    pairs_stream_t(ukv_t db,
                   ukv_col_t col = ukv_col_main_k,
                   std::size_t read_ahead = pairs_stream_t::default_read_ahead_k,
                   ukv_txn_t txn = nullptr)
        : db_(db), col_(col), txn_(txn), arena_scan_(db_), arena_read_(db_),
          read_ahead_(static_cast<ukv_size_t>(read_ahead)) {}

    pairs_stream_t(pairs_stream_t&&) = default;
    pairs_stream_t& operator=(pairs_stream_t&&) = default;

    pairs_stream_t(pairs_stream_t const&) = delete;
    pairs_stream_t& operator=(pairs_stream_t const&) = delete;

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
    pairs_stream_t& operator++() noexcept {
        status_t status = advance();
        if (status)
            return *this;

        fetched_keys_ = {};
        fetched_offset_ = 0;
        next_min_key_ = ukv_key_unknown_k;
        return *this;
    }

    ukv_key_t key() const noexcept { return fetched_keys_[fetched_offset_]; }
    value_view_t value() const noexcept {
        auto it = values_view.begin();
        for (size_t i = 0; i != fetched_offset_; ++i)
            ++it;
        return *it;
    }
    value_type item() const noexcept {
        auto it = values_view.begin();
        for (size_t i = 0; i != fetched_offset_; ++i)
            ++it;
        return std::make_pair(fetched_keys_[fetched_offset_], *it);
    }
    value_type operator*() const noexcept { return item(); }

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

    bool operator==(pairs_stream_t const& other) const noexcept {
        if (col_ != other.col_)
            return false;
        if (is_end() || other.is_end())
            return is_end() == other.is_end();
        return key() == other.key();
    }

    bool operator!=(pairs_stream_t const& other) const noexcept {
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

/**
 * @brief Slice of keys or key-value-pairs stored in a single collection.
 * In Python terms: @b `dict().items()` or @b `dict().keys()`.
 * Supports C++ range-based loops: `for (auto key : @b collection.items())`
 * It can also be use for @b loose cardinality and disk-usage estimates.
 *
 * @section Class Specs
 * > Concurrency: Thread-safe.
 * > Lifetime: @b Must live shorter then the collection it belongs to.
 * > Copyable: Yes.
 * > Exceptions: Possible on `begin()`, `end()` calls.
 *   That interface, however, may through exceptions.
 *   For exception-less interface use `keys_begin()`, `keys_end()`.
 */
class members_range_t {

    ukv_t db_;
    ukv_txn_t txn_;
    ukv_col_t col_;
    ukv_key_t min_key_;
    ukv_key_t max_key_;

    template <typename stream_at>
    expected_gt<stream_at> make_stream( //
        ukv_key_t target,
        std::size_t read_ahead = keys_stream_t::default_read_ahead_k) noexcept {
        stream_at stream {db_, col_, read_ahead, txn_};
        status_t status = stream.seek(target);
        return {std::move(status), std::move(stream)};
    }

  public:
    members_range_t(ukv_t db,
                    ukv_txn_t txn = nullptr,
                    ukv_col_t col = ukv_col_main_k,
                    ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                    ukv_key_t max_key = ukv_key_unknown_k) noexcept
        : db_(db), txn_(txn), col_(col), min_key_(min_key), max_key_(max_key) {}

    members_range_t(members_range_t&&) = default;
    members_range_t& operator=(members_range_t&&) = default;
    members_range_t(members_range_t const&) = default;
    members_range_t& operator=(members_range_t const&) = default;

    expected_gt<keys_stream_t> keys_begin(std::size_t read_ahead = keys_stream_t::default_read_ahead_k) noexcept {
        return make_stream<keys_stream_t>(min_key_, read_ahead);
    }

    expected_gt<keys_stream_t> keys_end() noexcept {
        return make_stream<keys_stream_t>(max_key_, max_key_ == ukv_key_unknown_k ? 0u : 1u);
    }

    expected_gt<pairs_stream_t> pairs_begin(std::size_t read_ahead = pairs_stream_t::default_read_ahead_k) noexcept {
        return make_stream<pairs_stream_t>(min_key_, read_ahead);
    }

    expected_gt<pairs_stream_t> pairs_end() noexcept {
        return make_stream<pairs_stream_t>(max_key_, max_key_ == ukv_key_unknown_k ? 0u : 1u);
    }

    expected_gt<size_estimates_t> size_estimates() noexcept {
        status_t status;
        arena_t arena(db_);
        size_estimates_t result;
        auto o = reinterpret_cast<ukv_size_t*>(&result.cardinality.min);
        auto a = arena.member_ptr();
        auto s = status.member_ptr();
        ukv_size(db_, txn_, 1, &col_, 0, &min_key_, 0, &max_key_, 0, ukv_options_default_k, o, a, s);
        return {std::move(status), std::move(result)};
    }

    members_range_t& since(ukv_key_t min_key) noexcept {
        min_key_ = min_key;
        return *this;
    }
    members_range_t& until(ukv_key_t max_key) noexcept {
        max_key_ = max_key;
        return *this;
    }

    ukv_key_t min_key() noexcept { return min_key_; }
    ukv_key_t max_key() noexcept { return max_key_; }
};

struct keys_range_t {
    using iterator_type = keys_stream_t;

    members_range_t members;

    keys_stream_t begin() noexcept(false) { return members.keys_begin().throw_or_release(); }
    keys_stream_t end() noexcept(false) { return members.keys_end().throw_or_release(); }
    std::size_t size() noexcept(false) { return members.size_estimates().throw_or_release().cardinality.max; }
};

struct pairs_range_t {
    using iterator_type = pairs_stream_t;

    members_range_t members;

    pairs_stream_t begin() noexcept(false) { return members.pairs_begin().throw_or_release(); }
    pairs_stream_t end() noexcept(false) { return members.pairs_end().throw_or_release(); }
    std::size_t size() noexcept(false) { return members.size_estimates().throw_or_release().cardinality.max; }
};

} // namespace unum::ukv
