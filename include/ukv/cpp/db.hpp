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

#include "ukv/ukv.h"
#include "ukv/cpp/members_ref.hpp"
#include "ukv/cpp/members_range.hpp"
#include "ukv/cpp/graph_ref.hpp"

namespace unum::ukv {

/**
 * @brief Collection is persistent associative container,
 * essentially a transactional @b `map<id,string>`.
 * Or in Python terms: @b `dict[int,str]`.
 *
 * Generally cheap to construct. Can address @b both collections
 * "HEAD" state, as well as some "snapshot"/"transaction" view.
 *
 * @section Class Specs
 * > Concurrency: Thread-safe, for @b unique arenas.
 *   For details, @see `members_ref_gt` @section "Memory Management"
 * > Lifetime: @b Must live shorter then the DB it belongs to.
 * > Exceptions: Only the `size` method.
 * > Copyable: Will create a new empty arena.
 *   Will remain attached to same transaction context, if any was set.
 *
 * @section Formats
 * Formats @b loosely describe the data stored in the collection
 * and @b exactly define the communication through this exact handle.
 * Example: Same collection can accept similar formats, such
 * as `ukv_format_json_k` and `ukv_format_msgpack_k`. Both will be
 * converted into some internal hierarchical representation
 * in "Document Collections", and can later be queried with
 * any "Document Format".
 */
class collection_t {
    ukv_database_t db_ = nullptr;
    ukv_collection_t col_ = ukv_collection_main_k;
    ukv_transaction_t txn_ = nullptr;
    arena_t arena_;
    ukv_format_t format_ = ukv_format_binary_k;

  public:
    inline collection_t() noexcept : arena_(nullptr) {}
    inline collection_t(ukv_database_t db_ptr,
                 ukv_collection_t col = ukv_collection_main_k,
                 ukv_transaction_t txn = nullptr,
                 ukv_format_t format = ukv_format_binary_k) noexcept
        : db_(db_ptr), col_(col), txn_(txn), arena_(db_), format_(format) {}

    inline collection_t(collection_t&& other) noexcept
        : db_(other.db_), col_(std::exchange(other.col_, ukv_collection_main_k)), txn_(std::exchange(other.txn_, nullptr)),
          arena_(std::exchange(other.arena_, {nullptr})), format_(std::exchange(other.format_, ukv_format_binary_k)) {}

    inline collection_t(collection_t const& other) noexcept
        : db_(other.db_), col_(other.col_), txn_(other.txn_), arena_(other.db_), format_(other.format_) {}

    inline collection_t& operator=(collection_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(col_, other.col_);
        std::swap(txn_, other.txn_);
        std::swap(arena_, other.arena_);
        std::swap(format_, other.format_);
        return *this;
    }

    inline collection_t& operator=(collection_t const& other) noexcept {
        db_ = other.db_;
        col_ = other.col_;
        txn_ = other.txn_;
        arena_ = arena_t(other.db_);
        format_ = other.format_;
        return *this;
    }

    inline operator ukv_collection_t() const noexcept { return col_; }
    inline ukv_collection_t* member_ptr() noexcept { return &col_; }
    inline ukv_arena_t* member_arena() noexcept { return arena_.member_ptr(); }
    inline ukv_database_t db() const noexcept { return db_; }
    inline ukv_transaction_t txn() const noexcept { return txn_; }
    inline graph_ref_t as_graph() noexcept { return {db_, txn_, col_, arena_}; }
    inline collection_t& as(ukv_format_t format) noexcept {
        format_ = format;
        return *this;
    }

    inline members_range_t members( //
        ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
        ukv_key_t max_key = ukv_key_unknown_k) const noexcept {
        return {db_, txn_, col_, min_key, max_key};
    }
    inline keys_range_t keys( //
        ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
        ukv_key_t max_key = ukv_key_unknown_k) const noexcept {
        return {members(min_key, max_key)};
    }

    inline pairs_range_t items( //
        ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
        ukv_key_t max_key = ukv_key_unknown_k) const noexcept {
        return {members_range_t {db_, txn_, col_, min_key, max_key}};
    }

    inline expected_gt<size_range_t> size_range() const noexcept {
        auto maybe = members().size_estimates();
        return {maybe.release_status(), std::move(maybe->cardinality)};
    }

    std::size_t size() const noexcept(false) {
        auto maybe = size_range();
        maybe.throw_unhandled();
        return (maybe->min + maybe->max) / 2;
    }

    status_t clear() noexcept {
        status_t status;
        ukv_collection_drop(db_, col_, nullptr, ukv_drop_keys_vals_k, status.member_ptr());
        return status;
    }

    status_t clear_values() noexcept {
        status_t status;
        ukv_collection_drop(db_, col_, nullptr, ukv_drop_vals_k, status.member_ptr());
        return status;
    }

    inline members_ref_gt<places_arg_t> operator[](std::initializer_list<ukv_key_t> keys) noexcept { return at(keys); }
    inline members_ref_gt<places_arg_t> at(std::initializer_list<ukv_key_t> keys) noexcept { //
        return at(strided_range(keys));
    }

    inline members_ref_gt<places_arg_t> operator[](keys_view_t keys) noexcept { return at(keys); }
    inline members_ref_gt<places_arg_t> at(keys_view_t keys) noexcept {
        places_arg_t arg;
        arg.cols_begin = &col_;
        arg.keys_begin = keys.begin();
        arg.count = keys.size();
        return {db_, txn_, {std::move(arg)}, arena_, format_};
    }

    template <typename keys_arg_at>
    auto operator[](keys_arg_at&& keys) noexcept { //
        return at(std::forward<keys_arg_at>(keys));
    }

    template <typename keys_arg_at>
    auto at(keys_arg_at&& keys) noexcept { //
        constexpr bool is_one_k = is_one<keys_arg_at>();
        if constexpr (is_one_k) {
            using result_t = members_ref_gt<col_key_field_t>;
            using plain_t = std::remove_reference_t<keys_arg_at>;
            // ? We may want to warn the users, that the collection property will be shadowed:
            // static_assert(!sfinae_has_collection_gt<plain_t>::value, "Overwriting existing collection!");
            col_key_field_t arg;
            arg.col = col_;
            if constexpr (std::is_integral_v<plain_t>)
                arg.key = keys;
            else
                arg.key = keys.key;

            if constexpr (sfinae_has_field_gt<plain_t>::value)
                arg.field = keys.field;
            return result_t {db_, txn_, arg, arena_, format_};
        }
        else {
            using locations_t = locations_in_collection_gt<keys_arg_at>;
            using result_t = members_ref_gt<locations_t>;
            return result_t {db_, txn_, locations_t {std::forward<keys_arg_at>(keys), col_}, arena_, format_};
        }
    }
};

/**
 * @brief Transaction in a classical DBMS sense.
 *
 * May be used not only as a consistency warrant, but also a performance
 * optimization, as batched writes will be stored in a DB-optimal way
 * until being commited, which reduces the preprocessing overhead for DB.
 * For details, @see ACID: https://en.wikipedia.org/wiki/ACID
 *
 * @section Class Specs
 * > Concurrency: Thread-safe, for @b unique arenas.
 *   For details, @see `members_ref_gt` @section "Memory Management"
 * > Lifetime: Doesn't commit on destruction. @see `txn_guard_t`.
 * > Copyable: No.
 * > Exceptions: Never.
 */
class transaction_t : public std::enable_shared_from_this<transaction_t> {
    ukv_database_t db_ = nullptr;
    ukv_transaction_t txn_ = nullptr;
    arena_t arena_;

  public:
    inline transaction_t() noexcept : arena_(nullptr) {}
    inline transaction_t(ukv_database_t db, ukv_transaction_t txn) noexcept : db_(db), txn_(txn), arena_(db) {}
    inline transaction_t(transaction_t const&) = delete;
    inline transaction_t(transaction_t&& other) noexcept
        : db_(other.db_), txn_(std::exchange(other.txn_, nullptr)), arena_(std::exchange(other.arena_, {nullptr})) {}

    inline transaction_t& operator=(transaction_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(txn_, other.txn_);
        std::swap(arena_, other.arena_);
        return *this;
    }

    inline ukv_database_t db() const noexcept { return db_; }
    inline operator ukv_transaction_t() const noexcept { return txn_; }

    inline ~transaction_t() noexcept {
        ukv_transaction_free(db_, txn_);
        txn_ = nullptr;
    }

    members_ref_gt<places_arg_t> operator[](strided_range_gt<col_key_t const> cols_and_keys) noexcept {
        places_arg_t arg;
        arg.cols_begin = cols_and_keys.members(&col_key_t::col).begin();
        arg.keys_begin = cols_and_keys.members(&col_key_t::key).begin();
        arg.count = cols_and_keys.size();
        return {db_, txn_, std::move(arg), arena_};
    }

    members_ref_gt<places_arg_t> operator[](strided_range_gt<col_key_field_t const> cols_and_keys) noexcept {
        places_arg_t arg;
        arg.cols_begin = cols_and_keys.members(&col_key_field_t::col).begin();
        arg.keys_begin = cols_and_keys.members(&col_key_field_t::key).begin();
        arg.fields_begin = cols_and_keys.members(&col_key_field_t::field).begin();
        arg.count = cols_and_keys.size();
        return {db_, txn_, std::move(arg), arena_};
    }

    members_ref_gt<places_arg_t> operator[](keys_view_t keys) noexcept { //
        places_arg_t arg;
        arg.keys_begin = keys.begin();
        arg.count = keys.size();
        return {db_, txn_, std::move(arg), arena_};
    }

    template <typename keys_arg_at>
    members_ref_gt<keys_arg_at> operator[](keys_arg_at keys) noexcept { //
        return {db_, txn_, std::move(keys), arena_};
    }

    /**
     * @brief Clears the stare of transaction, preserving the underlying memory,
     * cleaning it, and labeling it with a new "sequence number" or "generation".
     *
     * @param snapshot Controls whether a consistent view of the entirety of DB
     *                 must be created for this transaction. Is required for
     *                 long-running analytical tasks with strong consistency
     *                 requirements.
     */
    status_t reset(bool snapshot = false) noexcept {
        status_t status;
        auto options = snapshot ? ukv_option_txn_snapshot_k : ukv_options_default_k;
        ukv_transaction_begin(db_, 0, options, &txn_, status.member_ptr());
        return status;
    }

    /**
     * @brief Attempts to commit all the updates to the DB.
     * Fails if any single one of the updates fails.
     */
    status_t commit(bool flush = false) noexcept {
        status_t status;
        auto options = flush ? ukv_option_write_flush_k : ukv_options_default_k;
        ukv_transaction_commit(txn_, options, status.member_ptr());
        return status;
    }

    expected_gt<collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<collection_t>() noexcept { return collection(""); }

    /**
     * @brief Provides a view of a single collection synchronized with the transaction.
     */
    expected_gt<collection_t> collection(ukv_str_view_t name = "") noexcept {
        status_t status;
        ukv_collection_t col = ukv_collection_main_k;
        ukv_collection_upsert(db_, name, nullptr, &col, status.member_ptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col, txn_};
    }
};

/**
 * @brief DataBase is a "collection of named collections",
 * essentially a transactional @b `map<string,map<id,string>>`.
 * Or in Python terms: @b `dict[str,dict[int,str]]`.
 *
 * @section Class Specs
 * > Concurrency: @b Thread-Safe, except for `open`, `close`.
 * > Lifetime: @b Must live longer then last collection referencing it.
 * > Copyable: No.
 * > Exceptions: Never.
 */
class database_t : public std::enable_shared_from_this<database_t> {
    ukv_database_t db_ = nullptr;

  public:
    database_t() = default;
    database_t(database_t const&) = delete;
    database_t(database_t&& other) noexcept : db_(std::exchange(other.db_, nullptr)) {}

    status_t open(std::string const& config = "") noexcept {
        status_t status;
        ukv_database_open(config.c_str(), &db_, status.member_ptr());
        return status;
    }

    void close() noexcept {
        ukv_database_free(db_);
        db_ = nullptr;
    }

    ~database_t() noexcept {
        if (db_)
            close();
    }

    inline operator ukv_database_t() const noexcept { return db_; }

    /**
     * @brief Checks if a collection with requested `name` is present in the DB.
     * @param memory Temporary memory required for storing the execution results.
     */
    expected_gt<bool> contains(std::string_view name, arena_t& memory) noexcept {
        if (name.empty())
            return true;

        status_t status;
        ukv_size_t count = 0;
        ukv_collection_t* collections = 0;
        ukv_length_t* offsets = nullptr;
        ukv_str_view_t names = nullptr;
        ukv_collection_list(db_, &count, &collections, &offsets, &names, memory.member_ptr(), status.member_ptr());
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

    expected_gt<collection_t> collection(ukv_str_view_t name = "", ukv_format_t format = ukv_format_binary_k) noexcept {
        status_t status;
        ukv_collection_t col = ukv_collection_main_k;
        ukv_collection_upsert(db_, name, nullptr, &col, status.member_ptr());
        if (!status)
            return status;
        else
            return collection_t {db_, col, nullptr, format};
    }

    status_t remove(ukv_str_view_t name, ukv_drop_mode_t mode = ukv_drop_keys_vals_handle_k) noexcept {
        status_t status;
        ukv_collection_drop(db_, 0, name, mode, status.member_ptr());
        return status;
    }

    expected_gt<strings_tape_iterator_t> collection_names(arena_t& memory) {
        ukv_size_t count = 0;
        ukv_str_view_t names = nullptr;
        status_t status;
        ukv_collection_list(db_, &count, nullptr, nullptr, &names, memory.member_ptr(), status.member_ptr());
        return {std::move(status), {count, names}};
    }

    status_t clear(arena_t& memory) noexcept {
        status_t status;
        // Remove named collections
        auto iter = collection_names(memory);
        if (!iter)
            return iter.release_status();
        while (!iter->is_end()) {
            status = remove(**iter);
            if (!status)
                return status;
            iter->operator++();
        }

        // Remove main collection
        status = remove(nullptr, ukv_drop_vals_k);
        if (!status)
            return status;

        return status;
    }

    status_t clear() noexcept {
        // Remove named collections
        arena_t memory(db_);
        return clear(memory);
    }

    expected_gt<transaction_t> transact(bool snapshot = false) noexcept {
        status_t status;
        ukv_transaction_t raw = nullptr;
        ukv_transaction_begin(db_, 0, snapshot ? ukv_option_txn_snapshot_k : ukv_options_default_k, &raw, status.member_ptr());
        if (!status)
            return {std::move(status), transaction_t {db_, nullptr}};
        else
            return transaction_t {db_, raw};
    }
};

} // namespace unum::ukv
