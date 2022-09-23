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
#include "ukv/cpp/bins_collection.hpp"
#include "ukv/cpp/docs_collection.hpp"
#include "ukv/cpp/graph_collection.hpp"

namespace unum::ukv {

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
 *   For details, @see `bins_ref_gt` @section "Memory Management"
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

    bins_ref_gt<places_arg_t> operator[](strided_range_gt<collection_key_t const> collections_and_keys) noexcept {
        places_arg_t arg;
        arg.collections_begin = collections_and_keys.members(&collection_key_t::collection).begin();
        arg.keys_begin = collections_and_keys.members(&collection_key_t::key).begin();
        arg.count = collections_and_keys.size();
        return {db_, txn_, std::move(arg), arena_};
    }

    bins_ref_gt<places_arg_t> operator[](strided_range_gt<collection_key_field_t const> collections_and_keys) noexcept {
        places_arg_t arg;
        arg.collections_begin = collections_and_keys.members(&collection_key_field_t::collection).begin();
        arg.keys_begin = collections_and_keys.members(&collection_key_field_t::key).begin();
        arg.fields_begin = collections_and_keys.members(&collection_key_field_t::field).begin();
        arg.count = collections_and_keys.size();
        return {db_, txn_, std::move(arg), arena_};
    }

    bins_ref_gt<places_arg_t> operator[](keys_view_t keys) noexcept { //
        places_arg_t arg;
        arg.keys_begin = keys.begin();
        arg.count = keys.size();
        return {db_, txn_, std::move(arg), arena_};
    }

    template <typename keys_arg_at>
    bins_ref_gt<keys_arg_at> operator[](keys_arg_at keys) noexcept { //
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
        ukv_transaction_commit(db_, txn_, options, status.member_ptr());
        return status;
    }

    expected_gt<bins_collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<bins_collection_t>() noexcept { return collection(""); }

    /**
     * @brief Provides a view of a single collection synchronized with the transaction.
     * @tparam collection_at Can be a `bins_collection_t`, `docs_collection_t`, `graph_collection_t`.
     */
    template <typename collection_at = bins_collection_t>
    expected_gt<collection_at> collection(ukv_str_view_t name = "") noexcept {
        status_t status;
        ukv_collection_t collection = ukv_collection_main_k;
        ukv_collection_init(db_, name, nullptr, &collection, status.member_ptr());
        if (!status)
            return status;
        else
            return collection_at {db_, collection, txn_, arena_.member_ptr()};
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
        ukv_database_init(config.c_str(), &db_, status.member_ptr());
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
        ukv_str_span_t names = nullptr;
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

    expected_gt<bins_collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<bins_collection_t>() noexcept { return collection(""); }
    expected_gt<bins_collection_t> operator*() noexcept { return collection(""); }

    template <typename collection_at = bins_collection_t>
    expected_gt<collection_at> collection(ukv_str_view_t name = "", ukv_str_view_t config = "") noexcept {
        status_t status;
        ukv_collection_t collection = ukv_collection_main_k;
        ukv_collection_init(db_, name, config, &collection, status.member_ptr());
        if (!status)
            return status;
        else
            return collection_at {db_, collection, nullptr, nullptr};
    }

    status_t drop(ukv_str_view_t name, ukv_drop_mode_t mode = ukv_drop_keys_vals_handle_k) noexcept {
        status_t status;
        ukv_collection_drop(db_, 0, name, mode, status.member_ptr());
        return status;
    }

    expected_gt<strings_tape_iterator_t> collection_names(arena_t& memory) {
        ukv_size_t count = 0;
        ukv_str_span_t names = nullptr;
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
            status = drop(**iter);
            if (!status)
                return status;
            iter->operator++();
        }

        // Remove main collection
        status = drop(nullptr, ukv_drop_keys_vals_k);
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
        ukv_transaction_begin( //
            db_,
            0,
            snapshot ? ukv_option_txn_snapshot_k : ukv_options_default_k,
            &raw,
            status.member_ptr());
        if (!status)
            return {std::move(status), transaction_t {db_, nullptr}};
        else
            return transaction_t {db_, raw};
    }
};

} // namespace unum::ukv
