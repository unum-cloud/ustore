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

struct collections_list_t {
    ptr_range_gt<ukv_collection_t> ids;
    strings_tape_iterator_t names;
};

/**
 * @brief A DBMS client for a single thread.
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
class context_t : public std::enable_shared_from_this<context_t> {
  protected:
    ukv_database_t db_ = nullptr;
    ukv_transaction_t txn_ = nullptr;
    arena_t arena_;

  public:
    inline context_t() noexcept : arena_(nullptr) {}
    inline context_t(ukv_database_t db, ukv_transaction_t txn = nullptr) noexcept : db_(db), txn_(txn), arena_(db) {}
    inline context_t(context_t const&) = delete;
    inline context_t(context_t&& other) noexcept
        : db_(other.db_), txn_(std::exchange(other.txn_, nullptr)), arena_(std::exchange(other.arena_, {nullptr})) {}

    inline context_t& operator=(context_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(txn_, other.txn_);
        std::swap(arena_, other.arena_);
        return *this;
    }

    inline ~context_t() noexcept {
        ukv_transaction_free(db_, txn_);
        txn_ = nullptr;
    }

    inline ukv_database_t db() const noexcept { return db_; }
    inline ukv_transaction_t txn() const noexcept { return txn_; }
    inline operator ukv_transaction_t() const noexcept { return txn_; }

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

    expected_gt<bins_collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name); }
    operator expected_gt<bins_collection_t>() noexcept { return collection(); }

    expected_gt<collections_list_t> collections() noexcept {
        ukv_size_t count = 0;
        ukv_str_span_t names = nullptr;
        ukv_collection_t* ids = nullptr;
        status_t status;
        ukv_collection_list_t coll_list {
            .db = db_,
            .error = status.member_ptr(),
            .transaction = txn_,
            .arena = arena_.member_ptr(),
            .count = &count,
            .ids = &ids,
            .names = &names,
        };

        ukv_collection_list(&coll_list);
        collections_list_t result;
        result.ids = {ids, ids + count};
        result.names = {count, names};
        return {std::move(status), std::move(result)};
    }

    expected_gt<bool> contains(std::string_view name) noexcept {

        if (name.empty())
            return true;

        auto maybe_cols = collections();
        if (!maybe_cols)
            return maybe_cols.release_status();

        auto cols = *maybe_cols;
        auto name_it = cols.names;
        auto id_it = cols.ids.begin();
        for (; id_it != cols.ids.end(); ++id_it, ++name_it) {
            if (*name_it != name)
                continue;
            return true;
        }
        return false;
    }

    expected_gt<ukv_collection_t> find(std::string_view name) noexcept {

        if (name.empty())
            return ukv_collection_t(ukv_collection_main_k);

        auto maybe_cols = collections();
        if (!maybe_cols)
            return maybe_cols.release_status();

        auto cols = *maybe_cols;
        auto name_it = cols.names;
        auto id_it = cols.ids.begin();
        for (; id_it != cols.ids.end(); ++id_it, ++name_it) {
            if (*name_it != name)
                continue;
            return ukv_collection_t(*id_it);
        }
        return status_t {"Collection not found."};
    }

    /**
     * @brief Provides a view of a single collection synchronized with the transaction.
     * @tparam collection_at Can be a `bins_collection_t`, `docs_collection_t`, `graph_collection_t`.
     */
    template <typename collection_at = bins_collection_t>
    expected_gt<collection_at> collection(std::string_view name = {}) noexcept {

        if (name.empty())
            return collection_at {db_, ukv_collection_main_k, txn_, arena_.member_ptr()};

        auto maybe_cols = collections();
        if (!maybe_cols)
            return maybe_cols.release_status();

        auto cols = *maybe_cols;
        auto name_it = cols.names;
        auto id_it = cols.ids.begin();
        for (; id_it != cols.ids.end(); ++id_it, ++name_it) {
            if (*name_it != name)
                continue;
            return collection_at {db_, *id_it, txn_, arena_.member_ptr()};
        }
        return status_t {"No such collection is present"};
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
        auto options = snapshot ? ukv_option_transaction_snapshot_k : ukv_options_default_k;
        ukv_transaction_init_t txn_init {
            .db = db_,
            .error = status.member_ptr(),
            .options = options,
            .transaction = &txn_,
        };

        ukv_transaction_init(&txn_init);
        return status;
    }

    /**
     * @brief Attempts to commit all the updates to the DB.
     * Fails if any single one of the updates fails.
     */
    status_t commit(bool flush = false) noexcept {
        status_t status;
        auto options = flush ? ukv_option_write_flush_k : ukv_options_default_k;
        ukv_transaction_commit_t txn_commit {
            .db = db_,
            .error = status.member_ptr(),
            .transaction = txn_,
            .options = options,
        };
        ukv_transaction_commit(&txn_commit);
        return status;
    }
};

using transaction_t = context_t;

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
    operator ukv_database_t() const noexcept { return db_; }

    status_t open(ukv_str_view_t config = nullptr) noexcept {
        status_t status;
        ukv_database_init_t database {
            .config = config,
            .db = &db_,
            .error = status.member_ptr(),
        };
        ukv_database_init(&database);
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

    template <typename collection_at = bins_collection_t>
    expected_gt<collection_at> add_collection(ukv_str_view_t name, ukv_str_view_t config = "") noexcept {
        status_t status;
        ukv_collection_t collection = ukv_collection_main_k;
        ukv_collection_init_t coll_init {
            .db = db_,
            .error = status.member_ptr(),
            .name = name,
            .config = config,
            .id = &collection,
        };

        ukv_collection_init(&coll_init);
        if (!status)
            return status;
        else
            return collection_at {db_, collection, nullptr, nullptr};
    }

    status_t clear() noexcept {
        auto context = context_t {db_, nullptr};

        // Remove named collections
        auto maybe_cols = context.collections();
        if (!maybe_cols)
            return maybe_cols.release_status();

        status_t status;
        auto cols = *maybe_cols;
        ukv_collection_drop_t coll_drop {
            .db = db_,
            .error = status.member_ptr(),
            .mode = ukv_drop_keys_vals_handle_k,
        };
        for (auto id : cols.ids) {
            coll_drop.id = id;
            ukv_collection_drop(&coll_drop);
            if (!status)
                return status;
        }

        // Clear the main collection
        coll_drop.id = ukv_collection_main_k;
        coll_drop.mode = ukv_drop_keys_vals_k;
        ukv_collection_drop(&coll_drop);
        return status;
    }

    expected_gt<context_t> transact(bool snapshot = false) noexcept {
        status_t status;
        ukv_transaction_t raw = nullptr;
        ukv_transaction_init_t txn_init {
            .db = db_,
            .error = status.member_ptr(),
            .options = snapshot ? ukv_option_transaction_snapshot_k : ukv_options_default_k,
            .transaction = &raw,
        };

        ukv_transaction_init(&txn_init);
        if (!status)
            return {std::move(status), context_t {db_, nullptr}};
        else
            return context_t {db_, raw};
    }

    expected_gt<bins_collection_t> operator[](ukv_str_view_t name) noexcept { return collection(name, true); }
    operator expected_gt<bins_collection_t>() noexcept { return collection(); }
    template <typename collection_at = bins_collection_t>
    expected_gt<ukv_collection_t> find(std::string_view name, bool make = false) noexcept {
        auto ctx = context_t {db_, nullptr}.find(name);
        if (!ctx && make) {
            auto col = add_collection<collection_at>(name.data()).throw_or_release();
            return ukv_collection_t(*col.member_ptr());
        }
        return ctx;
    }
    expected_gt<bool> contains(std::string_view name) noexcept { return context_t {db_, nullptr}.contains(name); }
    status_t drop(std::string_view name) noexcept {
        auto maybe_collection = collection(name);
        if (!maybe_collection)
            return maybe_collection.release_status();
        return maybe_collection->drop();
    }

    template <typename collection_at = bins_collection_t>
    expected_gt<collection_at> collection(std::string_view name = {}, bool make = false) noexcept {
        auto maybe_id = find<collection_at>(name, make);
        if (!maybe_id)
            return maybe_id.release_status();
        return collection_at {db_, *maybe_id, nullptr, nullptr};
    }
};

} // namespace unum::ukv
