/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 9 Sep 2012
 * @brief C bindings for Unums Key-Value store, that provide:
 * > ABI stability for the essential CRUD operations,
 * > Interoperability with higher-level languages.
 * Assumptions and limitations:
 * > Keys are preset to 8-byte unsigned integers.
 * > Zero-length values are not allowed.
 * > Iterators often can't be fully consistent, to allow concurrency.
 *
 * @todo Manual state control over compactions.
 * @todo Bulk imports.
 *
 * @section Why prefer batch APIs?
 * Using the batch APIs to issue a single read/write request
 * is trivial, but achieving batch-level performance with
 * singular operations is impossible. Regardless of IO layer,
 * a lot of synchronization and locks must be issued to provide
 * consistency.
 *
 * @section Iterators
 * Implementing consistent iterators over concurrent state is exceptionally
 * expensive, thus we plan to implement those via "Pagination" in the future.
 *
 * @section Interface Conventions
 * 1. We try to expose just opaque struct pointers and functions to
 * 	  clients. This allows us to change internal representations
 *    without forcing clients to recompile code, that uses shared lib.
 * 2. Errors are encoded into NULL-terminated C strings.
 * 3. Functions that accept `columns`, @b can receive 0, 1 or N such
 *    arguments, where N is the number of passed `keys`.
 * 4. Collections, Iterators and Transactions are referencing the DB,
 *    so the DB shouldn't die/close before those objects are freed.
 *    This also allows us to reduce the number of function arguments for
 *    interface functions.
 *
 * @section Choosing between more functions vs more argument per function
 * We try to preserve balance in the number of function calls exposed in
 * this C API/ABI layer and the complexity of each call. As a result,
 * the @b read methods can be used to:
 * > insert
 * > update
 * > detele
 * and the @b write methods can be used to:
 * > check object existance
 * > retrieve an object
 * Interfaces for normal and transactional operations are identical,
 * exept for the `_txn_` name part.
 *
 * @section Reference Designs
 * This interface is design as a generalization over most CRUD APIs
 * for key-value stores. It can be used to wrap anything like:
 * * LevelDB:
 *      https://github.com/google/leveldb/blob/main/include/leveldb/c.h
 *      https://github.com/google/leveldb/blob/main/db/c.cc
 *      https://plyvel.readthedocs.io
 * * RocksDB:
 *      https://github.com/facebook/rocksdb/blob/main/include/rocksdb/c.h
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*********************************************************/
/*****************		  Structures	  ****************/
/*********************************************************/

typedef void* ukv_t;
typedef void* ukv_txn_t;
typedef void* ukv_column_t;
typedef void* ukv_options_read_t;
typedef void* ukv_options_write_t;

typedef uint64_t ukv_key_t;
typedef void* ukv_arena_ptr_t;
typedef void* ukv_val_ptr_t;
typedef uint32_t ukv_val_len_t;
typedef char const* ukv_error_t;

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief Opens the underlying Key-Value Store, which can be any of:
 * > embedded persistent transactional KVS
 * > embedded in-memory transactional KVS
 * > remote persistent transactional KVS
 * > remote in-memory transactional KVS
 *
 * @param[in] config  Configuration NULL-terminated string.
 * @param[out] db     A pointer to initialized and opened KVS, unless @p `error` is filled.
 * @param[out] error  The error message to be handled by callee.
 */
void ukv_open( //
    char const* config,
    ukv_t* db,
    ukv_error_t* error);

/**
 * @brief The primary "setter" interface that inserts into "HEAD" state.
 * Passing NULLs into @p `values` is identical to deleting entries.
 * If a fail had occured, @p `error` will be set to non-NULL.
 *
 * @section Functionality Matrix
 * This is one of the two primary methods, that knots together various kinds of reads:
 * > Transactional and Heads
 * > Insertions and Deletions
 *
 * @param[in] db             Already open database instance, @see `ukv_open`.
 * @param[in] txn            Transaction, through which the operation must go.
 *                           Can be `NULL`.
 * @param[in] keys           Array of keys in one or more collections.
 * @param[in] keys_count     Number of elements in @p `keys`.
 * @param[in] columns        Array of columns with 0, 1 or `keys_count`
 *                           elements describing the locations of keys.
 *                           If NULL is passed, the default collection
 *                           is assumed.
 * @param[in] options        Write options.
 * @param[in] values         Array of pointers to the first byte of each value.
 *                           NULLs, if you want to @b delete the values associated
 *                           with given @p `keys`.
 * @param[in] lengths        Array of lengths of buffers, storing the @p `values`.
 * @param[out] error         The error to be handled.
 */
void ukv_write( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_key_t const* keys,
    size_t const keys_count,
    ukv_column_t const* columns,
    ukv_options_write_t const options,
    ukv_val_ptr_t const* values,
    ukv_val_len_t const* lengths,
    ukv_error_t* error);

/**
 * @brief The primary "getter" interface.
 * If a fail had occured, @p `error` will be set to non-NULL.
 * If a key wasn't found in target collection, the value is empty.
 *
 * @section Functionality Matrix
 * This is one of the two primary methods, that knots together various kinds of reads:
 * > Transactional and Heads
 * > Single and Batch
 * > Size Estimates and Exports
 *
 * @param[in] db              Already open database instance, @see `ukv_open`.
 * @param[in] txn             Transaction or the snapshot, through which the
 *                            operation must go. Can be `NULL`.
 * @param[in] keys            Array of keys in one or more collections.
 * @param[in] keys_count      Number of elements in @p `keys`.
 * @param[in] columns         Array of columns with 0, 1 or `keys_count`
 *                            elements describing the locations of keys.
 *                            If NULL is passed, the default collection
 *                            is assumed.
 * @param[in] options         Write options.
 * @param[inout] arena        Points to a memory region that we use during
 *                            this request. If it's too small (@p `arena_length`),
 *                            we `realloc` a new buffer. You can't pass a memory
 *                            allocated by a third-party allocator.
 *                            During the first request you pass a `NULL`,
 *                            we allocate that buffer, put found values in it and
 *                            return you a pointer. You can later reuse it for future
 *                            requests, or `free` it via `ukv_arena_free`.
 * @param[inout] arena_length Current size of @p `arena`.
 * @param[out] values         Array of pointers to the first byte of each value.
 *                            If `NULL`, only the `value_lengths` will be pulled.
 * @param[out] lengths        Lengths of the values. Zero means value is missing.
 *                            Can't be `NULL`.
 * @param[out] error          The error message to be handled by callee.
 */
void ukv_read( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_key_t* keys,
    size_t const keys_count,
    ukv_column_t const* columns,
    ukv_options_read_t const options,
    ukv_arena_ptr_t* arena,
    size_t* arena_length,
    ukv_val_ptr_t* values,
    ukv_val_len_t* lengths,
    ukv_error_t* error);

/*********************************************************/
/*****************	Columns Management	  ****************/
/*********************************************************/

/**
 * @brief Upserts a new named column into DB.
 * This function may never be called, as the default
 * unnamed collection always exists.
 *
 * @param[in] db           Already open database instance, @see `ukv_open`.
 * @param[in] column_name  A `NULL`-terminated collection name.
 * @param[out] column      Address to which the column handle will be expored.
 * @param[out] error       The error message to be handled by callee.
 */
void ukv_column_upsert( //
    ukv_t const db,
    char const* column_name,
    ukv_column_t* column,
    ukv_error_t* error);

/**
 * @brief Removes column and all of its conntents from DB.
 * The default unnamed collection can't be removed, but it
 * will be @b cleared, if you pass a `NULL` as `column_name`.
 *
 * @param[in] db           Already open database instance, @see `ukv_open`.
 * @param[in] column_name  A `NULL`-terminated collection name.
 * @param[out] error       The error message to be handled by callee.
 */
void ukv_column_remove( //
    ukv_t const db,
    char const* column_name,
    ukv_error_t* error);

/**
 * @brief Triggers a "Compaction" or "Garbage Collection" procedure
 * on the entire collection. This is an exceptionally expensive task,
 * that only makes sense on exports, when you want to export production
 * data into a read-only collection for your products deployment.
 *
 * @param[in] db           Already open database instance, @see `ukv_open`.
 * @param[in] column       A column handle.
 * @param[out] error       The error message to be handled by callee.
 */
void ukv_column_compact( //
    ukv_t const db,
    ukv_column_t* column,
    ukv_error_t* error);

void ukv_columns_list( //
    ukv_t const db,
    char const** columns_names,
    size_t* columns_count,
    ukv_error_t* error);

/**
 * @brief Pulls metadata, mostly for logging and customer support.
 */
void ukv_status( //
    ukv_t db,
    size_t* version_major,
    size_t* version_minor,
    size_t* memory_usage,
    size_t* disk_usage,
    size_t* active_transactions,
    ukv_error_t* error);

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

/**
 * @brief Begins a new ACID transaction or resets an existing one.
 *
 * @param db                Already open database instance, @see `ukv_open`.
 * @param sequence_number   If equal to 0, a new number will be generated on the fly.
 * @param txn               May be pointing to an existing transaction.
 *                          In that case, it's reset to new @p `sequence_number`.
 * @param error             The error message to be handled by callee.
 */
void ukv_txn_begin( //
    ukv_t const db,
    size_t const sequence_number,
    ukv_txn_t* txn,
    ukv_error_t* error);

/**
 * @brief Commits an ACID transaction.
 * On success, the transaction content is wiped clean.
 * On failure, the entire transaction state is preserved to allow retries.
 */
void ukv_txn_commit( //
    ukv_txn_t const txn,
    ukv_options_write_t const options,
    ukv_error_t* error);

/*********************************************************/
/*****************	       Options        ****************/
/*********************************************************/

/**
 * @brief Conditionally forces non-transactional reads to create
 * a snapshot, so that all the reads in the batch are consistent with
 * each other.
 *
 * @param[inout] options Options flags to be updated.
 */
void ukv_option_read_consistent(ukv_options_read_t* options, bool);

/**
 * @brief Conditionally disables the tracking of the current batch of
 * reads, so that it doesn't stop transactions, which follow overwrites
 * of their dependencies.
 *
 * @param[inout] options Options flags to be updated.
 */
void ukv_option_read_transparent(ukv_options_read_t* options, bool);

/**
 * @brief Conditionally informs that there is only one collection
 * passed instead of the same number as input keys.
 *
 * @param[inout] options Options flags to be updated.
 */
void ukv_option_read_colocated(ukv_options_read_t* options, bool);

/**
 * @brief Conditionally forces the write to be flushed either to
 * Write-Ahead-Log or directly to disk by the end of the operation.
 *
 * @param[inout] options Options flags to be updated.
 */
void ukv_option_write_flush(ukv_options_write_t* options, bool);

/**
 * @brief Conditionally informs that there is only one collection
 * passed instead of the same number as input keys.
 *
 * @param[inout] options Options flags to be updated.
 */
void ukv_option_write_colocated(ukv_options_read_t* options, bool);

/*********************************************************/
/*****************	 Memory Reclamation   ****************/
/*********************************************************/

/**
 * @brief A function to be used in conjunction with:
 * > `ukv_read`
 * > `ukv_txn_read`
 * to deallocate and return memory to UnumDB and OS.
 */
void ukv_arena_free(ukv_t const db, ukv_arena_ptr_t, size_t);

void ukv_txn_free(ukv_t const db, ukv_txn_t const txn);

void ukv_column_free(ukv_t const db, ukv_column_t const column);

/**
 * @brief Closes the DB and deallocates the state.
 */
void ukv_free(ukv_t const db);

/**
 * @brief A function to be called after any function failure,
 * that resulted in a non-NULL `ukv_error_t`, even `ukv_open`.
 * That's why, unlike other `...free` methods, doesn't need `db`.
 */
void ukv_error_free(ukv_error_t const error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
