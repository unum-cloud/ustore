/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 12 Jun 2022
 * @brief C bindings for Unums Key-Value store, that provide:
 * > ABI stability for the essential CRUD operations,
 * > Interoperability with higher-level languages.
 *
 * @section Assumptions and Limitations (in current version):
 * > Keys are preset to 8-byte unsigned integers.
 * > Zero-length values are not allowed, same for values over 4GB.
 * > Fully @b synchronous for the simplicity of interface.
 * > Iterators often can't be fully consistent, to allow concurrency.
 * > Maximum collection name length is set to 64 characters. Postgres does 59 :)
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
 * 3. Functions that accept `collections`, @b can receive 0, 1 or N such
 *    arguments, where N is the number of passed `keys`.
 * 4. Collections, Iterators and Transactions are referencing the DB,
 *    so the DB shouldn't die/close before those objects are freed.
 *    This also allows us to reduce the number of function arguments for
 *    interface functions.
 * 5. Strides! Higher level systems may pack groups of arguments into AoS
 *    instead of SoA. To minimize the need of copies and data re-layout,
 *    we use byte-length strides arguments, similar to BLAS libraries.
 *    Passing Zero as a "stride" means repeating the same value.
 *
 * @section Choosing between more functions vs more argument per function
 * We try to preserve balance in the number of function calls exposed in
 * this C API/ABI layer and the complexity of each call. As a result,
 * the @b write methods can be used to:
 * > insert
 * > update
 * > detele
 * and the @b read methods can be used to:
 * > check object existance or it's length
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
/*****************   Structures & Consts  ****************/
/*********************************************************/

typedef void* ukv_t;
typedef void* ukv_txn_t;
typedef void* ukv_collection_t;

typedef uint64_t ukv_key_t;
typedef uint32_t ukv_val_len_t;
typedef uint8_t* ukv_tape_ptr_t;
typedef uint64_t ukv_size_t;
typedef char const* ukv_error_t;

/**
 * @brief Non-owning string reference.
 * Always provided by user and we don't participate
 * in its lifetime management in any way.
 */
typedef char const* ukv_str_view_t;

typedef enum {

    ukv_options_default_k = 0,
    /**
     * @brief Limits the "read" operations to just metadata retrieval.
     * Identical to the "HEAD" verb in the HTTP protocol.
     */
    ukv_option_read_lengths_k = 1 << 1,
    /**
     * @brief Forces absolute consistency on the write operations
     * flushing all the data to disk after each write. It's usage
     * may cause severe performance degradation in some implementations.
     * Yet the users must be warned, that modern IO drivers still often
     * can't guarantee that everything will reach the disk.
     */
    ukv_option_write_flush_k = 1 << 2,
    /**
     * @brief When reading from a transaction, avoids tracking the keys.
     * Which will increase the probability of writes, but levels-down the
     * consistency guarantees.
     */
    ukv_option_read_transparent_k = 1 << 3,

} ukv_options_t;

extern ukv_collection_t ukv_default_collection_k;

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
 * @param[in] config  A NULL-terminated @b JSON string with configuration specs.
 * @param[out] db     A pointer to the opened KVS, unless @p `error` is filled.
 * @param[out] error  The error message to be handled by callee.
 */
void ukv_open( //
    ukv_str_view_t config,
    ukv_t* db,
    ukv_error_t* error);

/**
 * @brief The primary "setter" interface.
 * Passing NULLs into @p `values` is identical to deleting entries.
 * If a fail had occured, @p `error` will be set to non-NULL.
 *
 * @section Functionality Matrix
 * This is one of the two primary methods, that knots together various kinds of reads:
 * > Transactional and Heads
 * > Insertions and Deletions
 *
 * If lengths aren't provided, they are inferred from the passed values,
 * as the offset of the first NULL-termination (zero) symbol.
 *
 * @param[in] db             Already open database instance, @see `ukv_open`.
 * @param[in] txn            Transaction, through which the operation must go.
 *                           Can be `NULL`.
 * @param[in] keys           Array of keys in one or more collections.
 * @param[in] keys_count     Number of elements in @p `keys`.
 * @param[in] collections    Array of collections owning the @p `keys`.
 *                           If NULL is passed, the default collection
 *                           is assumed. Instead of passing one collection for
 *                           each key, you can use `ukv_option_read_colocated`.
 * @param[in] options        Write options.
 * @param[in] values         Pointer to a tape of concatenated values to be imported.
 * @param[in] lengths        Pointer to lengths of chunks in packed into @p `values`.
 * @param[out] error         The error to be handled.
 */
void ukv_write( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_count,
    ukv_size_t const keys_stride,

    ukv_tape_ptr_t const* values,
    ukv_size_t const values_stride,

    ukv_val_len_t const* lengths,
    ukv_size_t const lengths_stride,

    ukv_options_t const options,
    ukv_error_t* error);

/**
 * @brief The primary "getter" interface.
 * If a fail had occured, @p `error` will be set to non-NULL.
 * Otherwise, the tape will be populated with @p `keys_count` objects
 * of type `ukv_val_len_t`, describing the lengths of objects packed
 * right after the lengths themselves.
 * If a key wasn't found in target collection, the length will be zero.
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
 * @param[in] collections     Array of collections owning the @p `keys`.
 *                            If NULL is passed, the default collection
 *                            is assumed. Instead of passing one collection for
 *                            each key, you can use `ukv_option_read_colocated`.
 * @param[in] options         Read options:
 *                            > transaparent: Bypassed any ACID checks on next write.
 *                            > lengths: Only fetches lengths of values, not content.
 *
 * @param[inout] tape         Points to a memory region that we use during
 *                            this request. If it's too small (@p `capacity`),
 *                            we `realloc` a new buffer. You can't pass a memory
 *                            allocated by a third-party allocator.
 *                            During the first request you pass a `NULL`,
 *                            we allocate that buffer, put found values in it and
 *                            return you a pointer. You can later reuse it for future
 *                            requests, or `free` it via `ukv_tape_free`.
 * @param[inout] capacity     Current size of @p `tape`.
 * @param[out] error          The error message to be handled by callee.
 */
void ukv_read( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_count,
    ukv_size_t const keys_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Retrieves the following (upto) `scan_length` keys starting
 * from `min_key` or the smallest following key in each collection.
 *
 * @param[in] options   Read options:
 *                      > transaparent: Bypassed any ACID checks on next write.
 *                      > lengths: Will fetches lengths of values, after the keys.
 */
void ukv_scan( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* min_keys,
    ukv_size_t const min_keys_count,
    ukv_size_t const min_keys_stride,

    ukv_size_t const* scan_lengths,
    ukv_size_t const scan_lengths_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/*********************************************************/
/***************** Collection Management  ****************/
/*********************************************************/

/**
 * @brief Upserts a new named collection into DB.
 * This function may never be called, as the default
 * unnamed collection always exists.
 *
 * @param[in] db           Already open database instance, @see `ukv_open`.
 * @param[in] name         A `NULL`-terminated collection name.
 * @param[out] collection  Address to which the collection handle will be expored.
 * @param[out] error       The error message to be handled by callee.
 */
void ukv_collection_upsert( //
    ukv_t const db,
    ukv_str_view_t name,
    ukv_collection_t* collection,
    ukv_error_t* error);

/**
 * @brief Removes collection and all of its conntents from DB.
 * The default unnamed collection can't be removed, but it
 * will be @b cleared, if you pass a `NULL` as `name`.
 *
 * @param[in] db      Already open database instance, @see `ukv_open`.
 * @param[in] name    A `NULL`-terminated collection name.
 * @param[out] error  The error message to be handled by callee.
 */
void ukv_collection_remove( //
    ukv_t const db,
    ukv_str_view_t name,
    ukv_error_t* error);

/**
 * @brief Performs free-form queries on the DB, that may not necesserily
 * have a stable API and a fixed format output. Generally, those requests
 * are very expensive and shouldn't be executed in most applications.
 * This is the "kitchensink" of UKV interface, similar to `fcntl` & `ioctl`.
 *
 * @param[in] db        Already open database instance, @see `ukv_open`.
 * @param[in] request   Textual representation of the command.
 * @param[out] response Output text of the request.
 * @param[out] error    The error message to be handled by callee.
 *
 * @section Supported Commands
 * > "clear":   Removes all the data from DB, while keeping collection names.
 * > "reset":   Removes all the data from DB, including collection names.
 * > "compact": Flushes and compacts all the data in LSM-tree implementations.
 * > "info":    Metadata about the current software version, used for debugging.
 * > "usage":   Metadata about approximate collection sizes, RAM and disk usage.
 */
void ukv_control( //
    ukv_t const db,
    ukv_str_view_t request,
    ukv_str_view_t* response,
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
    ukv_size_t const sequence_number,
    ukv_txn_t* txn,
    ukv_error_t* error);

/**
 * @brief Commits an ACID transaction.
 * On success, the transaction content is wiped clean.
 * On failure, the entire transaction state is preserved to allow retries.
 */
void ukv_txn_commit( //
    ukv_txn_t const txn,
    ukv_options_t const options,
    ukv_error_t* error);

/*********************************************************/
/*****************	 Memory Reclamation   ****************/
/*********************************************************/

/**
 * @brief A function to be used after `ukv_read` to
 * deallocate and return memory to UnumDB and OS.
 */
void ukv_tape_free(ukv_t const db, ukv_tape_ptr_t, ukv_size_t);

void ukv_txn_free(ukv_t const db, ukv_txn_t const txn);

void ukv_collection_free(ukv_t const db, ukv_collection_t const collection);

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
