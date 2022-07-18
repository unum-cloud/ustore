/**
 * @file db.h
 * @author Ashot Vardanian
 * @date 12 Jun 2022
 * @brief C bindings for binary collections.
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
 * expensive, thus we plan to implement those via "Pagination".
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

typedef int64_t ukv_key_t;
typedef uint32_t ukv_val_len_t;
typedef uint8_t* ukv_val_ptr_t;
typedef uint64_t ukv_size_t;
typedef char const* ukv_error_t;

/**
 * @brief Non-owning string reference.
 * Always provided by user and we don't participate
 * in its lifetime management in any way.
 */
typedef char const* ukv_str_view_t;

/**
 * @brief Temporary memory handle, used mostly for read requests.
 * It's allocated, resized and deallocated only by UKV itself.
 */
typedef void* ukv_arena_t;

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
     * consistency guarantees. Only works in transactions created with
     * the `ukv_option_txn_snapshot_k` flag.
     */
    ukv_option_read_transparent_k = 1 << 3,
    /**
     * @brief When a transaction is started with this flag, a persistent
     * snapsot is created. It guarantees that the global state of all the
     * keys in the DB will be unchanged during the entire lifetime of the
     * transaction. Will not affect the writes in any way.
     */
    ukv_option_txn_snapshot_k = 1 << 4,

} ukv_options_t;

extern ukv_collection_t ukv_default_collection_k;
extern ukv_val_len_t ukv_val_len_missing_k;
extern ukv_key_t ukv_key_unknown_k;

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
 *                           Can be NULL.
 * @param[in] keys           Array of keys in one or more collections.
 * @param[in] tasks_count    Number of elements in @p `keys`.
 * @param[in] collections    Array of collections owning the @p `keys`.
 *                           If NULL is passed, the default collection
 *                           is assumed. Instead of passing one collection for
 *                           each key, you can use `ukv_option_read_colocated`.
 * @param[in] options        Write options.
 *
 * @param[in] values         Pointer to a tape of concatenated values to be imported.
 *                           A NULL `value` means that the key mist be deleted.
 *                           To clear the `value` without removing the key, just
 *                           pass a zero length.
 *
 * @param[in] lengths        Pointer to lengths of chunks in packed into @p `values`.
 * @param[in] offsets        Pointer to offsets of relevant content within @p `values` chunks.
 * @param[out] error         The error to be handled.
 *
 * @section Upserts, Updates & Inserts
 * Higher-level interfaces may choose to implement any of those verbs:
 * 1. Insert: add if missing.
 * 2. Update: overwrite if present.
 * 3. Upsert: write.
 * Instead of adding all three to C interface, we focus on better ACID transactions,
 * which can be used to implement any advanced multi-step operations (often including
 * conditionals), like Compare-And-Swap, without losing atomicity.
 *
 * @section Why use offsets?
 *
 * In the underlying layer, using offsets to adds no additional overhead,
 * but what is the point of using them, if we can immediatelly pass adjusted
 * pointers?
 * It serves two primary purposes:
 * > Supporting input tapes (values_stride = 0, offsets_stride != 0).
 * > List-oriented wrappers (values_stride != 0, offsets_stride = 0).
 *
 * In the first case, we may have received a tape from `ukv_read`, which we
 * update in-place and write back, without changing the size of the original
 * entries.
 *
 * In the second case, we may be working with higher-level runtimes, like
 * CPython, where objects metadata (like its length) is stored in front of
 * the allocated region. In such cases, we may still need additional memory
 * to store the lengths of the objects, unless those are NULL-terminated
 * strings (lengths = NULL) or if all have the same length (length_stride = 0).
 *
 * Further reading on the implementation of strings and arrays of strings in
 * different languages:
 * > Python/CPython:
 *      https://docs.python.org/3/c-api/bytes.html
 * > JavaScript/V8:
 *      https://github.com/v8/v8/blob/main/include/v8-data.h
 *      https://github.com/v8/v8/blob/main/include/v8-array-buffer.h
 * > GoLang:
 *      https://boakye.yiadom.org/go/strings/
 *      https://github.com/golang/go/blob/master/src/runtime/string.go (`stringStruct`)
 *      https://github.com/golang/go/blob/master/src/runtime/slice.go (`slice`)
 */
void ukv_write( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_stride,

    ukv_val_ptr_t const* values,
    ukv_size_t const values_stride,

    ukv_val_len_t const* offsets,
    ukv_size_t const offsets_stride,

    ukv_val_len_t const* lengths,
    ukv_size_t const lengths_stride,

    ukv_options_t const options,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 * @brief The primary "getter" interface.
 * If a fail had occured, @p `error` will be set to non-NULL.
 * Otherwise, the tape will be populated with @p `tasks_count` objects
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
 *                            operation must go. Can be NULL.
 * @param[in] keys            Array of keys in one or more collections.
 * @param[in] tasks_count     Number of elements in @p `keys`.
 * @param[in] collections     Array of collections owning the @p `keys`.
 *                            If NULL is passed, the default collection
 *                            is assumed. Instead of passing one collection for
 *                            each key, you can use `ukv_option_read_colocated`.
 * @param[in] options         Read options:
 *                            > transaparent: Bypasses any ACID checks on following write.
 *                            > lengths: Only fetches lengths of values, not content.
 *
 * @param[inout] tape         Points to a memory region that we use during
 *                            this request. If it's too small (@p `capacity`),
 *                            we `realloc` a new buffer. You can't pass a memory
 *                            allocated by a third-party allocator.
 *                            During the first request you pass a NULL,
 *                            we allocate that buffer, put found values in it and
 *                            return you a pointer. You can later reuse it for future
 *                            requests, or `free` it via `ukv_arena_free`.
 * @param[out] error          The error message to be handled by callee.
 */
void ukv_read( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_stride,

    ukv_options_t const options,

    ukv_val_len_t** found_lengths,
    ukv_val_ptr_t* found_values,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 * @brief Retrieves the following (upto) `scan_length` keys starting
 * from `min_key` or the smallest following key in each collection.
 * Values are not exported, for that - follow up with `ukv_read`.
 * Fetching lengths of values is @b optional.
 *
 * @param[in] options   Read options:
 *                      > transaparent: Bypasses any ACID checks on following write.
 *                      > lengths: Will fetches lengths of values, after the keys.
 */
void ukv_scan( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* min_keys,
    ukv_size_t const min_keys_stride,

    ukv_size_t const* scan_lengths,
    ukv_size_t const scan_lengths_stride,

    ukv_options_t const options,

    ukv_key_t** found_keys,
    ukv_val_len_t** found_lengths,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 * @brief Estimates the number of entries within a range of keys.
 * Outputs are exported in @b six-tuples:
 * > min & max cardinality,
 * > min & max bytes in values,
 * > min & max (persistent) memory usage.
 */
void ukv_size( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* min_keys,
    ukv_size_t const min_keys_stride,

    ukv_key_t const* max_keys,
    ukv_size_t const max_keys_stride,

    ukv_options_t const options,

    ukv_size_t** found_estimates,

    ukv_arena_t* arena,
    ukv_error_t* error);

/*********************************************************/
/***************** Collection Management  ****************/
/*********************************************************/

/**
 * @brief Inserts a new named collection into DB or opens existing one.
 * This function may never be called, as the default unnamed collection
 * always exists and can be addressed via `ukv_default_collection_k`.
 *
 * @param[in] db           Already open database instance, @see `ukv_open`.
 * @param[in] name         A NULL-terminated collection name.
 * @param[in] config       A NULL-terminated configuration string.
 * @param[out] collection  Address to which the collection handle will be expored.
 * @param[out] error       The error message to be handled by callee.
 */
void ukv_collection_open( //
    ukv_t const db,
    ukv_str_view_t name,
    ukv_str_view_t config,
    ukv_collection_t* collection,
    ukv_error_t* error);

/**
 * @brief Retrieves a list of collection names in a comma-delimited form.
 *
 * @param[in] db     Already open database instance, @see `ukv_open`.
 * @param[out] names A NULL-terminated output string with comma-delimited column names.
 * @param[out] error The error message to be handled by callee.
 */
void ukv_collection_list( //
    ukv_t const db,
    ukv_str_view_t* names,
    ukv_error_t* error);

/**
 * @brief Removes collection and all of its conntents from DB.
 * The default unnamed collection can't be removed, but it
 * will be @b cleared, if you pass a NULL as `name`.
 *
 * @param[in] db      Already open database instance, @see `ukv_open`.
 * @param[in] name    A NULL-terminated collection name.
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
    ukv_options_t const options,
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
void ukv_arena_free(ukv_t const db, ukv_arena_t const arena);

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
