/**
 * @file db.h
 * @author Ashot Vardanian
 * @date 12 Jun 2022
 * @addtogroup C
 *
 * @brief C bindings for Key-Value Stores and binary collections.
 *
 * ## Usage Recommendations
 *
 * ### Pack operations into batches wherever possible
 *
 * Using the batch APIs to issue a single read/write request is trivial, but achieving
 * batch-level performance with singular operations is impossible. Especially, with
 * a client-server setup. Regardless of IO layer, a lot of synchronization and locks
 * must be issued to provide consistency.
 *
 * ## Why use offsets?
 *
 * In the underlying layer, using offsets to adds no additional overhead,
 * but what is the point of using them, if we can immediately pass adjusted
 * pointers? It serves two primary purposes:
 *
 * - Supporting input tapes (values_stride = 0, offsets_stride != 0).
 * - List-oriented wrappers (values_stride != 0, offsets_stride = 0).
 *
 * In the first case, we may have received a tape from `ukv_read()`, which we
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
 *
 * - Python/CPython:
 *      https://docs.python.org/3/c-api/bytes.html
 * - JavaScript/V8:
 *      https://github.com/v8/v8/blob/main/include/v8-data.h
 *      https://github.com/v8/v8/blob/main/include/v8-array-buffer.h
 * - GoLang:
 *      https://boakye.yiadom.org/go/strings/
 *      https://github.com/golang/go/blob/master/src/runtime/string.go (`stringStruct`)
 *      https://github.com/golang/go/blob/master/src/runtime/slice.go (`slice`)
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

/**
 * @brief Opaque multi-modal Database handle.
 * @see `ukv_database_free()`.
 *
 * Properties:
 * - Thread safety: Safe to use across threads after open and before free.
 * - Lifetime: Must live longer than all the transactions.
 *
 * ## Concurrency
 *
 * In embedded setup this handle manages the lifetime of the database.
 * In that case user must guarantee, that concurrent processes won't be
 * opening the same database (generally same directory).
 *
 * In standalone "client-server" setup, manages the lifetime of the "client".
 * Many concurrent clients can be connecting to the same server from the same
 * process.
 *
 * ## Collections
 *
 * Every database always has at least one collection - the `::ukv_collection_main_k`.
 * That one has no name and can't be deleted. Others are referenced by names.
 * The same database can have many collections, of different modalities:
 * - Binary Large Objects or BLOBs.
 * - Hierarchical documents, like JSONs, BSONs, MessagePacks.
 * - Discrete labeled and potentially directed Graphs.
 * - Paths or collections of string keys.
 *
 * ## Choosing the Engine
 *
 * Dynamic dispatch of engines isn't yet supported.
 *
 * ## CAP Theorem
 *
 * Distributed engines are not yet supported.
 */
typedef void* ukv_database_t;

/**
 * @brief Opaque Transaction handle.
 * @see `ukv_transaction_free()`.
 *
 * Allows ACID-ly grouping operations across different collections and even modalities.
 * This means, that the same transaction might be:
 * - inserting a blob of media data into a collection of images.
 * - updating users metadata in a documents collection to reference new avatar.
 * - introducing links between the user and other in a graph collection...
 * and all of the operations here either succeed or fail together. DBMS will
 * do the synchronization heavylifting, so you don't have to.
 *
 * Properties:
 * - Thread safety: None.
 * - Lifetime: Must be freed before the @c ukv_database_t is closed.
 * - Concurrency Control: Optimistic.
 *
 * ## ACI(D)-ity
 *
 * ### A: Atomicity !
 *
 * Atomicity is always guaranteed.
 * Even on non-transactional writes - either all updates pass or all fail.
 *
 * ### C: Consistency !
 *
 * Consistency is implemented in the strictest possible form - ["Strict Serializability"][ss]
 * meaning that:
 * - reads are ["Serializable"][s],
 * - writes are ["Linearizable"][l].
 * The default behaviour, however, can be tweaked at the level of specific operations.
 * For that the `::ukv_option_transaction_dont_watch_k` can be passed to `ukv_transaction_init()`
 * or any transactional read/write operation, to control the consistency checks during staging.
 *
 *  |                               |     Reads     |     Writes    |
 *  |:------------------------------|:-------------:|:-------------:|
 *  | Head                          | Strict Serial | Strict Serial |
 *  | Transactions over Snapshots   |     Serial    | Strict Serial |
 *  | Transactions w/out Snapshots  | Strict Serial | Strict Serial |
 *  | Transactions w/out Watches    | Strict Serial |  Sequential   |
 *
 * If this topic is new to you, please check out the [Jepsen.io][jepsen] blog on consistency.
 *
 * [ss]: https://jepsen.io/consistency/models/strict-serializable
 * [s]: https://jepsen.io/consistency/models/serializable
 * [l]: https://jepsen.io/consistency/models/linearizable
 * [jepsen]: https://jepsen.io/consistency
 *
 * ### I: Isolation !
 *
 *  |                               |     Reads     |     Writes    |
 *  |:------------------------------|:-------------:|:-------------:|
 *  | Transactions over Snapshots   |       ✓       |       ✓       |
 *  | Transactions w/out Snapshots  |       ✗       |       ✓       |
 *
 * ### D: Durability ?
 *
 * Durability doesn't apply to in-memory systems by definition.
 * In hybrid or persistent systems we prefer to disable it by default.
 * Almost every DBMS that builds on top of KVS prefers to implement its own durability mechanism.
 * Even more so in distributed databases, where three separate Write Ahead Logs may exist:
 * - in KVS,
 * - in DBMS,
 * - in Distributed Consensus implementation.
 * If you still need durability, flush writes or `ukv_transaction_commit()` with `::ukv_option_write_flush_k`.
 */
typedef void* ukv_transaction_t;

/**
 * @brief Some unique integer identifier of a collection.
 * A @c ukv_database_t database can have many of those,
 * but never with repeating names or identifiers.
 */
typedef uint64_t ukv_collection_t;

/**
 * @brief The unique identifier of any value within a single collection.
 *
 * ## On Variable Length Keys
 *
 * As of current version, 64-bit signed integers are used to allow unique
 * keys in the range from `[0, 2^63)`. 128-bit builds with UUIDs can be
 * considered, but variable length keys are highly discouraged.
 *
 * Using variable length keys forces numerous limitations on the design of a Key-Value store.
 * Besides slow character-wise comparisons it means solving the "persistent space allocation"
 * problem twice - for both keys and values.
 *
 * The recommended approach to dealing with string keys is:
 *
 * 1. Choose a mechanism to generate unique integer keys (UID). Ex: monotonically increasing values.
 * 2. Use "paths" modality to build-up a persistent hash-map of strings to UIDs.
 * 3. Use those UIDs to address the rest of the data in binary, document and graph modalities.
 *
 * This will result in a single conversion point from string to integer representations
 * and will keep most of the system snappy and the interfaces simpler than what they could have been.
 */
typedef int64_t ukv_key_t;

/**
 * @brief The elementary binary piece of any value.
 */
typedef uint8_t ukv_byte_t;

/**
 * @brief The elementary piece of any string, like collection name.
 */
typedef char ukv_char_t;

/**
 * @brief The length of any value in the DB.
 *
 * ## Why not use 64-bit lengths?
 *
 * Key-Value Stores are generally intended for high-frequency operations.
 * Frequently (thousands of times each second) accessing and modifing 4 GB and larger files
 * is impossible on modern hardware. So we stick to smaller length types, which also makes
 * using Apache Arrow representation slightly easier and allows the KVs to compress indexes
 * better.
 */
typedef uint32_t ukv_length_t;

/**
 * @brief Pointer-sized integer type.
 */
typedef uint64_t ukv_size_t;

/**
 * @brief The smallest possible "bitset" type, storing eight zeros or ones.
 */
typedef uint8_t ukv_octet_t;

/**
 * @brief Owning error message string.
 * If not null, must be deallocated via `ukv_error_free()`.
 */
typedef char const* ukv_error_t;

/**
 * @brief Non-owning string reference.
 * Always provided by user and we don't participate
 * in its lifetime management in any way.
 */
typedef char const* ukv_str_view_t;
typedef char* ukv_str_span_t;

/**
 * @brief Temporary memory handle, used mostly for read requests.
 * It's allocated, resized and deallocated only by UKV itself.
 * Once done, must be deallocated with `ukv_arena_free()`.
 * @see `ukv_arena_free()`.
 */
typedef void* ukv_arena_t;

typedef uint8_t* ukv_bytes_ptr_t;
typedef uint8_t const* ukv_bytes_cptr_t;

typedef void* ukv_callback_payload_t;
typedef void (*ukv_callback_t)(ukv_callback_payload_t);

typedef enum {

    ukv_options_default_k = 0,
    /**
     * @brief Forces absolute consistency on the write operations
     * flushing all the data to disk after each write. It's usage
     * may cause severe performance degradation in some implementations.
     * Yet the users must be warned, that modern IO drivers still often
     * can't guarantee that everything will reach the disk.
     */
    ukv_option_write_flush_k = 1 << 1,
    /**
     * @brief When reading from a transaction, we track the requested keys.
     * If the requested key was updated since the read, the transaction
     * will fail on commit or prior to that. This option disables collision
     * detection on separate parts of transactional reads and writes.
     */
    ukv_option_transaction_dont_watch_k = 1 << 2,
    /**
     * @brief This flag is intended for internal use.
     * When passed to `make_stl_arena`, old_arena is not released,
     * and rather a new one is casted and returned,
     * if it existed in the first place, otherwise behaviour is unaffected.
     */
    ukv_option_dont_discard_memory_k = 1 << 4,
    /**
     * @brief Will output data into shared memory, not the one privately
     * to do further transformations without any copies.
     * Is relevant for standalone distributions used with drivers supporting
     * Apache Arrow buffers or standardized Tensor representations.
     */
    ukv_option_read_shared_memory_k = 1 << 5,
    /**
     * @brief When set, the underlying engine may avoid strict keys ordering
     * and may include irrelevant (deleted & duplicate) keys in order to maximize
     * throughput. The purpose is not accelerating the `ukv_scan()`, but the
     * following `ukv_read()`. Generally used for Machine Learning applications.
     */
    ukv_option_scan_bulk_k = 0, // TODO

} ukv_options_t;

/**
 * @brief The "mode" of collection removal.
 */
typedef enum {
    /** @brief Clear the values, but keep the keys. */
    ukv_drop_vals_k = 0,
    /** @brief Remove keys and values, but keep the collection. */
    ukv_drop_keys_vals_k = 1,
    /** @brief Remove the handle and all of the contents. */
    ukv_drop_keys_vals_handle_k = 2,
} ukv_drop_mode_t;

/**
 * @brief The handle to the default nameless collection.
 * It exists from start, doesn't have to be created and can't be fully dropped.
 * Only `::ukv_drop_keys_vals_k` and `::ukv_drop_vals_k` apply to it.
 */
extern ukv_collection_t const ukv_collection_main_k;
extern ukv_length_t const ukv_length_missing_k;
extern ukv_key_t const ukv_key_unknown_k;

extern bool const ukv_supports_transactions_k;
extern bool const ukv_supports_named_collections_k;
extern bool const ukv_supports_snapshots_k;

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief Opens the underlying Key-Value Store.
 * @see `ukv_database_init()`.
 *
 * Depending on the selected distribution can be any of:
 *
 * - embedded persistent transactional KVS
 * - embedded in-memory transactional KVS
 * - remote persistent transactional KVS
 * - remote in-memory transactional KVS
 */
typedef struct ukv_database_init_t {
    /** @brief A NULL-terminated @b JSON string with configuration specs. */
    ukv_str_view_t config = NULL;
    /** @brief A pointer to the opened KVS, unless `error` is filled. */
    ukv_database_t* db;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;
} ukv_database_init_t;

/**
 * @brief Opens the underlying Key-Value Store.
 * @see `ukv_database_init()`.
 */
void ukv_database_init(ukv_database_init_t*);

/**
 * @brief Main "setter" or "scatter" interface.
 * @see `ukv_write()`.
 *
 * ## Functionality
 *
 * This is one of the two primary methods, together with `ukv_read()`,
 * so its functionality is wide. It knots together various kinds of updates:
 *
 * - Single writes and Batches.
 * - On Head state or a Transactional.
 * - Transparent or Watching through Transactions.
 * - Upserting, Clearing or Removing values.
 *
 * Check docs below to see how different variants can be invoked.
 *
 * ## Upserts, Updates & Inserts
 *
 * Higher-level interfaces may choose to implement any of those verbs:
 *
 * 1. Insert: add if missing.
 * 2. Update: overwrite if present.
 * 3. Upsert: write.
 * 4. Remove: overwrite with `NULL` if present.
 *
 * Instead of adding all three to the binary C interface, we focus on better ACID transactions,
 * which can be used to implement any advanced multi-step operations (often including conditionals),
 * like Compare-And-Swap, without losing atomicity.
 *
 * ## Understanding Contents Arguments
 *
 * Assuming `task_idx` is smaller than `tasks_count`, following approximates how the content chunks
 * are sliced.
 *
 * ```cpp
 *      std::size_t task_idx = ...;
 *      auto chunk_begin = values + values_stride * task_idx;
 *      auto offset_in_chunk = offsets + offsets_stride * task_idx;
 *      auto length = lengths + lengths_stride * task_idx;
 *      std::string_view content = { chunk_begin + offset_in_chunk, length };
 * ```
 *
 * It is not the simplest interface, but like Vulkan or BLAS, it gives you extreme flexibility.
 * Submitting a single write request can be as easy as:
 *
 * ```c
 *      ukv_key_t key { 42 };
 *      ukv_bytes_cptr_t value { "meaning of life" };
 *      ukv_write_t write {
 *          .db = &db,
 *          .keys = &key,
 *          .values = &value,
 *      };
 *      ukv_write(&write);
 * ```
 *
 * Similarly, submitting a batch may look like:
 *
 * ```c
 *      ukv_key_t keys[2] = { 42, 43 };
 *      ukv_bytes_cptr_t values[2] { "meaning of life", "is unknown" };
 *      ukv_write_t write {
 *          .db = &db,
 *          .tasks_count = 2,
 *          .keys = keys,
 *          .keys_stride = sizeof(ukv_key_t),
 *          .values = values,
 *          .values_stride = sizeof(ukv_bytes_cptr_t),
 *      };
 *      ukv_write(&write);
 * ```
 *
 * The beauty of strides is that your data may have an Array-of-Structures layout,
 * rather than Structure-of-Arrays. Like this:
 *
 * ```c
 *      struct pair_t {
 *          ukv_key_t key;
 *          ukv_bytes_cptr_t value;
 *      };
 *      pair_t pairs[2] = {
 *          { 42, "meaning of life" },
 *          { 43, "is unknown" },
 *      };
 *      ukv_write_t write {
 *          .db = &db,
 *          .tasks_count = 2,
 *          .keys = &pairs[0].key,
 *          .keys_stride = sizeof(pair_t),
 *          .values = &pairs[0].value,
 *          .values_stride = sizeof(pair_t),
 *      };
 *      ukv_write(&write);
 * ```
 *
 * To delete an entry, one can perform a similar trick:
 *
 * ```c
 *      ukv_key_t key { 42 };
 *      ukv_bytes_cptr_t value { NULL };
 *      ukv_write_t write {
 *          .db = &db,
 *          .keys = &key,
 *          .values = &value,
 *      };
 *      ukv_write(&write);
 * ```
 *
 * Or just pass `.values = NULL` all together.
 */
typedef struct ukv_write_t {

    /// @name Context
    /// @{

    /** @brief Already open database instance. */
    ukv_database_t db;
    /**
     * @brief Pointer to exported error message.
     * If not NULL, must be deallocated with `ukv_error_free()`.
     */
    ukv_error_t* error;
    /**
     * @brief The transaction in which the operation will be watched.
     * @see `ukv_transaction_init()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
     */
    ukv_transaction_t transaction = NULL;
    /**
     * @brief Reusable memory handle.
     * @see `ukv_arena_free()`.
     */
    ukv_arena_t* arena = NULL;
    /**
     * @brief Write options.
     *
     * Possible values:
     * - `::ukv_option_write_flush_k`: Forces to persist non-transactional writes on disk before returning.
     * - `::ukv_option_transaction_dont_watch_k`: Disables collision-detection for transactional writes.
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /// @}
    /// @name Locations
    /// @{

    /**
     * @brief Number of separate operations packed in this read.
     * Always equal to the number of provided `keys`.
     */
    ukv_size_t tasks_count = 1;
    /**
     * @brief Sequence of collections owning the `keys`.
     *
     * If `NULL` is passed, the default collection is assumed.
     * If multiple collections are passed, the step between them is defined by `collections_stride`.
     * Use `ukv_collection_create()` or `ukv_collection_list()` to obtain collection IDs for string names.
     * Is @b optional.
     */
    ukv_collection_t const* collections = NULL;
    /**
     * @brief Step between `collections`.
     *
     * Contains the number of bytes separating entries in the `collections` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve different keys from the same collection in one call.
     * Is @b optional.
     */
    ukv_size_t collections_stride = 0;
    /**
     * @brief Sequence of keys to update.
     *
     * Contains the pointer to the first of `tasks_count` keys to be collected from `collections`.
     * If multiple keys are passed, the step between them is defined by `keys_stride`.
     */
    ukv_key_t const* keys;
    /**
     * @brief Step between `keys`.
     *
     * The number of bytes separating entries in the `keys` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve the same key from different collections in one call.
     * Is @b optional.
     */
    ukv_size_t keys_stride = 0;

    /// @}
    /// @name Contents
    /// @{

    /**
     * @brief Bitmask of "presence" indicators with at least `tasks_count` bits.
     *
     * Each set bit means that the respective content chunk isn't `NULL`.
     * Is addressed the same way as in Apache Arrow.
     * Is @b optional.
     */
    ukv_octet_t const* presences = NULL;
    /**
     * @brief The pointer to the offset (in bytes) of the first content within
     * the first chunk of `values`.
     *
     * If tasks are executing, the step between offsets is defined by `offsets_stride`.
     * Not only it allows addressing different parts of a concatenated tape,
     * but also allows to skip some structure header, like the @c PyObject_HEAD
     * in every CPython runtime object.
     */
    ukv_length_t const* offsets = NULL;
    /**
     * @brief Step between `offsets`.
     *
     * The number of bytes separating entries in the `offsets` array.
     * Zero stride would reuse the same address for all tasks.
     * Is @b optional.
     */
    ukv_size_t offsets_stride = 0;
    /**
     * @brief The pointer to the offset (in bytes) of the first content within
     * the first chunk of `values`. Zero-length entries are allowed.
     *
     * Is @b optional, as lengths can be inferred from consecutive offsets.
     * If neither `offsets` nor `lengths` are passed, we assume values
     * to be `NULL`-terminated and infer the length from `values` themselves.
     */
    ukv_length_t const* lengths = NULL;
    /**
     * @brief Step between `keys`.
     *
     * The number of bytes separating entries in the `keys` array.
     * Zero stride would reuse the same address for all tasks.
     * Is @b optional.
     */
    ukv_size_t lengths_stride = 0;
    /**
     * @brief An array of pointers to data chunks.
     * If `NULL`, will simply delete all the `keys` from respective `collections`.
     *
     * Generally, we expect each chunk to be identical to the content we want to write,
     * but we can specify subspans by using `offsets` and `lengths`.
     */
    ukv_bytes_cptr_t const* values;
    /**
     * @brief The number of bytes separating entries in the `values` array.
     * Zero stride would reuse the same address for all tasks.
     * Is @b optional.
     */
    ukv_size_t values_stride = 0;

    /// @}

} ukv_write_t;

/**
 * @brief Main "setter" or "scatter" interface.
 * @see `ukv_write_t`.
 */
void ukv_write(ukv_write_t*);

/**
 * @brief Main "getter" or "gather" interface.
 * @see `ukv_read()`.
 *
 * ## Functionality
 *
 * This is one of the two primary methods, together with `ukv_write()`,
 * so its functionality is wide. It knots together various kinds of reads:
 *
 * - Single reads and Batches.
 * - On Head state or a Snapshot.
 * - Transparent or Watching through Transactions.
 * - Reading the entire values or just checking existance or lengths.
 *
 * Check docs below to see how different variants can be invoked.
 */
struct ukv_read_t {

    /// @name Context
    /// @{

    /** @brief Already open database instance. */
    ukv_database_t db;
    /**
     * @brief Pointer to exported error message.
     * If not NULL, must be deallocated with `ukv_error_free()`.
     */
    ukv_error_t* error;
    /**
     * @brief The transaction in which the operation will be watched.
     * @see `ukv_transaction_init()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
     */
    ukv_transaction_t transaction = NULL;
    ukv_transaction_t snapshot = NULL;
    /**
     * @brief Reusable memory handle.
     * @see `ukv_arena_free()`.
     */
    ukv_arena_t* arena = NULL;
    /**
     * @brief Read options.
     *
     * Possible values:
     * - `::ukv_option_transaction_dont_watch_k`: Disables collision-detection for transactional reads.
     * - `::ukv_option_read_shared_memory_k`: Exports to shared memory to accelerate inter-process communication.
     * - `::ukv_option_scan_bulk_k`: Suggests that the list of keys was received from a bulk scan.
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /// @}
    /// @name Inputs
    /// @{

    /**
     * @brief Number of separate operations packed in this read.
     * Always equal to the number of provided `keys`.
     */
    ukv_size_t tasks_count = 1;
    /**
     * @brief Sequence of collections owning the `keys`.
     *
     * If `NULL` is passed, the default collection is assumed.
     * If multiple collections are passed, the step between them is defined by `collections_stride`.
     * Use `ukv_collection_create()` or `ukv_collection_list()` to obtain collection IDs for string names.
     * Is @b optional.
     */
    ukv_collection_t const* collections = NULL;
    /**
     * @brief Step between `collections`.
     *
     * Contains the number of bytes separating entries in the `collections` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve different keys from the same collection in one call.
     * Is @b optional.
     */
    ukv_size_t collections_stride = 0;
    /**
     * @brief Sequence of `keys` to retrieve.
     *
     * Contains the pointer to the first of `tasks_count` keys to be collected from `collections`.
     * If multiple keys are passed, the step between them is defined by `keys_stride`.
     */
    ukv_key_t const* keys;
    /**
     * @brief Step between `keys`.
     *
     * The number of bytes separating entries in the `keys` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve the same key from different collections in one call.
     * Is @b optional.
     */
    ukv_size_t keys_stride = 0;

    /// @}
    /// @name Outputs
    /// @{

    /**
     * @brief Output presence (non-NULL) indicators.
     *
     * Will contain a bitset with at least `tasks_count` bits.
     * Each set bit means that such key is present in DB.
     * Is addressed the same way as in Apache Arrow.
     * Is @b optional.
     */
    ukv_octet_t** presences = NULL;
    /**
     * @brief Output content offsets within `values`.
     *
     * Will contain a pointer to an array of `tasks_count` integer offsets.
     * Each marks a response offset in bytes starting from `values`.
     * To be fully compatible with Apache Arrow we append one more offset at
     * the end to allow inferring the length of the last entry without using `lengths`.
     * Is @b optional, as you may only want to get `lengths` or check `presences`.
     */
    ukv_length_t** offsets = NULL;
    /**
     * @brief Output content lengths within `values`.
     *
     * Will contain a pointer to an array of `tasks_count` integer offsets.
     * Each defines a response length in bytes within `values`.
     * Is @b optional.
     */
    ukv_length_t** lengths = NULL;
    /**
     * @brief Output content tape.
     *
     * Will contain the base pointer for the `tasks_count` strings.
     * Instead of allocating every "string" separately, we join them into
     * a single "tape" structure, which later be exported into (often disjoint)
     * runtime- or library-specific implementations.
     * You should use `presences`, `offsets` and `lengths` to split the contents.
     * - If `offsets` and `lengths` are exported, entries aren't guaranteed to be in any order.
     * - If only `offsets` are exported, entries are concatenated in-order without any gaps.
     *   The resulting `offsets` are guaranteed to have `tasks_count + 1` entries for Apache Arrow.
     *   The last offset will be used to determine the length of the last entry.
     *   Zero-length and Missing entries will be indistinguishable unless you also check `presences`.
     * - If only `lengths` are exported, entries are concatenated in-order without any gaps.
     *   Missing entries will have a length equal to `::ukv_length_missing_k`.
     *
     * Is @b optional, as you may only want to get `lengths` or check `presences`.
     */
    ukv_byte_t** values = NULL;
    /// @}
};

/**
 * @brief Main "getter" interface.
 * @see `ukv_read_t`.
 */
void ukv_read(ukv_read_t*);

/**
 * @brief Main "scanning", "range selection", "iteration", "enumeration" interface.
 * @see `ukv_scan()`.
 *
 * Retrieves the following (upto) `count_limits[i]` keys starting
 * from `start_key[i]` or the smallest following key in each collection.
 * Values are not exported, for that - follow up with a `ukv_read()` or
 * a higher-level interface for Graphs, Docs or other modalities.
 *
 * ## Scans vs Iterators
 *
 * Implementing consistent iterators over concurrent state is exceptionally
 * expensive, thus we plan to implement those via "Pagination".
 *
 * ## Bulk Scans
 *
 */
typedef struct ukv_scan_t {

    /// @name Context
    /// @{

    /** @brief Already open database instance. */
    ukv_database_t db;
    /**
     * @brief Pointer to exported error message.
     * If not NULL, must be deallocated with `ukv_error_free()`.
     */
    ukv_error_t* error;
    /**
     * @brief The transaction in which the operation will be watched.
     * @see `ukv_transaction_init()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
     */
    ukv_transaction_t transaction = NULL;
    /**
     * @brief Reusable memory handle.
     * @see `ukv_arena_free()`.
     */
    ukv_arena_t* arena = NULL;
    /**
     * @brief Scan options.
     *
     * Possible values:
     * - `::ukv_option_scan_bulk_k`: Allows out-of-order retrieval for higher throughput.
     * - `::ukv_option_transaction_dont_watch_k`: Disables collision-detection for transactional reads.
     * - `::ukv_option_read_shared_memory_k`: Exports to shared memory to accelerate inter-process communication.
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /// @}
    /// @name Inputs
    /// @{

    /**
     * @brief Number of separate operations packed in this read.
     * Always equal to the number of provided `start_keys`.
     */
    ukv_size_t tasks_count = 1;
    /**
     * @brief Sequence of collections owning the `start_keys`.
     *
     * If `NULL` is passed, the default collection is assumed.
     * If multiple collections are passed, the step between them is defined by `collections_stride`.
     * Use `ukv_collection_create()` or `ukv_collection_list()` to obtain collection IDs for string names.
     * Is @b optional.
     */
    ukv_collection_t const* collections = NULL;
    /**
     * @brief Step between `collections`.
     *
     * Contains the number of bytes separating entries in the `collections` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve different keys from the same collection in one call.
     * Is @b optional.
     */
    ukv_size_t collections_stride = 0;
    /**
     * @brief Starting points for each scan or "range select".
     *
     * Contains the pointer to the first of `tasks_count` starting points.
     * If multiple scan tasks are passed, the step between them is defined by `start_keys_stride`.
     */
    ukv_key_t const* start_keys;
    /**
     * @brief Step between `start_keys`.
     *
     * Contains the number of bytes separating entries in the `start_keys` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve the same "ray" of keys from different `collections`.
     * Is @b optional.
     */
    ukv_size_t start_keys_stride = 0;
    /**
     * @brief Number of consecutive entries to read in each request.
     *
     * Contains the pointer to the first of `tasks_count` starting points.
     * If multiple scan tasks are passed, the step between them is defined by `count_limits_stride`.
     */
    ukv_length_t const* count_limits;
    /**
     * @brief Step between `count_limits`.
     *
     * Contains the number of bytes separating entries in the `count_limits` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve identical number of entries from different start points.
     * Is @b optional.
     */
    ukv_size_t count_limits_stride = 0;

    /// @}
    /// @name Outputs
    /// @{

    /**
     * @brief Output number of exported entries before for each scan.
     *
     * Will contain a pointer to an array of `tasks_count` integer offsets.
     * Each marks a response offset in number of @c ukv_key_t` keys starting from `*keys.
     * To be fully compatible with Apache Arrow we append one more offset at
     * the end to allow inferring the length of the last entry without using `lengths`.
     * Is @b optional, as you may only want to get `lengths` or check `presences`.
     */
    ukv_length_t** offsets = NULL;
    /**
     * @brief Output number of found entries for each scan.
     *
     * Will contain a pointer to an array of `tasks_count` integer offsets.
     * Each defines a response length in bytes within `keys`.
     * For any `i` under `tasks_count`, this holds: `count_limits[i] >= counts[i]`.
     * Is @b optional.
     */
    ukv_length_t** counts;
    /**
     * @brief Output keys tape.
     *
     * Will contain the base pointer for the `tasks_count` variable length arrays.
     * Instead of allocating every array of key ranges separately, we join them into
     * a single "tape" structure, which later be exported into (often disjoint)
     * runtime- or library-specific implementations.
     */
    ukv_key_t** keys;
    /// @}

} ukv_scan_t;

/**
 * @brief Main "scanning", "range selection", "iteration", "enumeration" interface.
 * @see `ukv_scan_t`.
 */
void ukv_scan(ukv_scan_t*);

/**
 * @brief Uniformly randomly samples keys from provided collections.
 * @see `ukv_sample()`.
 */
typedef struct ukv_sample_t {

    /// @name Context
    /// @{

    /** @brief Already open database instance. */
    ukv_database_t db;
    /**
     * @brief Pointer to exported error message.
     * If not NULL, must be deallocated with `ukv_error_free()`.
     */
    ukv_error_t* error;
    /**
     * @brief The transaction in which the operation will be watched.
     * @see `ukv_transaction_init()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
     */
    ukv_transaction_t transaction = NULL;
    /**
     * @brief Reusable memory handle.
     * @see `ukv_arena_free()`.
     */
    ukv_arena_t* arena = NULL;
    /**
     * @brief Scan options.
     *
     * Possible values:
     * - `::ukv_option_scan_bulk_k`: Allows out-of-order retrieval for higher throughput.
     * - `::ukv_option_transaction_dont_watch_k`: Disables collision-detection for transactional reads.
     * - `::ukv_option_read_shared_memory_k`: Exports to shared memory to accelerate inter-process communication.
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /// @}
    /// @name Inputs
    /// @{

    /**
     * @brief Number of separate operations packed in this read.
     * Always equal to the number of provided `start_keys`.
     */
    ukv_size_t tasks_count = 1;
    /**
     * @brief Sequence of collections owning the `start_keys`.
     *
     * If `NULL` is passed, the default collection is assumed.
     * If multiple collections are passed, the step between them is defined by `collections_stride`.
     * Use `ukv_collection_create()` or `ukv_collection_list()` to obtain collection IDs for string names.
     * Is @b optional.
     */
    ukv_collection_t const* collections = NULL;
    /**
     * @brief Step between `collections`.
     *
     * Contains the number of bytes separating entries in the `collections` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve different keys from the same collection in one call.
     * Is @b optional.
     */
    ukv_size_t collections_stride = 0;
    /**
     * @brief Number of samples to be gatherd for each request.
     *
     * Contains the pointer to the first of `tasks_count` starting points.
     * If multiple samples are requested, the step between them is defined by `count_limits_stride`.
     */
    ukv_length_t const* count_limits;
    /**
     * @brief Step between `count_limits`.
     *
     * Contains the number of bytes separating entries in the `count_limits` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve identical number of entries from different collections.
     * Is @b optional.
     */
    ukv_size_t count_limits_stride = 0;

    /// @}
    /// @name Outputs
    /// @{

    /**
     * @brief Output number of exported entries before for each scan.
     *
     * Will contain a pointer to an array of `tasks_count` integer offsets.
     * Each marks a response offset in number of @c ukv_key_t` keys starting from `*keys.
     * To be fully compatible with Apache Arrow we append one more offset at
     * the end to allow inferring the length of the last entry without using `lengths`.
     * Is @b optional, as you may only want to get `lengths` or check `presences`.
     */
    ukv_length_t** offsets = NULL;
    /**
     * @brief Output number of found entries for each scan.
     *
     * Will contain a pointer to an array of `tasks_count` integer offsets.
     * Each defines a response length in bytes within `keys`.
     * For any `i` under `tasks_count`, this holds: `count_limits[i] >= counts[i]`.
     * Is @b optional.
     */
    ukv_length_t** counts;
    /**
     * @brief Output keys tape.
     *
     * Will contain the base pointer for the `tasks_count` variable length arrays.
     * Instead of allocating every array of key ranges separately, we join them into
     * a single "tape" structure, which later be exported into (often disjoint)
     * runtime- or library-specific implementations.
     */
    ukv_key_t** keys;
    /// @}

} ukv_sample_t;

/**
 * @brief Uniformly randomly samples keys from provided collections.
 * @see `ukv_sample_t`.
 */
void ukv_sample(ukv_sample_t*);

/**
 * @brief Estimates the number of entries and memory usage for a range of keys.
 * @see `ukv_measure()`.
 */
typedef struct ukv_measure_t {

    /// @name Context
    /// @{

    /** @brief Already open database instance. */
    ukv_database_t db;
    /**
     * @brief Pointer to exported error message.
     * If not NULL, must be deallocated with `ukv_error_free()`.
     */
    ukv_error_t* error;
    /**
     * @brief The transaction in which the operation will be watched.
     * @see `ukv_transaction_init()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
     */
    ukv_transaction_t transaction = NULL;
    /**
     * @brief Reusable memory handle.
     * @see `ukv_arena_free()`.
     */
    ukv_arena_t* arena = NULL;
    /**
     * @brief Scan options.
     *
     * Possible values:
     * - `::ukv_option_scan_bulk_k`: Allows out-of-order retrieval for higher throughput.
     * - `::ukv_option_transaction_dont_watch_k`: Disables collision-detection for transactional reads.
     * - `::ukv_option_read_shared_memory_k`: Exports to shared memory to accelerate inter-process communication.
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /// @}
    /// @name Inputs
    /// @{

    /**
     * @brief Number of separate operations packed in this read.
     * Always equal to the number of provided `start_keys`.
     */
    ukv_size_t tasks_count = 1;
    /**
     * @brief Sequence of collections owning the `start_keys`.
     *
     * If `NULL` is passed, the default collection is assumed.
     * If multiple collections are passed, the step between them is defined by `collections_stride`.
     * Use `ukv_collection_create()` or `ukv_collection_list()` to obtain collection IDs for string names.
     * Is @b optional.
     */
    ukv_collection_t const* collections = NULL;
    /**
     * @brief Step between `collections`.
     *
     * Contains the number of bytes separating entries in the `collections` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve different keys from the same collection in one call.
     * Is @b optional.
     */
    ukv_size_t collections_stride = 0;
    /**
     * @brief Starting points for each estimate.
     *
     * Contains the pointer to the first of `tasks_count` starting points.
     * If multiple scan tasks are passed, the step between them is defined by `start_keys_stride`.
     */
    ukv_key_t const* start_keys;
    /**
     * @brief Step between `start_keys`.
     *
     * Contains the number of bytes separating entries in the `start_keys` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve the same "ray" of keys from different `collections`.
     * Is @b optional.
     */
    ukv_size_t start_keys_stride = 0;
    /**
     * @brief Ending points for each estimate.
     *
     * Contains the pointer to the first of `tasks_count` ending points.
     * If multiple scan tasks are passed, the step between them is defined by `end_keys_stride`.
     */
    ukv_key_t const* end_keys;
    /**
     * @brief Step between `end_keys`.
     *
     * Contains the number of bytes separating entries in the `end_keys` array.
     * Zero stride would reuse the same address for all tasks.
     * You can use it to retrieve the same "ray" of keys from different `collections`.
     * Is @b optional.
     */
    ukv_size_t end_keys_stride = 0;

    /// @}
    /// @name Outputs
    /// @{

    ukv_size_t** min_cardinalities;
    ukv_size_t** max_cardinalities;
    ukv_size_t** min_value_bytes;
    ukv_size_t** max_value_bytes;
    ukv_size_t** min_space_usages;
    ukv_size_t** max_space_usages;

    /// @}

} ukv_measure_t;

/**
 * @brief Estimates the number of entries and memory usage for a range of keys.
 * @see `ukv_measure()`.
 */
void ukv_measure(ukv_measure_t*);

/*********************************************************/
/***************** Collection Management  ****************/
/*********************************************************/

/**
 * @brief Lists all named collections in the DB.
 * @see `ukv_collection_list()`.
 *
 * Retrieves a list of collection IDs & names in a NULL-delimited form.
 * The default nameless collection won't be described in any form, as its always
 * present. This is the only collection-management operation that can be performed
 * on a DB state snapshot, and not just on the HEAD state.
 */
typedef struct ukv_collection_list_t {

    /// @name Context
    /// @{

    /** @brief Already open database instance. */
    ukv_database_t db;
    /**
     * @brief Pointer to exported error message.
     * If not NULL, must be deallocated with `ukv_error_free()`.
     */
    ukv_error_t* error;
    /**
     * @brief The snapshot in which the retrieval will be conducted.
     * @see `ukv_transaction_init()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
     */
    ukv_transaction_t transaction = NULL;
    /**
     * @brief Reusable memory handle.
     * @see `ukv_arena_free()`.
     */
    ukv_arena_t* arena = NULL;
    /**
     * @brief Listing options.
     *
     * Possible values:
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /// @}
    /// @name Contents
    /// @{

    /** @brief Number of present collections. */
    ukv_size_t* count;
    /** @brief Handles of all the collections in same order as `names`. */
    ukv_collection_t** ids;
    /** @brief Offsets of separate strings in the `names` tape. */
    ukv_length_t** offsets;
    /** @brief NULL-termainted collection names tape in same order as `ids`. */
    ukv_char_t** names;
    /// @}

} ukv_collection_list_t;

/**
 * @brief Lists all named collections in the DB.
 * @see `ukv_collection_list_t`.
 */
void ukv_collection_list(ukv_collection_list_t*);

/**
 * @brief Creates a new uniquely named collection in the DB.
 * @see `ukv_collection_create()`.
 *
 * This function may never be called, as the default nameless collection
 * always exists and can be addressed via `::ukv_collection_main_k`.
 * You can "re-create" an empty collection with a new config.
 */
typedef struct ukv_collection_create_t {
    /** @brief Already open database instance. */
    ukv_database_t db;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;
    /** @brief Unique name for the new collection. */
    ukv_str_view_t name;
    /** @brief Optional configuration JSON string. */
    ukv_str_view_t config = NULL;
    /** @brief Output for the collection handle. */
    ukv_collection_t* id;
} ukv_collection_create_t;

/**
 * @brief Creates a new uniquely named collection in the DB.
 * @see `ukv_collection_create_t`.
 */
void ukv_collection_create(ukv_collection_create_t*);

/**
 * @brief Removes or clears an existing collection.
 * @see `ukv_collection_drop()`.
 *
 * Removes a collection or its contents depending on `mode`.
 * The default nameless collection can't be removed, only cleared.
 */
typedef struct ukv_collection_drop_t {
    /** @brief Already open database instance. */
    ukv_database_t db;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;
    /** @brief Existing collection handle. */
    ukv_collection_t id;
    /** @brief Controls if values, pairs or the whole collection must be dropped. */
    ukv_drop_mode_t mode = ukv_drop_keys_vals_handle_k;
} ukv_collection_drop_t;

/**
 * @brief Removes or clears an existing collection.
 * @see `ukv_collection_drop_t`.
 */
void ukv_collection_drop(ukv_collection_drop_t*);

/**
 * @brief Free-form communication tunnel with the underlying engine.
 * @see `ukv_database_control()`.
 *
 * Performs free-form queries on the DB, that may not necessarily
 * have a stable API and a fixed format output. Generally, those requests
 * are very expensive and shouldn't be executed in most applications.
 * This is the "kitchen-sink" of UKV interface, similar to `fcntl` & `ioctl`.
 *
 * ## Possible Commands
 * - "clear":   Removes all the data from DB, while keeping collection names.
 * - "reset":   Removes all the data from DB, including collection names.
 * - "compact": Flushes and compacts all the data in LSM-tree implementations.
 * - "info":    Metadata about the current software version, used for debugging.
 * - "usage":   Metadata about approximate collection sizes, RAM and disk usage.
 */
typedef struct ukv_database_control_t {
    /** @brief Already open database instance. */
    ukv_database_t db;
    /** @brief Reusable memory handle. */
    ukv_arena_t* arena = NULL;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;
    /** @brief The input command as a NULL-terminated string. */
    ukv_str_view_t request;
    /** @brief The output response as a NULL-terminated string. */
    ukv_str_view_t* response;
} ukv_database_control_t;

/**
 * @brief Free-form communication tunnel with the underlying engine.
 * @see `ukv_database_control()`.
 */
void ukv_database_control(ukv_database_control_t*);

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

/**
 * @brief Begins a new ACID transaction or resets an existing one.
 * @see `ukv_transaction_init()`.
 */
typedef struct ukv_transaction_init_t {

    /** @brief Already open database instance. */
    ukv_database_t db;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;

    /**
     * @brief Transaction options.
     *
     * Possible values:
     * - `::ukv_option_transaction_dont_watch_k`
     * - `::ukv_option_dont_discard_memory_k`: Won't reset the `arena` before the operation begins.
     */
    ukv_options_t options = ukv_options_default_k;

    /** @brief In-out transaction handle. */
    ukv_transaction_t* transaction;
} ukv_transaction_init_t;

/**
 * @brief Begins a new ACID transaction or resets an existing one.
 * @see `ukv_transaction_init_t`.
 */
void ukv_transaction_init(ukv_transaction_init_t*);

/**
 * @brief Stages an ACID transaction for Two Phase Commits.
 * @see `ukv_transaction_stage()`.
 *
 * Regardless of result, the content is preserved to allow further
 * logging, serialization or retries. The underlying memory can be
 * cleaned and reused by consecutive `ukv_transaction_init()` call.
 */
typedef struct ukv_transaction_stage_t {

    /** @brief Already open database instance. */
    ukv_database_t db;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;
    /** @brief Initialized transaction handle. */
    ukv_transaction_t transaction;
    /** @brief Staging options. */
    ukv_options_t options = ukv_options_default_k;

} ukv_transaction_stage_t;

/**
 * @brief Stages an ACID transaction for Two Phase Commits.
 * @see `ukv_transaction_stage_t`.
 */
void ukv_transaction_stage(ukv_transaction_stage_t*);

/**
 * @brief Commits an ACID transaction.
 * @see `ukv_transaction_commit()`.
 *
 * Regardless of result, the content is preserved to allow further
 * logging, serialization or retries. The underlying memory can be
 * cleaned and reused by consecutive `ukv_transaction_init()` call.
 */
typedef struct ukv_transaction_commit_t {

    /** @brief Already open database instance. */
    ukv_database_t db;
    /** @brief Pointer to exported error message. */
    ukv_error_t* error;
    /** @brief Initialized transaction handle. */
    ukv_transaction_t transaction;
    /** @brief Staging options. */
    ukv_options_t options = ukv_options_default_k;

} ukv_transaction_commit_t;

/**
 * @brief Commits an ACID transaction.
 * @see `ukv_transaction_commit_t`.
 */
void ukv_transaction_commit(ukv_transaction_commit_t*);

/*********************************************************/
/*****************	 Memory Reclamation   ****************/
/*********************************************************/

/**
 * @brief Deallocates reusable memory arenas.
 * Passing NULLs is safe.
 */
void ukv_arena_free(ukv_arena_t);

/**
 * @brief Resets the transaction and deallocates the underlying memory.
 * Passing NULLs is safe.
 */
void ukv_transaction_free(ukv_transaction_t);

/**
 * @brief Closes the DB and deallocates used memory.
 * The database would still persist on disk.
 * Passing NULLs is safe.
 */
void ukv_database_free(ukv_database_t);

/**
 * @brief Deallocates error messages.
 * Passing NULLs is safe.
 */
void ukv_error_free(ukv_error_t);

#ifdef __cplusplus
} /* end extern "C" */
#endif
