/**
 * @file paths.h
 * @author Ashot Vardanian
 * @date 23 Sep 2022
 * @brief C bindings for paths ~ variable length string keys collections.
 */

#pragma once

#include "db.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 */
void ukv_paths_write( //
    ukv_database_t const db,
    ukv_transaction_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_length_t const* paths_offsets,
    ukv_size_t const paths_offsets_stride,

    ukv_length_t const* paths_lengths,
    ukv_size_t const paths_lengths_stride,

    ukv_str_view_t const* paths,
    ukv_size_t const paths_stride,

    ukv_octet_t const* values_presences,

    ukv_length_t const* values_offsets,
    ukv_size_t const values_offsets_stride,

    ukv_length_t const* values_lengths,
    ukv_size_t const values_lengths_stride,

    ukv_bytes_cptr_t const* values_bytes,
    ukv_size_t const values_bytes_stride,

    ukv_options_t const options,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 */
void ukv_paths_read( //
    ukv_database_t const db,
    ukv_transaction_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_length_t const* paths_offsets,
    ukv_size_t const paths_offsets_stride,

    ukv_length_t const* paths_lengths,
    ukv_size_t const paths_lengths_stride,

    ukv_str_view_t const* paths,
    ukv_size_t const paths_stride,

    ukv_options_t const options,

    ukv_octet_t** presences,
    ukv_key_t** keys,
    ukv_length_t** offsets,
    ukv_length_t** lengths,
    ukv_byte_t** values,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 */
void ukv_paths_scan_prefix( //
    ukv_database_t const db,
    ukv_transaction_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* start_keys,
    ukv_size_t const start_keys_stride,

    ukv_key_t const* end_keys,
    ukv_size_t const end_keys_stride,

    ukv_length_t const* scan_limits,
    ukv_size_t const scan_limits_stride,

    ukv_options_t const options,

    ukv_length_t** offsets,
    ukv_length_t** counts,
    ukv_key_t** keys,

    ukv_arena_t* arena,
    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
