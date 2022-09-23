/**
 * @file modality_paths.cpp
 * @author Ashot Vardanian
 *
 * @brief Paths (variable length keys) compatibility layer.
 * Sits on top of any @see "ukv.h"-compatible system.
 *
 * For every string key hash we store:
 * * N: number of entries (1 if no collisions appeared)
 * * N key lengths
 * * N value lengths
 * * N concatenated keys
 * * N concatenated values
 */

#include "helpers.hpp"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

void ukv_paths_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_length_t const* c_paths_offsets,
    ukv_size_t const c_paths_offsets_stride,

    ukv_length_t const* c_paths_lengths,
    ukv_size_t const c_paths_lengths_stride,

    ukv_str_view_t const* c_paths,
    ukv_size_t const c_paths_stride,

    ukv_octet_t const* c_values_presences,

    ukv_length_t const* c_values_offsets,
    ukv_size_t const c_values_offsets_stride,

    ukv_length_t const* c_values_lengths,
    ukv_size_t const c_values_lengths_stride,

    ukv_bytes_cptr_t const* c_values_bytes,
    ukv_size_t const c_values_bytes_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {
}

void ukv_paths_read( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_length_t const* c_paths_offsets,
    ukv_size_t const c_paths_offsets_stride,

    ukv_length_t const* c_paths_lengths,
    ukv_size_t const c_paths_lengths_stride,

    ukv_str_view_t const* c_paths,
    ukv_size_t const c_paths_stride,

    ukv_options_t const c_options,

    ukv_octet_t** c_presences,
    ukv_key_t** c_keys,
    ukv_length_t** c_offsets,
    ukv_length_t** c_lengths,
    ukv_byte_t** c_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {
}

void ukv_paths_scan( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_start_paths,
    ukv_size_t const c_start_paths_stride,

    ukv_key_t const* c_end_paths,
    ukv_size_t const c_end_paths_stride,

    ukv_length_t const* c_scan_limits,
    ukv_size_t const c_scan_limits_stride,

    ukv_options_t const c_options,

    ukv_length_t** c_offsets,
    ukv_length_t** c_counts,
    ukv_key_t** c_keys,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {
}