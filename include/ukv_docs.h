/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 27 Jun 2022
 * @brief C bindings for Unums Key-Value store collections of @b Documents.
 * It extends the basic "ukv.h" towards values storing hierarchical documents.
 * Examples: JSONs, MsgPacks, BSONs and a number of other similar formats.
 * Yet no guarantees are provided regarding the internal representation of the
 * values, so if if you want to access same values through binary interface,
 * you may not get the exact same bytes as you have provided in.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "ukv.h"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

typedef enum {
    ukv_format_binary_k = 0,
    ukv_format_json_k = 1,
    ukv_format_msgpack_k = 2,
    ukv_format_bson_k = 3,
    ukv_format_arrow_k = 4,
    ukv_format_parquet_k = 5,
    ukv_format_json_patch_k = 6,
} ukv_format_t;

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief The primary "setter" interface for sub-document-level data.
 * Is an extension of the @see `ukv_write` function for structured vals.
 *
 * @param[in] collections  Must have collections storing only docs!
 * @param[in] fields       Optional JSON-Pointer strings for field paths.
 * @param[in] format       Imported `values` format, which will be converted
 *                         to some internal representation. On retrieval, a
 *                         different format can be requested. Like importing
 *                         JSONs & BSONs from Mongo, but later exporting
 *                         Apache Arrow Tables.
 */
void ukv_docs_write( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_count,
    ukv_size_t const keys_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_count,
    ukv_size_t const fields_stride,

    ukv_options_t const options,
    ukv_format_t const format,

    ukv_tape_ptr_t const* values,
    ukv_size_t const values_stride,

    ukv_val_len_t const* lengths,
    ukv_size_t const lengths_stride,

    ukv_error_t* error);

/**
 * @brief The primary "getter" interface for sub-document-level data.
 * Is an extension of the @see `ukv_read` function for structured vals.
 *
 * @param[in] collections  Must have collections storing only docs!
 * @param[in] fields       Optional JSON-Pointer strings for field paths.
 * @param[in] format       Imported `values` format, which will be converted
 *                         to some internal representation. On retrieval, a
 *                         different format can be requested. Like importing
 *                         JSONs & BSONs from Mongo, but later exporting
 *                         Apache Arrow Tables.
 */
void ukv_docs_read( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_count,
    ukv_size_t const keys_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_count,
    ukv_size_t const fields_stride,

    ukv_options_t const options,
    ukv_format_t const format,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
