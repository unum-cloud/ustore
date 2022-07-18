/**
 * @file docs.h
 * @author Ashot Vardanian
 * @date 27 Jun 2022
 *
 * @brief C bindings for collections of @b Documents.
 * It extends the basic "ukv.h" towards values storing hierarchical documents.
 * Examples: JSONs, MsgPacks, BSONs and a number of other similar formats.
 * Yet no guarantees are provided regarding the internal representation of the
 * values, so if if you want to access same values through binary interface,
 * you may not get the exact same bytes as you have provided in.
 *
 * @section Number of Keys vs Number of Fields
 * One of the biggest questions to API is preferring "Zips" vs the "Cartesian Product"
 * of "Key" and "Field" combinations. When writing we may want to discard a certain
 * subset of fields in every document, but we may also be interested in having a
 * more targetted approach.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "ukv/ukv.h"

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
    ukv_format_cbor_k = 7,
    ukv_format_ubjson_k = 8,
    ukv_format_unknown_k = 0xFFFFFFFF,
} ukv_format_t;

/**
 * Type IDs needed to describe the values stored in the leafs of
 * hierarchical documents. Most types mimic what's present in
 * Apache Arrow. Most often the `ukv_type_i64_k` and `ukv_type_f64_k`
 * are used. Aside from those
 */
typedef enum {

    ukv_type_bool_k = 1 << 0,
    ukv_type_i64_k = 1 << 1,
    ukv_type_f64_k = 1 << 2,
    ukv_type_uuid_k = 1 << 3,
    ukv_type_str_k = 1 << 4,

} ukv_type_t;

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

 * @section Slicing Docs and Inferring IDs
 * In other interfaces it's necessary to explicitly provide the @c `ukv_key_t`s
 * and the number of input entries. With documents, if an array of objects is
 * supplied as `values[0]`, we slice it into separate objects.
 * With documents, we can often infer the ID from the documents @b "_id" field,
 * similar to MongoDB and ElasticSearch.
  */
void ukv_docs_write( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_stride,

    ukv_options_t const options,
    ukv_format_t const format,

    ukv_val_ptr_t const* values,
    ukv_size_t const values_stride,

    ukv_val_len_t const* offsets,
    ukv_size_t const offsets_stride,

    ukv_val_len_t const* lengths,
    ukv_size_t const lengths_stride,

    ukv_arena_t* arena,
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
    ukv_size_t const tasks_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_stride,

    ukv_options_t const options,
    ukv_format_t const format,

    ukv_val_len_t** found_lengths,
    ukv_val_ptr_t* found_values,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 * @brief The vectorized "gather" interface, that collects, type-checks
 * and casts (N*M) values for M fields in N documents.
 */
void ukv_docs_gather( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,
    ukv_size_t const fields_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_stride,

    ukv_type_t const* types,
    ukv_size_t const types_stride,

    ukv_options_t const options,
    ukv_format_t const format,

    ukv_val_len_t** found_lengths,
    ukv_val_ptr_t* found_values,

    ukv_arena_t* arena,
    ukv_error_t* error);

/**
 * @brief Describes the statistics (presence) of select or all fields among
 * specified documents. Will export a histogram of frequencies of every @c `ukv_type_t`
 * under every field. Can be used as a preparation step before `ukv_docs_gather`
 * or `ukv_docs_read`.
 */
void ukv_docs_gist( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const tasks_count,
    ukv_size_t const fields_count,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* keys,
    ukv_size_t const keys_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_stride,

    ukv_options_t const options,
    ukv_format_t const format,

    ukv_str_view_t** found_fields,
    ukv_size_t* found_frequencies,

    ukv_arena_t* arena,
    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
