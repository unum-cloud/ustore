/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 27 Jun 2022
 * @brief C bindings for Unums relations collections. Essentially, a Graph Index.
 * It extends the basic "ukv.h" towards values describing chunks of connections.
 * Unlike raw values and docs collections, this is an auxiliary index and the
 * data is transformed into @b Multi-Way @b Inverted-Index.
 *
 * Edges are represented as triplets: (source id, target id, edge id), where the
 * last argument is optional. Multiple edges between same nodes are possible,
 * forming a Multi-Graph. Every node id is mapped to an entire list of relations
 * that it forms.
 *
 * @section Linking keys across collections
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "ukv.h"

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief The primary "setter" interface for sub-document-level data.
 * It's identical to `ukv_write`, but also receives:
 *
 */
void ukv_graph_write( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_key_t const* edges_ids,
    ukv_size_t const edges_count,
    ukv_size_t const edges_stride,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_options_t const options,

    ukv_key_t const* sources_ids,
    ukv_size_t const sources_stride,

    ukv_key_t const* targets_ids,
    ukv_size_t const targets_stride,

    ukv_error_t* error);

/**
 * @brief The primary "getter" interface for sub-document-level data.
 * It's identical to `ukv_write`, but also receives:
 *
 */
void ukv_graph_read( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_key_t const* edges_ids,
    ukv_size_t const edges_count,
    ukv_size_t const edges_stride,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* sources_ids,
    ukv_size_t const sources_stride,

    ukv_key_t const* targets_ids,
    ukv_size_t const targets_stride,

    ukv_error_t* error);

/**
 * @brief
 *
 * @param db
 * @param txn
 * @param nodes_ids
 * @param nodes_count
 * @param nodes_stride
 * @param collections
 * @param collections_stride
 * @param edges_ids
 * @param edges_stride
 * @param neighbors_ids
 * @param neighbors_stride
 * @param error
 */
void ukv_graph_gather( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_key_t const* nodes_ids,
    ukv_size_t const nodes_count,
    ukv_size_t const nodes_stride,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* edges_ids,
    ukv_size_t const edges_stride,

    ukv_key_t const* neighbors_ids,
    ukv_size_t const neighbors_stride,

    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
