/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 27 Jun 2022
 * @brief C bindings for Unums collections of relations.
 * It essentially extends "ukv.h", to store @b Directed-Graphs.
 * Unlike raw values and docs collections, this is an index
 * and the data is transformed into @b Multi-Way @b Inverted-Index.
 *
 * Edges are represented as triplets: (source ID, target ID, edge ID), where the
 * last argument is optional. Multiple edges between same nodes are possible,
 * forming a @b Directed-Multi-Graph, but only if you provide the edge IDs.
 * Every node ID is mapped to an entire list of relations that it forms.
 *
 * @section Linking keys across collections
 * It's impossible to foresee every higher-level usage pattern, so certain
 * things are left for users to decide. Generally, if you would have graphs
 * with a lot of metadata, one could structure them as a set of following collections:
 *      * objs.docs
 *      * objs.graph
 * Or if it's a bipartite graph of `person_t` and `product_t`, like in
 * recommendation systems, one could imagine:
 *      * people.docs
 *      * products.docs
 *      * people->products.graph
 * Which means that in every edge the first ID will be a person and target
 * will be a product.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "ukv.h"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

static ukv_key_t ukv_default_edge_id_k;

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief Finds and extracts all the related edges and
 * neighbor IDs for the provided nodes set. The results
 * are exported onto a tape in the following order:
 *      1. number of outgoing edges per node
 *      2. number of incoming edges per node
 *      3. outgoing edges per node:
 *          1. all target ids
 *          2. all edge ids
 *      4. incoming edges per node:
 *          1. all source ids
 *          2. all edge ids
 *
 * To request node degrees, add the @c `ukv_option_read_lengths_k` flag.
 */
void ukv_graph_gather_neighbors( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* nodes_ids,
    ukv_size_t const nodes_count,
    ukv_size_t const nodes_stride,

    ukv_options_t const options,

    ukv_key_t* edges_ids,
    ukv_size_t const edges_stride,

    ukv_key_t* neighbors_ids,
    ukv_size_t const neighbors_stride,
    ukv_collection_t const* neighbors_collections,
    ukv_size_t const neighbors_collections_stride,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Inserts edges between provided nodes.
 * The edge IDs are optional, but the source AND target IDs are not.
 */
void ukv_graph_upsert_edges( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* edges_ids,
    ukv_size_t const edges_count,
    ukv_size_t const edges_stride,

    ukv_key_t const* sources_ids,
    ukv_size_t const sources_stride,

    ukv_key_t const* targets_ids,
    ukv_size_t const targets_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Removes edges from the graph.
 * The edge IDs are optional, but the source OR target IDs are not.
 */
void ukv_graph_remove_edges( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* edges_ids,
    ukv_size_t const edges_count,
    ukv_size_t const edges_stride,

    ukv_key_t const* sources_ids,
    ukv_size_t const sources_stride,

    ukv_key_t const* targets_ids,
    ukv_size_t const targets_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Removes nodes from the graph.
 */
void ukv_graph_remove_nodes( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* nodes_ids,
    ukv_size_t const nodes_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
