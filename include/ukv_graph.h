/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 27 Jun 2022
 * @brief C bindings for Unums collections of relations.
 * It essentially extends "ukv.h", to store @b Graphs.
 * Unlike raw values and docs collections, this is an index
 * and the data is transformed into @b Multi-Way @b Inverted-Index.
 *
 * Edges are represented as triplets: (first ID, second ID, edge ID), where the
 * last argument is optional. Multiple edges between same nodes are possible,
 * potentially forming a @b Directed-Multi-Graph, but only if you setedge IDs.
 * Every node ID is mapped to an entire list of relations that it forms.
 *
 * @section Supported Graph Kinds
 * 1. @b Undirected (Multi) Graph within nodes of same collection: (movies.graph)
 * 2. @b Directed (Multi) Graph within nodes of same collection: (movies.digraph)
 * 3. @b Joining (Multi) Graph linking two different collections: (movies->people.digraph)
 * In the last one, you can't choose directions at the level of edges, only collections.
 * In any one of those collections, storing metadata (a dictionary per each node/edge ID)
 * is @b optional. In theory, you may want to store metadata in a different DB, but that
 * would mean loosing ACID guarantees.
 *
 * @section Linking keys across collections
 * It's impossible to foresee every higher-level usage pattern, so certain
 * things are left for users to decide. Generally, if you would have graphs
 * with a lot of metadata, one could structure them as a set of following collections:
 *      * objs.docs
 *      * objs.graph
 * Or if it's a bipartite graph of `person_t` and `movie_t`, like in
 * recommendation systems, one could imagine:
 *      * people.docs
 *      * movies.docs
 *      * people->movies.digraph
 * Which means that in every edge the first ID will be a person and target
 * will be a movie. If you want to keep edge directed in an opposite way, add:
 *      * movies->people.digraph
 *
 * @section Hyper-Graphs
 * If working with Hyper-Graphs (multiple nodes linked by one edge), you are expected
 * to use Undirected Graphs, with nodes and hyper-edges mixed together. You would be
 * differentiating them not by parent collection, but by stored metadata at runtime.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "ukv.h"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

extern ukv_key_t ukv_default_edge_id_k;

/**
 * @brief Every node can be either a source or a target in a Directed Graph.
 * When working with undirected graphs, this argument is irrelevant and
 * should be set to `ukv_graph_node_any_k`. With directed graphs, where
 * source and target can belong to different collections its @b crucial
 * that members of each collection are fixed to be either only sources
 * or only edges.
 */
enum ukv_graph_node_role_t {
    ukv_graph_node_unknown_k = 0,
    ukv_graph_node_source_k = 1,
    ukv_graph_node_target_k = 2,
    ukv_graph_node_any_k = 3,
};

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief Finds and extracts all the related edges and
 * neighbor IDs for the provided nodes set. The results
 * are exported onto a tape in the following order:
 *      1. `ukv_size_t` number of outgoing edges per node
 *      2. `ukv_size_t` number of incoming edges per node
 *      3. outgoing edges per node:
 *          1. `ukv_key_t` all target ids
 *          2. `ukv_key_t` all edge ids
 *      4. incoming edges per node:
 *          1. `ukv_key_t` all source ids
 *          2. `ukv_key_t` all edge ids
 * A missing node will be represented with zero-length value,
 * that will not even have degrees listed. While a detached
 * node will have both degrees set to zero.
 *
 * This function mostly forward to `ukv_read`, but adjusts the
 * behaviour of certain flags.
 *
 * @param[in] roles     The roles of passed @p `nodes` in wanted edges.
 * @param[in] options   To request node degrees, add the
 *                      @c `ukv_option_read_lengths_k` flag.
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

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Inserts edges between provided nodes.
 *
 * @param[in] edge_ids  Optional edge identifiers.
 *                      Necessary for edges storing a lot of metadata.
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
 *
 * @param[in] member_ids Either source OR target node IDs.
 */
void ukv_graph_remove_edges( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* edges_ids,
    ukv_size_t const edges_count,
    ukv_size_t const edges_stride,

    ukv_key_t const* member_ids,
    ukv_size_t const member_stride,

    ukv_graph_node_role_t const* roles,
    ukv_size_t const roles_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Removes nodes from the graph and exports deleted edge IDs.
 * Those are then availiable in the tape in the following format:
 *      1. `ukv_size_t` counter for the number of edges
 *      2. `ukv_key_t`s edge IDs in no particular order
 */
void ukv_graph_remove_nodes( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* nodes_ids,
    ukv_size_t const nodes_count,
    ukv_size_t const nodes_stride,

    ukv_graph_node_role_t const* roles,
    ukv_size_t const roles_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
