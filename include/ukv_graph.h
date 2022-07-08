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
 * last argument is optional. Multiple edges between same vertices are possible,
 * potentially forming a @b Directed-Multi-Graph, but only if you setedge IDs.
 * Every vertex ID is mapped to an entire list of relations that it forms.
 *
 * @section Supported Graph Kinds
 * 1. @b Undirected (Multi) Graph over vertices of the same collection: (movies.graph)
 * 2. @b Directed (Multi) Graph over vertices of the same collection: (movies.digraph)
 * 3. @b Joining (Multi) Graph linking two different collections: (movies->people.digraph)
 * In the last one, you can't choose directions at the level of edges, only collections.
 * In any one of those collections, storing metadata (a dictionary per each vertex/edge ID)
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
 * If working with Hyper-Graphs (multiple vertices linked by one edge), you are expected
 * to use Undirected Graphs, with vertices and hyper-edges mixed together. You would be
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
 * @brief Every vertex can be either a source or a target in a Directed Graph.
 * When working with undirected graphs, this argument is irrelevant and
 * should be set to `ukv_vertex_role_any_k`. With directed graphs, where
 * source and target can belong to different collections its @b crucial
 * that members of each collection are fixed to be either only sources
 * or only edges.
 */
enum ukv_vertex_role_t {
    ukv_vertex_role_unknown_k = 0,
    ukv_vertex_source_k = 1,
    ukv_vertex_target_k = 2,
    ukv_vertex_role_any_k = 3,
};

typedef uint32_t ukv_vertex_degree_t;

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

/**
 * @brief Finds and extracts all the related edges and
 * neighbor IDs for the provided vertices set. The results
 * are exported onto a tape in the following order:
 *      1. `ukv_vertex_degree_t` number of outgoing edges per vertex
 *      2. `ukv_vertex_degree_t` number of incoming edges per vertex
 *      3. outgoing edges per vertex:
 *          1. `ukv_key_t` all target ids
 *          2. `ukv_key_t` all edge ids
 *      4. incoming edges per vertex:
 *          1. `ukv_key_t` all source ids
 *          2. `ukv_key_t` all edge ids
 * A missing vertex will be represented with zero-length value,
 * that will not even have degrees listed. While a detached
 * vertex will have both degrees set to zero.
 *
 * This function mostly forward to `ukv_read`, but adjusts the
 * behaviour of certain flags.
 *
 * @param[in] roles     The roles of passed @p `vertices` in wanted edges.
 * @param[in] options   To request vertex degrees, add the
 *                      @c `ukv_option_read_lengths_k` flag.
 */
void ukv_graph_gather_neighbors( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* vertices_ids,
    ukv_size_t const vertices_count,
    ukv_size_t const vertices_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Inserts edges between provided vertices.
 *
 * @param[in] edge_ids  Optional edge identifiers.
 *                      Necessary for edges storing a lot of metadata.
 *                      Normal graphs would use `ukv_default_edge_id_k`.
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
 * @brief Removed edges between provided vertices.
 *
 * @param[in] edge_ids  Optional edge identifiers.
 *                      Normal graphs would use `ukv_default_edge_id_k`.
 *                      If NULL is provided for a multi-graph with a deletion
 *                      request, all edges between mentioned nodes will be removed.
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

    ukv_vertex_role_t const* roles,
    ukv_size_t const roles_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

/**
 * @brief Removes vertices from the graph and exports deleted edge IDs.
 * Those are then availiable in the tape in the following format:
 *      1. `ukv_vertex_degree_t` counter for the number of edges
 *      2. `ukv_key_t`s edge IDs in no particular order
 *
 * @param roles[in] Needed only for @b Joining graphs.
 */
void ukv_graph_remove_vertices( //
    ukv_t const db,
    ukv_txn_t const txn,

    ukv_collection_t const* collections,
    ukv_size_t const collections_stride,

    ukv_key_t const* vertices_ids,
    ukv_size_t const vertices_count,
    ukv_size_t const vertices_stride,

    ukv_vertex_role_t const* roles,
    ukv_size_t const roles_stride,

    ukv_options_t const options,

    ukv_tape_ptr_t* tape,
    ukv_size_t* capacity,
    ukv_error_t* error);

#ifdef __cplusplus
} /* end extern "C" */
#endif
