/**
 * @file ukv.hpp
 * @author Ashot Vardanian
 * @date 30 Jun 2022
 * @brief C++ bindings built on top of @see "ukv_graph.h" with
 * two primary purposes:
 * > @b RAII controls for non-trivial & potentially heavy objects.
 * > syntactic @b sugar, iterators, containers and other C++  stuff.
 */

#pragma once
#include "ukv_graph.h"
#include "ukv.hpp"

namespace unum::ukv {

struct edge_t {
    ukv_key_t source_id;
    ukv_key_t target_id;
    ukv_key_t edge_id = ukv_default_edge_id_k;
};

struct neighborhood_t {
    ukv_key_t neighbor_id = 0;
    ukv_key_t edge_id = 0;
};

struct edges_t {
    strided_range_gt<ukv_key_t const> source_ids;
    strided_range_gt<ukv_key_t const> target_ids;
    strided_range_gt<ukv_key_t const> edge_ids;
};

inline ukv_graph_node_role_t invert(ukv_graph_node_role_t role) {
    switch (role) {
    case ukv_graph_node_source_k: return ukv_graph_node_target_k;
    case ukv_graph_node_target_k: return ukv_graph_node_source_k;
    case ukv_graph_node_any_k: return ukv_graph_node_unknown_k;
    case ukv_graph_node_unknown_k: return ukv_graph_node_any_k;
    }
    __builtin_unreachable();
}

inline range_gt<neighborhood_t const*> neighbors(value_view_t bytes,
                                                 ukv_graph_node_role_t role = ukv_graph_node_any_k) {
    auto degrees = reinterpret_cast<ukv_size_t const*>(bytes.begin());
    auto ids = reinterpret_cast<neighborhood_t const*>(degrees + 2);

    switch (role) {
    case ukv_graph_node_source_k: return {ids, ids + degrees[0]};
    case ukv_graph_node_target_k: return {ids + degrees[0], ids + degrees[1]};
    case ukv_graph_node_any_k: return {ids, ids + degrees[0] + degrees[1]};
    case ukv_graph_node_unknown_k: return {};
    }
    __builtin_unreachable();
}

/**
 * @brief Parses the a single `value_view_t` chunk from the output
 * of `ukv_graph_gather_neighbors`.
 */
inline std::pair<edges_t, edges_t> edges_from_neighbors(ukv_key_t* key_ptr, value_view_t bytes) {
    edges_t outgoing, incoming;
    auto targets = neighbors(bytes, ukv_graph_node_source_k);
    auto sources = neighbors(bytes, ukv_graph_node_target_k);

    outgoing.source_ids.raw = key_ptr;
    outgoing.source_ids.stride = 0;
    outgoing.source_ids.count = targets.size();
    outgoing.target_ids.raw = &targets.begin()->neighbor_id;
    outgoing.target_ids.stride = sizeof(neighborhood_t);
    outgoing.target_ids.count = targets.size();
    outgoing.edge_ids.raw = &targets.begin()->edge_id;
    outgoing.edge_ids.stride = sizeof(neighborhood_t);
    outgoing.edge_ids.count = targets.size();

    incoming.source_ids.raw = &sources.begin()->neighbor_id;
    incoming.source_ids.stride = sizeof(neighborhood_t);
    incoming.source_ids.count = sources.size();
    incoming.target_ids.raw = key_ptr;
    incoming.target_ids.stride = 0;
    incoming.target_ids.count = sources.size();
    incoming.edge_ids.raw = &sources.begin()->edge_id;
    incoming.edge_ids.stride = sizeof(neighborhood_t);
    incoming.edge_ids.count = sources.size();

    return {outgoing, incoming};
}

} // namespace unum::ukv
