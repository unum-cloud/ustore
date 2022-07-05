/**
 * @file ukv_graph.hpp
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

/**
 * @brief An asymmetric slice of a bond/relation.
 * Every vertex stores a list of such @c `neighborhood_t`s
 * in a sorted order.
 */
struct neighborhood_t {
    ukv_key_t neighbor_id = 0;
    ukv_key_t edge_id = 0;

    friend inline bool operator<(neighborhood_t a, neighborhood_t b) noexcept {
        return (a.neighbor_id < b.neighbor_id) | ((a.neighbor_id == b.neighbor_id) | (a.edge_id < b.edge_id));
    }
    friend inline bool operator==(neighborhood_t a, neighborhood_t b) noexcept {
        return (a.neighbor_id == b.neighbor_id) & (a.edge_id < b.edge_id);
    }

    friend inline bool operator<(ukv_key_t a_vertex_id, neighborhood_t b) noexcept {
        return a_vertex_id < b.neighbor_id;
    }
    friend inline bool operator<(neighborhood_t a, ukv_key_t b_vertex_id) noexcept {
        return a.neighbor_id < b_vertex_id;
    }
    friend inline bool operator==(ukv_key_t a_vertex_id, neighborhood_t b) noexcept {
        return a_vertex_id == b.neighbor_id;
    }
    friend inline bool operator==(neighborhood_t a, ukv_key_t b_vertex_id) noexcept {
        return a.neighbor_id == b_vertex_id;
    }
};

struct edges_soa_view_t {
    strided_range_gt<ukv_key_t const> source_ids;
    strided_range_gt<ukv_key_t const> target_ids;
    strided_range_gt<ukv_key_t const> edge_ids;
};

inline ukv_vertex_role_t invert(ukv_vertex_role_t role) {
    switch (role) {
    case ukv_vertex_source_k: return ukv_vertex_target_k;
    case ukv_vertex_target_k: return ukv_vertex_source_k;
    case ukv_vertex_role_any_k: return ukv_vertex_role_unknown_k;
    case ukv_vertex_role_unknown_k: return ukv_vertex_role_any_k;
    }
    __builtin_unreachable();
}

inline range_gt<neighborhood_t const*> neighbors(value_view_t bytes, ukv_vertex_role_t role = ukv_vertex_role_any_k) {
    // Handle missing vertices
    if (bytes.size() < 2 * sizeof(ukv_vertex_degree_t))
        return {};

    auto degrees = reinterpret_cast<ukv_vertex_degree_t const*>(bytes.begin());
    auto hoods = reinterpret_cast<neighborhood_t const*>(degrees + 2);

    switch (role) {
    case ukv_vertex_source_k: return {hoods, hoods + degrees[0]};
    case ukv_vertex_target_k: return {hoods + degrees[0], hoods + degrees[1]};
    case ukv_vertex_role_any_k: return {hoods, hoods + degrees[0] + degrees[1]};
    case ukv_vertex_role_unknown_k: return {};
    }
    __builtin_unreachable();
}

/**
 * @brief Parses the a single `value_view_t` chunk from the output
 * of `ukv_graph_gather_neighbors`.
 */
inline std::pair<edges_soa_view_t, edges_soa_view_t> edges_from_neighbors(ukv_key_t* key_ptr, value_view_t bytes) {
    edges_soa_view_t outgoing, incoming;
    auto targets = neighbors(bytes, ukv_vertex_source_k);
    auto sources = neighbors(bytes, ukv_vertex_target_k);

    outgoing.source_ids = {key_ptr, 0, targets.size()};
    outgoing.target_ids = {&targets.begin()->neighbor_id, sizeof(neighborhood_t), targets.size()};
    outgoing.edge_ids = {&targets.begin()->edge_id, sizeof(neighborhood_t), targets.size()};

    incoming.source_ids = {&sources.begin()->neighbor_id, sizeof(neighborhood_t), sources.size()};
    incoming.target_ids = {key_ptr, 0, sources.size()};
    incoming.edge_ids = {&sources.begin()->edge_id, sizeof(neighborhood_t), sources.size()};

    return {outgoing, incoming};
}

class graph_t {
    collection_t index_;
    collection_t sources_;
    collection_t targets_;
    collection_t relations_;
};

} // namespace unum::ukv
