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
 * Every vertex stores a list of such @c `neighborship_t`s
 * in a sorted order.
 */
struct neighborship_t {
    ukv_key_t neighbor_id = 0;
    ukv_key_t edge_id = 0;

    friend inline bool operator<(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id < b.neighbor_id) | ((a.neighbor_id == b.neighbor_id) | (a.edge_id < b.edge_id));
    }
    friend inline bool operator==(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id == b.neighbor_id) & (a.edge_id < b.edge_id);
    }

    friend inline bool operator<(ukv_key_t a_vertex_id, neighborship_t b) noexcept {
        return a_vertex_id < b.neighbor_id;
    }
    friend inline bool operator<(neighborship_t a, ukv_key_t b_vertex_id) noexcept {
        return a.neighbor_id < b_vertex_id;
    }
    friend inline bool operator==(ukv_key_t a_vertex_id, neighborship_t b) noexcept {
        return a_vertex_id == b.neighbor_id;
    }
    friend inline bool operator==(neighborship_t a, ukv_key_t b_vertex_id) noexcept {
        return a.neighbor_id == b_vertex_id;
    }
};

template <typename range_at, typename comparable_at>
inline range_at equal_subrange(range_at range, comparable_at&& comparable) {
    auto p = std::equal_range(range.begin(), range.end(), comparable);
    return range_at {p.first, p.second};
}

struct edges_soa_view_t {
    strided_range_gt<ukv_key_t const> source_ids;
    strided_range_gt<ukv_key_t const> target_ids;
    strided_range_gt<ukv_key_t const> edge_ids;

    inline edges_soa_view_t() = default;
    inline edges_soa_view_t(std::vector<edge_t> const& edges) noexcept {
        auto ptr = edges.data();
        auto strided = strided_range_gt<edge_t const>(ptr, ptr + edges.size());
        source_ids = strided.members(&edge_t::source_id);
        target_ids = strided.members(&edge_t::target_id);
        edge_ids = strided.members(&edge_t::edge_id);
    }

    inline std::size_t size() const noexcept { return edge_ids.size(); }

    inline edge_t operator[](std::size_t i) const noexcept {
        edge_t result;
        result.source_id = source_ids[i];
        result.target_id = target_ids[i];
        result.edge_id = edge_ids[i];
        return result;
    }
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

inline range_gt<neighborship_t const*> neighbors(value_view_t bytes, ukv_vertex_role_t role = ukv_vertex_role_any_k) {
    // Handle missing vertices
    if (bytes.size() < 2 * sizeof(ukv_vertex_degree_t))
        return {};

    auto degrees = reinterpret_cast<ukv_vertex_degree_t const*>(bytes.begin());
    auto ships = reinterpret_cast<neighborship_t const*>(degrees + 2);

    switch (role) {
    case ukv_vertex_source_k: return {ships, ships + degrees[0]};
    case ukv_vertex_target_k: return {ships + degrees[0], ships + degrees[0] + degrees[1]};
    case ukv_vertex_role_any_k: return {ships, ships + degrees[0] + degrees[1]};
    case ukv_vertex_role_unknown_k: return {};
    }
    __builtin_unreachable();
}

struct neighborhood_t {
    ukv_key_t center = 0;
    range_gt<neighborship_t const*> targets;
    range_gt<neighborship_t const*> sources;

    neighborhood_t() = default;
    neighborhood_t(neighborhood_t const&) = default;
    neighborhood_t(neighborhood_t&&) = default;

    /**
     * @brief Parses the a single `value_view_t` chunk
     * from the output of `ukv_graph_gather_neighbors`.
     */
    inline neighborhood_t(ukv_key_t center_vertex, value_view_t bytes) noexcept {
        targets = neighbors(bytes, ukv_vertex_source_k);
        sources = neighbors(bytes, ukv_vertex_target_k);
    }

    inline std::size_t size() const noexcept { return targets.size() + sources.size(); }

    inline edge_t operator[](std::size_t i) const noexcept {
        edge_t result;
        if (i > targets.size()) {
            i -= targets.size();
            result.source_id = center;
            result.target_id = targets[i].neighbor_id;
            result.edge_id = targets[i].edge_id;
        }
        else {
            result.source_id = sources[i].neighbor_id;
            result.target_id = center;
            result.edge_id = sources[i].edge_id;
        }
        return result;
    }

    inline edges_soa_view_t outgoing_edges() const& {
        edges_soa_view_t edges;
        edges.source_ids = {&center, 0, targets.size()};
        edges.target_ids = targets.strided().members(&neighborship_t::neighbor_id);
        edges.edge_ids = targets.strided().members(&neighborship_t::edge_id);
        return edges;
    }

    inline edges_soa_view_t incoming_edges() const& {
        edges_soa_view_t edges;
        edges.source_ids = sources.strided().members(&neighborship_t::neighbor_id);
        edges.target_ids = {&center, 0, sources.size()};
        edges.edge_ids = sources.strided().members(&neighborship_t::edge_id);
        return edges;
    }

    inline range_gt<neighborship_t const*> outgoing_to(ukv_key_t target) const noexcept {
        return equal_subrange(targets, target);
    }

    inline range_gt<neighborship_t const*> incoming_from(ukv_key_t source) const noexcept {
        return equal_subrange(sources, source);
    }

    inline neighborship_t const* outgoing_to(ukv_key_t target, ukv_key_t edge_id) const noexcept {
        auto r = equal_subrange(targets, neighborship_t {target, edge_id});
        return r.size() ? r.begin() : nullptr;
    }

    inline neighborship_t const* incoming_from(ukv_key_t source, ukv_key_t edge_id) const noexcept {
        auto r = equal_subrange(sources, neighborship_t {source, edge_id});
        return r.size() ? r.begin() : nullptr;
    }
};

/**
 * @brief Wraps relational/linking operations with cleaner type system.
 * Controls mainly just the inverted index collection and keeps a local
 * memory buffer (tape) for read operations, so isn't thread-safe.
 * You can have one such object in every working thread, even for the
 * same graph collection. Supports updates/reads from within a transaction.
 */
class graph_collection_session_t {
    collection_t index_;
    ukv_txn_t txn_ = nullptr;
    managed_tape_t read_tape_;

  public:
    graph_collection_session_t(collection_t&& col) : index_(std::move(col)), read_tape_(col.db()) {}
    graph_collection_session_t(collection_t&& col, txn_t& txn)
        : index_(std::move(col)), txn_(txn), read_tape_(col.db()) {}

    inline managed_tape_t& tape() noexcept { return read_tape_; }

    error_t upsert(edges_soa_view_t const& edges) noexcept {
        error_t error;
        ukv_graph_upsert_edges(index_.db(),
                               txn_,
                               index_.internal_cptr(),
                               0,
                               edges.edge_ids.begin().get(),
                               edges.edge_ids.size(),
                               edges.edge_ids.stride(),
                               edges.source_ids.begin().get(),
                               edges.source_ids.stride(),
                               edges.target_ids.begin().get(),
                               edges.target_ids.stride(),
                               ukv_options_default_k,
                               read_tape_.internal_memory(),
                               read_tape_.internal_capacity(),
                               error.internal_cptr());
        return error;
    }

    error_t remove(edges_soa_view_t const& edges) noexcept {
        error_t error;
        ukv_graph_remove_edges(index_.db(),
                               txn_,
                               index_.internal_cptr(),
                               0,
                               edges.edge_ids.begin().get(),
                               edges.edge_ids.size(),
                               edges.edge_ids.stride(),
                               edges.source_ids.begin().get(),
                               edges.source_ids.stride(),
                               edges.target_ids.begin().get(),
                               edges.target_ids.stride(),
                               ukv_options_default_k,
                               read_tape_.internal_memory(),
                               read_tape_.internal_capacity(),
                               error.internal_cptr());
        return error;
    }

    expected_gt<neighborhood_t> neighbors(ukv_key_t vertex) noexcept {
        error_t error;
        ukv_graph_gather_neighbors(index_.db(),
                                   txn_,
                                   index_.internal_cptr(),
                                   0,
                                   &vertex,
                                   1,
                                   0,
                                   ukv_options_default_k,
                                   read_tape_.internal_memory(),
                                   read_tape_.internal_capacity(),
                                   error.internal_cptr());
        if (error)
            return error;
        return neighborhood_t(vertex, *read_tape_.untape(1).begin());
    }
};

} // namespace unum::ukv
