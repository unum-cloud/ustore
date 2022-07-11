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
    friend inline bool operator!=(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id != b.neighbor_id) | (a.edge_id != b.edge_id);
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

struct edges_soa_view_t {
    strided_range_gt<ukv_key_t const> source_ids;
    strided_range_gt<ukv_key_t const> target_ids;
    strided_range_gt<ukv_key_t const> edge_ids;

    static_assert(sizeof(edge_t) == 3 * sizeof(ukv_key_t));

    inline edges_soa_view_t() = default;
    inline edges_soa_view_t(strided_range_gt<ukv_key_t const> sources,
                            strided_range_gt<ukv_key_t const> targets,
                            strided_range_gt<ukv_key_t const> edges) noexcept
        : source_ids(sources), target_ids(targets), edge_ids(edges) {}

    inline edges_soa_view_t(edge_t const* ptr, edge_t const* end) noexcept {
        auto strided = strided_range_gt<edge_t const>(ptr, end);
        source_ids = strided.members(&edge_t::source_id);
        target_ids = strided.members(&edge_t::target_id);
        edge_ids = strided.members(&edge_t::edge_id);
    }

    inline edges_soa_view_t(std::vector<edge_t> const& edges) noexcept
        : edges_soa_view_t(edges.data(), edges.data() + edges.size()) {}

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

/**
 * @brief Wraps relational/linking operations with cleaner type system.
 * Controls mainly just the inverted index collection and keeps a local
 * memory buffer (tape) for read operations, so isn't thread-safe.
 * You can have one such object in every working thread, even for the
 * same graph collection. Supports updates/reads from within a transaction.
 */
class graph_collection_session_t {
    collection_t collection_;
    ukv_txn_t txn_ = nullptr;
    managed_arena_t arena_;

  public:
    graph_collection_session_t(collection_t&& col) : collection_(std::move(col)), arena_(col.db()) {}
    graph_collection_session_t(collection_t&& col, txn_t& txn)
        : collection_(std::move(col)), txn_(txn), arena_(col.db()) {}

    inline managed_arena_t& arena() noexcept { return arena_; }
    inline collection_t& collection() noexcept { return collection_; };

    error_t upsert(edges_soa_view_t const& edges) noexcept {
        error_t error;
        ukv_graph_upsert_edges(collection_.db(),
                               txn_,
                               collection_.internal_cptr(),
                               0,
                               edges.edge_ids.begin().get(),
                               edges.edge_ids.size(),
                               edges.edge_ids.stride(),
                               edges.source_ids.begin().get(),
                               edges.source_ids.stride(),
                               edges.target_ids.begin().get(),
                               edges.target_ids.stride(),
                               ukv_options_default_k,
                               arena_.internal_cptr(),
                               error.internal_cptr());
        return error;
    }

    error_t remove(edges_soa_view_t const& edges) noexcept {
        error_t error;
        ukv_graph_remove_edges(collection_.db(),
                               txn_,
                               collection_.internal_cptr(),
                               0,
                               edges.edge_ids.begin().get(),
                               edges.edge_ids.size(),
                               edges.edge_ids.stride(),
                               edges.source_ids.begin().get(),
                               edges.source_ids.stride(),
                               edges.target_ids.begin().get(),
                               edges.target_ids.stride(),
                               ukv_options_default_k,
                               arena_.internal_cptr(),
                               error.internal_cptr());
        return error;
    }

    expected_gt<edges_soa_view_t> edges(ukv_key_t vertex, ukv_vertex_role_t role = ukv_vertex_role_any_k) noexcept {
        error_t error;
        ukv_vertex_degree_t* degrees_per_vertex = NULL;
        ukv_key_t* neighborships_per_vertex = NULL;

        ukv_graph_find_edges(collection_.db(),
                             txn_,
                             collection_.internal_cptr(),
                             0,
                             &vertex,
                             1,
                             0,
                             &role,
                             0,
                             ukv_options_default_k,
                             &degrees_per_vertex,
                             &neighborships_per_vertex,
                             arena_.internal_cptr(),
                             error.internal_cptr());
        if (error)
            return error;

        ukv_vertex_degree_t deg = degrees_per_vertex[0];
        if (deg == ukv_vertex_degree_missing_k)
            return edges_soa_view_t {};

        using strided_keys_t = strided_range_gt<ukv_key_t const>;
        ukv_size_t stride = sizeof(ukv_key_t) * 3;
        strided_keys_t sources(neighborships_per_vertex, stride, deg);
        strided_keys_t targets(neighborships_per_vertex + 1, stride, deg);
        strided_keys_t edges(neighborships_per_vertex + 2, stride, deg);
        return edges_soa_view_t {sources, targets, edges};
    }

    expected_gt<edges_soa_view_t> edges(ukv_key_t source, ukv_key_t target) noexcept {
        auto maybe_all = edges(source, ukv_vertex_source_k);
        if (!maybe_all)
            return maybe_all;

        edges_soa_view_t all = *maybe_all;
        auto begin_and_end = std::equal_range(all.target_ids.begin(), all.target_ids.end(), target);
        auto begin_offset = begin_and_end.first - all.target_ids.begin();
        auto count = begin_and_end.second - begin_and_end.first;
        all.source_ids = all.source_ids.subspan(begin_offset, count);
        all.target_ids = all.target_ids.subspan(begin_offset, count);
        all.edge_ids = all.edge_ids.subspan(begin_offset, count);
        return all;
    }
};

} // namespace unum::ukv
