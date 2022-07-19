/**
 * @file graph.hpp
 * @author Ashot Vardanian
 * @date 30 Jun 2022
 * @brief C++ bindings for @see "ukv/graph.h".
 */

#pragma once
#include <fmt/core.h>

#include "ukv/graph.h"
#include "ukv/ukv.hpp"

namespace unum::ukv {

class adjacency_stream_t;
class graph_t;

struct edge_t {
    ukv_key_t source_id;
    ukv_key_t target_id;
    ukv_key_t id = ukv_default_edge_id_k;

    inline bool operator==(edge_t const& other) const noexcept {
        return (source_id == other.source_id) & (target_id == other.target_id) & (id == other.id);
    }
    inline bool operator!=(edge_t const& other) const noexcept {
        return (source_id != other.source_id) | (target_id != other.target_id) | (id != other.id);
    }
};

struct edge_hash_t {
    inline std::size_t operator()(edge_t const& edge) const noexcept {
        std::size_t result = SIZE_MAX;
        hash_combine(result, edge.source_id);
        hash_combine(result, edge.target_id);
        hash_combine(result, edge.id);
        return result;
    }
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
        return (a.neighbor_id < b.neighbor_id) | ((a.neighbor_id == b.neighbor_id) & (a.edge_id < b.edge_id));
    }
    friend inline bool operator==(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id == b.neighbor_id) & (a.edge_id == b.edge_id);
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

template <typename id_at>
struct edges_range_gt {

    using id_t = id_at;
    using tuple_t = std::conditional_t<std::is_const_v<id_t>, edge_t const, edge_t>;
    static_assert(sizeof(tuple_t) == 3 * sizeof(id_t));

    strided_range_gt<id_t> source_ids;
    strided_range_gt<id_t> target_ids;
    strided_range_gt<id_t> edge_ids;

    inline edges_range_gt() = default;
    inline edges_range_gt(strided_range_gt<id_t> sources,
                          strided_range_gt<id_t> targets,
                          strided_range_gt<id_t> edges = {ukv_default_edge_id_k}) noexcept
        : source_ids(sources), target_ids(targets), edge_ids(edges) {}

    inline edges_range_gt(tuple_t* ptr, tuple_t* end) noexcept {
        auto strided = strided_range_gt<tuple_t>(ptr, end);
        source_ids = strided.members(&edge_t::source_id);
        target_ids = strided.members(&edge_t::target_id);
        edge_ids = strided.members(&edge_t::id);
    }

    inline edges_range_gt(std::vector<edge_t> const& edges) noexcept
        : edges_range_gt(edges.data(), edges.data() + edges.size()) {}

    inline std::size_t size() const noexcept { return edge_ids.count(); }

    inline edge_t operator[](std::size_t i) const noexcept {
        edge_t result;
        result.source_id = source_ids[i];
        result.target_id = target_ids[i];
        result.id = edge_ids[i];
        return result;
    }
};

using edges_span_t = edges_range_gt<ukv_key_t>;
using edges_view_t = edges_range_gt<ukv_key_t const>;

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
 * @brief A stream of all @c `edge_t`s in a graph.
 * No particular order is guaranteed.
 */
class adjacency_stream_t {

    ukv_t db_ = nullptr;
    ukv_collection_t col_ = ukv_default_collection_k;
    ukv_txn_t txn_ = nullptr;

    edges_span_t fetched_edges_ = {};
    std::size_t fetched_offset_ = 0;

    managed_arena_t arena_;
    keys_stream_t vertex_stream_;

    status_t prefetch_gather() noexcept {

        auto vertices = vertex_stream_.keys_batch().strided();

        status_t status;
        ukv_vertex_degree_t* degrees_per_vertex = nullptr;
        ukv_key_t* neighborships_per_vertex = nullptr;
        ukv_vertex_role_t role = ukv_vertex_role_any_k;
        ukv_graph_find_edges(db_,
                             txn_,
                             vertices.count(),
                             &col_,
                             0,
                             vertices.begin().get(),
                             vertices.stride(),
                             &role,
                             0,
                             ukv_options_default_k,
                             &degrees_per_vertex,
                             &neighborships_per_vertex,
                             arena_.internal_cptr(),
                             status.internal_cptr());
        if (!status)
            return status;

        auto edges_begin = reinterpret_cast<edge_t*>(neighborships_per_vertex);
        auto edges_count = transform_reduce_n(degrees_per_vertex, vertices.size(), 0ul, [](ukv_vertex_degree_t deg) {
            return deg == ukv_vertex_degree_missing_k ? 0 : deg;
        });
        fetched_offset_ = 0;
        fetched_edges_ = {edges_begin, edges_begin + edges_count};
        return {};
    }

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = ukv_key_t;
    using pointer = ukv_key_t*;
    using reference = ukv_key_t&;

    static constexpr std::size_t default_read_ahead_k = 256;

    adjacency_stream_t(ukv_t db,
                       ukv_collection_t col = ukv_default_collection_k,
                       std::size_t read_ahead_vertices = keys_stream_t::default_read_ahead_k,
                       ukv_txn_t txn = nullptr)
        : db_(db), col_(col), txn_(txn), arena_(db), vertex_stream_(db, col, read_ahead_vertices, txn) {}

    adjacency_stream_t(adjacency_stream_t&&) = default;
    adjacency_stream_t& operator=(adjacency_stream_t&&) = default;

    adjacency_stream_t(adjacency_stream_t const&) = delete;
    adjacency_stream_t& operator=(adjacency_stream_t const&) = delete;

    status_t seek(ukv_key_t vertex_id) noexcept {
        auto status = vertex_stream_.seek(vertex_id);
        if (!status)
            return status;
        return prefetch_gather();
    }

    status_t advance() noexcept {

        if (fetched_offset_ >= fetched_edges_.size()) {
            auto status = vertex_stream_.seek_to_next_batch();
            if (!status)
                return status;
            return prefetch_gather();
        }

        ++fetched_offset_;
        return {};
    }

    /**
     * ! Unlike the `advance()`, canonically returns a self-reference,
     * ! meaning that the error must be propagated in a different way.
     * ! So we promote this iterator to `end()`, once an error occurs.
     */
    inline adjacency_stream_t& operator++() noexcept {
        status_t status = advance();
        if (status)
            return *this;

        fetched_edges_ = {};
        fetched_offset_ = 0;
        return *this;
    }

    edge_t edge() const noexcept { return fetched_edges_[fetched_offset_]; }
    edge_t operator*() const noexcept { return edge(); }
    status_t seek_to_first() noexcept { return seek(std::numeric_limits<ukv_key_t>::min()); }
    status_t seek_to_next_batch() noexcept {
        auto status = vertex_stream_.seek_to_next_batch();
        if (!status)
            return status;
        return prefetch_gather();
    }

    /**
     * @brief Exposes all the fetched edges at once, including the passed ones.
     * Should be used with `seek_to_next_batch`. Next `advance` will do the same.
     */
    edges_span_t edges_batch() noexcept {
        fetched_offset_ = fetched_edges_.size();
        return fetched_edges_;
    }

    bool is_end() const noexcept { return vertex_stream_.is_end() && fetched_offset_ >= fetched_edges_.size(); }

    bool operator==(adjacency_stream_t const& other) const noexcept {
        return vertex_stream_ == other.vertex_stream_ && fetched_offset_ == other.fetched_offset_;
    }

    bool operator!=(adjacency_stream_t const& other) const noexcept {
        return vertex_stream_ != other.vertex_stream_ || fetched_offset_ != other.fetched_offset_;
    }
};

/**
 * @brief Wraps relational/linking operations with cleaner type system.
 * Controls mainly just the inverted index collection and keeps a local
 * memory buffer (tape) for read operations, so isn't thread-safe.
 * You can have one such object in every working thread, even for the
 * same graph collection. Supports updates/reads from within a transaction.
 */
class graph_t {
    collection_t& collection_;
    ukv_txn_t txn_ = nullptr;
    managed_arena_t arena_;

  public:
    graph_t(collection_t& col, ukv_txn_t txn = nullptr) : collection_(col), txn_(txn), arena_(col.db()) {}

    graph_t(graph_t&& other) noexcept
        : collection_(other.collection_), txn_(std::exchange(other.txn_, nullptr)), arena_(std::move(other.arena_)) {}

    inline managed_arena_t& arena() noexcept { return arena_; }
    inline collection_t& collection() noexcept { return collection_; };
    inline ukv_txn_t txn() const noexcept { return txn_; }

    status_t upsert(edges_view_t const& edges) noexcept {
        status_t status;
        ukv_graph_upsert_edges(collection_.db(),
                               txn_,
                               edges.edge_ids.count(),
                               collection_.internal_cptr(),
                               0,
                               edges.edge_ids.begin().get(),
                               edges.edge_ids.stride(),
                               edges.source_ids.begin().get(),
                               edges.source_ids.stride(),
                               edges.target_ids.begin().get(),
                               edges.target_ids.stride(),
                               ukv_options_default_k,
                               arena_.internal_cptr(),
                               status.internal_cptr());
        return status;
    }

    status_t remove(edges_view_t const& edges) noexcept {
        status_t status;
        ukv_graph_remove_edges(collection_.db(),
                               txn_,
                               edges.edge_ids.count(),
                               collection_.internal_cptr(),
                               0,
                               edges.edge_ids.begin().get(),
                               edges.edge_ids.stride(),
                               edges.source_ids.begin().get(),
                               edges.source_ids.stride(),
                               edges.target_ids.begin().get(),
                               edges.target_ids.stride(),
                               ukv_options_default_k,
                               arena_.internal_cptr(),
                               status.internal_cptr());
        return status;
    }

    status_t remove(strided_range_gt<ukv_key_t const> vertices,
                    strided_range_gt<ukv_vertex_role_t const> roles = {ukv_vertex_role_any_k, 1},
                    bool transparent = false) noexcept {

        status_t status;
        ukv_options_t options = transparent ? ukv_option_read_transparent_k : ukv_options_default_k;

        ukv_graph_remove_vertices(collection_.db(),
                                  txn_,
                                  vertices.count(),
                                  collection_.internal_cptr(),
                                  0,
                                  vertices.begin().get(),
                                  vertices.stride(),
                                  roles.begin().get(),
                                  roles.stride(),
                                  options,
                                  arena_.internal_cptr(),
                                  status.internal_cptr());
        return status;
    }

    expected_gt<ukv_vertex_degree_t> degree(ukv_key_t vertex,
                                            ukv_vertex_role_t role = ukv_vertex_role_any_k,
                                            bool transparent = false) noexcept {

        auto maybe_degrees = degrees({vertex}, {role}, transparent);
        if (!maybe_degrees)
            return maybe_degrees.release_status();
        auto degrees = *maybe_degrees;
        return ukv_vertex_degree_t(degrees[0]);
    }

    expected_gt<indexed_range_gt<ukv_vertex_degree_t*>> degrees(
        strided_range_gt<ukv_key_t const> vertices,
        strided_range_gt<ukv_vertex_role_t const> roles = {ukv_vertex_role_any_k, 1},
        bool transparent = false) noexcept {

        status_t status;
        ukv_vertex_degree_t* degrees_per_vertex = nullptr;
        ukv_key_t* neighborships_per_vertex = nullptr;
        ukv_options_t options = static_cast<ukv_options_t>(
            (transparent ? ukv_option_read_transparent_k : ukv_options_default_k) | ukv_option_read_lengths_k);

        ukv_graph_find_edges(collection_.db(),
                             txn_,
                             vertices.count(),
                             collection_.internal_cptr(),
                             0,
                             vertices.begin().get(),
                             vertices.stride(),
                             roles.begin().get(),
                             roles.stride(),
                             options,
                             &degrees_per_vertex,
                             &neighborships_per_vertex,
                             arena_.internal_cptr(),
                             status.internal_cptr());
        if (!status)
            return status;

        return indexed_range_gt<ukv_vertex_degree_t*> {degrees_per_vertex, degrees_per_vertex + vertices.size()};
    }

    expected_gt<bool> contains(ukv_key_t vertex, bool transparent = false) noexcept {

        auto maybe_exists = contains(strided_range_gt<ukv_key_t const> {vertex}, transparent);
        if (!maybe_exists)
            return maybe_exists.release_status();
        auto exists = *maybe_exists;
        bool one_exists = exists[0];
        return one_exists;
    }

    /**
     * @brief Checks if certain vertices are present in the graph.
     * They maybe disconnected from everything else.
     */
    expected_gt<strided_range_gt<bool>> contains(strided_range_gt<ukv_key_t const> vertices,
                                                 bool transparent = false) noexcept {
        return entries_ref_t {
            collection_.db(),
            txn_,
            arena_.internal_cptr(),
            strided_range_gt<ukv_collection_t const> {collection_.internal_cptr(), 0, vertices.size()},
            vertices,
        }
            .contains(ukv_format_binary_k, transparent);
    }

    using adjacency_range_t = range_gt<adjacency_stream_t>;

    inline expected_gt<adjacency_range_t> edges(
        std::size_t vertices_read_ahead = keys_stream_t::default_read_ahead_k) const noexcept {

        adjacency_stream_t b {collection_.db(), collection_, vertices_read_ahead, txn_};
        adjacency_stream_t e {collection_.db(), collection_, vertices_read_ahead, txn_};
        status_t status = b.seek_to_first();
        if (!status)
            return status;
        status = e.seek(ukv_key_unknown_k);
        if (!status)
            return status;

        adjacency_range_t result {std::move(b), std::move(e)};
        return result;
    }

    expected_gt<edges_span_t> edges(ukv_key_t vertex,
                                    ukv_vertex_role_t role = ukv_vertex_role_any_k,
                                    bool transparent = false) noexcept {
        status_t status;
        ukv_vertex_degree_t* degrees_per_vertex = nullptr;
        ukv_key_t* neighborships_per_vertex = nullptr;

        ukv_graph_find_edges(collection_.db(),
                             txn_,
                             1,
                             collection_.internal_cptr(),
                             0,
                             &vertex,
                             0,
                             &role,
                             0,
                             transparent ? ukv_option_read_transparent_k : ukv_options_default_k,
                             &degrees_per_vertex,
                             &neighborships_per_vertex,
                             arena_.internal_cptr(),
                             status.internal_cptr());
        if (!status)
            return status;

        ukv_vertex_degree_t edges_count = degrees_per_vertex[0];
        if (edges_count == ukv_vertex_degree_missing_k)
            return edges_span_t {};

        auto edges_begin = reinterpret_cast<edge_t*>(neighborships_per_vertex);
        return edges_span_t {edges_begin, edges_begin + edges_count};
    }

    expected_gt<edges_span_t> edges(ukv_key_t source, ukv_key_t target, bool transparent = false) noexcept {
        auto maybe_all = edges(source, ukv_vertex_source_k, transparent);
        if (!maybe_all)
            return maybe_all;

        edges_span_t all = *maybe_all;
        auto begin_and_end = std::equal_range(all.target_ids.begin(), all.target_ids.end(), target);
        auto begin_offset = begin_and_end.first - all.target_ids.begin();
        auto count = begin_and_end.second - begin_and_end.first;
        all.source_ids = all.source_ids.subspan(begin_offset, count);
        all.target_ids = all.target_ids.subspan(begin_offset, count);
        all.edge_ids = all.edge_ids.subspan(begin_offset, count);
        return all;
    }

    /**
     * @brief Finds all the edges, that have any of the supplied nodes in allowed roles.
     * In undirected graphs, some edges may come with inverse duplicates.
     */
    expected_gt<edges_span_t> edges_containing(
        strided_range_gt<ukv_key_t const> vertices,
        strided_range_gt<ukv_vertex_role_t const> roles = {ukv_vertex_role_any_k},
        bool transparent = false) noexcept {

        status_t status;
        ukv_vertex_degree_t* degrees_per_vertex = nullptr;
        ukv_key_t* neighborships_per_vertex = nullptr;

        ukv_graph_find_edges(collection_.db(),
                             txn_,
                             vertices.count(),
                             collection_.internal_cptr(),
                             0,
                             vertices.begin().get(),
                             vertices.stride(),
                             roles.begin().get(),
                             roles.stride(),
                             transparent ? ukv_option_read_transparent_k : ukv_options_default_k,
                             &degrees_per_vertex,
                             &neighborships_per_vertex,
                             arena_.internal_cptr(),
                             status.internal_cptr());
        if (!status)
            return status;

        auto edges_begin = reinterpret_cast<edge_t*>(neighborships_per_vertex);
        auto edges_count = transform_reduce_n(degrees_per_vertex, vertices.size(), 0ul, [](ukv_vertex_degree_t deg) {
            return deg == ukv_vertex_degree_missing_k ? 0 : deg;
        });

        return edges_span_t {edges_begin, edges_begin + edges_count};
    }

    status_t export_adjacency_list(std::string const& path,
                                   std::string_view column_separator,
                                   std::string_view line_delimiter);

    status_t import_adjacency_list(std::string const& path,
                                   std::string_view column_separator,
                                   std::string_view line_delimiter);
};

} // namespace unum::ukv
