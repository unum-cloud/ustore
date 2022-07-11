/**
 * @file ukv_graph_pfor.cpp
 * @author Ashot Vardanian
 *
 * @brief Graph implementation using fast integer compression.
 * Sits on top of any @see "ukv.h"-compatiable system.
 */

#include <vector>
#include <algorithm> // `std::sort`
#include <optional>  // `std::optional`

#include "ukv_graph.hpp"
#include "helpers.hpp"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

ukv_key_t ukv_default_edge_id_k = std::numeric_limits<ukv_key_t>::max();

constexpr std::size_t bytes_in_degrees_header_k = 2 * sizeof(ukv_vertex_degree_t);

void sort_and_deduplicate(std::vector<located_key_t>& keys) {
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
}

std::size_t offset_in_sorted(std::vector<located_key_t> const& keys, located_key_t wanted) {
    return std::lower_bound(keys.begin(), keys.end(), wanted) - keys.begin();
}

/**
 * @return true  If such an entry didn't exist and was added.
 * @return false In every other case.
 */
bool upsert(value_t& value, ukv_vertex_role_t role, ukv_key_t neighbor_id, ukv_key_t edge_id) {

    auto ship = neighborship_t {neighbor_id, edge_id};
    if (!value.size()) {
        value = value_t(sizeof(ukv_vertex_degree_t) * 2 + sizeof(neighborship_t));
        auto degrees = reinterpret_cast<ukv_vertex_degree_t*>(value.begin());
        auto ships = reinterpret_cast<neighborship_t*>(degrees + 2);
        degrees[role != ukv_vertex_target_k] = 0;
        degrees[role == ukv_vertex_target_k] = 1;
        ships[0] = ship;
        return true;
    }

    auto neighbors_range = neighbors(value, role);
    auto it = std::lower_bound(neighbors_range.begin(), neighbors_range.end(), ship);
    if (it != neighbors_range.end())
        if (*it == ship)
            return false;

    auto off = reinterpret_cast<byte_t const*>(it) - value.begin();
    auto ship_bytes = reinterpret_cast<byte_t const*>(&ship);
    value.insert(off, ship_bytes, ship_bytes + sizeof(neighborship_t));
    auto degrees = reinterpret_cast<ukv_vertex_degree_t*>(value.begin());
    degrees[role == ukv_vertex_target_k] += 1;
    return true;
}

/**
 * @return true  If such a atching entry was found and deleted.
 * @return false In every other case.
 */
bool erase(value_t& value, ukv_vertex_role_t role, ukv_key_t neighbor_id, std::optional<ukv_key_t> edge_id = {}) {

    if (!value.size())
        return false;

    std::size_t off = 0;
    std::size_t len = 0;

    auto neighbors_range = neighbors(value, role);
    if (edge_id) {
        auto ship = neighborship_t {neighbor_id, *edge_id};
        auto it = std::lower_bound(neighbors_range.begin(), neighbors_range.end(), ship);
        if (it != neighbors_range.end())
            if (*it != ship)
                return false;

        off = reinterpret_cast<byte_t const*>(it) - value.begin();
        len = sizeof(neighborship_t);
    }
    else {
        auto pair = std::equal_range(neighbors_range.begin(), neighbors_range.end(), neighbor_id);
        if (pair.first == pair.second)
            return false;

        off = reinterpret_cast<byte_t const*>(pair.first) - value.begin();
        len = sizeof(neighborship_t) * (pair.second - pair.first);
    }

    value.erase(off, len);
    auto degrees = reinterpret_cast<ukv_vertex_degree_t*>(value.begin());
    degrees[role == ukv_vertex_target_k] -= 1;
    return true;
}

void ukv_graph_gather_neighbors( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_vertices_ids,
    ukv_size_t const c_vertices_count,
    ukv_size_t const c_vertices_stride,

    ukv_options_t const c_options,

    ukv_vertex_degree_t** c_degrees_per_vertex,
    ukv_key_t** c_neighborships_per_vertex,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    ukv_val_len_t* c_found_lengths = nullptr;
    ukv_val_ptr_t c_found_values = nullptr;

    ukv_read(c_db,
             c_txn,
             c_collections,
             c_collections_stride,
             c_vertices_ids,
             c_vertices_count,
             c_vertices_stride,
             c_options,
             &c_found_lengths,
             &c_found_values,
             c_arena,
             c_error);
    if (*c_error)
        return;

    stl_arena_t& arena = **reinterpret_cast<stl_arena_t**>(c_arena);
    taped_values_view_t values {c_found_lengths, c_found_values, c_vertices_count};

    // Estimate the amount of memory we will need for the arena
    std::size_t total_neighborships = 0;
    for (value_view_t value : values)
        total_neighborships += neighbors(value).size();
    arena.unpacked_tape.resize(total_neighborships * sizeof(neighborship_t) +
                               c_vertices_count * bytes_in_degrees_header_k);

    // Export into arena
    std::size_t offset_bytes_degrees = 0;
    std::size_t offset_bytes_neighborships = c_vertices_count * bytes_in_degrees_header_k;
    for (value_view_t value : values) {

        // Some values may be missing
        if (value.empty()) {
            ukv_vertex_degree_t degs[2] = {0, 0};
            std::copy(reinterpret_cast<byte_t const*>(&degs[0]),
                      reinterpret_cast<byte_t const*>(&degs[0]) + bytes_in_degrees_header_k,
                      arena.unpacked_tape.begin() + offset_bytes_degrees);
            offset_bytes_degrees += bytes_in_degrees_header_k;
            continue;
        }

        std::copy(value.begin(),
                  value.begin() + bytes_in_degrees_header_k,
                  arena.unpacked_tape.begin() + offset_bytes_degrees);
        std::copy(value.begin() + bytes_in_degrees_header_k,
                  value.end(),
                  arena.unpacked_tape.begin() + offset_bytes_neighborships);

        offset_bytes_degrees += bytes_in_degrees_header_k;
        offset_bytes_neighborships += value.size() - bytes_in_degrees_header_k;
    }

    *c_degrees_per_vertex = reinterpret_cast<ukv_vertex_degree_t*>(arena.unpacked_tape.data());
    *c_neighborships_per_vertex = reinterpret_cast<ukv_key_t*>(arena.unpacked_tape.data() + offset_bytes_degrees);
}

void gather_disjoint( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_vertices_ids,
    ukv_size_t const c_vertices_count,
    ukv_size_t const c_vertices_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    ukv_val_len_t* c_found_lengths = nullptr;
    ukv_val_ptr_t c_found_values = nullptr;

    ukv_read(c_db,
             c_txn,
             c_collections,
             c_collections_stride,
             c_vertices_ids,
             c_vertices_count,
             c_vertices_stride,
             c_options,
             &c_found_lengths,
             &c_found_values,
             c_arena,
             c_error);
    if (*c_error)
        return;

    stl_arena_t& arena = **reinterpret_cast<stl_arena_t**>(c_arena);
    taped_values_view_t values {c_found_lengths, c_found_values, c_vertices_count};
    for (value_view_t value : values)
        arena.updated_vals.emplace_back(value);
}

void _ukv_graph_update_edges( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_edges_ids,
    ukv_size_t const c_edges_count,
    ukv_size_t const c_edges_stride,

    ukv_key_t const* c_sources_ids,
    ukv_size_t const c_sources_stride,

    ukv_key_t const* c_targets_ids,
    ukv_size_t const c_targets_stride,

    ukv_options_t const c_options,
    bool const should_erase,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!*c_arena) {
    }

    strided_ptr_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_ptr_gt<ukv_key_t const> edges_ids {c_edges_ids, c_edges_stride};
    strided_ptr_gt<ukv_key_t const> sources_ids {c_sources_ids, c_sources_stride};
    strided_ptr_gt<ukv_key_t const> targets_ids {c_targets_ids, c_targets_stride};
    stl_arena_t& arena = **reinterpret_cast<stl_arena_t**>(c_arena);

    // Fetch all the data related to touched vertices
    arena.updated_keys.resize(c_edges_count + c_edges_count);
    for (ukv_size_t i = 0; i != c_edges_count; ++i)
        arena.updated_keys[i] = {collections[i], sources_ids[i]};
    for (ukv_size_t i = 0; i != c_edges_count; ++i)
        arena.updated_keys[c_edges_count + i] = {collections[i], targets_ids[i]};

    // Keep only the unique items
    sort_and_deduplicate(arena.updated_keys);

    gather_disjoint(c_db,
                    c_txn,
                    &arena.updated_keys[0].collection,
                    sizeof(located_key_t),
                    &arena.updated_keys[0].key,
                    static_cast<ukv_size_t>(arena.updated_keys.size()),
                    sizeof(located_key_t),
                    c_options,
                    c_arena,
                    c_error);
    if (*c_error)
        return;

    // Upsert into in-memory arrays
    for (ukv_size_t i = 0; i != c_edges_count; ++i) {
        auto collection = collections[i];
        auto source_id = sources_ids[i];
        auto target_id = targets_ids[i];

        auto& source_value = arena.updated_vals[offset_in_sorted(arena.updated_keys, {collection, source_id})];
        auto& target_value = arena.updated_vals[offset_in_sorted(arena.updated_keys, {collection, target_id})];

        if (should_erase) {
            std::optional<ukv_key_t> edge_id;
            if (edges_ids)
                edge_id = edges_ids[i];

            erase(source_value, ukv_vertex_source_k, target_id, edge_id);
            erase(target_value, ukv_vertex_target_k, source_id, edge_id);
        }
        else {
            auto edge_id = edges_ids[i];
            upsert(source_value, ukv_vertex_source_k, target_id, edge_id);
            upsert(target_value, ukv_vertex_target_k, source_id, edge_id);
        }
    }

    // Dump the data back to disk!
    ukv_val_len_t offset_in_val = 0;
    ukv_write(c_db,
              c_txn,
              &arena.updated_keys[0].collection,
              sizeof(located_key_t),
              &arena.updated_keys[0].key,
              static_cast<ukv_size_t>(arena.updated_keys.size()),
              sizeof(located_key_t),
              arena.updated_vals[0].internal_cptr(),
              sizeof(value_t),
              &offset_in_val,
              0,
              arena.updated_vals[0].internal_length(),
              sizeof(value_t),
              c_options,
              c_arena,
              c_error);
}

void ukv_graph_upsert_edges( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_edges_ids,
    ukv_size_t const c_edges_count,
    ukv_size_t const c_edges_stride,

    ukv_key_t const* c_sources_ids,
    ukv_size_t const c_sources_stride,

    ukv_key_t const* c_targets_ids,
    ukv_size_t const c_targets_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return _ukv_graph_update_edges(c_db,
                                   c_txn,
                                   c_collections,
                                   c_collections_stride,
                                   c_edges_ids,
                                   c_edges_count,
                                   c_edges_stride,
                                   c_sources_ids,
                                   c_sources_stride,
                                   c_targets_ids,
                                   c_targets_stride,
                                   c_options,
                                   false,
                                   c_arena,
                                   c_error);
}

void ukv_graph_remove_edges( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_edges_ids,
    ukv_size_t const c_edges_count,
    ukv_size_t const c_edges_stride,

    ukv_key_t const* c_sources_ids,
    ukv_size_t const c_sources_stride,

    ukv_key_t const* c_targets_ids,
    ukv_size_t const c_targets_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return _ukv_graph_update_edges(c_db,
                                   c_txn,
                                   c_collections,
                                   c_collections_stride,
                                   c_edges_ids,
                                   c_edges_count,
                                   c_edges_stride,
                                   c_sources_ids,
                                   c_sources_stride,
                                   c_targets_ids,
                                   c_targets_stride,
                                   c_options,
                                   true,
                                   c_arena,
                                   c_error);
}

void ukv_graph_remove_vertices( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_vertices_ids,
    ukv_size_t const c_vertices_count,
    ukv_size_t const c_vertices_stride,

    ukv_vertex_role_t const* c_roles,
    ukv_size_t const c_roles_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    strided_ptr_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_range_gt<ukv_key_t const> vertices_ids {c_vertices_ids, c_vertices_stride, c_vertices_count};
    strided_ptr_gt<ukv_vertex_role_t const> roles {c_roles, c_roles_stride};

    // Initially, just retrieve the bare minimum information about the vertices
    ukv_vertex_degree_t* degrees_per_vertex = nullptr;
    ukv_key_t* neighborships_per_vertex = nullptr;
    ukv_graph_gather_neighbors(c_db,
                               c_txn,
                               c_collections,
                               c_collections_stride,
                               c_vertices_ids,
                               c_vertices_count,
                               c_vertices_stride,
                               c_options,
                               &degrees_per_vertex,
                               &neighborships_per_vertex,
                               c_arena,
                               c_error);

    if (*c_error)
        return;

    stl_arena_t& arena = **reinterpret_cast<stl_arena_t**>(c_arena);
    neighborhoods_t neighborhoods {vertices_ids, degrees_per_vertex, neighborships_per_vertex};
    std::size_t count_edges = 0;
    {
        neighborhoods_iterator_t neighborhoods_it = neighborhoods.begin();
        for (ukv_size_t i = 0; i != c_vertices_count; ++i, ++neighborhoods_it) {
            neighborhood_t neighborhood = *neighborhoods_it;
            auto role = roles[i];
            count_edges += neighborhood.only(role).size();
        }
    }

    // Enumerate the opposite ends, from which that same reference must be removed.
    // Here all the keys will be in the sorted order.
    arena.updated_keys.reserve(count_edges * 2);
    {
        neighborhoods_iterator_t neighborhoods_it = neighborhoods.begin();
        for (ukv_size_t i = 0; i != c_vertices_count; ++i, ++neighborhoods_it) {
            neighborhood_t neighborhood = *neighborhoods_it;
            auto collection = collections[i];
            auto role = roles[i];
            for (neighborship_t n : neighborhood.only(role))
                arena.updated_keys.push_back({collection, n.neighbor_id});
        }
    }

    // Sorting the tasks would help us faster locate them in the future.
    // We may also face repetitions when connected vertices are removed.
    sort_and_deduplicate(arena.updated_keys);
    arena.updated_vals.resize(arena.updated_keys.size());

    // Fetch the opposite ends, from which that same reference must be removed.
    // Here all the keys will be in the sorted order.
    gather_disjoint(c_db,
                    c_txn,
                    &arena.updated_keys[0].collection,
                    sizeof(located_key_t),
                    &arena.updated_keys[0].key,
                    static_cast<ukv_size_t>(arena.updated_keys.size()),
                    sizeof(located_key_t),
                    c_options,
                    c_arena,
                    c_error);
    if (c_error)
        return;

    // From every sequence remove the matching range of neighbors
    for (ukv_size_t i = 0; i != arena.updated_keys.size(); ++i) {
        auto collection = collections[i];
        auto vertex_id = vertices_ids[i];
        auto role = roles[i];

        auto vertex_idx = offset_in_sorted(arena.updated_keys, {collection, vertex_id});
        value_t& vertex_value = arena.updated_vals[vertex_idx];

        for (neighborship_t n : neighbors(vertex_value, role)) {
            auto neighbor_idx = offset_in_sorted(arena.updated_keys, {collection, n.neighbor_id});
            value_t& neighbor_value = arena.updated_vals[neighbor_idx];
            erase(neighbor_value, invert(role), vertex_id);
        }

        vertex_value.clear();
    }

    // Now we will go through all the explicitly deleted vertices
    ukv_val_len_t offset_in_val = 0;
    ukv_write(c_db,
              c_txn,
              &arena.updated_keys[0].collection,
              sizeof(located_key_t),
              &arena.updated_keys[0].key,
              static_cast<ukv_size_t>(arena.updated_keys.size()),
              sizeof(located_key_t),
              arena.updated_vals[0].internal_cptr(),
              sizeof(value_t),
              &offset_in_val,
              0,
              arena.updated_vals[0].internal_length(),
              sizeof(value_t),
              c_options,
              c_arena,
              c_error);
}