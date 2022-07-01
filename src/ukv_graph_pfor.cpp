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

void sort_and_deduplicate(std::vector<located_key_t>& keys) {
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
}

std::size_t offset_in_sorted(std::vector<located_key_t> const& keys, located_key_t wanted) {
    return std::lower_bound(keys.begin(), keys.end(), wanted) - keys.begin();
}

void upsert(value_t& value, ukv_graph_node_role_t role, ukv_key_t neighbor_id, ukv_key_t edge_id) {
    auto node_degrees = reinterpret_cast<ukv_size_t*>(value.begin());
    auto neighbor_and_edge_ids = reinterpret_cast<ukv_key_t*>(node_degrees + 2);
}

void erase(value_t& value, ukv_graph_node_role_t role, ukv_key_t neighbor_id, std::optional<ukv_key_t> edge_id = {}) {
}

void ukv_graph_gather_neighbors( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_nodes_ids,
    ukv_size_t const c_nodes_count,
    ukv_size_t const c_nodes_stride,

    ukv_options_t const c_options,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    return ukv_read(c_db,
                    c_txn,
                    c_collections,
                    c_collections_stride,
                    c_nodes_ids,
                    c_nodes_count,
                    c_nodes_stride,
                    c_options,
                    c_tape,
                    c_capacity,
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

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    strided_ptr_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_ptr_gt<ukv_key_t const> edges_ids {c_edges_ids, c_edges_stride};
    strided_ptr_gt<ukv_key_t const> sources_ids {c_sources_ids, c_sources_stride};
    strided_ptr_gt<ukv_key_t const> targets_ids {c_targets_ids, c_targets_stride};

    // Fetch all the data related to touched nodes
    std::vector<located_key_t> updated_ids {c_edges_count + c_edges_count};
    for (ukv_size_t i = 0; i != c_edges_count; ++i)
        updated_ids[i] = {collections[i], sources_ids[i]};
    for (ukv_size_t i = 0; i != c_edges_count; ++i)
        updated_ids[c_edges_count + i] = {collections[i], targets_ids[i]};

    // Keep only the unique items
    sort_and_deduplicate(updated_ids);
    std::vector<value_t> updated_vals(updated_ids.size());

    ukv_read(c_db,
             c_txn,
             &updated_ids[0].collection,
             sizeof(located_key_t),
             &updated_ids[0].key,
             static_cast<ukv_size_t>(updated_ids.size()),
             sizeof(located_key_t),
             c_options,
             c_tape,
             c_capacity,
             c_error);
    if (*c_error)
        return;

    // Copy data from tape into disjoint mutable arrays
    {
        tape_iterator_t values {*c_tape, c_edges_count};
        for (ukv_size_t i = 0; i != c_edges_count; ++i, ++values)
            updated_vals[i] = *values;
    }

    // Upsert into in-memory arrays
    for (ukv_size_t i = 0; i != c_edges_count; ++i) {
        auto collection = collections[i];
        auto source_id = sources_ids[i];
        auto target_id = targets_ids[i];
        auto edge_id = edges_ids[i];

        auto& source_value = updated_vals[offset_in_sorted(updated_ids, {collection, target_id})];
        auto& target_value = updated_vals[offset_in_sorted(updated_ids, {collection, source_id})];

        upsert(source_value, ukv_graph_node_source_k, target_id, edge_id);
        upsert(target_value, ukv_graph_node_target_k, source_id, edge_id);
    }

    // Dump the data back to disk!
    ukv_write(c_db,
              c_txn,
              &updated_ids[0].collection,
              sizeof(located_key_t),
              &updated_ids[0].key,
              static_cast<ukv_size_t>(updated_ids.size()),
              sizeof(located_key_t),
              &updated_vals[0].ptr,
              sizeof(value_t),
              &updated_vals[0].length,
              sizeof(value_t),
              c_options,
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

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {
}

void ukv_graph_remove_nodes( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_nodes_ids,
    ukv_size_t const c_nodes_count,
    ukv_size_t const c_nodes_stride,

    ukv_graph_node_role_t const* c_roles,
    ukv_size_t const c_roles_stride,

    ukv_options_t const c_options,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    // Initially, just retrieve the bare minimum information about the nodes
    ukv_read(c_db,
             c_txn,
             c_collections,
             c_collections_stride,
             c_nodes_ids,
             c_nodes_count,
             c_nodes_stride,
             c_options,
             c_tape,
             c_capacity,
             c_error);
    if (*c_error)
        return;

    // Estimate the memory usage to avoid re-allocating
    // Copy data from tape into disjoint mutable arrays
    std::size_t count_edges = 0ull;
    {
        tape_iterator_t values {*c_tape, c_nodes_count};
        for (ukv_size_t i = 0; i != c_nodes_count; ++i, ++values)
            count_edges += neighbors(*values).size();
    }

    strided_ptr_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_ptr_gt<ukv_key_t const> nodes_ids {c_nodes_ids, c_nodes_stride};
    strided_ptr_gt<ukv_graph_node_role_t const> roles {c_roles, c_roles_stride};

    // Enumerate the opposite ends, from which that same reference must be removed.
    // Here all the keys will be in the sorted order.
    std::vector<located_key_t> updated_ids;
    std::vector<value_t> updated_vals;
    updated_ids.reserve(count_edges * 2);
    {
        tape_iterator_t values {*c_tape, c_nodes_count};
        for (ukv_size_t i = 0; i != c_nodes_count; ++i, ++values) {
            value_view_t value = *values;
            if (!value)
                continue;

            auto collection = collections[i];
            auto role = roles[i];
            for (neighborhood_t n : neighbors(value, role))
                updated_ids.push_back({collection, n.neighbor_id});
        }
    }

    // Sorting the tasks would help us faster locate them in the future.
    // We may also face repetitions when connected nodes are removed.
    sort_and_deduplicate(updated_ids);
    updated_vals.resize(updated_ids.size());

    // Fetch the opposite ends, from which that same reference must be removed.
    // Here all the keys will be in the sorted order.
    auto role = ukv_graph_node_any_k;
    ukv_read(c_db,
             c_txn,
             &updated_ids[0].collection,
             sizeof(located_key_t),
             &updated_ids[0].key,
             static_cast<ukv_size_t>(updated_ids.size()),
             sizeof(located_key_t),
             c_options,
             c_tape,
             c_capacity,
             c_error);
    if (c_error)
        return;

    // Export taped values into disjoint buffers
    {
        tape_iterator_t values {*c_tape, c_nodes_count};
        for (ukv_size_t i = 0; i != c_nodes_count; ++i, ++values)
            updated_vals[i] = *values;
    }

    // From every sequence remove the matching range of neighbors
    {
        tape_iterator_t values {*c_tape, c_nodes_count};
        for (ukv_size_t i = 0; i != c_nodes_count; ++i, ++values) {
            auto collection = collections[i];
            auto node_id = nodes_ids[i];
            auto role = roles[i];

            auto node_idx = offset_in_sorted(updated_ids, {collection, node_id});
            value_t& node_value = updated_vals[node_idx];

            for (neighborhood_t n : neighbors(node_value, role)) {
                auto neighbor_idx = offset_in_sorted(updated_ids, {collection, n.neighbor_id});
                value_t& neighbor_value = updated_vals[neighbor_idx];
                erase(neighbor_value, invert(role), node_id);
            }

            node_value.clear();
        }
    }

    // Now we will go through all the explicitly deleted nodes
    ukv_write(c_db,
              c_txn,
              &updated_ids[0].collection,
              sizeof(located_key_t),
              &updated_ids[0].key,
              static_cast<ukv_size_t>(updated_ids.size()),
              sizeof(located_key_t),
              &updated_vals[0].ptr,
              sizeof(value_t),
              &updated_vals[0].length,
              sizeof(value_t),
              c_options,
              c_error);
}