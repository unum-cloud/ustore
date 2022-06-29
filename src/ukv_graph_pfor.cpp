/**
 * @file ukv_graph_pfor.cpp
 * @author Ashot Vardanian
 *
 * @brief Graph implementation using fast Integer Compression schemes.
 *
 */

#include <vector>
#include <algorithm> // `std::sort`

#include "ukv_graph.h"
#include "helpers.hpp"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

ukv_key_t ukv_default_edge_id_k = std::numeric_limits<ukv_key_t>::max();

struct edge_t {
    ukv_key_t source_id;
    ukv_key_t target_id;
    ukv_key_t edge_id = ukv_default_edge_id_k;
};

struct located_node_t {
    ukv_collection_t collection;
    ukv_key_t node_id;
};

void upsert(value_t& value, ukv_graph_node_role_t role, ukv_key_t neighbor_id, ukv_key_t edge_id) {
    auto node_degrees = reinterpret_cast<ukv_size_t*>(value.begin());
    auto neighbor_and_edge_ids = reinterpret_cast<ukv_key_t*>(node_degrees + 2);
}

void erase(value_t&, ukv_graph_node_role_t role, ukv_key_t neighbor_id, ukv_key_t edge_id) {
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
    std::vector<located_node_t> updated_ids {c_edges_count + c_edges_count};
    for (ukv_size_t i = 0; i != c_edges_count; ++i)
        updated_ids[i] = {collections[i], sources_ids[i]};
    for (ukv_size_t i = 0; i != c_edges_count; ++i)
        updated_ids[c_edges_count + i] = {collections[i], targets_ids[i]};

    // Keep only the unique items
    std::sort(updated_ids.begin(), updated_ids.end());
    updated_ids.erase(std::unique(updated_ids.begin(), updated_ids.end()), updated_ids.end());
    std::vector<value_t> updated_vals(updated_ids.size());

    ukv_read(c_db,
             c_txn,
             &updated_ids[0].collection,
             sizeof(located_node_t),
             &updated_ids[0].node_id,
             static_cast<ukv_size_t>(updated_ids.size()),
             sizeof(located_node_t),
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

        auto source_idx = std::lower_bound(updated_ids.begin(), updated_ids.end(), target_id) - updated_ids.begin();
        auto target_idx = std::lower_bound(updated_ids.begin(), updated_ids.end(), source_id) - updated_ids.begin();
        auto& source_value = updated_vals[source_idx];
        auto& target_value = updated_vals[target_idx];

        upsert(source_value, ukv_graph_node_source_k, target_id, edge_id);
        upsert(target_value, ukv_graph_node_target_k, source_id, edge_id);
    }

    // Dump the data back to disk!
    ukv_write(c_db,
              c_txn,
              &updated_ids[0].collection,
              sizeof(located_node_t),
              &updated_ids[0].node_id,
              static_cast<ukv_size_t>(updated_ids.size()),
              sizeof(located_node_t),
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

    auto arr_lengths = reinterpret_cast<ukv_val_len_t*>(c_tape);
    auto arr_bytes = c_tape + sizeof(ukv_val_len_t) * c_nodes_count;

    // Estimate the memory usage to avoid re-allocating
    // Copy data from tape into disjoint mutable arrays
    std::size_t count_edges = 0ull;
    {
        tape_iterator_t values {*c_tape, c_nodes_count};
        for (ukv_size_t i = 0; i != c_nodes_count; ++i, ++values) {
            value_view_t value = *values;
            auto degrees = reinterpret_cast<ukv_size_t const*>(value.begin());
            count_edges += degrees[0] + degrees[1];
        }
    }

    strided_ptr_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_ptr_gt<ukv_key_t const> nodes_ids {c_nodes_ids, c_nodes_stride};
    strided_ptr_gt<ukv_graph_node_role_t const> roles {c_roles, c_roles_stride};

    // Enumerate the opposite ends, from which that same reference must be removed.
    // Here all the keys will be in the sorted order.
    std::vector<located_node_t> updated_ids;
    std::vector<value_t> updated_vals;
    updated_ids.reserve(count_edges * 2);
    {
        ukv_size_t unpacked_bytes = 0;
        for (ukv_size_t i = 0; i != c_nodes_count; ++i) {
            auto collection = collections[i];
            auto node_id = nodes_ids[i];
            auto role = roles[i];
            auto len = arr_lengths[i];
            if (!len)
                continue;

            auto bytes = arr_bytes + unpacked_bytes;
            auto node_degrees = reinterpret_cast<ukv_size_t*>(bytes);
            auto neighbor_and_edge_ids = reinterpret_cast<ukv_key_t*>(node_degrees + 2);

            // Parse outgoing neighbor ids, to fetch their incoming edges
            if (role == ukv_graph_node_source_k)
                for (ukv_size_t j = 0; j != node_degrees[0]; ++j, neighbor_and_edge_ids += 2)
                    updated_ids.push_back({collection, neighbor_and_edge_ids[0]});
            else
                neighbor_and_edge_ids += 2 * node_degrees[0];

            // Parse incoming neighbor ids
            if (role == ukv_graph_node_target_k)
                for (ukv_size_t j = 0; j != node_degrees[1]; ++j, neighbor_and_edge_ids += 2)
                    updated_ids.push_back({collection, neighbor_and_edge_ids[0]});
            else
                neighbor_and_edge_ids += 2 * node_degrees[1];

            // Proceed to next list of neighbors
            unpacked_bytes += len;
        }
    }

    // Sorting the tasks would help us faster locate them in the future.
    std::sort(updated_ids.begin(), updated_ids.end(), [](located_node_t const& a, located_node_t const& b) {
        return a.node_id < b.node_id;
    });

    // We may face repetitions when connected nodes are removed
    updated_ids.erase(std::unique(updated_ids.begin(), updated_ids.end()), updated_ids.end());
    updated_vals.resize(updated_ids.size());

    // Fetch the opposite ends, from which that same reference must be removed.
    // Here all the keys will be in the sorted order.
    auto role = ukv_graph_node_any_k;
    ukv_read(c_db,
             c_txn,
             &updated_ids[0].collection,
             sizeof(located_node_t),
             &updated_ids[0].node_id,
             static_cast<ukv_size_t>(updated_ids.size()),
             sizeof(located_node_t),
             c_options,
             c_tape,
             c_capacity,
             c_error);
    if (c_error)
        return;

    // From every sequence remove the matching range of neightbors
    //
    //
    //

    // Now we will go through all the explicitly deleted nodes
    ukv_write(c_db,
              c_txn,
              &updated_ids[0].collection,
              sizeof(located_node_t),
              &updated_ids[0].node_id,
              static_cast<ukv_size_t>(updated_ids.size()),
              sizeof(located_node_t),
              &updated_vals[0].ptr,
              sizeof(value_t),
              &updated_vals[0].length,
              sizeof(value_t),
              c_options,
              c_error);
}