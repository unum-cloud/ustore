/**
 * @brief Python bindings for Unums Graph Store, that mimics NetworkX.
 * Is similar in it's purpose to a pure-Python NetworkXum:
 * https://github.com/unum-cloud/NetworkXum
 *
 * @section Supported Graph Types
 * We support all the NetworkX graph kinds and more:
 * https://networkx.org/documentation/stable/reference/classes/index.html#which-graph-class-should-i-use
 *
 *      | Class          | Type         | Self-loops | Parallel edges |
 *      | Graph          | undirected   | Yes        | No             |
 *      | DiGraph        | directed     | Yes        | No             |
 *      | MultiGraph     | undirected   | Yes        | Yes            |
 *      | MultiDiGraph   | directed     | Yes        | Yes            |
 *
 * Aside frim those, you can instantiate the most generic `ukv.Network`,
 * controlling wheather graph should be directed, allow loops, or have
 * attributes in source/target nodes or edges.
 *
 * @section Interface
 * Primary single element methods:
 *      * add_edge(first, second, key?, attrs?)
 *      * remove_edge(first, second, key?, attrs?)
 * Additional batch methods:
 *      * add_edges_from(firsts, seconds, keys?, attrs?)
 *      * remove_edges_from(firsts, seconds, keys?, attrs?)
 * Intentionally not implemented:
 *      * __len__() ~ It's hard to consistently estimate the collection.
 */

#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include "ukv.h"
#include "ukv.hpp"

namespace py = pybind11;
using namespace unum::ukv;
using namespace unum;

/**
 * @brief A generalization of the graph supported by NetworkX.
 *
 */
struct network_t : public std::enable_shared_from_this<network_t> {

    ukv_t db = NULL;
    ukv_collection_t sources_attrs = NULL;
    ukv_collection_t targets_attrs = NULL;
    ukv_collection_t relations_attrs = NULL;
    ukv_collection_t inverted_index = NULL;

    std::string name;

    bool is_directed_ = false;
    bool is_multi_ = false;
    bool allows_self_loops_ = false;
    bool has_source_attrs_ = false;
    bool has_target_attrs_ = false;
    bool has_edge_attrs_ = false;

    network_t() = default;
    network_t(network_t&&) = delete;
    network_t(network_t const&) = delete;

    /**
     * @brief
     * Sources and targets can match.
     * Relations attributes can be banned all together.
     *
     * Example for simple non-atttributed undirected graphs:
     * > sources_name: ""
     * > targets_name: ""
     * > relations_name: ".graph"
     * > attributes_name: ""
     *
     * Example for recommender systems
     * > sources_name: "people.docs"
     * > targets_name: "movies.docs"
     * > relations_name: "views.graph"
     * > attributes_name: "views.docs"
     *
     */
    network_t(ukv_t db,
              std::string const& relations_name,
              std::string const& sources_name,
              std::string const& targets_name,
              bool directed,
              bool attributed_relations,
              bool allow_self_loops,
              bool allow_parallel_edges) {}

    ~network_t() {
        if (raw)
            ukv_collection_free(db_ptr->raw, raw);
        raw = NULL;
    }
};

PYBIND11_MODULE(ukv.networkx, m) {
    m.doc() =
        "Python Graph/Network processing workloads.\n"
        "Similar to NetworkX, but implemented in C/C++ and \n"
        "with support for persistent storage and ACID operations.\n"
        "---------------------------------------------\n";

    auto net = py::class_<network_t, std::shared_ptr<network_t>>(m, "Network");
    net.def(py::init([](std::string relation_name,
                        std::optional<std::string> source_attributes,
                        std::optional<std::string> target_attributes,
                        bool multi = false,
                        bool attributed_relations = false) { return std::make_shared<network_t>(); }),
            py::arg("source_attributes") = std::nullopt,
            py::arg("target_attributes") = std::nullopt,
            py::arg("multi") = false,
            py::arg("attributed_relations") = false);

    // Random scalar operations
    net.def("has_node", [](network_t& net, ukv_key_t v) {
        // Checks if there is such node ID both among sources and targets
        return false;
    });
    net.def("has_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        //
        return false;
    });
    net.def("has_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
        //
        return false;
    });
    net.def("count_edges", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {});

    // Batch retrieval into dynamically sized NumPy arrays
    net.def("neighbors", [](network_t& net, ukv_key_t n) {

    });
    net.def("successors", [](network_t& net, ukv_key_t n) {

    });
    net.def("predecessors", [](network_t& net, ukv_key_t n) {

    });

    // Random Writes
    net.def("add_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {});
    net.def("add_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {});
    net.def("remove_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {});
    net.def("remove_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {});
    net.def("add_edge_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s) {});
    net.def("add_edge_from",
            [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {});
    net.def("remove_edge_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s) {});
    net.def("remove_edge_from",
            [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {});

    // Bulk Writes
    net.def("clear_edges", [](network_t& net) {});
    net.def("clear", [](network_t& net) {});

    // Bulk Reads
    net.def("nodes", [](network_t& net) {});
    net.def("edges", [](network_t& net) {});

    // Two hop batch retrieval, not a classical API
    net.def("neighbors_of_group", [](network_t& net, py::handle ns) {

    });
    net.def("neighbors_of_neighbors", [](network_t& net, ukv_key_t n) {

    });
}
