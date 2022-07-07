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
 * attrs in source/target vertices or edges.
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

#include "pybind.hpp"
#include "ukv_graph.hpp"

using namespace unum::ukv;
using namespace unum;

/**
 * @brief A generalization of the graph supported by NetworkX.
 *
 * Sources and targets can match.
 * Relations attrs can be banned all together.
 *
 * Example for simple non-atttributed undirected graphs:
 * > relations_name: ".graph"
 * > attrs_name: ""
 * > sources_name: ""
 * > targets_name: ""
 *
 * Example for recommender systems
 * > relations_name: "views.graph"
 * > attrs_name: "views.docs"
 * > sources_name: "people.docs"
 * > targets_name: "movies.docs"
 */
struct network_t : public std::enable_shared_from_this<network_t> {

    std::shared_ptr<py_col_t> inverted_index;
    collection_t sources_attrs;
    collection_t targets_attrs;
    collection_t relations_attrs;

    bool is_directed_ = false;
    bool is_multi_ = false;
    bool allow_self_loops_ = false;

    network_t() {}
    network_t(network_t&&) = delete;
    network_t(network_t const&) = delete;
    ~network_t() {}

    inline ukv_t db() const noexcept { return inverted_index->db_ptr->native; }
    inline ukv_txn_t txn() const noexcept {
        return inverted_index->txn_ptr ? inverted_index->txn_ptr->native : ukv_txn_t(nullptr);
    }
    inline ukv_collection_t* index_col() const noexcept { return inverted_index->native.internal_cptr(); }
    inline managed_tape_t& tape() noexcept {
        return inverted_index->txn_ptr ? inverted_index->txn_ptr->native.tape()
                                       : inverted_index->db_ptr->session.tape();
    }
};

void ukv::wrap_network(py::module& m) {

    auto net = py::class_<network_t, std::shared_ptr<network_t>>(m, "Network", py::module_local());
    net.def(py::init([](std::shared_ptr<py_col_t> index_collection,
                        std::optional<std::string> sources_attrs,
                        std::optional<std::string> targets_attrs,
                        std::optional<std::string> relations_attrs,
                        bool directed = false,
                        bool multi = false,
                        bool loops = false) {
                auto net_ptr = std::make_shared<network_t>();
                net_ptr->inverted_index = index_collection;
                net_ptr->is_directed_ = directed;
                net_ptr->is_multi_ = multi;
                net_ptr->allow_self_loops_ = loops;

                // Attach the additional collections
                auto& db = index_collection->db_ptr->native;
                if (sources_attrs) {
                    auto col = db.collection(*sources_attrs);
                    col.throw_unhandled();
                    net_ptr->sources_attrs = *std::move(col);
                }
                if (targets_attrs) {
                    auto col = db.collection(*targets_attrs);
                    col.throw_unhandled();
                    net_ptr->targets_attrs = *std::move(col);
                }
                if (relations_attrs) {
                    auto col = db.collection(*relations_attrs);
                    col.throw_unhandled();
                    net_ptr->relations_attrs = *std::move(col);
                }
                return net_ptr;
            }),
            py::arg("index"),
            py::arg("sources") = std::nullopt,
            py::arg("targets") = std::nullopt,
            py::arg("relations") = std::nullopt,
            py::arg("directed") = false,
            py::arg("multi") = false,
            py::arg("loops") = false);

    // Random scalar operations
    net.def("has_node", [](network_t& net, ukv_key_t v) {
        error_t error;
        ukv_read(net.db(),
                 net.txn(),
                 net.index_col(),
                 0,
                 &v,
                 1,
                 0,
                 ukv_option_read_lengths_k,
                 net.tape().internal_memory(),
                 net.tape().internal_capacity(),
                 error.internal_cptr());
        error.throw_unhandled();

        std::cout << "Received a tape of length" << *net.tape().internal_capacity() << std::endl;
        taped_values_view_t vals = net.tape().untape(1);
        if (!vals.size())
            return false;
        value_view_t val = *vals.begin();

        return val.size() != 0;
    });

    net.def("degree", [](network_t& net, ukv_key_t v) {
        error_t error;
        ukv_graph_gather_neighbors(net.db(),
                                   net.txn(),
                                   net.index_col(),
                                   0,
                                   &v,
                                   1,
                                   0,
                                   ukv_options_default_k,
                                   net.tape().internal_memory(),
                                   net.tape().internal_capacity(),
                                   error.internal_cptr());
        error.throw_unhandled();

        taped_values_view_t vals = net.tape().untape(1);
        if (!vals.size())
            return 0ul;
        value_view_t val = *vals.begin();
        return neighbors(val, ukv_vertex_role_any_k).size();
    });

    net.def("number_of_edges", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        error_t error;
        ukv_graph_gather_neighbors(net.db(),
                                   net.txn(),
                                   net.index_col(),
                                   0,
                                   &v1,
                                   1,
                                   0,
                                   ukv_options_default_k,
                                   net.tape().internal_memory(),
                                   net.tape().internal_capacity(),
                                   error.internal_cptr());
        error.throw_unhandled();

        taped_values_view_t vals = net.tape().untape(1);
        if (!vals.size())
            return 0ul;
        value_view_t val = *vals.begin();
        auto neighbors_range = neighbors(val, ukv_vertex_source_k);
        auto matching_range = std::equal_range(neighbors_range.begin(), neighbors_range.end(), v2);
        auto matching_len = static_cast<std::size_t>(matching_range.second - matching_range.first);
        std::cout << val.size() << " neighbors " << neighbors_range.size() << " matching " << matching_len << std::endl;
        return matching_len;
    });

    net.def("has_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        error_t error;
        ukv_graph_gather_neighbors(net.db(),
                                   net.txn(),
                                   net.index_col(),
                                   0,
                                   &v1,
                                   1,
                                   0,
                                   ukv_options_default_k,
                                   net.tape().internal_memory(),
                                   net.tape().internal_capacity(),
                                   error.internal_cptr());
        error.throw_unhandled();

        taped_values_view_t vals = net.tape().untape(1);
        if (!vals.size())
            return false;
        value_view_t val = *vals.begin();
        auto neighbors_range = neighbors(val, ukv_vertex_source_k);
        return std::binary_search(neighbors_range.begin(), neighbors_range.end(), v2);
    });

    net.def("has_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
        error_t error;
        ukv_graph_gather_neighbors(net.db(),
                                   net.txn(),
                                   net.index_col(),
                                   0,
                                   &v1,
                                   1,
                                   0,
                                   ukv_options_default_k,
                                   net.tape().internal_memory(),
                                   net.tape().internal_capacity(),
                                   error.internal_cptr());
        error.throw_unhandled();

        taped_values_view_t vals = net.tape().untape(1);
        if (!vals.size())
            return false;
        value_view_t val = *vals.begin();
        auto neighbors_range = neighbors(val, ukv_vertex_source_k);
        return std::binary_search(neighbors_range.begin(), neighbors_range.end(), neighborship_t {v2, eid});
    });

    // Batch retrieval into dynamically sized NumPy arrays
    net.def("neighbors", [](network_t& net, ukv_key_t n) {

    });
    net.def("successors", [](network_t& net, ukv_key_t n) {

    });
    net.def("predecessors", [](network_t& net, ukv_key_t n) {

    });

    // Random Writes
    net.def("add_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        error_t error;
        ukv_graph_upsert_edges(net.db(),
                               net.txn(),
                               net.index_col(),
                               0,
                               &ukv_default_edge_id_k,
                               1,
                               0,
                               &v1,
                               0,
                               &v2,
                               0,
                               ukv_options_default_k,
                               net.tape().internal_memory(),
                               net.tape().internal_capacity(),
                               error.internal_cptr());
        error.throw_unhandled();
    });
    net.def("add_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
        error_t error;
        ukv_graph_upsert_edges(net.db(),
                               net.txn(),
                               net.index_col(),
                               0,
                               &eid,
                               1,
                               0,
                               &v1,
                               0,
                               &v2,
                               0,
                               ukv_options_default_k,
                               net.tape().internal_memory(),
                               net.tape().internal_capacity(),
                               error.internal_cptr());
        error.throw_unhandled();
    });
    net.def("remove_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {});
    net.def("remove_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {});
    net.def("add_edge_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s) {});
    net.def("add_edge_from",
            [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {});
    net.def("remove_edge_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s) {});
    net.def("remove_edge_from",
            [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {});

    // Bulk Writes
    net.def("clear_edges", [](network_t& net) {

    });
    net.def("clear", [](network_t& net) {});

    // Bulk Reads
    net.def("nodes", [](network_t& net) {});
    net.def("edges", [](network_t& net) {});

    // Two hop batch retrieval, not a classical API
    net.def("neighbors_of_group", [](network_t& net, py::handle ns) {

    });
    net.def("neighbors_of_neighbors", [](network_t& net, ukv_key_t n) {

    });

    // https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.density.html
    // https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.is_directed.html?highlight=is_directed
    m.def("is_directed", [](network_t& net) { return net.is_directed_; });
    m.def("is_multi", [](network_t& net) { return net.is_multi_; });
    m.def("allows_loops", [](network_t& net) { return net.allow_self_loops_; });
    m.def("density", [](network_t& net) { return 0.0; });
}
