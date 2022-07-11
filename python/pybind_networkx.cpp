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

    std::shared_ptr<py_db_t> db_ptr;
    graph_collection_session_t inverted_index;
    collection_t sources_attrs;
    collection_t targets_attrs;
    collection_t relations_attrs;

    bool is_directed_ = false;
    bool is_multi_ = false;
    bool allow_self_loops_ = false;

    Py_ssize_t last_buffer_shape[3];
    Py_ssize_t last_buffer_strides[3];

    network_t(graph_collection_session_t&& session) : inverted_index(std::move(session)) {}
    network_t(network_t&&) = delete;
    network_t(network_t const&) = delete;
    ~network_t() {}

    // inline ukv_t db() const noexcept { return db_ptr->native; }
    // inline ukv_txn_t txn() const noexcept { return inverted_index.txn(); }
    // inline ukv_collection_t* internal_cptr() const noexcept { return inverted_index.collection().internal_cptr(); }
    // inline managed_arena_t& arena() noexcept { return inverted_index.arena(); }
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
                //
                if (!index_collection)
                    return std::shared_ptr<network_t> {};

                db_t& db = index_collection->db_ptr->native;
                ukv_txn_t txn_raw = index_collection->txn_ptr ? index_collection->txn_ptr->native : ukv_txn_t(nullptr);
                auto maybe_col = db.collection(index_collection->name);
                maybe_col.throw_unhandled();

                graph_collection_session_t g(*std::move(maybe_col), txn_raw);
                auto net_ptr = std::make_shared<network_t>(std::move(g));

                net_ptr->db_ptr = index_collection->db_ptr;
                net_ptr->is_directed_ = directed;
                net_ptr->is_multi_ = multi;
                net_ptr->allow_self_loops_ = loops;

                // Attach the additional collections
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
        auto maybe_exists = net.inverted_index.contains(v);
        maybe_exists.throw_unhandled();
        return *maybe_exists;
    });

    net.def("degree", [](network_t& net, ukv_key_t v) {
        auto maybe_degree = net.inverted_index.degree(v);
        maybe_degree.throw_unhandled();
        return *maybe_degree;
    });

    net.def("number_of_edges", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        auto maybe_edges = net.inverted_index.edges(v1, v2);
        maybe_edges.throw_unhandled();
        return maybe_edges->size();
    });

    net.def("has_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        auto maybe_edges = net.inverted_index.edges(v1, v2);
        maybe_edges.throw_unhandled();
        return maybe_edges->size() != 0;
    });

    net.def("has_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
        auto maybe_edges = net.inverted_index.edges(v1, v2);
        maybe_edges.throw_unhandled();
        return std::find(maybe_edges->edge_ids.begin(), maybe_edges->edge_ids.end(), eid) !=
               maybe_edges->edge_ids.end();
    });

    // Batch retrieval into dynamically sized NumPy arrays
    net.def("neighbors", [](network_t& net, ukv_key_t n) {
        // taped_values_view_t range = net.inverted_index().arena().untape(1);
        // value_view_t first = *range.begin();
        // neighborhood_t neighborhood {n, first};
        // // https://docs.python.org/3/c-api/buffer.html
        // struct Py_buffer py_buf;
        // py_buf.buf = neighborhood.targets.begin();
        // py_buf.obj = NULL;
        // py_buf.len = neighborhood.size() * sizeof(ukv_key_t);
        // py_buf.itemsize = sizeof(ukv_key_t);
        // // https://docs.python.org/3/library/struct.html#format-characters
        // py_buf.format = "Q";
        // py_buf.ndim = 1;
        // py_buf.shape = &net.last_buffer_shape[0];
        // py_buf.strides = &net.last_buffer_strides[0];
        // return py_buf;
    });
    net.def("successors", [](network_t& net, ukv_key_t n) {

    });
    net.def("predecessors", [](network_t& net, ukv_key_t n) {

    });

    // Random Writes
    net.def("add_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        edges_soa_view_t edges {
            strided_range_gt<ukv_key_t const>(v1),
            strided_range_gt<ukv_key_t const>(v2),
        };
        net.inverted_index.upsert(edges).throw_unhandled();
    });
    net.def("add_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
        edges_soa_view_t edges {
            strided_range_gt<ukv_key_t const>(v1),
            strided_range_gt<ukv_key_t const>(v2),
            strided_range_gt<ukv_key_t const>(eid),
        };
        net.inverted_index.upsert(edges).throw_unhandled();
    });
    net.def("remove_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
        edges_soa_view_t edges {
            strided_range_gt<ukv_key_t const>(v1),
            strided_range_gt<ukv_key_t const>(v2),
        };
        net.inverted_index.remove(edges).throw_unhandled();
    });
    net.def("remove_edge", [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
        edges_soa_view_t edges {
            strided_range_gt<ukv_key_t const>(v1),
            strided_range_gt<ukv_key_t const>(v2),
            strided_range_gt<ukv_key_t const>(eid),
        };
        net.inverted_index.remove(edges).throw_unhandled();
    });

    net.def("add_edges_from", [](network_t& net, py::handle const& adjacency_list) {
        auto handle_and_list = strided_array<ukv_key_t const>(adjacency_list);
        if (handle_and_list.second.cols() != 2 || handle_and_list.second.cols() != 3)
            throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

        edges_soa_view_t edges {
            handle_and_list.second.col(0),
            handle_and_list.second.col(1),
            handle_and_list.second.cols() == 3 ? handle_and_list.second.col(2)
                                               : strided_range_gt<ukv_key_t const>(ukv_default_edge_id_k),
        };
        net.inverted_index.upsert(edges).throw_unhandled();
    });
    net.def("remove_edges_from", [](network_t& net, py::handle const& adjacency_list) {
        auto handle_and_list = strided_array<ukv_key_t const>(adjacency_list);
        if (handle_and_list.second.cols() != 2 || handle_and_list.second.cols() != 3)
            throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

        edges_soa_view_t edges {
            handle_and_list.second.col(0),
            handle_and_list.second.col(1),
            handle_and_list.second.cols() == 3 ? handle_and_list.second.col(2)
                                               : strided_range_gt<ukv_key_t const>(ukv_default_edge_id_k),
        };
        net.inverted_index.remove(edges).throw_unhandled();
    });

    net.def("add_edges_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s) {
        auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
        auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
        edges_soa_view_t edges {
            handle_and_sources.second,
            handle_and_targets.second,
        };
        net.inverted_index.upsert(edges).throw_unhandled();
    });
    net.def("remove_edges_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s) {
        auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
        auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
        edges_soa_view_t edges {
            handle_and_sources.second,
            handle_and_targets.second,
        };
        net.inverted_index.remove(edges).throw_unhandled();
    });

    net.def("add_edges_from", [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {
        auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
        auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
        auto handle_and_edge_ids = strided_array<ukv_key_t const>(eids);
        edges_soa_view_t edges {
            handle_and_sources.second,
            handle_and_targets.second,
            handle_and_edge_ids.second,
        };
        net.inverted_index.upsert(edges).throw_unhandled();
    });
    net.def("remove_edges_from",
            [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {
                auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
                auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
                auto handle_and_edge_ids = strided_array<ukv_key_t const>(eids);
                edges_soa_view_t edges {
                    handle_and_sources.second,
                    handle_and_targets.second,
                    handle_and_edge_ids.second,
                };
                net.inverted_index.remove(edges).throw_unhandled();
            });

    // Bulk Writes
    net.def("clear_edges", [](network_t& net) {

    });
    net.def("clear", [](network_t& net) {
        net.inverted_index.collection().clear();
        net.sources_attrs.clear();
        targets_attrs.clear();
        relations_attrs.clear();
    });

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
