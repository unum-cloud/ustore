/**
 * @brief Python bindings for a Graph index, that mimics NetworkX.
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
 * Aside from those, you can instantiate the most generic `ukv.Network`,
 * controlling whether graph should be directed, allow loops, or have
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
#include "ukv/graph.hpp"

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
    graph_t graph;
    collection_t sources_attrs;
    collection_t targets_attrs;
    collection_t relations_attrs;

    bool is_directed_ = false;
    bool is_multi_ = false;
    bool allow_self_loops_ = false;

    Py_buffer last_buffer;
    Py_ssize_t last_buffer_shape[3];
    Py_ssize_t last_buffer_strides[3];

    network_t(graph_t&& g) : graph(std::move(g)) {}
    network_t(network_t&&) = delete;
    network_t(network_t const&) = delete;
    ~network_t() {}
};

struct degree_view_t : public std::enable_shared_from_this<degree_view_t> {
    std::shared_ptr<network_t> net_ptr;
    ukv_vertex_role_t roles = ukv_vertex_role_any_k;
};

template <typename element_at>
py::handle wrap_into_buffer(network_t& net, strided_range_gt<element_at> range) {
    printf("received %i strided objects\n", (int)range.size());

    net.last_buffer_strides[0] = range.stride();
    net.last_buffer_strides[1] = net.last_buffer_strides[2] = 1;
    net.last_buffer_shape[0] = range.size();
    net.last_buffer_shape[1] = net.last_buffer_shape[2] = 1;

    // https://docs.python.org/3/c-api/buffer.html
    net.last_buffer.buf = (void*)range.begin().get();
    net.last_buffer.obj = NULL;
    net.last_buffer.len = range.size() * sizeof(element_at);
    net.last_buffer.itemsize = sizeof(element_at);
    // https://docs.python.org/3/library/struct.html#format-characters
    net.last_buffer.format = (char*)format_code_gt<std::remove_const_t<element_at>>::format_k;
    net.last_buffer.ndim = 1;
    net.last_buffer.shape = &net.last_buffer_shape[0];
    net.last_buffer.strides = &net.last_buffer_strides[0];
    net.last_buffer.suboffsets = nullptr;
    net.last_buffer.readonly = true;
    net.last_buffer.internal = nullptr;
    return PyMemoryView_FromBuffer(&net.last_buffer);
}

void ukv::wrap_network(py::module& m) {

    auto degs = py::class_<degree_view_t, std::shared_ptr<degree_view_t>>(m, "DegreeView", py::module_local());
    degs.def("__getitem__", [](degree_view_t& degs, ukv_key_t v) {
        network_t& net = *degs.net_ptr;
        auto maybe = net.graph.degree(v, degs.roles);
        maybe.throw_unhandled();
        ukv_vertex_degree_t result = *maybe;
        return result;
    });
    degs.def("__getitem__", [](degree_view_t& degs, py::handle vs) {
        network_t& net = *degs.net_ptr;
        auto handle_and_ids = strided_array<ukv_key_t const>(vs);
        auto maybe = net.graph.degrees(handle_and_ids.second, degs.roles);
        maybe.throw_unhandled();
        return wrap_into_buffer<ukv_vertex_degree_t const>(net, {maybe->begin(), maybe->end()});
    });

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
                collection_t& col = index_collection->native;
                ukv_txn_t txn_raw = index_collection->txn_ptr ? index_collection->txn_ptr->native : ukv_txn_t(nullptr);

                graph_t g(col, txn_raw);
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

    // Counting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/graph.html#counting-nodes-edges-and-neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#counting-nodes-edges-and-neighbors
    net.def(
        "order",
        [](network_t& net, ukv_key_t v) {
            auto maybe = net.graph.collection().size();
            maybe.throw_unhandled();
            return *maybe;
        },
        "Returns the number of nodes in the graph.");
    net.def(
        "number_of_nodes",
        [](network_t& net, ukv_key_t v) {
            auto maybe = net.graph.collection().size();
            maybe.throw_unhandled();
            return *maybe;
        },
        "Returns the number of nodes in the graph.");
    net.def(
        "__len__",
        [](network_t& net, ukv_key_t v) {
            auto maybe = net.graph.collection().size();
            maybe.throw_unhandled();
            return *maybe;
        },
        "Returns the number of nodes in the graph.");
    net.def_property_readonly(
        "degree",
        [](network_t& net) {
            auto degs_ptr = std::make_shared<degree_view_t>();
            degs_ptr->net_ptr = net.shared_from_this();
            degs_ptr->roles = ukv_vertex_role_any_k;
            return degs_ptr;
        },
        "A DegreeView for the graph.");
    net.def_property_readonly(
        "in_degree",
        [](network_t& net) {
            auto degs_ptr = std::make_shared<degree_view_t>();
            degs_ptr->net_ptr = net.shared_from_this();
            degs_ptr->roles = ukv_vertex_target_k;
            return degs_ptr;
        },
        "A DegreeView with the number incoming edges for each Vertex.");
    net.def_property_readonly(
        "out_degree",
        [](network_t& net) {
            auto degs_ptr = std::make_shared<degree_view_t>();
            degs_ptr->net_ptr = net.shared_from_this();
            degs_ptr->roles = ukv_vertex_source_k;
            return degs_ptr;
        },
        "A DegreeView with the number outgoing edges for each Vertex.");
    net.def(
        "size",
        [](network_t& net) {
            auto maybe = net.relations_attrs.size();
            maybe.throw_unhandled();
            return *maybe;
        },
        "Returns the number of attributed edges.");
    net.def(
        "number_of_edges",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
            auto maybe = net.graph.edges(v1, v2);
            maybe.throw_unhandled();
            return maybe->size();
        },
        "Returns the number of edges between two nodes.");

    // Reporting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#reporting-nodes-edges-and-neighbors
    net.def(
        "nodes",
        [](network_t& net) { throw_not_implemented(); },
        "A NodeView of the graph.");
    net.def(
        "__iter__",
        [](network_t& net) { throw_not_implemented(); },
        "Iterate over the nodes.");
    net.def(
        "has_node",
        [](network_t& net, ukv_key_t v) {
            auto maybe = net.graph.contains(v);
            maybe.throw_unhandled();
            return *maybe;
        },
        py::arg("n"),
        "Returns True if the graph contains the node n.");
    net.def(
        "__contains__",
        [](network_t& net, ukv_key_t v) {
            auto maybe = net.graph.contains(v);
            maybe.throw_unhandled();
            return *maybe;
        },
        py::arg("n"),
        "Returns True if the graph contains the node n.");

    net.def("edges", [](network_t& net) { throw_not_implemented(); });
    net.def("out_edges", [](network_t& net) { throw_not_implemented(); });
    net.def("in_edges", [](network_t& net) { throw_not_implemented(); });

    net.def(
        "has_edge",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
            auto maybe = net.graph.edges(v1, v2);
            maybe.throw_unhandled();
            return maybe->size() != 0;
        },
        py::arg("u"),
        py::arg("v"));
    net.def(
        "has_edge",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
            auto maybe = net.graph.edges(v1, v2);
            maybe.throw_unhandled();
            return std::find(maybe->edge_ids.begin(), maybe->edge_ids.end(), eid) != maybe->edge_ids.end();
        },
        py::arg("u"),
        py::arg("v"),
        py::arg("key"));
    net.def(
        "get_edge_data",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2) { throw_not_implemented(); },
        py::arg("u"),
        py::arg("v"));

    net.def(
        "neighbors",
        [](network_t& net, ukv_key_t n) {
            // Retrieving neighbors is trickier than just `successors` or `predecessors`.
            // We are receiving an adjacency list, where both incoming an edges exist.
            // So the stride/offset is not uniform across the entire list.
            auto maybe = net.graph.edges(n, ukv_vertex_role_any_k);
            maybe.throw_unhandled();

            // We can gobble the contents a little bit by swapping the members of some
            // edges to make it uniform.
            auto edges = *maybe;
            auto count = edges.size();
            for (std::size_t i = 0; i != count; ++i) {
                ukv_key_t& u = const_cast<ukv_key_t&>(edges.source_ids[i]);
                ukv_key_t& v = const_cast<ukv_key_t&>(edges.target_ids[i]);
                if (u == n)
                    std::swap(u, v);
            }

            return wrap_into_buffer(net, edges.target_ids);
        },
        py::arg("n"),
        "Returns an iterable of incoming and outgoing nodes of n. Potentially with duplicates.");
    net.def(
        "successors",
        [](network_t& net, ukv_key_t n) {
            auto maybe = net.graph.edges(n, ukv_vertex_source_k);
            maybe.throw_unhandled();
            return wrap_into_buffer(net, maybe->target_ids);
        },
        py::arg("n"),
        "Returns an iterable of successor nodes of n.");
    net.def(
        "predecessors",
        [](network_t& net, ukv_key_t n) {
            auto maybe = net.graph.edges(n, ukv_vertex_target_k);
            maybe.throw_unhandled();
            return wrap_into_buffer(net, maybe->source_ids);
        },
        py::arg("n"),
        "Returns an iterable of follower nodes of n.");
    net.def(
        "nbunch_iter",
        [](network_t& net, py::handle const& vs) {
            auto handle_and_ids = strided_array<ukv_key_t const>(vs);
            auto maybe = net.graph.contains(handle_and_ids.second);
            maybe.throw_unhandled();
            return wrap_into_buffer(net, *maybe);
        },
        "Checks given nodes against graph members and returns a filtered iterable object");

    // Adding and Removing Nodes and Edges
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#adding-and-removing-nodes-and-edges
    net.def(
        "add_edge",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
            edges_view_t edges {
                strided_range_gt<ukv_key_t const>(v1),
                strided_range_gt<ukv_key_t const>(v2),
            };
            net.graph.upsert(edges).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"));
    net.def(
        "add_edge",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
            edges_view_t edges {
                strided_range_gt<ukv_key_t const>(v1),
                strided_range_gt<ukv_key_t const>(v2),
                strided_range_gt<ukv_key_t const>(eid),
            };
            net.graph.upsert(edges).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"),
        py::arg("key"));
    net.def(
        "remove_edge",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2) {
            edges_view_t edges {
                strided_range_gt<ukv_key_t const>(v1),
                strided_range_gt<ukv_key_t const>(v2),
            };
            net.graph.remove(edges).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"));
    net.def(
        "remove_edge",
        [](network_t& net, ukv_key_t v1, ukv_key_t v2, ukv_key_t eid) {
            edges_view_t edges {
                strided_range_gt<ukv_key_t const>(v1),
                strided_range_gt<ukv_key_t const>(v2),
                strided_range_gt<ukv_key_t const>(eid),
            };
            net.graph.remove(edges).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"),
        py::arg("key"));
    net.def(
        "add_edges_from",
        [](network_t& net, py::handle const& adjacency_list) {
            auto handle_and_list = strided_matrix<ukv_key_t const>(adjacency_list);
            if (handle_and_list.second.cols() != 2 || handle_and_list.second.cols() != 3)
                throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

            edges_view_t edges {
                handle_and_list.second.col(0),
                handle_and_list.second.col(1),
                handle_and_list.second.cols() == 3 ? handle_and_list.second.col(2)
                                                   : strided_range_gt<ukv_key_t const>(ukv_default_edge_id_k),
            };
            net.graph.upsert(edges).throw_unhandled();
        },
        py::arg("ebunch_to_add"),
        "Adds an adjacency list (in a form of 2 or 3 columnar matrix) to the graph.");
    net.def(
        "remove_edges_from",
        [](network_t& net, py::handle const& adjacency_list) {
            auto handle_and_list = strided_matrix<ukv_key_t const>(adjacency_list);
            if (handle_and_list.second.cols() != 2 || handle_and_list.second.cols() != 3)
                throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

            edges_view_t edges {
                handle_and_list.second.col(0),
                handle_and_list.second.col(1),
                handle_and_list.second.cols() == 3 ? handle_and_list.second.col(2)
                                                   : strided_range_gt<ukv_key_t const>(ukv_default_edge_id_k),
            };
            net.graph.remove(edges).throw_unhandled();
        },
        py::arg("ebunch"),
        "Removes all edges in supplied adjacency list (in a form of 2 or 3 columnar matrix) from the graph.");

    net.def(
        "add_edges_from",
        [](network_t& net, py::handle const& v1s, py::handle const& v2s) {
            auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
            auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
            edges_view_t edges {
                handle_and_sources.second,
                handle_and_targets.second,
            };
            net.graph.upsert(edges).throw_unhandled();
        },
        py::arg("us"),
        py::arg("vs"),
        "Adds edges from members of the first array to members of the second array.");
    net.def(
        "remove_edges_from",
        [](network_t& net, py::handle const& v1s, py::handle const& v2s) {
            auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
            auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
            edges_view_t edges {
                handle_and_sources.second,
                handle_and_targets.second,
            };
            net.graph.remove(edges).throw_unhandled();
        },
        py::arg("us"),
        py::arg("vs"),
        "Removes edges from members of the first array to members of the second array.");

    net.def(
        "add_edges_from",
        [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {
            auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
            auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
            auto handle_and_edge_ids = strided_array<ukv_key_t const>(eids);
            edges_view_t edges {
                handle_and_sources.second,
                handle_and_targets.second,
                handle_and_edge_ids.second,
            };
            net.graph.upsert(edges).throw_unhandled();
        },
        py::arg("us"),
        py::arg("vs"),
        py::arg("keys"),
        "Adds edges from members of the first array to members of the second array using keys from the third array.");
    net.def(
        "remove_edges_from",
        [](network_t& net, py::handle const& v1s, py::handle const& v2s, py::handle const& eids) {
            auto handle_and_sources = strided_array<ukv_key_t const>(v1s);
            auto handle_and_targets = strided_array<ukv_key_t const>(v2s);
            auto handle_and_edge_ids = strided_array<ukv_key_t const>(eids);
            edges_view_t edges {
                handle_and_sources.second,
                handle_and_targets.second,
                handle_and_edge_ids.second,
            };
            net.graph.remove(edges).throw_unhandled();
        },
        py::arg("us"),
        py::arg("vs"),
        py::arg("keys"),
        "Removes edges from members of the first array to members of the second array using keys from the third "
        "array.");
    net.def("clear_edges", [](network_t& net) { throw_not_implemented(); });
    net.def(
        "clear",
        [](network_t& net) {
            // db_t& db = net.db_ptr->native;
            // db.clear(net.graph.collection());
            // db.clear(net.sources_attrs);
            // db.clear(net.targets_attrs);
            // db.clear(net.relations_attrs);
            // throw_not_implemented();
        },
        "Removes both vertices and edges from the graph.");

    // Making copies and subgraphs
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#making-copies-and-subgraphs
    net.def("copy", [](network_t& net) { throw_not_implemented(); });
    net.def("to_directed", [](network_t& net) { throw_not_implemented(); });
    net.def("to_undirected", [](network_t& net) { throw_not_implemented(); });
    net.def("reverse", [](network_t& net) { throw_not_implemented(); });
    net.def("subgraph", [](network_t& net) { throw_not_implemented(); });
    net.def("edge_subgraph", [](network_t& net) { throw_not_implemented(); });
    net.def(
        "subgraph",
        [](network_t& net, py::handle ns) { throw_not_implemented(); },
        "Returns a subgraph in a form of an adjacency list with 3 columns, where every edge (row) "
        "contains at least one vertex from the supplied list. Some edges may be duplicated.");
    net.def(
        "subgraph",
        [](network_t& net, ukv_key_t n, std::size_t hops) { throw_not_implemented(); },
        "Returns a subgraph in a form of an adjacency list with 3 columns, where every edge (row) "
        "contains at least one vertex from the supplied list at a distance withing a given number "
        "`hops` from the supplied `n`.");

    // Free-standing Functions and Properties
    // https://networkx.org/documentation/stable/reference/functions.html#graph
    // https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.density.html
    // https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.is_directed.html?highlight=is_directed
    net.def_property_readonly("is_directed", [](network_t& net) { return net.is_directed_; });
    net.def_property_readonly("is_multi", [](network_t& net) { return net.is_multi_; });
    net.def_property_readonly("allows_loops", [](network_t& net) { return net.allow_self_loops_; });
    m.def("is_directed", [](network_t& net) { return net.is_directed_; });
    m.def("is_multi", [](network_t& net) { return net.is_multi_; });
    m.def("allows_loops", [](network_t& net) { return net.allow_self_loops_; });
    m.def("density", [](network_t& net) {
        throw_not_implemented();
        return 0.0;
    });

    // Reading and Writing Graphs
    // https://networkx.org/documentation/stable/reference/readwrite/
    // https://networkx.org/documentation/stable/reference/readwrite/adjlist.html
    // https://networkx.org/documentation/stable/reference/readwrite/json_graph.html
    m.def(
        "write_adjlist",
        [](network_t& net,
           std::string const& path,
           std::string const& comments,
           std::string const& delimiter,
           std::string const& encoding) { return; },
        py::arg("G"),
        py::arg("path"),
        py::arg("comments") = "#",
        py::arg("delimiter") = " ",
        py::arg("encoding") = "utf-8");
}
