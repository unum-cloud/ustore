#include "pybind.hpp"
#include "crud.hpp"
#include "cast.hpp"

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

struct degree_view_t : public std::enable_shared_from_this<degree_view_t> {
    std::shared_ptr<py_graph_t> net_ptr;
    ukv_vertex_role_t roles = ukv_vertex_role_any_k;
};

template <typename element_at>
py::object wrap_into_buffer(py_graph_t& g, strided_range_gt<element_at> range) {

    g.last_buffer_strides[0] = range.stride();
    g.last_buffer_strides[1] = g.last_buffer_strides[2] = 1;
    g.last_buffer_shape[0] = range.size();
    g.last_buffer_shape[1] = g.last_buffer_shape[2] = 1;

    // https://docs.python.org/3/c-api/buffer.html
    g.last_buffer.buf = (void*)range.begin().get();
    g.last_buffer.obj = NULL;
    g.last_buffer.len = range.size() * sizeof(element_at);
    g.last_buffer.itemsize = sizeof(element_at);
    // https://docs.python.org/3/library/struct.html#format-characters
    g.last_buffer.format = (char*)&format_code_gt<std::remove_const_t<element_at>>::value[0];
    g.last_buffer.ndim = 1;
    g.last_buffer.shape = &g.last_buffer_shape[0];
    g.last_buffer.strides = &g.last_buffer_strides[0];
    g.last_buffer.suboffsets = nullptr;
    g.last_buffer.readonly = true;
    g.last_buffer.internal = nullptr;
    PyObject* obj = PyMemoryView_FromBuffer(&g.last_buffer);
    return py::reinterpret_steal<py::object>(obj);
}

template <typename edges_view_callback_at>
void adjacency_list_to_edges(PyObject* adjacency_list, edges_view_callback_at call) {

    // Check if we can do zero-copy
    if (PyObject_CheckBuffer(adjacency_list)) {
        py_buffer_t buf = py_buffer(adjacency_list);
        if (!can_cast_internal_scalars<ukv_key_t>(buf))
            throw std::invalid_argument("Expecting `ukv_key_t` scalars in zero-copy interface");
        auto mat = py_strided_matrix<ukv_key_t const>(buf);
        auto cols = mat.cols();
        if (cols != 2 && cols != 3)
            throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

        edges_view_t edges_view {
            mat.col(0),
            mat.col(1),
            cols == 3 ? mat.col(2) : strided_range_gt<ukv_key_t const>(&ukv_default_edge_id_k, 0, mat.rows()),
        };
        call(edges_view);
    }
    // Otherwise, we expect a sequence of 2-tuples or 3-tuples
    else {
        std::vector<edge_t> edges_vec;
        auto adj_len = py_sequence_length(adjacency_list);
        if (adj_len)
            edges_vec.reserve(*adj_len);

        auto to_edge = [](PyObject* obj) -> edge_t {
            if (!PyTuple_Check(obj))
                throw std::invalid_argument("Each edge must be represented by a tuple");
            auto cols = PyTuple_Size(obj);
            if (cols != 2 && cols != 3)
                throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

            edge_t result;
            result.source_id = py_to_scalar<ukv_key_t>(PyTuple_GetItem(obj, 0));
            result.target_id = py_to_scalar<ukv_key_t>(PyTuple_GetItem(obj, 1));
            result.id = cols == 3 ? py_to_scalar<ukv_key_t>(PyTuple_GetItem(obj, 2)) : ukv_default_edge_id_k;
            return result;
        };
        py_transform_n(adjacency_list, to_edge, std::back_inserter(edges_vec));
        call(edges(edges_vec));
    }
}

template <typename edges_view_callback_at>
void adjacency_list_to_edges(PyObject* source_ids,
                             PyObject* target_ids,
                             PyObject* edge_ids,
                             edges_view_callback_at call) {
    //
    auto source_ids_is_buf = PyObject_CheckBuffer(source_ids);
    auto target_ids_is_buf = PyObject_CheckBuffer(target_ids);
    auto edge_ids_is_buf = PyObject_CheckBuffer(edge_ids);
    auto all_same = source_ids_is_buf == target_ids_is_buf;
    if (edge_ids != Py_None)
        all_same &= source_ids_is_buf = edge_ids_is_buf;

    // Check if we can do zero-copy
    if (source_ids_is_buf) {
        if (!all_same)
            throw std::invalid_argument("Expecting `ukv_key_t` scalars in zero-copy interface");

        auto sources_handle = py_buffer(source_ids);
        auto sources = py_strided_range<ukv_key_t const>(sources_handle);
        auto targets_handle = py_buffer(target_ids);
        auto targets = py_strided_range<ukv_key_t const>(targets_handle);
        if (edge_ids != Py_None) {
            auto edge_ids_handle = py_buffer(edge_ids);
            auto edge_ids = py_strided_range<ukv_key_t const>(edge_ids_handle);
            edges_view_t edges_view {sources, targets, edge_ids};
            call(edges_view);
        }
        else {
            edges_view_t edges_view {sources, targets};
            call(edges_view);
        }
    }
    // Otherwise, we expect a sequence of 2-tuples or 3-tuples
    else {
        auto sources_n = py_sequence_length(source_ids);
        auto targets_n = py_sequence_length(target_ids);
        if (!sources_n || !targets_n || sources_n != targets_n)
            throw std::invalid_argument("Sequence lengths must match");

        auto n = *sources_n;
        std::vector<edge_t> edges_vec(n);
        edges_span_t edges_span = edges(edges_vec);

        py_transform_n(source_ids, &py_to_scalar<ukv_key_t>, edges_span.source_ids.begin(), n);
        py_transform_n(target_ids, &py_to_scalar<ukv_key_t>, edges_span.target_ids.begin(), n);
        if (edge_ids != Py_None)
            py_transform_n(edge_ids, &py_to_scalar<ukv_key_t>, edges_span.edge_ids.begin(), n);

        call(edges_span);
    }
}

void ukv::wrap_networkx(py::module& m) {

    auto degs = py::class_<degree_view_t, std::shared_ptr<degree_view_t>>(m, "DegreeView", py::module_local());
    degs.def("__getitem__", [](degree_view_t& degs, ukv_key_t v) {
        py_graph_t& g = *degs.net_ptr;
        auto maybe = g.ref().degree(v, degs.roles);
        maybe.throw_unhandled();
        ukv_vertex_degree_t result = *maybe;
        return result;
    });
    degs.def("__getitem__", [](degree_view_t& degs, PyObject* vs) {
        py_graph_t& g = *degs.net_ptr;
        auto ids_handle = py_buffer(vs);
        auto ids = py_strided_range<ukv_key_t const>(ids_handle);
        auto maybe = g.ref().degrees(ids, {&degs.roles});
        maybe.throw_unhandled();
        return wrap_into_buffer<ukv_vertex_degree_t const>(g, {maybe->begin(), maybe->end()});
    });

    auto g = py::class_<py_graph_t, std::shared_ptr<py_graph_t>>(m, "Network", py::module_local());
    g.def( //
        py::init([](std::shared_ptr<py_db_t> py_db,
                    std::optional<std::string> index,
                    std::optional<std::string> sources_attrs,
                    std::optional<std::string> targets_attrs,
                    std::optional<std::string> relations_attrs,
                    bool directed = false,
                    bool multi = false,
                    bool loops = false) {
            //
            if (!py_db)
                return std::shared_ptr<py_graph_t> {};

            auto net_ptr = std::make_shared<py_graph_t>();
            net_ptr->db_ptr = py_db;
            net_ptr->is_directed_ = directed;
            net_ptr->is_multi_ = multi;
            net_ptr->allow_self_loops_ = loops;

            // Attach the primary collection
            db_t& db = py_db->native;
            {
                auto col = db.collection(index ? index->c_str() : "");
                col.throw_unhandled();
                net_ptr->index = *std::move(col);
            }
            // Attach the additional collections
            if (sources_attrs) {
                auto col = db.collection(sources_attrs->c_str());
                col.throw_unhandled();
                net_ptr->sources_attrs = *std::move(col);
            }
            if (targets_attrs) {
                auto col = db.collection(targets_attrs->c_str());
                col.throw_unhandled();
                net_ptr->targets_attrs = *std::move(col);
            }
            if (relations_attrs) {
                auto col = db.collection(relations_attrs->c_str());
                col.throw_unhandled();
                net_ptr->relations_attrs = *std::move(col);
            }
            return net_ptr;
        }),
        py::arg("db"),
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
    g.def(
        "order",
        [](py_graph_t& g, ukv_key_t v) { return g.index.size(); },
        "Returns the number of nodes in the graph.");
    g.def(
        "number_of_nodes",
        [](py_graph_t& g, ukv_key_t v) { return g.index.size(); },
        "Returns the number of nodes in the graph.");
    g.def(
        "__len__",
        [](py_graph_t& g, ukv_key_t v) { return g.index.size(); },
        "Returns the number of nodes in the graph.");
    g.def_property_readonly(
        "degree",
        [](py_graph_t& g) {
            auto degs_ptr = std::make_shared<degree_view_t>();
            degs_ptr->net_ptr = g.shared_from_this();
            degs_ptr->roles = ukv_vertex_role_any_k;
            return degs_ptr;
        },
        "A DegreeView for the graph.");
    g.def_property_readonly(
        "in_degree",
        [](py_graph_t& g) {
            auto degs_ptr = std::make_shared<degree_view_t>();
            degs_ptr->net_ptr = g.shared_from_this();
            degs_ptr->roles = ukv_vertex_target_k;
            return degs_ptr;
        },
        "A DegreeView with the number incoming edges for each Vertex.");
    g.def_property_readonly(
        "out_degree",
        [](py_graph_t& g) {
            auto degs_ptr = std::make_shared<degree_view_t>();
            degs_ptr->net_ptr = g.shared_from_this();
            degs_ptr->roles = ukv_vertex_source_k;
            return degs_ptr;
        },
        "A DegreeView with the number outgoing edges for each Vertex.");
    g.def(
        "size",
        [](py_graph_t& g) { return g.relations_attrs.size(); },
        "Returns the number of attributed edges.");
    g.def(
        "number_of_edges",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) { return g.ref().edges(v1, v2).throw_or_ref().size(); },
        "Returns the number of edges between two nodes.");

    // Reporting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#reporting-nodes-edges-and-neighbors
    g.def(
        "nodes",
        [](py_graph_t& g) {
            // TODO: Give our the keys stream
            throw_not_implemented();
        },
        "A NodeView of the graph.");
    g.def(
        "__iter__",
        [](py_graph_t& g) { throw_not_implemented(); },
        "Iterate over the nodes.");
    g.def(
        "has_node",
        [](py_graph_t& g, ukv_key_t v) {
            auto maybe = g.ref().contains(v);
            maybe.throw_unhandled();
            return *maybe;
        },
        py::arg("n"),
        "Returns True if the graph contains the node n.");
    g.def(
        "__contains__",
        [](py_graph_t& g, ukv_key_t v) { return g.ref().contains(v).throw_or_ref(); },
        py::arg("n"),
        "Returns True if the graph contains the node n.");

    g.def("edges", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("out_edges", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("in_edges", [](py_graph_t& g) { throw_not_implemented(); });

    g.def(
        "has_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) { return g.ref().edges(v1, v2).throw_or_ref().size() != 0; },
        py::arg("u"),
        py::arg("v"));
    g.def(
        "has_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2, ukv_key_t e) {
            auto ids = g.ref().edges(v1, v2).throw_or_ref().edge_ids;
            return std::find(ids.begin(), ids.end(), e) != ids.end();
        },
        py::arg("u"),
        py::arg("v"),
        py::arg("key"));
    g.def(
        "get_edge_data",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) { throw_not_implemented(); },
        py::arg("u"),
        py::arg("v"));

    g.def(
        "neighbors",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().neighbors(n).throw_or_ref()); },
        py::arg("n"),
        "Returns an iterable of incoming and outgoing nodes of n. Potentially with duplicates.");
    g.def(
        "successors",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().successors(n).throw_or_ref()); },
        py::arg("n"),
        "Returns an iterable of successor nodes of n.");
    g.def(
        "predecessors",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().predecessors(n).throw_or_ref()); },
        py::arg("n"),
        "Returns an iterable of follower nodes of n.");
    g.def(
        "nbunch_iter",
        [](py_graph_t& g, PyObject* vs) {
            auto ids_handle = py_buffer(vs);
            auto ids = py_strided_range<ukv_key_t const>(ids_handle);
            auto maybe = g.ref().contains(ids);
            maybe.throw_unhandled();
            return wrap_into_buffer(g, *maybe);
        },
        "Checks given nodes against graph members and returns a filtered iterable object");

    // Adding and Removing Nodes and Edges
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#adding-and-removing-nodes-and-edges
    g.def(
        "add_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) {
            g.ref().upsert(edge_t {v1, v2}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"));
    g.def(
        "add_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2, ukv_key_t e) {
            g.ref().upsert(edge_t {v1, v2, e}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"),
        py::arg("key"));
    g.def(
        "remove_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) {
            g.ref().remove(edge_t {v1, v2}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"));
    g.def(
        "remove_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2, ukv_key_t e) {
            g.ref().remove(edge_t {v1, v2, e}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"),
        py::arg("key"));
    g.def(
        "add_edges_from",
        [](py_graph_t& g, py::object adjacency_list) {
            adjacency_list_to_edges(adjacency_list.ptr(),
                                    [&](edges_view_t edges) { g.ref().upsert(edges).throw_unhandled(); });
        },
        py::arg("ebunch_to_add"),
        "Adds an adjacency list (in a form of 2 or 3 columnar matrix) to the graph.");
    g.def(
        "remove_edges_from",
        [](py_graph_t& g, py::object adjacency_list) {
            adjacency_list_to_edges(adjacency_list.ptr(),
                                    [&](edges_view_t edges) { g.ref().remove(edges).throw_unhandled(); });
        },
        py::arg("ebunch"),
        "Removes all edges in supplied adjacency list (in a form of 2 or 3 columnar matrix) from the graph.");

    g.def(
        "add_edges_from",
        [](py_graph_t& g, py::object v1s, py::object v2s, py::object es) {
            adjacency_list_to_edges(v1s.ptr(), v2s.ptr(), es.ptr(), [&](edges_view_t edges) {
                g.ref().upsert(edges).throw_unhandled();
            });
        },
        py::arg("us"),
        py::arg("vs"),
        py::arg("keys") = nullptr,
        "Adds edges from members of the first array to members of the second array.");
    g.def(
        "remove_edges_from",
        [](py_graph_t& g, py::object v1s, py::object v2s, py::object es) {
            adjacency_list_to_edges(v1s.ptr(), v2s.ptr(), es.ptr(), [&](edges_view_t edges) {
                g.ref().remove(edges).throw_unhandled();
            });
        },
        py::arg("us"),
        py::arg("vs"),
        py::arg("keys") = nullptr,
        "Removes edges from members of the first array to members of the second array.");

    g.def("clear_edges", [](py_graph_t& g) { throw_not_implemented(); });
    g.def(
        "clear",
        [](py_graph_t& g) {
            // db_t& db = g.db_ptr->native;
            // db.clear(g.index);
            // db.clear(g.sources_attrs);
            // db.clear(g.targets_attrs);
            // db.clear(g.relations_attrs);
            // throw_not_implemented();
        },
        "Removes both vertices and edges from the graph.");

    // Making copies and subgraphs
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#making-copies-and-subgraphs
    g.def("copy", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("to_directed", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("to_undirected", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("reverse", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("subgraph", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("edge_subgraph", [](py_graph_t& g) { throw_not_implemented(); });
    g.def(
        "subgraph",
        [](py_graph_t& g, py::object ns) { throw_not_implemented(); },
        "Returns a subgraph in a form of an adjacency list with 3 columns, where every edge (row) "
        "contains at least one vertex from the supplied list. Some edges may be duplicated.");
    g.def(
        "subgraph",
        [](py_graph_t& g, ukv_key_t n, std::size_t hops) { throw_not_implemented(); },
        "Returns a subgraph in a form of an adjacency list with 3 columns, where every edge (row) "
        "contains at least one vertex from the supplied list at a distance withing a given number "
        "`hops` from the supplied `n`.");

    // Free-standing Functions and Properties
    // https://networkx.org/documentation/stable/reference/functions.html#graph
    // https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.density.html
    // https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.is_directed.html?highlight=is_directed
    g.def_property_readonly("is_directed", [](py_graph_t& g) { return g.is_directed_; });
    g.def_property_readonly("is_multi", [](py_graph_t& g) { return g.is_multi_; });
    g.def_property_readonly("allows_loops", [](py_graph_t& g) { return g.allow_self_loops_; });
    m.def("is_directed", [](py_graph_t& g) { return g.is_directed_; });
    m.def("is_multi", [](py_graph_t& g) { return g.is_multi_; });
    m.def("allows_loops", [](py_graph_t& g) { return g.allow_self_loops_; });
    m.def("density", [](py_graph_t& g) {
        throw_not_implemented();
        return 0.0;
    });

    // Reading and Writing Graphs
    // https://networkx.org/documentation/stable/reference/readwrite/
    // https://networkx.org/documentation/stable/reference/readwrite/adjlist.html
    // https://networkx.org/documentation/stable/reference/readwrite/json_graph.html
    m.def(
        "write_adjlist",
        [](py_graph_t& g,
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
