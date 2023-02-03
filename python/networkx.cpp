#include <charconv>

#include "pybind.hpp"
#include "crud.hpp"
#include "nlohmann.hpp"
#include "cast_args.hpp"
#include "algorithms/louvain.cpp"

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

struct nodes_stream_t {
    keys_stream_t native;
    docs_collection_t& collection;
    bool read_data;
    ukv_str_view_t field;
    ukv_str_view_t default_value;

    embedded_blobs_t attrs;
    ptr_range_gt<ukv_key_t const> nodes;
    std::size_t index = 0;

    nodes_stream_t(
        keys_stream_t&& stream, docs_collection_t& col, bool data, ukv_str_view_t field, ukv_str_view_t default_value)
        : native(std::move(stream)), collection(col), read_data(data), field(field), default_value(default_value) {
        nodes = native.keys_batch();
        read_attributes();
    }

    void read_attributes() {
        if (!read_data)
            return;

        status_t status;
        ukv_length_t* found_offsets = nullptr;
        ukv_length_t* found_lengths = nullptr;
        ukv_bytes_ptr_t found_values = nullptr;
        auto count = static_cast<ukv_size_t>(nodes.size());

        ukv_docs_read_t docs_read {
            .db = collection.db(),
            .error = status.member_ptr(),
            .transaction = collection.txn(),
            .arena = collection.member_arena(),
            .type = ukv_doc_field_json_k,
            .tasks_count = count,
            .collections = collection.member_ptr(),
            .keys = nodes.begin(),
            .keys_stride = sizeof(ukv_key_t),
            .fields = &field,
            .offsets = &found_offsets,
            .lengths = &found_lengths,
            .values = &found_values,
        };

        ukv_docs_read(&docs_read);
        status.throw_unhandled();
        attrs = embedded_blobs_t {count, found_offsets, found_lengths, found_values};
    }

    void next_batch() {
        native.seek_to_next_batch();
        nodes = native.keys_batch();
        read_attributes();
        index = 0;
    }

    ukv_key_t key() { return nodes[index]; }
    value_view_t data() { return attrs[index] && !attrs[index].empty() ? attrs[index] : default_value; }
};

struct nodes_range_t {
    keys_range_t native;
    docs_collection_t& collection;
    bool read_data = false;
    std::string field;
    std::string default_value;
};

struct edges_stream_t {
    graph_stream_t native;
    docs_collection_t& collection;
    bool read_data;
    ukv_str_view_t field;
    ukv_str_view_t default_value;

    embedded_blobs_t attrs;
    edges_span_t edges;
    std::size_t index = 0;

    edges_stream_t(
        graph_stream_t&& stream, docs_collection_t& col, bool data, ukv_str_view_t field, ukv_str_view_t default_value)
        : native(std::move(stream)), collection(col), read_data(data), field(field), default_value(default_value) {
        edges = native.edges_batch();
        read_attributes();
    }

    void read_attributes() {
        if (!read_data)
            return;

        status_t status;
        ukv_length_t* found_offsets = nullptr;
        ukv_length_t* found_lengths = nullptr;
        ukv_bytes_ptr_t found_values = nullptr;
        auto count = static_cast<ukv_size_t>(edges.size());

        ukv_docs_read_t docs_read {
            .db = collection.db(),
            .error = status.member_ptr(),
            .transaction = collection.txn(),
            .arena = collection.member_arena(),
            .type = ukv_doc_field_json_k,
            .tasks_count = count,
            .collections = collection.member_ptr(),
            .keys = edges.edge_ids.data(),
            .keys_stride = edges.edge_ids.stride(),
            .fields = &field,
            .offsets = &found_offsets,
            .lengths = &found_lengths,
            .values = &found_values,
        };

        ukv_docs_read(&docs_read);
        status.throw_unhandled();
        attrs = embedded_blobs_t {count, found_offsets, found_lengths, found_values};
    }

    void next_batch() {
        native.seek_to_next_batch();
        edges = native.edges_batch();
        read_attributes();
        index = 0;
    }

    edge_t edge() { return edges[index]; }
    value_view_t data() { return attrs[index] && !attrs[index].empty() ? attrs[index] : default_value; }
};

struct edges_range_t {
    range_gt<graph_stream_t> native;
    docs_collection_t& collection;
    bool read_data = false;
    std::string field;
    std::string default_value;
};

void compute_degrees(py_graph_t& graph,
                     strided_range_gt<ukv_key_t const> vertices,
                     ukv_vertex_role_t role,
                     ukv_str_view_t weight,
                     ukv_vertex_degree_t** degrees) {

    status_t status;
    ukv_key_t* edges_per_vertex = nullptr;
    auto count = static_cast<ukv_size_t>(vertices.size());

    ukv_graph_find_edges_t graph_find_edges {
        .db = graph.index.db(),
        .error = status.member_ptr(),
        .transaction = graph.index.txn(),
        .arena = graph.index.member_arena(),
        .tasks_count = count,
        .collections = graph.index.member_ptr(),
        .vertices = vertices.begin().get(),
        .vertices_stride = vertices.stride(),
        .roles = &role,
        .degrees_per_vertex = degrees,
        .edges_per_vertex = &edges_per_vertex,
    };

    ukv_graph_find_edges(&graph_find_edges);
    status.throw_unhandled();
    if (!weight)
        return;

    auto edges_begin = reinterpret_cast<edge_t*>(edges_per_vertex);
    auto all_edges_count = transform_reduce_n(*degrees, count, 0ul, [](ukv_vertex_degree_t deg) {
        return deg == ukv_vertex_degree_missing_k ? 0 : deg;
    });
    auto edges = edges_span_t {edges_begin, edges_begin + all_edges_count};

    ukv_length_t* found_offsets = nullptr;
    ukv_length_t* found_lengths = nullptr;
    ukv_bytes_ptr_t found_values = nullptr;

    ukv_docs_read_t docs_read {
        .db = graph.relations_attrs.db(),
        .error = status.member_ptr(),
        .transaction = graph.relations_attrs.txn(),
        .arena = graph.relations_attrs.member_arena(),
        .type = ukv_doc_field_json_k,
        .tasks_count = count,
        .collections = graph.relations_attrs.member_ptr(),
        .keys = edges.edge_ids.data(),
        .keys_stride = edges.edge_ids.stride(),
        .fields = &weight,
        .fields_stride = 0,
        .offsets = &found_offsets,
        .lengths = &found_lengths,
        .values = &found_values,
    };

    ukv_docs_read(&docs_read);
    status.throw_unhandled();
    auto values = embedded_blobs_t {count, found_offsets, found_lengths, found_values};

    auto value_iterator = values.begin();
    for (std::size_t i = 0; i != count; ++i) {
        auto edges_count = (*degrees)[i];
        (*degrees)[i] = transform_reduce_n(value_iterator, edges_count, 0ul, [](value_view_t edge_weight) {
            ukv_vertex_degree_t weight;
            std::from_chars(edge_weight.c_str(), edge_weight.c_str() + edge_weight.size(), weight);
            return weight;
        });
    }
}

struct degrees_stream_t {
    keys_stream_t keys_stream;
    py_graph_t& graph;
    ukv_str_view_t weight_field;
    ukv_vertex_role_t vertex_role;

    ptr_range_gt<ukv_key_t const> fetched_nodes;
    ukv_vertex_degree_t* degrees = nullptr;
    std::size_t index = 0;

    degrees_stream_t(keys_stream_t&& stream, py_graph_t& net, ukv_str_view_t field, ukv_vertex_role_t role)
        : keys_stream(std::move(stream)), graph(net), weight_field(field), vertex_role(role) {
        fetched_nodes = keys_stream.keys_batch();
        compute_degrees(graph, fetched_nodes, vertex_role, weight_field, &degrees);
    }

    void next_batch() {
        keys_stream.seek_to_next_batch();
        fetched_nodes = keys_stream.keys_batch();
        compute_degrees(graph, fetched_nodes, vertex_role, weight_field, &degrees);
        index = 0;
    }

    ukv_key_t key() { return fetched_nodes[index]; }
    ukv_vertex_degree_t degree() { return degrees[index]; }
};

struct degree_view_t {
    std::weak_ptr<py_graph_t> net_ptr;
    ukv_vertex_role_t roles = ukv_vertex_role_any_k;
    std::string weight = "";
};

template <typename element_at>
py::object wrap_into_buffer(py_graph_t& g, strided_range_gt<element_at> range) {

    py_buffer_memory_t& buf = g.last_buffer;

    buf.strides[0] = range.stride();
    buf.strides[1] = buf.strides[2] = 1;
    buf.shape[0] = range.size();
    buf.shape[1] = buf.shape[2] = 1;

    // https://docs.python.org/3/c-api/buffer.html
    buf.raw.buf = (void*)range.begin().get();
    buf.raw.obj = NULL;
    buf.raw.len = range.size() * sizeof(element_at);
    buf.raw.itemsize = sizeof(element_at);
    // https://docs.python.org/3/library/struct.html#format-characters
    buf.raw.format = (char*)&format_code_gt<std::remove_const_t<element_at>>::value[0];
    buf.raw.ndim = 1;
    buf.raw.shape = &buf.shape[0];
    buf.raw.strides = &buf.strides[0];
    buf.raw.suboffsets = nullptr;
    buf.raw.readonly = true;
    buf.raw.internal = nullptr;
    PyObject* obj = PyMemoryView_FromBuffer(&buf.raw);
    return py::reinterpret_steal<py::object>(obj);
}

void ukv::wrap_networkx(py::module& m) {

    auto degs = py::class_<degree_view_t>(m, "DegreeView", py::module_local());
    auto degs_stream = py::class_<degrees_stream_t>(m, "DegreesStream", py::module_local());
    degs.def("__getitem__", [](degree_view_t& degs, ukv_key_t v) {
        py_graph_t& g = *degs.net_ptr.lock().get();
        auto result = g.ref().degree(v, degs.roles).throw_or_release();
        return result;
    });

    degs.def(
        "__call__",
        [](degree_view_t& degs, py::object vs, std::string weight = "") {
            py_graph_t& g = *degs.net_ptr.lock().get();
            ukv_vertex_degree_t* degrees;

            if (PyList_Check(vs.ptr())) {
                std::vector<ukv_key_t> vertices(PySequence_Size(vs.ptr()));
                py_transform_n(vs.ptr(), &py_to_scalar<ukv_key_t>, vertices.begin());
                compute_degrees(g,
                                strided_range(vertices).immutable(),
                                degs.roles,
                                weight.size() ? weight.c_str() : nullptr,
                                &degrees);
                py::list res(vertices.size());
                for (std::size_t i = 0; i != vertices.size(); ++i)
                    res[i] = py::make_tuple(vertices[i], degrees[i]);
                return py::object(res);
            }

            auto vs_handle = py_buffer(vs.ptr());
            auto vertices = py_strided_range<ukv_key_t const>(vs_handle);
            compute_degrees(g, vertices, degs.roles, weight.size() ? weight.c_str() : nullptr, &degrees);
            return wrap_into_buffer<ukv_vertex_degree_t const>(
                g,
                strided_range<ukv_vertex_degree_t const>(degrees, degrees + vertices.size()));
        },
        py::arg("vs"),
        py::arg("weight") = "");

    degs.def(
        "__call__",
        [](degree_view_t& degs, std::string weight = "") {
            degs.weight = weight;
            return degs;
        },
        py::arg("weight") = "");

    degs.def("__iter__", [](degree_view_t& degs) {
        py_graph_t& g = *degs.net_ptr.lock().get();
        blobs_range_t members(g.index.db(), g.index.txn(), g.index);
        keys_stream_t stream = keys_range_t({members}).begin();
        return degrees_stream_t(std::move(stream), g, degs.weight.size() ? degs.weight.c_str() : nullptr, degs.roles);
    });

    degs_stream.def("__next__", [](degrees_stream_t& stream) {
        if (stream.index >= stream.fetched_nodes.size()) {
            if (stream.keys_stream.is_end())
                throw py::stop_iteration();
            stream.next_batch();
        }

        auto ret = py::make_tuple(stream.key(), stream.degree());
        ++stream.index;
        return ret;
    });

    auto nodes_range = py::class_<nodes_range_t>(m, "NodesRange", py::module_local());
    nodes_range.def("__iter__", [](nodes_range_t& range) {
        return nodes_stream_t {std::move(range.native).begin(),
                               range.collection,
                               range.read_data,
                               range.field.size() ? range.field.c_str() : nullptr,
                               range.default_value.c_str()};
    });

    nodes_range.def(
        "__call__",
        [](nodes_range_t& range, bool data = false) {
            range.read_data = data;
            range.default_value = "{}";
            return range;
        },
        py::arg("data") = false);

    nodes_range.def(
        "__call__",
        [](nodes_range_t& range, std::string& data, py::object default_value) {
            range.read_data = true;
            range.field = data;
            std::string str;
            to_string(default_value.ptr(), str);
            range.default_value = std::move(str);
            return range;
        },
        py::arg("data"),
        py::arg("default") = py::reinterpret_steal<py::object>(Py_None));

    auto nodes_stream = py::class_<nodes_stream_t>(m, "NodesStream", py::module_local());
    nodes_stream.def("__next__", [](nodes_stream_t& stream) {
        if (stream.index >= stream.nodes.size()) {
            if (stream.native.is_end())
                throw py::stop_iteration();
            stream.next_batch();
        }
        py::object ret;
        if (stream.read_data)
            ret = py::make_tuple(stream.key(),
                                 py::reinterpret_steal<py::object>(from_json(json_t::parse(stream.data()))));
        else
            ret = py::cast(stream.key());
        ++stream.index;
        return ret;
    });

    auto edges_range = py::class_<edges_range_t>(m, "EdgesRange", py::module_local());
    edges_range.def("__iter__", [](edges_range_t& range) {
        return edges_stream_t(std::move(range.native).begin(),
                              range.collection,
                              range.read_data,
                              range.field.size() ? range.field.c_str() : nullptr,
                              range.default_value.c_str());
    });

    edges_range.def(
        "__call__",
        [](edges_range_t& range, bool data = false) {
            range.read_data = data;
            range.default_value = "{}";
            return std::move(range);
        },
        py::arg("data") = false);

    edges_range.def(
        "__call__",
        [](edges_range_t& range, std::string& data, py::object default_value) {
            range.read_data = true;
            range.field = data;
            std::string str;
            to_string(default_value.ptr(), str);
            range.default_value = std::move(str);
            return std::move(range);
        },
        py::arg("data"),
        py::arg("default") = py::reinterpret_steal<py::object>(Py_None));

    auto edges_stream = py::class_<edges_stream_t>(m, "EdgesStream", py::module_local());
    edges_stream.def("__next__", [](edges_stream_t& stream) {
        if (stream.index >= stream.edges.size()) {
            if (stream.native.is_end())
                throw py::stop_iteration();
            stream.next_batch();
        }
        auto edge = stream.edge();
        py::object ret;
        if (stream.read_data)
            ret = py::make_tuple(edge.source_id,
                                 edge.target_id,
                                 py::reinterpret_steal<py::object>(from_json(json_t::parse(stream.data()))));
        else
            ret = py::make_tuple(edge.source_id, edge.target_id);
        ++stream.index;
        return ret;
    });

    auto g = py::class_<py_graph_t, std::shared_ptr<py_graph_t>>(m, "Network", py::module_local());
    g.def( //
        py::init([](std::shared_ptr<py_db_t> py_db,
                    std::optional<std::string> index,
                    std::optional<std::string> vertices_attrs,
                    std::optional<std::string> relations_attrs,
                    bool directed = false,
                    bool multi = false,
                    bool loops = false) {
            //
            if (!py_db)
                return std::shared_ptr<py_graph_t> {};

            auto net_ptr = std::make_shared<py_graph_t>();
            net_ptr->py_db_ptr = py_db;
            net_ptr->is_directed = directed;
            net_ptr->is_multi = multi;
            net_ptr->allow_self_loops = loops;

            // Attach the primary collection
            database_t& db = py_db->native;
            net_ptr->index = db.find_or_create(index ? index->c_str() : "").throw_or_release();

            // Attach the additional collections
            if (vertices_attrs)
                net_ptr->vertices_attrs =
                    db.find_or_create<docs_collection_t>(vertices_attrs->c_str()).throw_or_release();
            if (relations_attrs)
                net_ptr->relations_attrs =
                    db.find_or_create<docs_collection_t>(relations_attrs->c_str()).throw_or_release();

            return net_ptr;
        }),
        py::arg("db"),
        py::arg("index"),
        py::arg("vertices") = std::nullopt,
        py::arg("relations") = std::nullopt,
        py::arg("directed") = false,
        py::arg("multi") = false,
        py::arg("loops") = false);

    // Counting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/graph.html#counting-nodes-edges-and-neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#counting-nodes-edges-and-neighbors
    g.def(
        "order",
        [](py_graph_t& g) { return g.index.size(); },
        "Returns the number of nodes in the graph.");
    g.def(
        "number_of_nodes",
        [](py_graph_t& g) { return g.index.size(); },
        "Returns the number of nodes in the graph.");
    g.def(
        "__len__",
        [](py_graph_t& g) { return g.index.size(); },
        "Returns the number of nodes in the graph.");
    g.def_property_readonly(
        "degree",
        [](py_graph_t& g) {
            auto degs_ptr = std::make_unique<degree_view_t>();
            degs_ptr->net_ptr = g.shared_from_this();
            degs_ptr->roles = ukv_vertex_role_any_k;
            return degs_ptr;
        },
        "A DegreeView for the graph.");
    g.def_property_readonly(
        "in_degree",
        [](py_graph_t& g) {
            auto degs_ptr = std::make_unique<degree_view_t>();
            degs_ptr->net_ptr = g.shared_from_this();
            degs_ptr->roles = ukv_vertex_target_k;
            return degs_ptr;
        },
        "A DegreeView with the number incoming edges for each Vertex.");
    g.def_property_readonly(
        "out_degree",
        [](py_graph_t& g) {
            auto degs_ptr = std::make_unique<degree_view_t>();
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
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) { return g.ref().edges(v1, v2).throw_or_release().size(); },
        "Returns the number of edges between two nodes.");

    g.def(
        "number_of_edges",
        [](py_graph_t& g) { return g.ref().number_of_edges(); },
        "Returns edges count");

    // Reporting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#reporting-nodes-edges-and-neighbors
    g.def_property_readonly(
        "nodes",
        [](py_graph_t& g) {
            blobs_range_t members(g.index.db(), g.index.txn(), g.index);
            nodes_range_t range {members, g.vertices_attrs};
            return py::cast(std::make_unique<nodes_range_t>(range));
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
        [](py_graph_t& g, ukv_key_t v) { return g.ref().contains(v).throw_or_release(); },
        py::arg("n"),
        "Returns True if the graph contains the node n.");

    g.def_property_readonly("edges", [](py_graph_t& g) {
        return edges_range_t {g.ref().edges(ukv_vertex_source_k).throw_or_release(), g.relations_attrs};
    });

    g.def(
        "has_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) { return g.ref().edges(v1, v2).throw_or_release().size() != 0; },
        py::arg("u"),
        py::arg("v"));
    g.def(
        "has_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2, ukv_key_t e) {
            auto ids = g.ref().edges(v1, v2).throw_or_release().edge_ids;
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
        "__getitem__",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().neighbors(n).throw_or_release()); },
        py::arg("n"),
        "Returns an iterable of incoming and outgoing nodes of n. Potentially with duplicates.");
    g.def(
        "neighbors",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().neighbors(n).throw_or_release()); },
        py::arg("n"),
        "Returns an iterable of incoming and outgoing nodes of n. Potentially with duplicates.");
    g.def(
        "successors",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().successors(n).throw_or_release()); },
        py::arg("n"),
        "Returns an iterable of successor nodes of n.");
    g.def(
        "predecessors",
        [](py_graph_t& g, ukv_key_t n) { return wrap_into_buffer(g, g.ref().predecessors(n).throw_or_release()); },
        py::arg("n"),
        "Returns an iterable of follower nodes of n.");
    g.def(
        "nbunch_iter",
        [](py_graph_t& g, PyObject* vs) {
            auto ids_handle = py_buffer(vs);
            auto ids = py_strided_range<ukv_key_t const>(ids_handle);
            auto result = g.ref().contains(ids).throw_or_release();

            py::array_t<ukv_key_t> res_array(ids.size());
            size_t j = 0;
            for (size_t i = 0; i < ids.size(); i++)
                if (result[i])
                    res_array.mutable_at(j++) = ids[i];
            res_array.resize(py::array::ShapeContainer({std::max<size_t>(j, 1) - 1}));

            return res_array;
        },
        "Filters given nodes which are also in the graph and returns an iterator over them.");

    // Adding and Removing Nodes and Edges
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#adding-and-removing-nodes-and-edges
    g.def(
        "add_node",
        [](py_graph_t& g, ukv_key_t v, py::kwargs const& attrs) {
            g.ref().upsert_vertex(v).throw_unhandled();
            if (!attrs.size())
                return;
            std::string json_str;
            to_string(attrs.ptr(), json_str);
            g.vertices_attrs[v].assign(value_view_t(json_str)).throw_unhandled();
        },
        py::arg("v_to_upsert"));
    g.def(
        "add_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) {
            g.ref().upsert_edge(edge_t {v1, v2}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"));
    g.def(
        "add_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2, ukv_key_t e, py::kwargs const& attrs) {
            g.ref().upsert_edge(edge_t {v1, v2, e}).throw_unhandled();
            if (!attrs.size())
                return;
            std::string json_str;
            to_string(attrs.ptr(), json_str);
            g.relations_attrs[e].assign(value_view_t(json_str)).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"),
        py::arg("id"));
    g.def(
        "remove_node",
        [](py_graph_t& g, ukv_key_t v) {
            g.ref().remove_vertex(v).throw_unhandled();
            if (g.vertices_attrs.db())
                g.vertices_attrs[v].clear().throw_unhandled();
        },
        py::arg("v_to_remove"));
    g.def(
        "remove_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2) {
            g.ref().remove_edge(edge_t {v1, v2}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"));
    g.def(
        "remove_edge",
        [](py_graph_t& g, ukv_key_t v1, ukv_key_t v2, ukv_key_t e) {
            g.ref().remove_edge(edge_t {v1, v2, e}).throw_unhandled();
        },
        py::arg("u_for_edge"),
        py::arg("v_for_edge"),
        py::arg("key"));
    g.def("add_nodes_from", [](py_graph_t& g, py::object vs, py::kwargs const& attrs) {
        if (PyObject_CheckBuffer(vs.ptr())) {
            py_buffer_t buf = py_buffer(vs.ptr());
            if (!can_cast_internal_scalars<ukv_key_t>(buf))
                throw std::invalid_argument("Expecting @c ukv_key_t scalars in zero-copy interface");
            auto vertices = py_strided_range<ukv_key_t const>(buf);
            g.ref().upsert_vertices(vertices).throw_unhandled();
            if (!attrs.size())
                return;
            std::string json_str;
            to_string(attrs.ptr(), json_str);
            g.vertices_attrs[vertices].assign(value_view_t(json_str)).throw_unhandled();
        }
        else {
            if (!PySequence_Check(vs.ptr()))
                throw std::invalid_argument("Nodes Must Be Sequence");
            std::vector<ukv_key_t> vertices(PySequence_Size(vs.ptr()));
            py_transform_n(vs.ptr(), &py_to_scalar<ukv_key_t>, vertices.begin());
            g.ref().upsert_vertices(vertices).throw_unhandled();
            if (!attrs.size())
                return;
            std::string json_str;
            to_string(attrs.ptr(), json_str);
            g.vertices_attrs[vertices].assign(value_view_t(json_str)).throw_unhandled();
        }
    });
    g.def(
        "add_edges_from",
        [](py_graph_t& g, py::object adjacency_list) {
            g.ref().upsert_edges(parsed_adjacency_list_t(adjacency_list.ptr())).throw_unhandled();
        },
        py::arg("ebunch_to_add"),
        "Adds an adjacency list (in a form of 2 or 3 columnar matrix) to the graph.");
    g.def("remove_nodes_from", [](py_graph_t& g, py::object vs) {
        if (PyObject_CheckBuffer(vs.ptr())) {
            py_buffer_t buf = py_buffer(vs.ptr());
            if (!can_cast_internal_scalars<ukv_key_t>(buf))
                throw std::invalid_argument("Expecting @c ukv_key_t scalars in zero-copy interface");
            auto vertices = py_strided_range<ukv_key_t const>(buf);
            g.ref().remove_vertices(vertices).throw_unhandled();
            if (g.vertices_attrs.db())
                g.vertices_attrs[vertices].clear().throw_unhandled();
        }
        else {
            if (!PySequence_Check(vs.ptr()))
                throw std::invalid_argument("Nodes Must Be Sequence");
            std::vector<ukv_key_t> vertices(PySequence_Size(vs.ptr()));
            py_transform_n(vs.ptr(), &py_to_scalar<ukv_key_t>, vertices.begin());
            g.ref().remove_vertices(vertices).throw_unhandled();
            if (g.vertices_attrs.db())
                g.vertices_attrs[vertices].clear().throw_unhandled();
        }
    });
    g.def(
        "remove_edges_from",
        [](py_graph_t& g, py::object adjacency_list) {
            g.ref().remove_edges(parsed_adjacency_list_t(adjacency_list.ptr())).throw_unhandled();
        },
        py::arg("ebunch"),
        "Removes all edges in supplied adjacency list (in a form of 2 or 3 columnar matrix) from the graph.");

    g.def(
        "add_edges_from",
        [](py_graph_t& g, py::object v1s, py::object v2s, py::object es, py::kwargs const& attrs) {
            g.ref().upsert_edges(parsed_adjacency_list_t(v1s.ptr(), v2s.ptr(), es.ptr())).throw_unhandled();

            if (!attrs.size())
                return;

            std::string json_str;
            to_string(attrs.ptr(), json_str);
            if (PyObject_CheckBuffer(es.ptr())) {
                py_buffer_t buf = py_buffer(es.ptr());
                if (!can_cast_internal_scalars<ukv_key_t>(buf))
                    throw std::invalid_argument("Expecting @c ukv_key_t scalars in zero-copy interface");
                auto edge_ids = py_strided_range<ukv_key_t const>(buf);
                g.relations_attrs[edge_ids].assign(value_view_t(json_str)).throw_unhandled();
            }
            else {
                if (!PySequence_Check(es.ptr()))
                    throw std::invalid_argument("Edge Ids Must Be Sequence");
                std::vector<ukv_key_t> edge_ids(PySequence_Size(es.ptr()));
                py_transform_n(es.ptr(), &py_to_scalar<ukv_key_t>, edge_ids.begin());
                g.relations_attrs[edge_ids].assign(value_view_t(json_str)).throw_unhandled();
            }
        },
        py::arg("us"),
        py::arg("vs"),
        py::arg("keys") = nullptr,
        "Adds edges from members of the first array to members of the second array.");
    g.def(
        "remove_edges_from",
        [](py_graph_t& g, py::object v1s, py::object v2s, py::object es) {
            g.ref().remove_edges(parsed_adjacency_list_t(v1s.ptr(), v2s.ptr(), es.ptr())).throw_unhandled();

            if (!g.relations_attrs.db())
                return;

            if (PyObject_CheckBuffer(es.ptr())) {
                py_buffer_t buf = py_buffer(es.ptr());
                if (!can_cast_internal_scalars<ukv_key_t>(buf))
                    throw std::invalid_argument("Expecting @c ukv_key_t scalars in zero-copy interface");
                auto edge_ids = py_strided_range<ukv_key_t const>(buf);
                g.relations_attrs[edge_ids].clear().throw_unhandled();
            }
            else {
                if (!PySequence_Check(es.ptr()))
                    throw std::invalid_argument("Edge Ids Must Be Sequence");
                std::vector<ukv_key_t> edge_ids(PySequence_Size(es.ptr()));
                py_transform_n(es.ptr(), &py_to_scalar<ukv_key_t>, edge_ids.begin());
                g.relations_attrs[edge_ids].clear().throw_unhandled();
            }
        },
        py::arg("us"),
        py::arg("vs"),
        py::arg("keys") = nullptr,
        "Removes edges from members of the first array to members of the second array.");

    g.def(
        "clear_edges",
        [](py_graph_t& g) {
            g.index.clear_values().throw_unhandled();
            if (g.relations_attrs.db())
                g.relations_attrs.clear_values().throw_unhandled();
        },
        "Removes edges from the graph.");
    g.def(
        "clear",
        [](py_graph_t& g) {
            g.index.clear();
            if (g.vertices_attrs.db())
                g.vertices_attrs.clear();
            if (g.relations_attrs.db())
                g.relations_attrs.clear();
        },
        "Removes both vertices and edges from the graph.");

    g.def(
        "community_louvain",
        [](py_graph_t& g) {
            graph_collection_t graph = g.ref();
            auto partition = best_partition(graph);
            return py::cast(partition);
        },
        "Community Louvain.");

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
    g.def("is_directed", [](py_graph_t& g) { return g.is_directed; });
    g.def_property_readonly("is_multi", [](py_graph_t& g) { return g.is_multi; });
    g.def_property_readonly("allows_loops", [](py_graph_t& g) { return g.allow_self_loops; });
    m.def("is_directed", [](py_graph_t& g) { return g.is_directed; });
    m.def("is_multi", [](py_graph_t& g) { return g.is_multi; });
    m.def("allows_loops", [](py_graph_t& g) { return g.allow_self_loops; });
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
