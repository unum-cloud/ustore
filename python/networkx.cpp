#include <charconv>

#include "pybind.hpp"
#include "crud.hpp"
#include "nlohmann.hpp"
#include "cast_args.hpp"
#include "algorithms/louvain.cpp"

using namespace unum::ustore::pyb;
using namespace unum::ustore;
using namespace unum;

embedded_blobs_t read_attributes( //
    docs_collection_t& collection,
    strided_range_gt<ustore_key_t const> keys,
    ustore_str_view_t field) {

    status_t status;
    ustore_length_t* found_offsets = nullptr;
    ustore_length_t* found_lengths = nullptr;
    ustore_bytes_ptr_t found_values = nullptr;
    auto count = static_cast<ustore_size_t>(keys.size());

    ustore_docs_read_t docs_read {};
    docs_read.db = collection.db();
    docs_read.error = status.member_ptr();
    docs_read.options = ustore_option_dont_discard_memory_k;
    docs_read.transaction = collection.txn();
    docs_read.snapshot = collection.snap();
    docs_read.arena = collection.member_arena();
    docs_read.type = ustore_doc_field_json_k;
    docs_read.tasks_count = count;
    docs_read.collections = collection.member_ptr();
    docs_read.keys = keys.begin().get();
    docs_read.keys_stride = keys.stride();
    docs_read.fields = &field;
    docs_read.offsets = &found_offsets;
    docs_read.lengths = &found_lengths;
    docs_read.values = &found_values;

    ustore_docs_read(&docs_read);
    status.throw_unhandled();
    return embedded_blobs_t {count, found_offsets, found_lengths, found_values};
}

template <graph_type_t type_ak>
void compute_degrees(py_network_t<type_ak>& graph,
                     strided_range_gt<ustore_key_t const> vertices,
                     ustore_vertex_role_t role,
                     ustore_str_view_t weight,
                     ustore_vertex_degree_t** degrees) {

    status_t status;
    ustore_key_t* edges_per_vertex = nullptr;
    auto count = static_cast<ustore_size_t>(vertices.size());

    ustore_graph_find_edges_t graph_find_edges {};
    graph_find_edges.db = graph.index.db();
    graph_find_edges.error = status.member_ptr();
    graph_find_edges.transaction = graph.index.txn();
    graph_find_edges.arena = graph.index.member_arena();
    graph_find_edges.tasks_count = count;
    graph_find_edges.collections = graph.index.member_ptr();
    graph_find_edges.vertices = vertices.begin().get();
    graph_find_edges.vertices_stride = vertices.stride();
    graph_find_edges.roles = &role;
    graph_find_edges.degrees_per_vertex = degrees;
    graph_find_edges.edges_per_vertex = &edges_per_vertex;

    ustore_graph_find_edges(&graph_find_edges);
    status.throw_unhandled();
    if (!weight)
        return;

    auto edges_begin = reinterpret_cast<edge_t*>(edges_per_vertex);
    auto all_edges_count = transform_reduce_n(*degrees, count, 0ul, [](ustore_vertex_degree_t deg) {
        return deg == ustore_vertex_degree_missing_k ? 0 : deg;
    });
    auto edges = edges_span_t {edges_begin, edges_begin + all_edges_count};

    ustore_length_t* found_offsets = nullptr;
    ustore_length_t* found_lengths = nullptr;
    ustore_bytes_ptr_t found_values = nullptr;

    auto values = read_attributes(graph.relations_attrs, edges.edge_ids.immutable(), weight);
    auto value_iterator = values.begin();
    for (std::size_t i = 0; i != count; ++i) {
        auto edges_count = (*degrees)[i];
        (*degrees)[i] = transform_reduce_n(value_iterator, edges_count, 0ul, [](value_view_t edge_weight) {
            ustore_vertex_degree_t weight;
            std::from_chars(edge_weight.c_str(), edge_weight.c_str() + edge_weight.size(), weight);
            return weight;
        });
    }
}

struct nodes_stream_t {
    keys_stream_t native;
    docs_collection_t& collection;
    bool read_data;
    std::string field;
    std::string default_value;

    embedded_blobs_t attrs;
    ptr_range_gt<ustore_key_t const> nodes;
    std::size_t index = 0;

    nodes_stream_t(
        keys_stream_t&& stream, docs_collection_t& col, bool data, std::string field, std::string default_value)
        : native(std::move(stream)), collection(col), read_data(data), field(field), default_value(default_value) {
        nodes = native.keys_batch();
        if (read_data)
            attrs = read_attributes(collection, nodes.strided(), field.size() ? field.c_str() : nullptr);
    }

    py::object next() {
        if (index >= nodes.size()) {
            if (native.is_end())
                throw py::stop_iteration();
            native.seek_to_next_batch();
            nodes = native.keys_batch();
            if (read_data)
                attrs = read_attributes(collection, nodes.strided(), field.size() ? field.c_str() : nullptr);
            index = 0;
        }
        py::object ret;
        if (read_data) {
            auto data = attrs[index] && !attrs[index].empty() ? attrs[index] : value_view_t(default_value);
            ret = py::make_tuple(nodes[index], py::reinterpret_steal<py::object>(from_json(json_t::parse(data))));
        }
        else
            ret = py::cast(nodes[index]);
        ++index;
        return ret;
    }
};

struct edges_stream_t {
    graph_stream_t native;
    docs_collection_t& collection;
    bool read_data;
    std::string field;
    std::string default_value;

    embedded_blobs_t attrs;
    edges_span_t edges;
    std::size_t index = 0;

    edges_stream_t(
        graph_stream_t&& stream, docs_collection_t& col, bool data, std::string field, std::string default_value)
        : native(std::move(stream)), collection(col), read_data(data), field(field), default_value(default_value) {
        edges = native.edges_batch();
        if (read_data)
            attrs = read_attributes(collection, edges.edge_ids.immutable(), field.size() ? field.c_str() : nullptr);
    }

    py::object next() {
        if (index >= edges.size()) {
            if (native.is_end())
                throw py::stop_iteration();
            native.seek_to_next_batch();
            edges = native.edges_batch();
            if (read_data)
                attrs = read_attributes(collection, edges.edge_ids.immutable(), field.size() ? field.c_str() : nullptr);
            index = 0;
        }
        auto edge = edges[index];
        py::object ret;
        if (read_data) {
            auto data = attrs[index] && !attrs[index].empty() ? attrs[index] : value_view_t(default_value);
            ret = py::make_tuple(edge.source_id,
                                 edge.target_id,
                                 py::reinterpret_steal<py::object>(from_json(json_t::parse(data))));
        }
        else
            ret = py::make_tuple(edge.source_id, edge.target_id);
        ++index;
        return ret;
    }
};

struct edges_nbunch_iter_t {
    edges_span_t edges;
    embedded_blobs_t attrs;
    bool read_data;
    std::string default_value;
    std::size_t index = 0;

    edges_nbunch_iter_t(edges_span_t edges_span, embedded_blobs_t attributes, bool data, std::string default_value)
        : edges(edges_span), attrs(attributes), read_data(data), default_value(default_value) {}

    py::object next() {
        if (index == edges.size())
            throw py::stop_iteration();
        edge_t edge = edges[index];
        py::object ret;
        if (read_data) {
            value_view_t data = attrs[index] && !attrs[index].empty() ? attrs[index] : value_view_t(default_value);
            ret = py::make_tuple(edge.source_id,
                                 edge.target_id,
                                 py::reinterpret_steal<py::object>(from_json(json_t::parse(data))));
        }
        else
            ret = py::make_tuple(edge.source_id, edge.target_id);
        ++index;
        return ret;
    }
};

template <graph_type_t type_ak>
struct degrees_stream_t {
    keys_stream_t keys_stream;
    py_network_t<type_ak>& graph;
    std::string weight_field;
    ustore_vertex_role_t vertex_role;

    ptr_range_gt<ustore_key_t const> fetched_nodes;
    ustore_vertex_degree_t* degrees = nullptr;
    std::size_t index = 0;

    degrees_stream_t(keys_stream_t&& stream, py_network_t<type_ak>& net, std::string field, ustore_vertex_role_t role)
        : keys_stream(std::move(stream)), graph(net), weight_field(field), vertex_role(role) {
        fetched_nodes = keys_stream.keys_batch();
        compute_degrees(graph,
                        fetched_nodes,
                        vertex_role,
                        weight_field.size() ? weight_field.c_str() : nullptr,
                        &degrees);
    }

    py::object next() {
        if (index >= fetched_nodes.size()) {
            if (keys_stream.is_end())
                throw py::stop_iteration();
            keys_stream.seek_to_next_batch();
            fetched_nodes = keys_stream.keys_batch();
            compute_degrees(graph,
                            fetched_nodes,
                            vertex_role,
                            weight_field.size() ? weight_field.c_str() : nullptr,
                            &degrees);
            index = 0;
        }

        auto ret = py::make_tuple(fetched_nodes[index], degrees[index]);
        ++index;
        return ret;
    }
};

struct nodes_range_t : public std::enable_shared_from_this<nodes_range_t> {
    keys_range_t native;
    docs_collection_t& collection;
    bool read_data = false;
    std::string field;
    std::string default_value;

    nodes_range_t(keys_range_t n, docs_collection_t& c) : native(n), collection(c) {}
};

template <graph_type_t type_ak>
struct edges_range_t : public std::enable_shared_from_this<edges_range_t<type_ak>> {
    std::weak_ptr<py_network_t<type_ak>> net_ptr;
    std::vector<ustore_key_t> vertices;
    bool read_data = false;
    std::string field;
    std::string default_value;
};

template <graph_type_t type_ak>
struct degree_view_t {
    std::weak_ptr<py_network_t<type_ak>> net_ptr;
    ustore_vertex_role_t roles = ustore_vertex_role_any_k;
    std::string weight = "";
};

template <graph_type_t type_ak, typename element_at>
py::object wrap_into_buffer(py_network_t<type_ak>& g, strided_range_gt<element_at> range) {

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

// nodes_range_t functions

auto nodes_iter(nodes_range_t& range) {
    return nodes_stream_t {std::move(range.native).begin(),
                           range.collection,
                           range.read_data,
                           range.field,
                           range.default_value};
}

auto nodes_call(nodes_range_t& range, bool data = false) {
    range.read_data = data;
    range.default_value = "{}";
    return range.shared_from_this();
}

auto nodes_call_with_data(nodes_range_t& range, std::string& data, py::object def_value) {
    range.read_data = true;
    range.field = data;
    std::string str;
    to_string(def_value.ptr(), str);
    range.default_value = std::move(str);
    return range.shared_from_this();
}

// edges_range_t functions
template <graph_type_t type_ak>
auto edges_iter(edges_range_t<type_ak>& range) {
    py_network_t<type_ak>& g = *range.net_ptr.lock().get();
    if (range.vertices.size()) {
        auto vertices = strided_range(range.vertices).immutable();
        auto role = ustore_vertex_source_k;
        auto edges = g.ref().edges_containing(vertices, {{&role}, 1}).throw_or_release();
        auto attrs = read_attributes(g.relations_attrs,
                                     edges.edge_ids.immutable(),
                                     range.field.size() ? range.field.c_str() : nullptr);
        return py::cast(edges_nbunch_iter_t(edges, attrs, range.read_data, range.default_value));
    }
    auto edges = g.ref().edges(ustore_vertex_source_k).throw_or_release();
    return py::cast(
        edges_stream_t(std::move(edges).begin(), g.relations_attrs, range.read_data, range.field, range.default_value));
}

template <graph_type_t type_ak>
auto edges_call(edges_range_t<type_ak>& range, bool data = false) {
    range.read_data = data;
    range.default_value = "{}";
    return range.shared_from_this();
}

template <graph_type_t type_ak>
auto edges_call_with_data(edges_range_t<type_ak>& range, std::string& data, py::object def_value) {
    range.read_data = true;
    range.field = data;
    std::string str;
    to_string(def_value.ptr(), str);
    range.default_value = std::move(str);
    return range.shared_from_this();
}

template <graph_type_t type_ak>
auto edges_call_with_array(edges_range_t<type_ak>& range, py::object vs, bool data = false) {
    range.read_data = data;
    range.default_value = "{}";

    if (PyNumber_Check(vs.ptr()))
        range.vertices.push_back(py_to_scalar<ustore_key_t>(vs.ptr()));
    else {
        range.vertices.resize(PySequence_Size(vs.ptr()));
        py_transform_n(vs.ptr(), &py_to_scalar<ustore_key_t>, range.vertices.begin());
    }
    return range.shared_from_this();
}

template <graph_type_t type_ak>
auto edges_call_with_array_and_data(edges_range_t<type_ak>& range,
                                    py::object vs,
                                    std::string data,
                                    py::object def_value) {
    range.read_data = true;
    range.field = data;
    std::string str;
    to_string(def_value.ptr(), str);
    range.default_value = std::move(str);

    if (PyNumber_Check(vs.ptr()))
        range.vertices.push_back(py_to_scalar<ustore_key_t>(vs.ptr()));
    else {
        range.vertices.resize(PySequence_Size(vs.ptr()));
        py_transform_n(vs.ptr(), &py_to_scalar<ustore_key_t>, range.vertices.begin());
    }
    return range.shared_from_this();
}

// degree_view_t functions

template <graph_type_t type_ak>
auto degs_getitem(degree_view_t<type_ak>& degs, ustore_key_t v) {
    auto& g = *degs.net_ptr.lock().get();
    auto result = g.ref().degree(v, degs.roles).throw_or_release();
    return result;
}

template <graph_type_t type_ak>
auto degs_call_with_array(degree_view_t<type_ak>& degs, py::object vs, std::string weight = "") {
    auto& g = *degs.net_ptr.lock().get();
    ustore_vertex_degree_t* degrees;

    if (PyObject_CheckBuffer(vs.ptr())) {
        auto vs_handle = py_buffer(vs.ptr());
        auto vertices = py_strided_range<ustore_key_t const>(vs_handle);
        compute_degrees(g, vertices, degs.roles, weight.size() ? weight.c_str() : nullptr, &degrees);
        return wrap_into_buffer<type_ak, ustore_vertex_degree_t const>(
            g,
            strided_range<ustore_vertex_degree_t const>(degrees, degrees + vertices.size()));
    }
    if (!PySequence_Check(vs.ptr()))
        throw std::invalid_argument("Nodes Must Be Sequence");

    std::vector<ustore_key_t> vertices(PySequence_Size(vs.ptr()));
    py_transform_n(vs.ptr(), &py_to_scalar<ustore_key_t>, vertices.begin());
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

template <graph_type_t type_ak>
auto degs_call(degree_view_t<type_ak>& degs, std::string weight = "") {
    degs.weight = weight;
    return degs;
}

template <graph_type_t type_ak>
auto degs_iter(degree_view_t<type_ak>& degs) {
    auto& g = *degs.net_ptr.lock().get();
    blobs_range_t members(g.index.db(), g.index.txn(), 0, g.index);
    keys_stream_t stream = keys_range_t({members}).begin();
    return degrees_stream_t(std::move(stream), g, degs.weight, degs.roles);
}

// py_graph_t functions

template <typename network_at, typename array_at>
void add_key_value( //
    network_at& g,
    strided_range_gt<ustore_key_t const> keys,
    std::shared_ptr<arrow::Array> values,
    std::string_view attr) {
    auto numeric_values = std::static_pointer_cast<array_at>(values);

    std::string jsons_str;
    jsons_str.reserve(keys.size() * (attr.size() + 4));
    std::vector<ustore_length_t> offsets(keys.size() + 1);
    for (size_t idx = 0; idx != keys.size(); idx++) {
        offsets[idx] = jsons_str.size();
        fmt::format_to(std::back_inserter(jsons_str), "{{\"{}\": {}}}", attr.data(), numeric_values->Value(idx));
    }
    offsets.back() = jsons_str.size();

    contents_arg_t contents {};
    contents.offsets_begin = {offsets.data(), sizeof(ustore_length_t)};
    auto vals_begin = reinterpret_cast<ustore_bytes_ptr_t>(jsons_str.data());
    contents.contents_begin = {&vals_begin, 0};

    g.relations_attrs[keys].merge(contents).throw_unhandled();
}

template <typename network_at, typename array_at>
void add_key_value_binary( //
    network_at& g,
    strided_range_gt<ustore_key_t const> keys,
    std::shared_ptr<arrow::Array> values,
    std::string_view attr) {
    auto binary_values = std::static_pointer_cast<arrow::BinaryArray>(values);

    std::string jsons_str;
    jsons_str.reserve(keys.size() * (attr.size() + 4));
    std::vector<ustore_length_t> offsets(keys.size() + 1);

    for (size_t idx = 0; idx != keys.size(); idx++) {
        offsets[idx] = jsons_str.size();
        auto value = binary_values->Value(idx);
        fmt::format_to(std::back_inserter(jsons_str),
                       "{{\"{}\":\"{}\"}}",
                       attr.data(),
                       std::string_view(value.data(), value.size()));
    }
    offsets.back() = jsons_str.size();

    contents_arg_t contents {};
    contents.offsets_begin = {offsets.data(), sizeof(ustore_length_t)};
    auto vals_begin = reinterpret_cast<ustore_bytes_ptr_t>(jsons_str.data());
    contents.contents_begin = {&vals_begin, 0};

    g.relations_attrs[keys].merge(contents).throw_unhandled();
}

template <typename network_at>
auto graph_init(std::shared_ptr<py_db_t> py_db,
                std::optional<std::string> index,
                std::optional<std::string> vertices_attrs,
                std::optional<std::string> relations_attrs) {
    if (!py_db)
        return std::shared_ptr<network_at> {};

    auto net_ptr = std::make_shared<network_at>();
    net_ptr->py_db_ptr = py_db;

    // Attach the primary collection
    database_t& db = py_db->native;
    net_ptr->index = db.find_or_create(index ? index->c_str() : "").throw_or_release();

    // Attach the additional collections
    if (vertices_attrs)
        net_ptr->vertices_attrs = db.find_or_create<docs_collection_t>(vertices_attrs->c_str()).throw_or_release();
    if (relations_attrs)
        net_ptr->relations_attrs = db.find_or_create<docs_collection_t>(relations_attrs->c_str()).throw_or_release();

    return net_ptr;
}

template <graph_type_t type_ak>
auto degree(py_network_t<type_ak>& g) {
    auto degs_ptr = std::make_unique<degree_view_t<type_ak>>();
    degs_ptr->net_ptr = g.shared_from_this();
    degs_ptr->roles = ustore_vertex_role_any_k;
    return degs_ptr;
}

template <graph_type_t type_ak>
auto in_degree(py_network_t<type_ak>& g) {
    auto degs_ptr = std::make_unique<degree_view_t<type_ak>>();
    degs_ptr->net_ptr = g.shared_from_this();
    degs_ptr->roles = ustore_vertex_target_k;
    return degs_ptr;
}

template <graph_type_t type_ak>
auto out_degree(py_network_t<type_ak>& g) {
    auto degs_ptr = std::make_unique<degree_view_t<type_ak>>();
    degs_ptr->net_ptr = g.shared_from_this();
    degs_ptr->roles = ustore_vertex_source_k;
    return degs_ptr;
}

template <typename network_at>
auto size(network_at& g, std::string weight) {
    if (!weight.size())
        return g.ref().number_of_edges();

    std::size_t size = 0;
    auto stream = g.ref().edges(ustore_vertex_source_k).throw_or_release().begin();
    while (!stream.is_end()) {
        auto edge_ids = stream.edges_batch().edge_ids.immutable();
        auto attrs = read_attributes(g.relations_attrs, edge_ids, weight.c_str());
        for (std::size_t i = 0; i != edge_ids.size(); ++i) {
            if (attrs[i] && attrs[i].size()) {
                std::size_t number = 0;
                auto result = std::from_chars((char*)attrs[i].begin(), (char*)attrs[i].end(), number);
                if (result.ec == std::errc())
                    size += number;
                else if (result.ec == std::errc::invalid_argument)
                    throw std::runtime_error("Unsupported Type");
                else
                    throw std::runtime_error("Failed To Read Attribute");
            }
            else
                size += 1;
        }
        stream.seek_to_next_batch();
    }
    return size;
}

template <typename network_at>
auto number_of_edges_between(network_at& g, ustore_key_t v1, ustore_key_t v2) {
    return g.ref().edges_between(v1, v2).throw_or_release().size();
}

template <typename network_at>
auto number_of_edges(network_at& g) {
    return g.ref().number_of_edges();
}

template <typename network_at>
auto nodes(network_at& g) {
    blobs_range_t members(g.index.db(), g.index.txn(), 0, g.index);
    keys_range_t keys {members};
    auto range = std::make_shared<nodes_range_t>(keys, g.vertices_attrs);
    return range;
}

template <typename network_at>
auto contains(network_at& g, ustore_key_t v) {
    return g.ref().contains(v).throw_or_release();
}

template <typename network_at>
auto getitem(network_at& g, ustore_key_t n) {
    return wrap_into_buffer(g, g.ref().neighbors(n).throw_or_release());
}

template <typename network_at>
auto len(network_at& g) {
    return g.index.size();
}

template <typename network_at>
auto number_of_nodes(network_at& g) {
    return g.index.size();
}

template <typename network_at>
auto has_node(network_at& g, ustore_key_t v) {
    auto maybe = g.ref().contains(v);
    maybe.throw_unhandled();
    return *maybe;
}

template <graph_type_t type_ak>
auto has_edge(py_network_t<type_ak>& g, ustore_key_t v1, ustore_key_t v2) {
    if (type_ak == digraph_k)
        return g.ref().edges_between(v1, v2).throw_or_release().size() != 0;
    return g.ref().edges_between(v1, v2).throw_or_release().size() != 0 ||
           g.ref().edges_between(v2, v1).throw_or_release().size() != 0;
}

template <graph_type_t type_ak>
auto has_edge_with_id(py_network_t<type_ak>& g, ustore_key_t v1, ustore_key_t v2, ustore_key_t e) {
    auto ids = g.ref().edges_between(v1, v2).throw_or_release().edge_ids;
    return std::find(ids.begin(), ids.end(), e) != ids.end();
}

template <graph_type_t type_ak>
auto get_edges(py_network_t<type_ak>& g) {
    auto edges_ptr = std::make_shared<edges_range_t<type_ak>>();
    edges_ptr->net_ptr = g.shared_from_this();
    return edges_ptr;
}

template <graph_type_t type_ak>
auto neighbors(py_network_t<type_ak>& g, ustore_key_t n) {
    if (type_ak == graph_k)
        return wrap_into_buffer(g, g.ref().neighbors(n).throw_or_release());
    return wrap_into_buffer(g, g.ref().neighbors(n, ustore_vertex_source_k).throw_or_release());
}

template <typename network_at>
auto successors(network_at& g, ustore_key_t n) {
    return wrap_into_buffer(g, g.ref().successors(n).throw_or_release());
}

template <typename network_at>
auto predecessors(network_at& g, ustore_key_t n) {
    return wrap_into_buffer(g, g.ref().predecessors(n).throw_or_release());
}

template <typename network_at>
auto community_louvain(network_at& g) {
    graph_collection_t graph = g.ref();
    auto partition = best_partition(graph);
    return py::cast(partition);
}

template <typename network_at>
void set_node_attributes(network_at& g, py::object obj, std::optional<std::string> name) {
    std::string json_to_merge;

    if (PyDict_Check(obj.ptr())) {
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(obj.ptr(), &pos, &key, &value)) {
            json_to_merge.clear();
            auto vertex = py_to_scalar<ustore_key_t>(key);
            if (!PyDict_Check(value)) {
                fmt::format_to(std::back_inserter(json_to_merge), "{{\"{}\":", name.value());
                to_string(value, json_to_merge);
                json_to_merge += "}";
            }
            else
                to_string(value, json_to_merge);

            g.vertices_attrs[vertex].merge(json_to_merge.c_str());
        }
    }
    else {
        if (!name)
            throw std::invalid_argument("Invalid Argument");

        fmt::format_to(std::back_inserter(json_to_merge), "{{\"{}\":", name.value());
        to_string(obj.ptr(), json_to_merge);
        json_to_merge += "}";

        auto stream = g.ref().vertex_stream().throw_or_release();
        while (!stream.is_end()) {
            g.vertices_attrs[stream.keys_batch().strided()].merge(json_to_merge.c_str());
            stream.seek_to_next_batch();
        }
    }
}

template <typename network_at>
auto get_node_attributes(network_at& g, std::string& name) {
    std::unordered_map<ustore_key_t, py::object> map;
    auto stream = g.ref().vertex_stream().throw_or_release();
    while (!stream.is_end()) {
        auto keys = stream.keys_batch().strided();
        auto attrs = read_attributes(g.vertices_attrs, keys, name.c_str());
        for (std::size_t i = 0; i != keys.size(); ++i)
            map[keys[i]] = py::reinterpret_steal<py::object>(from_json(json_t::parse(attrs[i])));

        stream.seek_to_next_batch();
    }
    return py::cast(map);
}

template <graph_type_t type_ak>
auto get_edge_data(py_network_t<type_ak>& g, ustore_key_t v1, ustore_key_t v2, py::object default_value) -> py::object {
    std::string default_value_str;
    to_string(default_value.ptr(), default_value_str);
    auto edges = g.ref().edges_between(v1, v2).throw_or_release();
    if (!edges.size())
        return py::none();

    auto edge_ids = edges.edge_ids.immutable();
    auto attrs = read_attributes(g.relations_attrs, edge_ids, nullptr);
    if (attrs[0] && attrs[0].size())
        return py::reinterpret_steal<py::object>(from_json(json_t::parse(attrs[0])));
    if (default_value_str.size())
        return py::reinterpret_steal<py::object>(from_json(json_t::parse(default_value_str)));
    return py::dict();
}

template <graph_type_t type_ak>
auto get_edge_attributes(py_network_t<type_ak>& g, std::string& name) {
    auto hash = [](py::tuple const& tuple) {
        return py_to_scalar<ustore_key_t>(PyTuple_GetItem(tuple.ptr(), 2));
    };
    std::unordered_map<py::tuple, py::object, decltype(hash)> map(0, hash);

    auto stream = g.ref().edges().throw_or_release().begin();
    while (!stream.is_end()) {
        auto edges = stream.edges_batch();
        map.reserve(edges.size());
        auto sources = edges.source_ids;
        auto targets = edges.target_ids;
        auto edge_ids = edges.edge_ids.immutable();
        auto attrs = read_attributes(g.relations_attrs, edge_ids, name.c_str());
        for (std::size_t i = 0; i != edge_ids.size(); ++i) {
            auto key = py::make_tuple(sources[i], targets[i], edge_ids[i]);
            if (attrs[i] && attrs[i].size())
                map[key] = py::reinterpret_steal<py::object>(from_json(json_t::parse(attrs[i])));
            else
                map[key] = py::dict();
        }

        stream.seek_to_next_batch();
    }
    return py::cast(map);
}

template <graph_type_t type_ak>
void set_edge_attributes(py_network_t<type_ak>& g, py::object obj, std::optional<std::string> name) {
    std::string json_to_merge;

    if (PyDict_Check(obj.ptr())) {
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(obj.ptr(), &pos, &key, &value)) {
            json_to_merge.clear();
            if (!PyTuple_Check(key) || PyTuple_Size(key) != 3)
                throw std::invalid_argument("Invalid Argument");
            auto attr_key = py_to_scalar<ustore_key_t>(PyTuple_GetItem(key, 2));
            if (!PyDict_Check(value)) {
                fmt::format_to(std::back_inserter(json_to_merge), "{{\"{}\":", name.value());
                to_string(value, json_to_merge);
                json_to_merge += "}";
            }
            else
                to_string(value, json_to_merge);

            g.relations_attrs[attr_key].merge(json_to_merge.c_str());
        }
    }
    else {
        if (!name)
            throw std::invalid_argument("Invalid Argument");

        fmt::format_to(std::back_inserter(json_to_merge), "{{\"{}\":", name.value());
        to_string(obj.ptr(), json_to_merge);
        json_to_merge += "}";

        auto stream = g.ref().edges().throw_or_release().begin();
        while (!stream.is_end()) {
            g.relations_attrs[stream.edges_batch().edge_ids].merge(json_to_merge.c_str());
            stream.seek_to_next_batch();
        }
    }
}

template <typename network_at>
auto nbunch_iter(network_at& g, PyObject* vs) {
    auto ids_handle = py_buffer(vs);
    auto ids = py_strided_range<ustore_key_t const>(ids_handle);
    auto result = g.ref().contains(ids).throw_or_release();

    py::array_t<ustore_key_t> res_array(ids.size());
    size_t j = 0;
    for (size_t i = 0; i < ids.size(); i++)
        if (result[i])
            res_array.mutable_at(j++) = ids[i];
    res_array.resize(py::array::ShapeContainer({std::max<size_t>(j, 1) - 1}));

    return res_array;
}

template <typename network_at>
void add_node(network_at& g, ustore_key_t v, py::kwargs const& attrs) {
    g.ref().upsert_vertex(v).throw_unhandled();
    if (!attrs.size())
        return;
    std::string json_str;
    to_string(attrs.ptr(), json_str);
    g.vertices_attrs[v].assign(value_view_t(json_str)).throw_unhandled();
}

template <typename network_at>
void add_edge(network_at& g, ustore_key_t v1, ustore_key_t v2) {
    g.ref().upsert_edge(edge_t {v1, v2}).throw_unhandled();
}

template <typename network_at>
void add_edge_with_id(network_at& g, ustore_key_t v1, ustore_key_t v2, ustore_key_t e, py::kwargs const& attrs) {
    g.ref().upsert_edge(edge_t {v1, v2, e}).throw_unhandled();
    if (!attrs.size())
        return;
    std::string json_str;
    to_string(attrs.ptr(), json_str);
    g.relations_attrs[e].assign(value_view_t(json_str)).throw_unhandled();
}

template <typename network_at>
void remove_node(network_at& g, ustore_key_t v) {
    g.ref().remove_vertex(v).throw_unhandled();
    if (g.vertices_attrs.db())
        g.vertices_attrs[v].clear().throw_unhandled();
}

template <typename network_at>
void remove_edge(network_at& g, ustore_key_t v1, ustore_key_t v2) {
    g.ref().remove_edge(edge_t {v1, v2}).throw_unhandled();
}

template <typename network_at>
void remove_edge_with_id(network_at& g, ustore_key_t v1, ustore_key_t v2, ustore_key_t e) {
    g.ref().remove_edge(edge_t {v1, v2, e}).throw_unhandled();
}

template <typename network_at>
void add_nodes_from(network_at& g, py::object vs, py::kwargs const& attrs) {
    if (PyObject_CheckBuffer(vs.ptr())) {
        py_buffer_t buf = py_buffer(vs.ptr());
        if (!can_cast_internal_scalars<ustore_key_t>(buf))
            throw std::invalid_argument("Expecting @c ustore_key_t scalars in zero-copy interface");
        auto vertices = py_strided_range<ustore_key_t const>(buf);
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
        std::vector<ustore_key_t> vertices(PySequence_Size(vs.ptr()));
        py_transform_n(vs.ptr(), &py_to_scalar<ustore_key_t>, vertices.begin());
        g.ref().upsert_vertices(vertices).throw_unhandled();
        if (!attrs.size())
            return;
        std::string json_str;
        to_string(attrs.ptr(), json_str);
        g.vertices_attrs[vertices].assign(value_view_t(json_str)).throw_unhandled();
    }
}

template <typename network_at>
void add_edges_from_adjacency_list(network_at& g, py::object adjacency_list) {
    g.ref().upsert_edges(parsed_adjacency_list_t(adjacency_list.ptr())).throw_unhandled();
}

template <typename network_at>
void remove_nodes_from(network_at& g, py::object vs) {
    if (PyObject_CheckBuffer(vs.ptr())) {
        py_buffer_t buf = py_buffer(vs.ptr());
        if (!can_cast_internal_scalars<ustore_key_t>(buf))
            throw std::invalid_argument("Expecting @c ustore_key_t scalars in zero-copy interface");
        auto vertices = py_strided_range<ustore_key_t const>(buf);
        g.ref().remove_vertices(vertices).throw_unhandled();
        if (g.vertices_attrs.db())
            g.vertices_attrs[vertices].clear().throw_unhandled();
    }
    else {
        if (!PySequence_Check(vs.ptr()))
            throw std::invalid_argument("Nodes Must Be Sequence");
        std::vector<ustore_key_t> vertices(PySequence_Size(vs.ptr()));
        py_transform_n(vs.ptr(), &py_to_scalar<ustore_key_t>, vertices.begin());
        g.ref().remove_vertices(vertices).throw_unhandled();
        if (g.vertices_attrs.db())
            g.vertices_attrs[vertices].clear().throw_unhandled();
    }
}

template <typename network_at>
void remove_edges_from_adjacency_list(network_at& g, py::object adjacency_list) {
    g.ref().remove_edges(parsed_adjacency_list_t(adjacency_list.ptr())).throw_unhandled();
}

template <typename network_at>
void add_edges_from_arrays(network_at& g, py::object v1s, py::object v2s, py::object es, py::kwargs const& attrs) {
    g.ref().upsert_edges(parsed_adjacency_list_t(v1s.ptr(), v2s.ptr(), es.ptr())).throw_unhandled();

    if (!attrs.size())
        return;

    std::string json_str;
    to_string(attrs.ptr(), json_str);
    if (PyObject_CheckBuffer(es.ptr())) {
        py_buffer_t buf = py_buffer(es.ptr());
        if (!can_cast_internal_scalars<ustore_key_t>(buf))
            throw std::invalid_argument("Expecting @c ustore_key_t scalars in zero-copy interface");
        auto edge_ids = py_strided_range<ustore_key_t const>(buf);
        g.relations_attrs[edge_ids].assign(value_view_t(json_str)).throw_unhandled();
    }
    else {
        if (!PySequence_Check(es.ptr()))
            throw std::invalid_argument("Edge Ids Must Be Sequence");
        std::vector<ustore_key_t> edge_ids(PySequence_Size(es.ptr()));
        py_transform_n(es.ptr(), &py_to_scalar<ustore_key_t>, edge_ids.begin());
        g.relations_attrs[edge_ids].assign(value_view_t(json_str)).throw_unhandled();
    }
}

template <typename network_at>
void remove_edges_from_arrays(network_at& g, py::object v1s, py::object v2s, py::object es) {
    g.ref().remove_edges(parsed_adjacency_list_t(v1s.ptr(), v2s.ptr(), es.ptr())).throw_unhandled();

    if (!g.relations_attrs.db())
        return;

    if (PyObject_CheckBuffer(es.ptr())) {
        py_buffer_t buf = py_buffer(es.ptr());
        if (!can_cast_internal_scalars<ustore_key_t>(buf))
            throw std::invalid_argument("Expecting @c ustore_key_t scalars in zero-copy interface");
        auto edge_ids = py_strided_range<ustore_key_t const>(buf);
        g.relations_attrs[edge_ids].clear().throw_unhandled();
    }
    else {
        if (!PySequence_Check(es.ptr()))
            throw std::invalid_argument("Edge Ids Must Be Sequence");
        std::vector<ustore_key_t> edge_ids(PySequence_Size(es.ptr()));
        py_transform_n(es.ptr(), &py_to_scalar<ustore_key_t>, edge_ids.begin());
        g.relations_attrs[edge_ids].clear().throw_unhandled();
    }
}

template <typename network_at>
void add_edges_from_table(
    network_at& g, py::object table, ustore_str_view_t source, ustore_str_view_t target, ustore_str_view_t edge) {
    if (!arrow::py::is_table(table.ptr()))
        throw std::runtime_error("Wrong arg py::object isn't table");

    arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::py::unwrap_table(table.ptr());
    if (!result.ok())
        throw std::runtime_error("Failed to unwrap table");

    auto table_ptr = result.ValueOrDie();
    auto count_column = table_ptr->num_columns();
    if (count_column < 3)
        throw std::runtime_error("Wrong column count");

    auto source_col = table_ptr->GetColumnByName(source);
    auto target_col = table_ptr->GetColumnByName(target);
    auto edge_col = table_ptr->GetColumnByName(edge);

    if (source_col->type()->id() != arrow::Type::INT64 || target_col->type()->id() != arrow::Type::INT64 ||
        edge_col->type()->id() != arrow::Type::INT64)
        throw std::runtime_error("Nodes and edge ids must be int64");

    auto column_names = table_ptr->ColumnNames();
    for (std::size_t chunk_idx = 0; chunk_idx != source_col->num_chunks(); ++chunk_idx) {

        auto source_array = std::static_pointer_cast<arrow::Int64Array>(source_col->chunk(chunk_idx));
        auto target_array = std::static_pointer_cast<arrow::Int64Array>(target_col->chunk(chunk_idx));
        auto edge_array = std::static_pointer_cast<arrow::Int64Array>(edge_col->chunk(chunk_idx));

        edges_view_t edges;
        edges.source_ids = {{source_array->raw_values(), sizeof(ustore_key_t)}, source_array->length()};
        edges.target_ids = {{target_array->raw_values(), sizeof(ustore_key_t)}, target_array->length()};
        edges.edge_ids = {{edge_array->raw_values(), sizeof(ustore_key_t)}, edge_array->length()};

        g.ref().upsert_edges(edges).throw_unhandled();

        if (count_column == 3)
            continue;

        for (size_t col_idx = 0; col_idx != count_column; col_idx++) {
            auto attr = column_names[col_idx];
            if (attr == source || attr == target || attr == edge)
                continue;

            auto attr_col = table_ptr->GetColumnByName(attr);
            auto values = attr_col->chunk(chunk_idx);
            auto keys = edges.edge_ids;

            using type = arrow::Type;
            switch (values->type_id()) {
            case type::HALF_FLOAT: add_key_value<network_at, arrow::HalfFloatArray>(g, keys, values, attr); break;
            case type::FLOAT: add_key_value<network_at, arrow::FloatArray>(g, keys, values, attr); break;
            case type::DOUBLE: add_key_value<network_at, arrow::DoubleArray>(g, keys, values, attr); break;
            case type::BOOL: add_key_value<network_at, arrow::BooleanArray>(g, keys, values, attr); break;
            case type::UINT8: add_key_value<network_at, arrow::UInt8Array>(g, keys, values, attr); break;
            case type::INT8: add_key_value<network_at, arrow::Int8Array>(g, keys, values, attr); break;
            case type::UINT16: add_key_value<network_at, arrow::UInt16Array>(g, keys, values, attr); break;
            case type::INT16: add_key_value<network_at, arrow::Int16Array>(g, keys, values, attr); break;
            case type::UINT32: add_key_value<network_at, arrow::UInt32Array>(g, keys, values, attr); break;
            case type::INT32: add_key_value<network_at, arrow::Int32Array>(g, keys, values, attr); break;
            case type::UINT64: add_key_value<network_at, arrow::UInt64Array>(g, keys, values, attr); break;
            case type::INT64: add_key_value<network_at, arrow::Int64Array>(g, keys, values, attr); break;
            case type::STRING:
            case type::BINARY: add_key_value_binary<network_at, arrow::BinaryArray>(g, keys, values, attr); break;
            }
        }
    }
}

template <typename network_at>
void clear_edges(network_at& g) {
    g.index.clear_values().throw_unhandled();
    if (g.relations_attrs.db())
        g.relations_attrs.clear_values().throw_unhandled();
}

template <typename network_at>
void clear(network_at& g) {
    g.index.clear();
    if (g.vertices_attrs.db())
        g.vertices_attrs.clear().throw_unhandled();
    if (g.relations_attrs.db())
        g.relations_attrs.clear().throw_unhandled();
}

void ustore::wrap_networkx(py::module& m) {

    auto g_degs = py::class_<degree_view_t<graph_k>>(m, "GraphDegreeView", py::module_local());
    g_degs.def("__getitem__", &degs_getitem<graph_k>);
    g_degs.def("__call__", &degs_call_with_array<graph_k>, py::arg("vs"), py::arg("weight") = "");
    g_degs.def("__call__", &degs_call<graph_k>, py::arg("weight") = "");
    g_degs.def("__iter__", &degs_iter<graph_k>);

    auto dg_degs = py::class_<degree_view_t<digraph_k>>(m, "DiGraphDegreeView", py::module_local());
    dg_degs.def("__getitem__", &degs_getitem<digraph_k>);
    dg_degs.def("__call__", &degs_call_with_array<digraph_k>, py::arg("vs"), py::arg("weight") = "");
    dg_degs.def("__call__", &degs_call<digraph_k>, py::arg("weight") = "");
    dg_degs.def("__iter__", &degs_iter<digraph_k>);

    auto g_degs_stream = py::class_<degrees_stream_t<graph_k>>(m, "GraphDegreesStream", py::module_local());
    g_degs_stream.def("__next__", [](degrees_stream_t<graph_k>& stream) { return stream.next(); });

    auto dg_degs_stream = py::class_<degrees_stream_t<digraph_k>>(m, "DiGraphDegreesStream", py::module_local());
    dg_degs_stream.def("__next__", [](degrees_stream_t<digraph_k>& stream) { return stream.next(); });

    auto nodes_range = py::class_<nodes_range_t, std::shared_ptr<nodes_range_t>>(m, "NodesRange", py::module_local());
    nodes_range.def("__iter__", &nodes_iter);
    nodes_range.def("__call__", nodes_call, py::arg("data") = false);
    nodes_range.def("__call__", &nodes_call_with_data, py::arg("data"), py::arg("default") = py::none());

    auto nodes_stream = py::class_<nodes_stream_t>(m, "NodesStream", py::module_local());
    nodes_stream.def("__next__", [](nodes_stream_t& stream) { return stream.next(); });

    auto g_edges_range =
        py::class_<edges_range_t<graph_k>, std::shared_ptr<edges_range_t<graph_k>>>(m,
                                                                                    "GraphEdgesRange",
                                                                                    py::module_local());
    g_edges_range.def("__iter__", &edges_iter<graph_k>);
    g_edges_range.def("__call__", &edges_call<graph_k>, py::arg("data") = false);
    g_edges_range.def("__call__", &edges_call_with_data<graph_k>, py::arg("data"), py::arg("default") = py::none());
    g_edges_range.def("__call__", &edges_call_with_array<graph_k>, py::arg("vs"), py::arg("data"));
    g_edges_range.def("__call__",
                      &edges_call_with_array_and_data<graph_k>,
                      py::arg("vs"),
                      py::arg("data"),
                      py::arg("default") = py::none());

    auto dg_edges_range =
        py::class_<edges_range_t<digraph_k>, std::shared_ptr<edges_range_t<digraph_k>>>(m,
                                                                                        "DiGraphEdgesRange",
                                                                                        py::module_local());
    dg_edges_range.def("__iter__", &edges_iter<digraph_k>);
    dg_edges_range.def("__call__", &edges_call<digraph_k>, py::arg("data") = false);
    dg_edges_range.def("__call__", &edges_call_with_data<digraph_k>, py::arg("data"), py::arg("default") = py::none());
    dg_edges_range.def("__call__", &edges_call_with_array<digraph_k>, py::arg("vs"), py::arg("data"));
    dg_edges_range.def("__call__",
                       &edges_call_with_array_and_data<digraph_k>,
                       py::arg("vs"),
                       py::arg("data"),
                       py::arg("default") = py::none());

    auto edges_iter = py::class_<edges_nbunch_iter_t>(m, "EdgesIter", py::module_local());
    edges_iter.def("__next__", [](edges_nbunch_iter_t& iter) { return iter.next(); });

    auto edges_stream = py::class_<edges_stream_t>(m, "EdgesStream", py::module_local());
    edges_stream.def("__next__", [](edges_stream_t& stream) { return stream.next(); });

    auto g = py::class_<py_graph_t, std::shared_ptr<py_graph_t>>(m, "Graph", py::module_local());
    auto dg = py::class_<py_digraph_t, std::shared_ptr<py_digraph_t>>(m, "DiGraph", py::module_local());

    g.def(py::init(&graph_init<py_graph_t>),
          py::arg("db"),
          py::arg("index") = std::nullopt,
          py::arg("vertices") = std::nullopt,
          py::arg("relations") = std::nullopt);

    dg.def(py::init(&graph_init<py_digraph_t>),
           py::arg("db"),
           py::arg("index") = std::nullopt,
           py::arg("vertices") = std::nullopt,
           py::arg("relations") = std::nullopt);

    // Counting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/graph.html#counting-nodes-edges-and-neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#counting-nodes-edges-and-neighbors
    g.def("number_of_nodes", &number_of_nodes<py_graph_t>);
    g.def("__len__", &len<py_graph_t>);
    g.def_property_readonly("degree", &degree<graph_k>);
    g.def("size", &size<py_graph_t>, py::arg("weight") = "");
    g.def("number_of_edges", &number_of_edges_between<py_graph_t>);
    g.def("number_of_edges", &number_of_edges<py_graph_t>);

    dg.def("number_of_nodes", &number_of_nodes<py_digraph_t>);
    dg.def("__len__", &len<py_digraph_t>);
    dg.def_property_readonly("degree", &degree<digraph_k>);
    dg.def_property_readonly("in_degree", &in_degree<digraph_k>);
    dg.def_property_readonly("out_degree", &out_degree<digraph_k>);
    dg.def("size", &size<py_digraph_t>, py::arg("weight") = "");
    dg.def("number_of_edges", &number_of_edges_between<py_digraph_t>);
    dg.def("number_of_edges", &number_of_edges<py_digraph_t>);

    // Reporting nodes edges and neighbors
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#reporting-nodes-edges-and-neighbors
    g.def_property_readonly("nodes", &nodes<py_graph_t>);
    g.def("__iter__", [](py_graph_t& g) { throw_not_implemented(); });
    g.def("has_node", &has_node<py_graph_t>, py::arg("n"));
    g.def("__contains__", &contains<py_graph_t>, py::arg("n"));
    g.def("set_node_attributes", &set_node_attributes<py_graph_t>, py::arg("values"), py::arg("name") = std::nullopt);
    g.def("get_node_attributes", &get_node_attributes<py_graph_t>, py::arg("name"));
    g.def_property_readonly("edges", &get_edges<graph_k>);
    g.def("has_edge", &has_edge<graph_k>, py::arg("u"), py::arg("v"));
    g.def("has_edge_with_id", &has_edge_with_id<graph_k>, py::arg("u"), py::arg("v"), py::arg("id"));
    g.def("get_edge_data", &get_edge_data<graph_k>, py::arg("u"), py::arg("v"), py::arg("default") = py::none());
    g.def("get_edge_attributes", &get_edge_attributes<graph_k>, py::arg("name"));
    g.def("set_edge_attributes", &set_edge_attributes<graph_k>, py::arg("values"), py::arg("name") = std::nullopt);
    g.def("__getitem__", &getitem<py_graph_t>, py::arg("n"));
    g.def("neighbors", &neighbors<graph_k>, py::arg("n"));
    g.def("nbunch_iter", &nbunch_iter<py_graph_t>);

    dg.def_property_readonly("nodes", &nodes<py_digraph_t>);
    dg.def("__iter__", [](py_digraph_t& g) { throw_not_implemented(); });
    dg.def("has_node", &has_node<py_digraph_t>, py::arg("n"));
    dg.def("__contains__", &contains<py_digraph_t>, py::arg("n"));
    dg.def("set_node_attributes",
           &set_node_attributes<py_digraph_t>,
           py::arg("values"),
           py::arg("name") = std::nullopt);
    dg.def("get_node_attributes", &get_node_attributes<py_digraph_t>, py::arg("name"));
    dg.def_property_readonly("edges", &get_edges<digraph_k>);
    dg.def("has_edge", &has_edge<digraph_k>, py::arg("u"), py::arg("v"));
    dg.def("has_edge_with_id", &has_edge_with_id<digraph_k>, py::arg("u"), py::arg("v"), py::arg("id"));
    dg.def("get_edge_data", &get_edge_data<digraph_k>, py::arg("u"), py::arg("v"), py::arg("default") = py::none());
    dg.def("get_edge_attributes", &get_edge_attributes<digraph_k>, py::arg("name"));
    dg.def("set_edge_attributes", &set_edge_attributes<digraph_k>, py::arg("values"), py::arg("name") = std::nullopt);
    dg.def("__getitem__", &getitem<py_digraph_t>, py::arg("n"));
    dg.def("neighbors", &neighbors<digraph_k>, py::arg("n"));
    dg.def("successors", &successors<py_digraph_t>, py::arg("n"));
    dg.def("predecessors", &predecessors<py_digraph_t>, py::arg("n"));
    dg.def("nbunch_iter", &nbunch_iter<py_digraph_t>);

    // Adding and Removing Nodes and Edges
    // https://networkx.org/documentation/stable/reference/classes/multidigraph.html#adding-and-removing-nodes-and-edges
    g.def("add_node", &add_node<py_graph_t>, py::arg("v_to_upsert"));
    g.def("add_edge", &add_edge<py_graph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"));
    g.def("add_edge", &add_edge_with_id<py_graph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"), py::arg("id"));
    g.def("remove_node", &remove_node<py_graph_t>, py::arg("v_to_remove"));
    g.def("remove_edge", &remove_edge<py_graph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"));
    g.def("remove_edge", &remove_edge_with_id<py_graph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"), py::arg("id"));
    g.def("add_nodes_from", &add_nodes_from<py_graph_t>);
    g.def("add_edges_from", &add_edges_from_adjacency_list<py_graph_t>, py::arg("ebunch_to_add"));
    g.def("remove_nodes_from", &remove_nodes_from<py_graph_t>);
    g.def("remove_edges_from", &remove_edges_from_adjacency_list<py_graph_t>, py::arg("ebunch"));
    g.def("add_edges_from",
          &add_edges_from_arrays<py_graph_t>,
          py::arg("us"),
          py::arg("vs"),
          py::arg("keys") = nullptr);
    g.def("remove_edges_from",
          &remove_edges_from_arrays<py_graph_t>,
          py::arg("us"),
          py::arg("vs"),
          py::arg("keys") = nullptr);
    g.def("add_edges_from",
          &add_edges_from_table<py_graph_t>,
          py::arg("table"),
          py::arg("source"),
          py::arg("target"),
          py::arg("edge"));
    g.def("clear_edges", &clear_edges<py_graph_t>);
    g.def("clear", &clear<py_graph_t>);
    g.def("community_louvain", &community_louvain<py_graph_t>);

    dg.def("add_node", &add_node<py_digraph_t>, py::arg("v_to_upsert"));
    dg.def("add_edge", &add_edge<py_digraph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"));
    dg.def("add_edge", &add_edge_with_id<py_digraph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"), py::arg("id"));
    dg.def("remove_node", &remove_node<py_digraph_t>, py::arg("v_to_remove"));
    dg.def("remove_edge", &remove_edge<py_digraph_t>, py::arg("u_for_edge"), py::arg("v_for_edge"));
    dg.def("remove_edge",
           &remove_edge_with_id<py_digraph_t>,
           py::arg("u_for_edge"),
           py::arg("v_for_edge"),
           py::arg("id"));
    dg.def("add_nodes_from", &add_nodes_from<py_digraph_t>);
    dg.def("add_edges_from", &add_edges_from_adjacency_list<py_digraph_t>, py::arg("ebunch_to_add"));
    dg.def("remove_nodes_from", &remove_nodes_from<py_digraph_t>);
    dg.def("remove_edges_from", &remove_edges_from_adjacency_list<py_digraph_t>, py::arg("ebunch"));
    dg.def("add_edges_from",
           &add_edges_from_arrays<py_digraph_t>,
           py::arg("us"),
           py::arg("vs"),
           py::arg("keys") = nullptr);
    dg.def("remove_edges_from",
           &remove_edges_from_arrays<py_digraph_t>,
           py::arg("us"),
           py::arg("vs"),
           py::arg("keys") = nullptr);
    dg.def("add_edges_from",
           &add_edges_from_table<py_digraph_t>,
           py::arg("table"),
           py::arg("source"),
           py::arg("target"),
           py::arg("edge"));
    dg.def("clear_edges", &clear_edges<py_digraph_t>);
    dg.def("clear", &clear<py_digraph_t>);
    dg.def("community_louvain", &community_louvain<py_digraph_t>);
}
