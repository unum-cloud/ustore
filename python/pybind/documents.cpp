#include "pybind.hpp"
#include <cast.hpp>
#include <crud.hpp>
#include <nlohmann.hpp>

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

static void write_one_doc(py_docs_col_t& col, PyObject* key_py, PyObject* val_py) {
    json_t json = to_json(val_py);
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    col.binary.native[key] = json.dump().c_str();
}

static void write_many_docs(py_docs_col_t& col, PyObject* keys_py, PyObject* vals_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    std::vector<json_t> vals;
    py_transform_n(vals_py, &to_json, std::back_inserter(vals));
    if (keys.size() != vals.size())
        throw std::invalid_argument("Keys count must match values count");
    for (size_t i = 0; i < keys.size(); ++i)
        col.binary.native[keys[i]] = vals[i].dump().c_str();
}

static void write_same_doc(py_docs_col_t& col, PyObject* keys_py, PyObject* val_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    auto json = to_json(val_py).dump();
    for (size_t i = 0; i < keys.size(); ++i)
        col.binary.native[keys[i]] = json.c_str();
}

static void write_doc(py_docs_col_t& col, py::object key_py, py::object val_py) {
    auto is_single_key = PyLong_Check(key_py.ptr());
    auto is_single_val = PyDict_Check(val_py.ptr());
    auto func = !is_single_val ? &write_many_docs : is_single_key ? &write_one_doc : &write_same_doc;
    return func(col, key_py.ptr(), val_py.ptr());
}

static py::object read_one_doc(py_docs_col_t& col, PyObject* key_py) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    auto json = json_t::parse(col.binary.native[key].value()->c_str());
    return from_json(json);
}

static py::object read_many_docs(py_docs_col_t& col, PyObject* keys_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    py::list values(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        values[i] = from_json(json_t::parse(col.binary.native[keys[i]].value()->c_str()));
    return values;
}

static py::object read_doc(py_docs_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &read_one_doc : &read_many_docs;
    return func(col, key_py.ptr());
}

static void remove_one_doc(py_docs_col_t& col, PyObject* key_py) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    col.binary.native[key] = nullptr;
}

static void remove_many_docs(py_docs_col_t& col, PyObject* keys_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    for (auto key : keys)
        col.binary.native[key] = nullptr;
}

static void remove_doc(py_docs_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &remove_one_doc : &remove_many_docs;
    return func(col, key_py.ptr());
}

static py::object has_doc(py_docs_col_t& col, py::object key_py) {
    return has_binary(col.binary, key_py);
}

static py::object scan_doc(py_docs_col_t& col, ukv_key_t min_key, ukv_size_t scan_length) {
    return scan_binary(col.binary, min_key, scan_length);
}

static void merge_patch(py_docs_col_t& col, py::object key_py, py::object val_py, ukv_format_t format) {
    col.binary.native.as(format);
    write_one_doc(col, key_py.ptr(), val_py.ptr());
    col.binary.native.as(ukv_format_json_k);
}

void ukv::wrap_document(py::module& m) {
    auto py_docs_col = py::class_<py_docs_col_t>(m, "DocsCollection", py::module_local());

    py_docs_col.def("set", &write_doc);
    py_docs_col.def("get", &read_doc);
    py_docs_col.def("remove", &remove_doc);
    py_docs_col.def("has_key", &has_doc);
    py_docs_col.def("scan", &scan_doc);

    py_docs_col.def("__setitem__", &write_doc);
    py_docs_col.def("__delitem__", &remove_doc);
    py_docs_col.def("__getitem__", &read_doc);
    py_docs_col.def("__contains__", &has_doc);

    py_docs_col.def("patch", [](py_docs_col_t& col, py::object key_py, py::object val_py) {
        merge_patch(col, key_py, val_py, ukv_format_json_patch_k);
    });

    py_docs_col.def("merge", [](py_docs_col_t& col, py::object key_py, py::object val_py) {
        merge_patch(col, key_py, val_py, ukv_format_json_merge_patch_k);
    });

    py_docs_col.def_property_readonly("keys", [](py_docs_col_t& col) {
        members_range_t members(col.binary.db(), col.binary.txn(), *col.binary.member_col());
        keys_range_t range {members};
        return py::cast(std::make_unique<keys_range_t>(range));
    });
}