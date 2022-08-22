#include <cast.hpp>
#include "pybind.hpp"
#include <nlohmann.hpp>

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;
using namespace pybind11::literals;

static py::object has_doc(py_docs_col_t& col, py::object key_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py.ptr());
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_offsets = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    auto options = static_cast<ukv_options_t>(col.binary.options() | ukv_option_read_lengths_k);

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(col.binary.db(),
                 col.binary.txn(),
                 1,
                 col.binary.member_col(),
                 0,
                 &key,
                 0,
                 options,
                 &found_values,
                 &found_offsets,
                 &found_lengths,
                 col.binary.member_arena(),
                 status.member_ptr());
        status.throw_unhandled();
    }

    PyObject* obj_ptr = *found_lengths != ukv_val_len_missing_k ? Py_True : Py_False;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

static void write_doc(py_docs_col_t& col, py::object key_py, py::object val_py) {
    json_t json = to_json(val_py);
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py.ptr());
    col.binary.native[key] = json.dump().c_str();
}

static void remove_doc(py_docs_col_t& col, py::object key_py) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py.ptr());
    col.binary.native[key] = nullptr;
}

static py::object read_doc(py_docs_col_t& col, py::object key_py) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py.ptr());
    auto json = json_t::parse(col.binary.native[key].value()->c_str());
    return from_json(json);
}

void ukv::wrap_document(py::module& m) {
    auto py_docs_col = py::class_<py_docs_col_t>(m, "DocsCollection", py::module_local());

    py_docs_col.def("set", &write_doc);
    py_docs_col.def("get", &read_doc);
    py_docs_col.def("has_key", &has_doc);

    py_docs_col.def("__setitem__", &write_doc);
    py_docs_col.def("__delitem__", &remove_doc);
    py_docs_col.def("__getitem__", &read_doc);
    py_docs_col.def("__contains__", &has_doc);

    py_docs_col.def("patch", [](py_docs_col_t& col, py::object key_py, py::object val_py) {
        col.binary.native.as(ukv_format_json_patch_k);
        write_doc(col, key_py, val_py);
        col.binary.native.as(ukv_format_json_k);
    });

    py_docs_col.def("merge", [](py_docs_col_t& col, py::object key_py, py::object val_py) {
        col.binary.native.as(ukv_format_json_merge_patch_k);
        write_doc(col, key_py, val_py);
        col.binary.native.as(ukv_format_json_k);
    });
}