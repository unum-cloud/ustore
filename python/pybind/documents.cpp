#include <cast.hpp>
#include "pybind.hpp"
#include <nlohmann.hpp>

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;
using namespace pybind11::literals;

static void write_doc(py_docs_col_t& col, py::object key_py, py::object val_py) {
    json_t json = to_json(val_py);
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py.ptr());
    col.binary.native[key] = json.dump().c_str();
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