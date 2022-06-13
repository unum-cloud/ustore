/**
 * @brief Python bindings for Unums Key Value Store.
 *
 * @section Features
 * > Zero-Copy data forwarding into Python runtime
 *   https://stackoverflow.com/questions/58113973/returning-multiple-pyarray-without-copying-in-pybind11
 * > Calls the C functions outside of the Global Interpret Lock
 *   https://stackoverflow.com/a/55205951
 *
 * @section Interface
 * * update([mapping]) ~ Batch Insert/Put
 * * get(key[, default]) ~ Single & Batch Read
 * * clear() ~ Removes all items
 * * pop(key[, default]) ~ Removes the key in and returns its value.
 * * setdefault(key[, default])
 * * popitem() ~ Pop (key, value) pairs in Last-In First-Out order.
 * * __in__(key) ~ Single & Batch Contains
 * * __len__()
 * Full @c `dict` API:
 * https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
 * https://python-reference.readthedocs.io/en/latest/docs/dict/
 * https://docs.python.org/3/tutorial/datastructures.html#dictionaries
 * https://docs.python.org/3/c-api/dict.html
 *
 * @section Low-level CPython bindings
 * The complexit of implementing the low-level interface boils
 * down to frequent manual calls to `PyArg_ParseTuple()`.
 * It also gives us a more fine-grained control over `PyGILState_Release()`.
 * https://docs.python.org/3/extending/extending.html
 * https://realpython.com/build-python-c-extension-module/
 * https://docs.python.org/3/c-api/arg.html
 * https://docs.python.org/3/c-api/mapping.html
 * https://docs.python.org/3/c-api/init.html#thread-state-and-the-global-interpreter-lock
 *
 * @section High-level Python bindings generators
 * https://realpython.com/python-bindings-overview/
 * http://blog.behnel.de/posts/cython-pybind11-cffi-which-tool-to-choose.html
 * https://pythonspeed.com/articles/python-extension-performance/
 * https://github.com/wjakob/nanobind
 * https://wiki.python.org/moin/IntegratingPythonWithOtherLanguages
 *
 * @sections TODOs:
 * * Nanobind or raw C reimplementation of the interface.
 */

#include <pybind11/pybind11.h>
#include "ukv.h"

namespace py = pybind11;

struct py_ukv_t {
    ukv_t raw = NULL;
};

struct py_ctx_t {
    void* ptr = NULL;
    size_t length = 0;
};

PYBIND11_MODULE(ukv_py, m) {
    m.doc() =
        "Python bindings for Universal Key Value Store abstraction.\n"
        "Supports most basic collection operations, like `dict`.\n"
        "---------------------------------------------\n";

    // auto ctx = py::class_<py_ctx_t, std::shared_ptr<py_ctx_t>>(m, "Context");

    auto ukv = py::class_<py_ukv_t, std::shared_ptr<py_ukv_t>>(m, "UKV");
    ukv.def(py::init([](std::string const& config) {
                auto db_ptr = std::make_shared<py_ukv_t>();
                ukv_error_t error = NULL;
                ukv_open(config.c_str(), &db_ptr->raw, &error);
                if (error) [[unlikely]]
                    throw std::runtime_error(error);
                return db_ptr;
            }),
            py::arg("config"));

    // To fetch data without copies, there is a hacky way:
    // https://github.com/pybind/pybind11/issues/1236#issuecomment-527730864
    // But in that case we can't guarantee memory alignment, so doing a copy
    // is hard to avoid in Python.
    // https://github.com/pybind/pybind11/blob/a05bc3d2359d12840ef2329d68f613f1a7df9c5d/include/pybind11/pytypes.h#L1474
    // https://docs.python.org/3/c-api/bytes.html
    // https://github.com/python/cpython/blob/main/Objects/bytesobject.c
    ukv.def("__getitem__", [](py_ukv_t& db, ukv_key_t k) {
        py_ctx_t ctx;
        ukv_val_ptr_t vals[1];
        ukv_val_len_t lens[1];
        ukv_error_t error = NULL;
        ukv_options_read_t options = NULL;
        ukv_get(db.raw, &k, 1, NULL, 0, options, &ctx.ptr, &ctx.length, vals, lens, &error);
        if (error) [[unlikely]]
            throw std::runtime_error(error);

        py::bytes result {reinterpret_cast<char const*>(vals[0]), lens[1]};
        ukv_get_free(db.raw, ctx.ptr, ctx.length);
        return result;
    });

    ukv.def("__setitem__", [](py_ukv_t& db, ukv_key_t key, py::bytes const& value) {
        ukv_options_write_t options = NULL;
        std::string_view str = value;
        ukv_val_ptr_t ptr = ukv_val_ptr_t(str.data());
        ukv_val_len_t len = static_cast<ukv_val_len_t>(str.size());
        ukv_error_t error = NULL;
        ukv_put(db.raw, &key, 1, NULL, 0, options, &ptr, &len, &error);
        if (error) [[unlikely]]
            throw std::runtime_error(error);
    });

    // We can't estimate the size of most key-value stores
    // ukv.def("__len__", [](py_ukv_t& db) {
    //     size_t count_keys = 0;
    //     size_t count_values_bytes = 0;
    //     ukv_error_t error = NULL;
    //     ukv_columns_size(db.raw, NULL, 0, &count_keys, &count_values_bytes, &error);
    //     if (error) [[unlikely]]
    //         throw std::runtime_error(error);
    //     return count_keys;
    // });

    // ukv.def(
    //     "get",
    //     [](py_ukv_t& db, ukv_key_t k) {
    //         py_ctx_t ctx;
    //         ukv_val_ptr_t vals[1];
    //         ukv_val_len_t lens[1];
    //         ukv_error_t error = NULL;
    //         ukv_options_read_t options = NULL;
    //         ukv_get(db.raw, &k, 1, NULL, 0, options, &ctx.ptr, &ctx.length, vals, lens, &error);
    //         if (error) [[unlikely]]
    //             throw std::runtime_error(error);
    //         return db;
    //     },
    //     py::arg("key"));
}