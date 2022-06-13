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

struct py_db_t;
struct py_column_t;
struct py_arena_t;

struct py_arena_t {
    void* ptr = NULL;
    size_t length = 0;
};

struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    ukv_t raw = NULL;
    std::string config;
    py_arena_t temporary_arena;
};

struct py_column_t : public std::enable_shared_from_this<py_column_t> {
    ukv_column_t raw = NULL;
    std::string name;
};

std::runtime_error make_exception(py_db_t& db, char const* error) {
    std::runtime_error result(error);
    ukv_error_free(db.raw, error);
    return result;
}

std::shared_ptr<py_db_t> open_if_closed(py_db_t& db) {
    if (db.raw)
        return db.shared_from_this();

    ukv_error_t error = NULL;
    ukv_open(db.config.c_str(), &db.raw, &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    return db.shared_from_this();
}

void close_if_opened(py_db_t& db,
                     py::object const& exc_type,
                     py::object const& exc_value,
                     py::object const& traceback) {
    if (!db.raw)
        return;

    if (db.temporary_arena.ptr) {
        ukv_get_free(db.raw, db.temporary_arena.ptr, db.temporary_arena.length);
        db.temporary_arena.ptr = NULL;
        db.temporary_arena.length = 0;
    }

    ukv_free(db.raw);
    db.raw = NULL;
}

py::bytes get_item(py_db_t& db, ukv_column_t column_ptr, ukv_key_t key) {
    ukv_val_ptr_t vals[1];
    ukv_val_len_t lens[1];
    ukv_error_t error = NULL;
    ukv_options_read_t options = NULL;

    ukv_get(db.raw,
            &key,
            1,
            &column_ptr,
            column_ptr != NULL,
            options,
            &db.temporary_arena.ptr,
            &db.temporary_arena.length,
            vals,
            lens,
            &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    // To fetch data without copies, there is a hacky way:
    // https://github.com/pybind/pybind11/issues/1236#issuecomment-527730864
    // But in that case we can't guarantee memory alignment, so doing a copy
    // is hard to avoid in Python.
    // https://github.com/pybind/pybind11/blob/a05bc3d2359d12840ef2329d68f613f1a7df9c5d/include/pybind11/pytypes.h#L1474
    // https://docs.python.org/3/c-api/bytes.html
    // https://github.com/python/cpython/blob/main/Objects/bytesobject.c
    return py::bytes {reinterpret_cast<char const*>(vals[0]), lens[0]};
}

void set_item(py_db_t& db, ukv_column_t column_ptr, ukv_key_t key, py::bytes const& value) {
    ukv_options_write_t options = NULL;
    std::string_view str = value;
    ukv_val_ptr_t ptr = ukv_val_ptr_t(str.data());
    ukv_val_len_t len = static_cast<ukv_val_len_t>(str.size());
    ukv_error_t error = NULL;

    ukv_put(db.raw, &key, 1, &column_ptr, column_ptr != NULL, options, &ptr, &len, &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);
}

ukv_column_t column_named(py_db_t& db, std::string const& column_name) {
    ukv_column_t column_ptr = NULL;
    ukv_error_t error = NULL;

    ukv_column_upsert(db.raw, column_name.c_str(), &column_ptr, &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);
    return column_ptr;
}

PYBIND11_MODULE(ukv, m) {
    m.doc() =
        "Python bindings for Universal Key Value Store abstraction.\n"
        "Supports most basic collection operations, like `dict`.\n"
        "---------------------------------------------\n";

    // auto ctx = py::class_<py_arena_t, std::shared_ptr<py_arena_t>>(m, "Context");

    auto db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase");
    db.def(py::init([](std::string const& config) {
               auto db_ptr = std::make_shared<py_db_t>();
               db_ptr->config = config;
               open_if_closed(*db_ptr);
               return db_ptr;
           }),
           py::arg("config") = "");

    // Opening and closing:
    db.def("__enter__", &open_if_closed);
    db.def("__exit__", &close_if_opened);

    // Head state updates:
    db.def("__getitem__", [](py_db_t& db, ukv_key_t key) { return get_item(db, NULL, key); });

    db.def("__setitem__",
           [](py_db_t& db, ukv_key_t key, py::bytes const& value) { return set_item(db, NULL, key, value); });

    db.def(
        "get",
        [](py_db_t& db, ukv_key_t key) { return get_item(db, NULL, key); },
        py::arg("key"));

    db.def(
        "set",
        [](py_db_t& db, ukv_key_t key, py::bytes const& value) { return set_item(db, NULL, key, value); },
        py::arg("key"),
        py::arg("value"));

    db.def(
        "get",
        [](py_db_t& db, std::string const& collection, ukv_key_t key) {
            return get_item(db, column_named(db, collection), key);
        },
        py::arg("collection"),
        py::arg("key"));

    db.def(
        "set",
        [](py_db_t& db, std::string const& collection, ukv_key_t key, py::bytes const& value) {
            return set_item(db, column_named(db, collection), key, value);
        },
        py::arg("collection"),
        py::arg("key"),
        py::arg("value"));
}