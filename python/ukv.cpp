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
 * Primary DataBase Methods:
 *      * get(collection?, key, default?) ~ Single Read
 *      * set(collection?, key, default?) ~ Single Insert
 *      * __in__(key) ~ Single & Batch Contains
 *      * __getitem__(key: int) ~ Value Lookup
 *      * __getitem__(collection: str) ~ Sub-Collection Lookup
 *      * clear() ~ Removes all items
 *      * TODO: pop(key, default?) ~ Removes the key in and returns its value.
 *      * TODO: update(mapping) ~ Batch Insert/Put
 * Additional Batch Methods:
 *      * TODO: get_matrix(collection?, keys, max_length: int, padding: byte)
 *      * TODO: get_table(collection?, keys, field_ids)
 * Intentionally not implemented:
 *      * __len__() ~ It's hard to consistently estimate the collection.
 *      * popitem() ~ We can't guarantee Last-In First-Out semantics.
 *      * setdefault(key[, default]) ~ As default values are useless in DBs.
 *
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

#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "ukv.h"

namespace py = pybind11;

struct py_db_t;
struct py_txn_t;
struct py_column_t;
struct py_arena_t;

struct py_arena_t {
    void* ptr = NULL;
    size_t length = 0;
};

struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    ukv_t raw = NULL;
    std::string config;
    py_arena_t arena;

    py_db_t() = default;
    py_db_t(py_db_t&&) = delete;
    py_db_t(py_db_t const&) = delete;

    ~py_db_t() {
        if (raw)
            ukv_free(raw);
        raw = NULL;
    }
};

struct py_txn_t : public std::enable_shared_from_this<py_txn_t> {
    ukv_txn_t raw = NULL;
    py_db_t* db_ptr = NULL;
    py_arena_t arena;

    py_txn_t() = default;
    py_txn_t(py_txn_t&&) = delete;
    py_txn_t(py_txn_t const&) = delete;

    ~py_txn_t() {

        if (raw)
            ukv_txn_free(db_ptr->raw, raw);
        raw = NULL;
    }
};

struct py_column_t : public std::enable_shared_from_this<py_column_t> {
    ukv_column_t raw = NULL;
    std::string name;

    py_db_t* db_ptr = NULL;
    py_txn_t* txn_ptr = NULL;

    py_column_t() = default;
    py_column_t(py_column_t&&) = delete;
    py_column_t(py_column_t const&) = delete;

    ~py_column_t() {
        if (raw)
            ukv_column_free(db_ptr->raw, raw);
        raw = NULL;
    }
};

std::runtime_error make_exception([[maybe_unused]] py_db_t& db, char const* error) {
    std::runtime_error result(error);
    ukv_error_free(error);
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

void free_temporary_memory(py_db_t& db, py_arena_t& arena) {
    if (arena.ptr)
        ukv_arena_free(db.raw, arena.ptr, arena.length);
    arena.ptr = NULL;
    arena.length = 0;
}

void close_if_opened(py_db_t& db,
                     py::object const& exc_type,
                     py::object const& exc_value,
                     py::object const& traceback) {
    if (!db.raw)
        return;

    free_temporary_memory(db, db.arena);
    ukv_free(db.raw);
    db.raw = NULL;
}

std::shared_ptr<py_txn_t> begin_if_needed(py_txn_t& txn) {

    py_db_t& db = *txn.db_ptr;
    ukv_error_t error = NULL;
    ukv_txn_begin(db.raw, 0, &txn.raw, &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    return txn.shared_from_this();
}

void commit(py_txn_t& txn, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
    if (!txn.raw)
        return;

    py_db_t& db = *txn.db_ptr;
    ukv_error_t error = NULL;
    ukv_options_write_t options = NULL;
    ukv_txn_commit(txn.raw, options, &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    free_temporary_memory(db, txn.arena);
    ukv_txn_free(db.raw, txn.raw);
    txn.raw = NULL;
}

bool contains_item(py_db_t& db, ukv_txn_t txn_ptr, ukv_column_t column_ptr, py_arena_t& arena, ukv_key_t key) {
    ukv_val_len_t value_length = 0;
    ukv_error_t error = NULL;
    ukv_options_read_t options = NULL;

    ukv_read(db.raw,
             txn_ptr,
             &key,
             1,
             column_ptr ? &column_ptr : NULL,
             options,
             &arena.ptr,
             &arena.length,
             NULL,
             &value_length,
             &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    return value_length != 0;
}

std::optional<py::bytes> get_item(
    py_db_t& db, ukv_txn_t txn_ptr, ukv_column_t column_ptr, py_arena_t& arena, ukv_key_t key) {

    ukv_val_ptr_t value = NULL;
    ukv_val_len_t value_length = 0;
    ukv_error_t error = NULL;
    ukv_options_read_t options = NULL;

    ukv_read(db.raw,
             txn_ptr,
             &key,
             1,
             column_ptr ? &column_ptr : NULL,
             options,
             &arena.ptr,
             &arena.length,
             &value,
             &value_length,
             &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    if (!value_length)
        return {};

    // To fetch data without copies, there is a hacky way:
    // https://github.com/pybind/pybind11/issues/1236#issuecomment-527730864
    // But in that case we can't guarantee memory alignment, so doing a copy
    // is hard to avoid in Python.
    // https://github.com/pybind/pybind11/blob/a05bc3d2359d12840ef2329d68f613f1a7df9c5d/include/pybind11/pytypes.h#L1474
    // https://docs.python.org/3/c-api/bytes.html
    // https://github.com/python/cpython/blob/main/Objects/bytesobject.c
    return py::bytes {reinterpret_cast<char const*>(value), value_length};
}

void set_item(py_db_t& db, ukv_txn_t txn_ptr, ukv_column_t column_ptr, ukv_key_t key, py::bytes const* value = NULL) {
    ukv_options_write_t options = NULL;
    ukv_val_ptr_t ptr = value ? ukv_val_ptr_t(std::string_view {*value}.data()) : NULL;
    ukv_val_len_t len = value ? static_cast<ukv_val_len_t>(std::string_view {*value}.size()) : 0;
    ukv_error_t error = NULL;

    ukv_write(db.raw, txn_ptr, &key, 1, column_ptr ? &column_ptr : NULL, options, &ptr, &len, &error);
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

    // Define our primary classes: `DataBase`, `Collection`, `Transaction`
    auto db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase");
    auto col = py::class_<py_column_t, std::shared_ptr<py_column_t>>(m, "Collection");
    auto txn = py::class_<py_txn_t, std::shared_ptr<py_txn_t>>(m, "Transaction");

    // Define `DataBase`
    db.def(py::init([](std::string const& config) {
               auto db_ptr = std::make_shared<py_db_t>();
               db_ptr->config = config;
               open_if_closed(*db_ptr);
               return db_ptr;
           }),
           py::arg("config") = "");

    db.def(
        "get",
        [](py_db_t& db, ukv_key_t key) { return get_item(db, NULL, NULL, db.arena, key); },
        py::arg("key"));

    db.def(
        "get",
        [](py_db_t& db, std::string const& collection, ukv_key_t key) {
            return get_item(db, NULL, column_named(db, collection), db.arena, key);
        },
        py::arg("collection"),
        py::arg("key"));

    db.def(
        "set",
        [](py_db_t& db, ukv_key_t key, py::bytes const& value) { return set_item(db, NULL, NULL, key, &value); },
        py::arg("key"),
        py::arg("value"));

    db.def(
        "set",
        [](py_db_t& db, std::string const& collection, ukv_key_t key, py::bytes const& value) {
            return set_item(db, NULL, column_named(db, collection), key, &value);
        },
        py::arg("collection"),
        py::arg("key"),
        py::arg("value"));
    col.def("clear", [](py_db_t& db) {
        // TODO:
    });

    // Define `Collection`s member method, without defining any external constructors
    col.def(
        "get",
        [](py_column_t& col, ukv_key_t key) {
            return get_item(*col.db_ptr,
                            col.txn_ptr ? col.txn_ptr->raw : NULL,
                            col.raw,
                            col.txn_ptr ? col.txn_ptr->arena : col.db_ptr->arena,
                            key);
        },
        py::arg("key"));

    col.def(
        "set",
        [](py_column_t& col, ukv_key_t key, py::bytes const& value) {
            return set_item(*col.db_ptr, col.txn_ptr ? col.txn_ptr->raw : NULL, col.raw, key, &value);
        },
        py::arg("key"),
        py::arg("value"));
    col.def("clear", [](py_column_t& col) {
        ukv_error_t error = NULL;
        ukv_column_remove(col.db_ptr->raw, col.name.c_str(), &error);
        if (error) [[unlikely]]
            throw make_exception(*col.db_ptr, error);
    });

    // Unlike `DataBase`, it won't begin before the `__enter__` call.
    txn.def(py::init([](py_db_t& db) {
        auto txn_ptr = std::make_shared<py_txn_t>();
        txn_ptr->db_ptr = &db;
        return txn_ptr;
    }));
    txn.def(
        "get",
        [](py_txn_t& txn, ukv_key_t key) { return get_item(*txn.db_ptr, txn.raw, NULL, txn.arena, key); },
        py::arg("key"));

    txn.def(
        "get",
        [](py_txn_t& txn, std::string const& collection, ukv_key_t key) {
            return get_item(*txn.db_ptr, txn.raw, column_named(*txn.db_ptr, collection), txn.arena, key);
        },
        py::arg("collection"),
        py::arg("key"));

    txn.def(
        "set",
        [](py_txn_t& txn, ukv_key_t key, py::bytes const& value) {
            return set_item(*txn.db_ptr, txn.raw, NULL, key, &value);
        },
        py::arg("key"),
        py::arg("value"));

    txn.def(
        "set",
        [](py_txn_t& txn, std::string const& collection, ukv_key_t key, py::bytes const& value) {
            return set_item(*txn.db_ptr, txn.raw, column_named(*txn.db_ptr, collection), key, &value);
        },
        py::arg("collection"),
        py::arg("key"),
        py::arg("value"));

    // Resource management
    db.def("__enter__", &open_if_closed);
    db.def("__exit__", &close_if_opened);
    txn.def("__enter__", &begin_if_needed);
    txn.def("__exit__", &commit);

    // Operator overaloads used to edit entries
    db.def("__contains__", [](py_db_t& db, ukv_key_t key) { return contains_item(db, NULL, NULL, db.arena, key); });
    db.def("__getitem__", [](py_db_t& db, ukv_key_t key) { return get_item(db, NULL, NULL, db.arena, key); });
    db.def("__setitem__",
           [](py_db_t& db, ukv_key_t key, py::bytes const& value) { return set_item(db, NULL, NULL, key, &value); });
    db.def("__delitem__", [](py_db_t& db, ukv_key_t key) { return set_item(db, NULL, NULL, key); });

    txn.def("__contains__",
            [](py_txn_t& txn, ukv_key_t key) { return contains_item(*txn.db_ptr, txn.raw, NULL, txn.arena, key); });
    txn.def("__getitem__",
            [](py_txn_t& txn, ukv_key_t key) { return get_item(*txn.db_ptr, txn.raw, NULL, txn.arena, key); });
    txn.def("__setitem__", [](py_txn_t& txn, ukv_key_t key, py::bytes const& value) {
        return set_item(*txn.db_ptr, txn.raw, NULL, key, &value);
    });
    txn.def("__delitem__", [](py_txn_t& txn, ukv_key_t key) { return set_item(*txn.db_ptr, txn.raw, NULL, key); });

    col.def("__contains__", [](py_column_t& col, ukv_key_t key) {
        return contains_item(*col.db_ptr,
                             col.txn_ptr ? col.txn_ptr->raw : NULL,
                             col.raw,
                             col.txn_ptr ? col.txn_ptr->arena : col.db_ptr->arena,
                             key);
    });
    col.def("__getitem__", [](py_column_t& col, ukv_key_t key) {
        return get_item(*col.db_ptr,
                        col.txn_ptr ? col.txn_ptr->raw : NULL,
                        col.raw,
                        col.txn_ptr ? col.txn_ptr->arena : col.db_ptr->arena,
                        key);
    });
    col.def("__setitem__", [](py_column_t& col, ukv_key_t key, py::bytes const& value) {
        return set_item(*col.db_ptr, col.txn_ptr ? col.txn_ptr->raw : NULL, col.raw, key, &value);
    });
    col.def("__delitem__", [](py_column_t& col, ukv_key_t key) {
        return set_item(*col.db_ptr, col.txn_ptr ? col.txn_ptr->raw : NULL, col.raw, key);
    });

    // Operator overaloads used to access collections
    db.def("__getitem__", [](py_db_t& db, std::string const& collection) {
        auto col = std::make_shared<py_column_t>();
        col->name = collection;
        col->raw = column_named(db, collection);
        col->db_ptr = &db;
        return col;
    });
    txn.def("__getitem__", [](py_txn_t& txn, std::string const& collection) {
        auto col = std::make_shared<py_column_t>();
        col->name = collection;
        col->raw = column_named(*txn.db_ptr, collection);
        col->db_ptr = txn.db_ptr;
        col->txn_ptr = &txn;
        return col;
    });
    txn.def("__delitem__", [](py_db_t& db, std::string const& collection) {
        ukv_error_t error = NULL;
        ukv_column_remove(db.raw, collection.c_str(), &error);
        if (error) [[unlikely]]
            throw make_exception(db, error);
    });
}