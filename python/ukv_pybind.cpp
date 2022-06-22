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
#include <pybind11/numpy.h>

#include "ukv.h"

namespace py = pybind11;

struct py_db_t;
struct py_txn_t;
struct py_collection_t;
struct py_tape_t;

struct py_tape_t {
    ukv_tape_ptr_t ptr = NULL;
    size_t length = 0;
};

struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    ukv_t raw = NULL;
    std::string config;
    py_tape_t tape;

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
    py_tape_t tape;

    py_txn_t() = default;
    py_txn_t(py_txn_t&&) = delete;
    py_txn_t(py_txn_t const&) = delete;

    ~py_txn_t() {

        if (raw)
            ukv_txn_free(db_ptr->raw, raw);
        raw = NULL;
    }
};

struct py_collection_t : public std::enable_shared_from_this<py_collection_t> {
    ukv_collection_t raw = NULL;
    std::string name;

    py_db_t* db_ptr = NULL;
    py_txn_t* txn_ptr = NULL;

    py_collection_t() = default;
    py_collection_t(py_collection_t&&) = delete;
    py_collection_t(py_collection_t const&) = delete;

    ~py_collection_t() {
        if (raw)
            ukv_collection_free(db_ptr->raw, raw);
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

void free_temporary_memory(py_db_t& db, py_tape_t& tape) {
    if (tape.ptr)
        ukv_tape_free(db.raw, tape.ptr, tape.length);
    tape.ptr = NULL;
    tape.length = 0;
}

void close_if_opened(py_db_t& db) {
    if (!db.raw)
        return;

    free_temporary_memory(db, db.tape);
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

void commit(py_txn_t& txn) {
    if (!txn.raw)
        return;

    py_db_t& db = *txn.db_ptr;
    ukv_error_t error = NULL;
    ukv_options_write_t options = NULL;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_txn_commit(txn.raw, options, &error);
    if (error) [[unlikely]]
        throw make_exception(db, error);

    free_temporary_memory(db, txn.tape);
    ukv_txn_free(db.raw, txn.raw);
    txn.raw = NULL;
}

bool contains_item( //
    py_db_t& db,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    py_tape_t& tape,
    ukv_key_t key) {

    ukv_error_t error = NULL;
    ukv_options_read_t options = NULL;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_option_read_lengths(&options, true);
    ukv_read(db.raw, txn_ptr, &key, 1, &collection_ptr, options, &tape.ptr, &tape.length, &error);

    if (error) [[unlikely]]
        throw make_exception(db, error);

    auto lengths = reinterpret_cast<ukv_val_len_t*>(tape.ptr);
    return lengths[0] != 0;
}

std::optional<py::bytes> get_item( //
    py_db_t& db,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    py_tape_t& tape,
    ukv_key_t key) {

    ukv_error_t error = NULL;
    ukv_options_read_t options = NULL;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_read(db.raw, txn_ptr, &key, 1, &collection_ptr, options, &tape.ptr, &tape.length, &error);

    if (error) [[unlikely]]
        throw make_exception(db, error);

    auto lengths = reinterpret_cast<ukv_val_len_t*>(tape.ptr);
    if (!lengths[0])
        return {};

    // To fetch data without copies, there is a hacky way:
    // https://github.com/pybind/pybind11/issues/1236#issuecomment-527730864
    // But in that case we can't guarantee memory alignment, so doing a copy
    // is hard to avoid in Python.
    // https://github.com/pybind/pybind11/blob/a05bc3d2359d12840ef2329d68f613f1a7df9c5d/include/pybind11/pytypes.h#L1474
    // https://docs.python.org/3/c-api/bytes.html
    // https://github.com/python/cpython/blob/main/Objects/bytesobject.c
    auto data = reinterpret_cast<char const*>(lengths + 1);
    return py::bytes {data, lengths[0]};
}

struct buffer_t {
    ~buffer_t() {
        if (initialized)
            PyBuffer_Release(&py);
        initialized = false;
    }
    buffer_t() = default;
    buffer_t(buffer_t&&) = delete;
    buffer_t(buffer_t const&) = delete;

    Py_buffer py;
    bool initialized = false;
};

/**
 * @brief Exports keys into a 2-dimensional preallocated NumPy buffer.
 *        The most performant batch-reading method, ideal for ML.
 *
 * Contrary to most data types exposed by the Python interpreter,
 * buffers are not PyObject pointers but rather simple C structures.
 * This allows them to be created and copied very simply.
 * When a generic wrapper around a buffer is needed, a memoryview
 * object can be created.
 * https://docs.python.org/3/c-api/buffer.html#buffer-structure
 *
 * @param keys      A NumPy array of keys.
 * @param valus     A buffer-protocol object, whose `shape` has 2 dims
 *                  the `itemsize` is just one byte. The first dimensions
 *                  must match with `len(keys)` and the second one must be
 *                  at least 8 for us be able to efficiently reuse that memory.
 *
 * @param values_lengths May be NULL.
 *
 * https://pybind11.readthedocs.io/en/stable/advanced/pycpp/numpy.html#buffer-protocol
 * https://pybind11.readthedocs.io/en/stable/advanced/cast/overview.html#list-of-all-builtin-conversions
 * https://docs.python.org/3/c-api/buffer.html
 */
void export_matrix( //
    py_db_t& db,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    py_tape_t& tape,
    py::handle keys_arr,
    py::handle values_arr,
    py::handle values_lengths_arr,
    uint8_t padding_char = 0) {

    // Check if we are receiving protocol buffers
    PyObject* keys_obj = keys_arr.ptr();
    PyObject* values_obj = values_arr.ptr();
    PyObject* values_lengths_obj = values_lengths_arr.ptr();
    if (!PyObject_CheckBuffer(keys_obj) | !PyObject_CheckBuffer(values_obj) | !PyObject_CheckBuffer(values_lengths_obj))
        throw std::invalid_argument("All arguments must implement the buffer protocol");

    // Take buffer protocol handles
    // Flags can be: https://docs.python.org/3/c-api/buffer.html#readonly-format
    auto output_flags = PyBUF_WRITABLE | PyBUF_ANY_CONTIGUOUS | PyBUF_STRIDED;
    buffer_t keys, values, values_lengths;
    keys.initialized = PyObject_GetBuffer(keys_obj, &keys.py, PyBUF_ANY_CONTIGUOUS) == 0;
    values.initialized = PyObject_GetBuffer(values_obj, &values.py, output_flags) == 0;
    values_lengths.initialized = PyObject_GetBuffer(values_lengths_obj, &values_lengths.py, output_flags) == 0;
    if (!keys.initialized | !values.initialized | !values_lengths.initialized)
        throw std::invalid_argument("Couldn't obtain buffer overviews");
    if (!values.py.shape | !values_lengths.py.shape | !values.py.strides | !values_lengths.py.strides)
        throw std::invalid_argument("Outputs shape wasn't inferred");

    // Validate the format of `keys`
    if (keys.py.itemsize != sizeof(ukv_key_t))
        throw std::invalid_argument("Keys type mismatch");
    if (keys.py.ndim != 1 || !PyBuffer_IsContiguous(&keys.py, 'A'))
        throw std::invalid_argument("Keys must be placed in a coninuous 1 dimensional array");
    if (keys.py.strides[0] != sizeof(ukv_key_t))
        throw std::invalid_argument("Keys can't be strided");
    size_t const keys_count = static_cast<size_t>(keys.py.len / keys.py.itemsize);
    ukv_key_t const* keys_ptr = reinterpret_cast<ukv_key_t const*>(keys.py.buf);

    // Validate the format of `values`
    if (values.py.ndim != 2)
        throw std::invalid_argument("Output tensor must have rank 2");
    if (values.py.itemsize != sizeof(uint8_t))
        throw std::invalid_argument("Output tensor must have single-byte entries");
    if ((values.py.shape[0] <= 0) || values.py.shape[1] <= 0)
        throw std::invalid_argument("Output tensor sides can't be zero");
    if ((values.py.strides[0] <= 0) || values.py.strides[1] <= 0)
        throw std::invalid_argument("Output tensor strides can't be negative");
    if (keys_count != static_cast<size_t>(values.py.shape[0]))
        throw std::invalid_argument("Number of input keys and output slots doesn't match");
    auto outputs_bytes = reinterpret_cast<uint8_t*>(values.py.buf);
    auto outputs_bytes_stride = static_cast<size_t>(values.py.strides[0]);
    auto output_bytes_cap = static_cast<ukv_val_len_t>(values.py.shape[1]);

    // Validate the format of `values_lengths`
    if (values_lengths.py.ndim != 1)
        throw std::invalid_argument("Lengths tensor must have rank 1");
    if (values_lengths.py.itemsize != sizeof(ukv_val_len_t))
        throw std::invalid_argument("Lengths tensor must have 4-byte entries");
    if (values_lengths.py.shape[0] <= 0)
        throw std::invalid_argument("Lengths tensor sides can't be zero");
    if (values_lengths.py.strides[0] <= 0)
        throw std::invalid_argument("Lengths tensor strides can't be negative");
    if (keys_count != static_cast<size_t>(values_lengths.py.shape[0]))
        throw std::invalid_argument("Number of input keys and output slots doesn't match");
    auto outputs_lengths_bytes = reinterpret_cast<uint8_t*>(values_lengths.py.buf);
    auto outputs_lengths_bytes_stride = static_cast<size_t>(values_lengths.py.strides[0]);

    // Perform the read
    [[maybe_unused]] py::gil_scoped_release release;
    ukv_error_t error = NULL;
    ukv_options_read_t options = NULL;
    ukv_option_read_colocated(&options, true);
    ukv_read(db.raw, txn_ptr, keys_ptr, keys_count, &collection_ptr, options, &tape.ptr, &tape.length, &error);

    if (error) [[unlikely]]
        throw make_exception(db, error);

    // Export the data into the matrix
    auto inputs_lengths = reinterpret_cast<ukv_val_len_t*>(tape.ptr);
    auto inputs_bytes = reinterpret_cast<uint8_t const*>(tape.ptr) + keys_count * sizeof(ukv_val_len_t);
    auto skipped_input_bytes = 0ul;
    for (size_t i = 0; i != keys_count; ++i) {

        uint8_t const* input_bytes = inputs_bytes + skipped_input_bytes;
        ukv_val_len_t const input_length = inputs_lengths[i];
        uint8_t* output_bytes = outputs_bytes + outputs_bytes_stride * i;
        ukv_val_len_t& output_length =
            *reinterpret_cast<ukv_val_len_t*>(outputs_lengths_bytes + outputs_lengths_bytes_stride * i);

        size_t count_copy = std::min(output_bytes_cap, input_length);
        size_t count_pads = output_bytes_cap - count_copy;
        std::memcpy(output_bytes, input_bytes, count_copy);
        std::memset(output_bytes + count_copy, padding_char, count_pads);

        output_length = count_copy;
        skipped_input_bytes += input_length;
    }
}

void set_item( //
    py_db_t& db,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    ukv_key_t key,
    py::bytes const* value = NULL) {

    ukv_options_write_t options = NULL;
    ukv_tape_ptr_t ptr = value ? ukv_tape_ptr_t(std::string_view {*value}.data()) : NULL;
    ukv_val_len_t len = value ? static_cast<ukv_val_len_t>(std::string_view {*value}.size()) : 0;
    ukv_error_t error = NULL;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write( //
        db.raw,
        txn_ptr,
        &key,
        1,
        &collection_ptr,
        options,
        ptr,
        &len,
        &error);

    if (error) [[unlikely]]
        throw make_exception(db, error);
}

ukv_collection_t collection_named(py_db_t& db, std::string const& collection_name) {
    ukv_collection_t collection_ptr = NULL;
    ukv_error_t error = NULL;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_collection_upsert( //
        db.raw,
        collection_name.c_str(),
        &collection_ptr,
        &error);

    if (error) [[unlikely]]
        throw make_exception(db, error);
    return collection_ptr;
}

void collection_remove(py_db_t& db, std::string const& collection_name) {
    ukv_error_t error = NULL;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_collection_remove( //
        db.raw,
        collection_name.c_str(),
        &error);

    if (error) [[unlikely]]
        throw make_exception(db, error);
}

PYBIND11_MODULE(ukv, m) {
    m.doc() =
        "Python bindings for Universal Key Value Store abstraction.\n"
        "Supports most basic collection operations, like `dict`.\n"
        "---------------------------------------------\n";

    // Define our primary classes: `DataBase`, `Collection`, `Transaction`
    auto db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase");
    auto col = py::class_<py_collection_t, std::shared_ptr<py_collection_t>>(m, "Collection");
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
        [](py_db_t& db, ukv_key_t key) { return get_item(db, NULL, NULL, db.tape, key); },
        py::arg("key"));

    db.def(
        "get",
        [](py_db_t& db, std::string const& collection, ukv_key_t key) {
            return get_item(db, NULL, collection_named(db, collection), db.tape, key);
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
            return set_item(db, NULL, collection_named(db, collection), key, &value);
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
        [](py_collection_t& col, ukv_key_t key) {
            return get_item(*col.db_ptr,
                            col.txn_ptr ? col.txn_ptr->raw : NULL,
                            col.raw,
                            col.txn_ptr ? col.txn_ptr->tape : col.db_ptr->tape,
                            key);
        },
        py::arg("key"));

    col.def(
        "set",
        [](py_collection_t& col, ukv_key_t key, py::bytes const& value) {
            return set_item(*col.db_ptr, col.txn_ptr ? col.txn_ptr->raw : NULL, col.raw, key, &value);
        },
        py::arg("key"),
        py::arg("value"));
    col.def("clear", [](py_collection_t& col) {
        py_db_t& db = *col.db_ptr;
        std::string name {std::move(col.name)};
        collection_remove(db, name);
        collection_named(db, name);
    });

    // `Transaction`:
    txn.def(py::init([](py_db_t& db, bool begin) {
                auto txn_ptr = std::make_shared<py_txn_t>();
                txn_ptr->db_ptr = &db;
                begin_if_needed(*txn_ptr);
                return txn_ptr;
            }),
            py::arg("db"),
            py::arg("begin") = true);
    txn.def(
        "get",
        [](py_txn_t& txn, ukv_key_t key) { return get_item(*txn.db_ptr, txn.raw, NULL, txn.tape, key); },
        py::arg("key"));

    txn.def(
        "get",
        [](py_txn_t& txn, std::string const& collection, ukv_key_t key) {
            return get_item(*txn.db_ptr, txn.raw, collection_named(*txn.db_ptr, collection), txn.tape, key);
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
            return set_item(*txn.db_ptr, txn.raw, collection_named(*txn.db_ptr, collection), key, &value);
        },
        py::arg("collection"),
        py::arg("key"),
        py::arg("value"));

    // Resource management
    db.def("__enter__", &open_if_closed);
    db.def("close", &close_if_opened);
    txn.def("__enter__", &begin_if_needed);
    txn.def("commit", &commit);

    db.def("__exit__",
           [](py_db_t& db, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
               close_if_opened(db);
           });
    txn.def("__exit__",
            [](py_txn_t& txn, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
                try {
                    commit(txn);
                }
                catch (...) {
                    // We must now propagate this exception upwards:
                    // https://gist.github.com/YannickJadoul/f1fc8db711ed980cf02610277af058e4
                }
            });

    // Operator overaloads used to edit entries
    db.def("__contains__", [](py_db_t& db, ukv_key_t key) { return contains_item(db, NULL, NULL, db.tape, key); });
    db.def("__getitem__", [](py_db_t& db, ukv_key_t key) { return get_item(db, NULL, NULL, db.tape, key); });
    db.def("__setitem__",
           [](py_db_t& db, ukv_key_t key, py::bytes const& value) { return set_item(db, NULL, NULL, key, &value); });
    db.def("__delitem__", [](py_db_t& db, ukv_key_t key) { return set_item(db, NULL, NULL, key); });

    txn.def("__contains__",
            [](py_txn_t& txn, ukv_key_t key) { return contains_item(*txn.db_ptr, txn.raw, NULL, txn.tape, key); });
    txn.def("__getitem__",
            [](py_txn_t& txn, ukv_key_t key) { return get_item(*txn.db_ptr, txn.raw, NULL, txn.tape, key); });
    txn.def("__setitem__", [](py_txn_t& txn, ukv_key_t key, py::bytes const& value) {
        return set_item(*txn.db_ptr, txn.raw, NULL, key, &value);
    });
    txn.def("__delitem__", [](py_txn_t& txn, ukv_key_t key) { return set_item(*txn.db_ptr, txn.raw, NULL, key); });

    col.def("__contains__", [](py_collection_t& col, ukv_key_t key) {
        return contains_item(*col.db_ptr,
                             col.txn_ptr ? col.txn_ptr->raw : NULL,
                             col.raw,
                             col.txn_ptr ? col.txn_ptr->tape : col.db_ptr->tape,
                             key);
    });
    col.def("__getitem__", [](py_collection_t& col, ukv_key_t key) {
        return get_item(*col.db_ptr,
                        col.txn_ptr ? col.txn_ptr->raw : NULL,
                        col.raw,
                        col.txn_ptr ? col.txn_ptr->tape : col.db_ptr->tape,
                        key);
    });
    col.def("__setitem__", [](py_collection_t& col, ukv_key_t key, py::bytes const& value) {
        return set_item(*col.db_ptr, col.txn_ptr ? col.txn_ptr->raw : NULL, col.raw, key, &value);
    });
    col.def("__delitem__", [](py_collection_t& col, ukv_key_t key) {
        return set_item(*col.db_ptr, col.txn_ptr ? col.txn_ptr->raw : NULL, col.raw, key);
    });

    // Operator overaloads used to access collections
    db.def("__getitem__", [](py_db_t& db, std::string const& collection) {
        auto col = std::make_shared<py_collection_t>();
        col->name = collection;
        col->raw = collection_named(db, collection);
        col->db_ptr = &db;
        return col;
    });
    txn.def("__getitem__", [](py_txn_t& txn, std::string const& collection) {
        auto col = std::make_shared<py_collection_t>();
        col->name = collection;
        col->raw = collection_named(*txn.db_ptr, collection);
        col->db_ptr = txn.db_ptr;
        col->txn_ptr = &txn;
        return col;
    });
    db.def("__delitem__", [](py_db_t& db, std::string const& collection) { collection_remove(db, collection); });

    // Batch Matrix Operations
    db.def(
        "fill_matrix",
        [](py_db_t& db, py::handle keys, py::handle values, py::handle values_lengths, uint8_t padding_char = 0) {
            return export_matrix(db, NULL, NULL, db.tape, keys, values, values_lengths, padding_char);
        },
        py::arg("keys"),
        py::arg("values"),
        py::arg("values_lengths"),
        py::arg("padding") = 0);
    col.def(
        "fill_matrix",
        [](py_collection_t& col,
           py::handle keys,
           py::handle values,
           py::handle values_lengths,
           uint8_t padding_char = 0) {
            return export_matrix(*col.db_ptr,
                                 col.txn_ptr ? col.txn_ptr->raw : NULL,
                                 col.raw,
                                 col.txn_ptr ? col.txn_ptr->tape : col.db_ptr->tape,
                                 keys,
                                 values,
                                 values_lengths,
                                 padding_char);
        },
        py::arg("keys"),
        py::arg("values"),
        py::arg("values_lengths"),
        py::arg("padding") = 0);
    txn.def(
        "fill_matrix",
        [](py_txn_t& txn, py::handle keys, py::handle values, py::handle values_lengths, uint8_t padding_char = 0) {
            return export_matrix(*txn.db_ptr, txn.raw, NULL, txn.tape, keys, values, values_lengths, padding_char);
        },
        py::arg("keys"),
        py::arg("values"),
        py::arg("values_lengths"),
        py::arg("padding") = 0);
}