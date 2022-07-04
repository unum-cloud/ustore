/**
 * @brief Python bindings for Unums Key Value Store.
 *
 * @section Interface
 *
 * Primary DataBase Methods:
 *      * get(collection?, key, default?) ~ Single Read
 *      * set(collection?, key, value, default?) ~ Single Insert
 *      * __in__(key) ~ Single & Batch Contains
 *      * __getitem__(key: int) ~ Value Lookup
 *      * __setitem__(key: int, value) ~ Value Upserts
 *      * __getitem__(collection: str) ~ Sub-Collection Lookup
 *      * clear() ~ Removes all items
 *      * TODO: pop(key, default?) ~ Removes the key in and returns its value.
 *      * TODO: update(mapping) ~ Batch Insert/Put
 *
 * Additional Batch Methods:
 *      * TODO: get_matrix(collection?, keys, max_length: int, padding: byte)
 *      * TODO: get_table(collection?, keys, field_ids)
 *
 * Intentionally not implemented:
 *      * __len__() ~ It's hard to consistently estimate the collection.
 *      * popitem() ~ We can't guarantee Last-In First-Out semantics.
 *      * setdefault(key[, default]) ~ As default values are useless in DBs.
 *
 * Similarly, those operations are supported at "collection-level", not just
 * DataBase level.
 *
 * Full @c `dict` API:
 * https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
 * https://python-reference.readthedocs.io/en/latest/docs/dict/
 * https://docs.python.org/3/tutorial/datastructures.html#dictionaries
 * https://docs.python.org/3/c-api/dict.html
 *
 * @section Understanding Python Strings
 *
 * Most dynamic allocations in CPython are done via `PyObject_Malloc`,
 * `PyMem_Malloc`, `PyMem_Calloc`, so understanding the memory layout
 * is as easy as searching the git repo for those function calls.
 * What you will see, is that metadata is generally stored in the same allocated
 * region, as a prefix, as in most dynamically typed or List-oriented systems.
 * It's identical for `PyBytes_FromStringAndSize`, `PyUnicode_New`, `PyList_New`.
 *
 * Same way for lists of lists. The `PyListObject` stores a vector of pointers
 * to it's internal entries in a member @p `ob_item`. So we forward thatlist directly
 * to our C bindings, checking beforehand, that the internal objects are either strings,
 * byte-strings, or NumPy arrays.
 *
 * > PEP 393 – Flexible String Representation
 *   Describes the 3 possible memory layouts, including the @c `PyASCIIObject`,
 *   the @c `PyCompactUnicodeObject` and the @c `PyUnicodeObject`.
 *   https://peps.python.org/pep-0393/
 *   https://docs.python.org/3/c-api/unicode.html
 *
 */

#include "pybind.hpp"

using namespace unum::ukv;
using namespace unum;

std::shared_ptr<py_txn_t> begin_if_needed(py_txn_t& py_txn) {

    [[maybe_unused]] py::gil_scoped_release release;
    auto error = py_txn.native.reset();
    error.throw_unhandled();
    return py_txn.shared_from_this();
}

void commit(py_txn_t& py_txn) {

    [[maybe_unused]] py::gil_scoped_release release;
    auto error = py_txn.native.commit();
    error.throw_unhandled();
}

bool contains_item( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    managed_tape_t& tape,
    ukv_key_t key) {

    ukv::error_t error;
    ukv_options_t options = ukv_option_read_lengths_k;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_read( //
        db_ptr,
        txn_ptr,
        &collection_ptr,
        0,
        &key,
        1,
        0,
        options,
        tape.internal_memory(),
        tape.internal_capacity(),
        error.internal_cptr());
    error.throw_unhandled();

    auto lengths = reinterpret_cast<ukv_val_len_t*>(*tape.internal_memory());
    return lengths[0] != 0;
}

std::optional<py::bytes> get_item( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    managed_tape_t& tape,
    ukv_key_t key) {

    ukv::error_t error;
    ukv_options_t options = ukv_options_default_k;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_read( //
        db_ptr,
        txn_ptr,
        &collection_ptr,
        0,
        &key,
        1,
        0,
        options,
        tape.internal_memory(),
        tape.internal_capacity(),
        error.internal_cptr());
    error.throw_unhandled();

    auto lengths = reinterpret_cast<ukv_val_len_t*>(*tape.internal_memory());
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
 * @param values_lengths May be nullptr.
 *
 * https://pybind11.readthedocs.io/en/stable/advanced/pycpp/numpy.html#buffer-protocol
 * https://pybind11.readthedocs.io/en/stable/advanced/cast/overview.html#list-of-all-builtin-conversions
 * https://docs.python.org/3/c-api/buffer.html
 */
void export_matrix( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    managed_tape_t& tape,
    py::handle keys_arr,
    py::handle values_arr,
    py::handle values_lengths_arr,
    std::uint8_t padding_char = 0) {

    // Check if we are receiving protocol buffers
    PyObject* keys_obj = keys_arr.ptr();
    PyObject* values_obj = values_arr.ptr();
    PyObject* values_lengths_obj = values_lengths_arr.ptr();
    if (!PyObject_CheckBuffer(keys_obj) | !PyObject_CheckBuffer(values_obj) | !PyObject_CheckBuffer(values_lengths_obj))
        throw std::invalid_argument("All arguments must implement the buffer protocol");

    // Take buffer protocol handles
    // Flags can be: https://docs.python.org/3/c-api/buffer.html#readonly-format
    auto output_flags = PyBUF_WRITABLE | PyBUF_ANY_CONTIGUOUS | PyBUF_STRIDED;
    py_buffer_t keys, values, values_lengths;
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
    ukv_size_t const keys_count = static_cast<ukv_size_t>(keys.py.len / keys.py.itemsize);
    ukv_key_t const* keys_ptr = reinterpret_cast<ukv_key_t const*>(keys.py.buf);

    // Validate the format of `values`
    if (values.py.ndim != 2)
        throw std::invalid_argument("Output tensor must have rank 2");
    if (values.py.itemsize != sizeof(std::uint8_t))
        throw std::invalid_argument("Output tensor must have single-byte entries");
    if ((values.py.shape[0] <= 0) || values.py.shape[1] <= 0)
        throw std::invalid_argument("Output tensor sides can't be zero");
    if ((values.py.strides[0] <= 0) || values.py.strides[1] <= 0)
        throw std::invalid_argument("Output tensor strides can't be negative");
    if (keys_count != static_cast<ukv_size_t>(values.py.shape[0]))
        throw std::invalid_argument("Number of input keys and output slots doesn't match");
    auto outputs_bytes = reinterpret_cast<std::uint8_t*>(values.py.buf);
    auto outputs_bytes_stride = static_cast<std::size_t>(values.py.strides[0]);
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
    if (keys_count != static_cast<ukv_size_t>(values_lengths.py.shape[0]))
        throw std::invalid_argument("Number of input keys and output slots doesn't match");
    auto outputs_lengths_bytes = reinterpret_cast<std::uint8_t*>(values_lengths.py.buf);
    auto outputs_lengths_bytes_stride = static_cast<std::size_t>(values_lengths.py.strides[0]);

    // Perform the read
    [[maybe_unused]] py::gil_scoped_release release;
    ukv::error_t error;
    ukv_options_t options = ukv_options_default_k;
    ukv_read( //
        db_ptr,
        txn_ptr,
        &collection_ptr,
        0,
        keys_ptr,
        keys_count,
        sizeof(ukv_key_t),
        options,
        tape.internal_memory(),
        tape.internal_capacity(),
        error.internal_cptr());

    error.throw_unhandled();

    // Export the data into the matrix
    taped_values_view_t inputs = tape;
    tape_iterator_t input_it = inputs.begin();
    auto skipped_input_bytes = 0ul;
    for (ukv_size_t i = 0; i != keys_count; ++i, ++input_it) {
        value_view_t input = *input_it;
        auto input_bytes = reinterpret_cast<std::uint8_t const*>(input.begin());
        auto input_length = static_cast<ukv_val_len_t const>(input.size());
        std::uint8_t* output_bytes = outputs_bytes + outputs_bytes_stride * i;
        ukv_val_len_t& output_length =
            *reinterpret_cast<ukv_val_len_t*>(outputs_lengths_bytes + outputs_lengths_bytes_stride * i);

        std::size_t count_copy = std::min(output_bytes_cap, input_length);
        std::size_t count_pads = output_bytes_cap - count_copy;
        std::memcpy(output_bytes, input_bytes, count_copy);
        std::memset(output_bytes + count_copy, padding_char, count_pads);

        output_length = count_copy;
        skipped_input_bytes += input_length;
    }
}

void set_item( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t collection_ptr,
    ukv_key_t key,
    py::bytes const* value = nullptr) {

    ukv_options_t options = ukv_options_default_k;
    ukv_tape_ptr_t ptr = value ? ukv_tape_ptr_t(std::string_view {*value}.data()) : nullptr;
    ukv_val_len_t len = value ? static_cast<ukv_val_len_t>(std::string_view {*value}.size()) : 0;
    ukv::error_t error;
    ukv_val_len_t offset_in_val = 0;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write( //
        db_ptr,
        txn_ptr,
        &collection_ptr,
        0,
        &key,
        1,
        0,
        &ptr,
        0,
        &offset_in_val,
        0,
        &len,
        0,
        options,
        error.internal_cptr());

    error.throw_unhandled();
}

void ukv::wrap_database(py::module& m) {

    // Define our primary classes: `DataBase`, `Collection`, `Transaction`
    auto py_db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase", py::module_local());
    auto py_col = py::class_<py_col_t, std::shared_ptr<py_col_t>>(m, "Collection", py::module_local());
    auto py_txn = py::class_<py_txn_t, std::shared_ptr<py_txn_t>>(m, "Transaction", py::module_local());

    // Define `DataBase`
    py_db.def( //
        py::init([](std::string const& config) {
            db_t db;
            auto error = db.open(config);
            error.throw_unhandled();
            session_t session = db.session();
            return std::make_shared<py_db_t>(std::move(db), std::move(session), config);
        }),
        py::arg("config") = "");

    py_db.def(
        "get",
        [](py_db_t& py_db, ukv_key_t key) {
            return get_item(py_db.native, nullptr, nullptr, py_db.session.tape(), key);
        },
        py::arg("key"));

    py_db.def(
        "get",
        [](py_db_t& py_db, std::string const& collection, ukv_key_t key) {
            auto maybe_col = py_db.native[collection];
            maybe_col.throw_unhandled();
            return get_item(py_db.native, nullptr, *maybe_col, py_db.session.tape(), key);
        },
        py::arg("collection"),
        py::arg("key"));

    py_db.def(
        "set",
        [](py_db_t& py_db, ukv_key_t key, py::bytes const& value) {
            return set_item(py_db.native, nullptr, nullptr, key, &value);
        },
        py::arg("key"),
        py::arg("value"));

    py_db.def(
        "set",
        [](py_db_t& py_db, std::string const& collection, ukv_key_t key, py::bytes const& value) {
            auto maybe_col = py_db.native[collection];
            maybe_col.throw_unhandled();
            return set_item(py_db.native, nullptr, *maybe_col, key, &value);
        },
        py::arg("collection"),
        py::arg("key"),
        py::arg("value"));
    py_col.def("clear", [](py_db_t& py_db) {
        // TODO:
    });

    // Define `Collection`s member method, without defining any external constructors
    py_col.def(
        "get",
        [](py_col_t& py_col, ukv_key_t key) {
            return get_item(py_col.db_ptr->native,
                            py_col.txn_ptr->native,
                            py_col.native,
                            py_col.txn_ptr ? py_col.txn_ptr->native.tape() : py_col.db_ptr->session.tape(),
                            key);
        },
        py::arg("key"));

    py_col.def(
        "set",
        [](py_col_t& py_col, ukv_key_t key, py::bytes const& value) {
            return set_item(py_col.db_ptr->native, py_col.txn_ptr->native, py_col.native, key, &value);
        },
        py::arg("key"),
        py::arg("value"));
    py_col.def("clear", [](py_col_t& py_col) {
        db_t& db = py_col.db_ptr->native;
        db.remove(py_col.name).throw_unhandled();
        auto maybe_col = db[py_col.name];
        maybe_col.throw_unhandled();
        py_col.native = *std::move(maybe_col);
    });

    // `Transaction`:
    py_txn.def( //
        py::init([](py_db_t& py_db, bool begin) {
            auto db_ptr = py_db.shared_from_this();
            auto maybe_txn = py_db.session.transact();
            maybe_txn.throw_unhandled();
            return std::make_shared<py_txn_t>(std::move(db_ptr), *std::move(maybe_txn));
        }),
        py::arg("db"),
        py::arg("begin") = true);
    py_txn.def(
        "get",
        [](py_txn_t& py_txn, ukv_key_t key) {
            return get_item(py_txn.db_ptr->native, py_txn.native, nullptr, py_txn.native.tape(), key);
        },
        py::arg("key"));

    py_txn.def(
        "get",
        [](py_txn_t& py_txn, std::string const& collection, ukv_key_t key) {
            auto maybe_col = py_txn.db_ptr->native[collection];
            maybe_col.throw_unhandled();
            return get_item(py_txn.db_ptr->native, py_txn.native, *maybe_col, py_txn.native.tape(), key);
        },
        py::arg("collection"),
        py::arg("key"));

    py_txn.def(
        "set",
        [](py_txn_t& py_txn, ukv_key_t key, py::bytes const& value) {
            return set_item(py_txn.db_ptr->native, py_txn.native, nullptr, key, &value);
        },
        py::arg("key"),
        py::arg("value"));

    py_txn.def(
        "set",
        [](py_txn_t& py_txn, std::string const& collection, ukv_key_t key, py::bytes const& value) {
            auto maybe_col = py_txn.db_ptr->native[collection];
            maybe_col.throw_unhandled();
            return set_item(py_txn.db_ptr->native, py_txn.native, *maybe_col, key, &value);
        },
        py::arg("collection"),
        py::arg("key"),
        py::arg("value"));

    // Resource management
    py_txn.def("__enter__", &begin_if_needed);
    py_txn.def("commit", &commit);

    py_db.def("__enter__", [](py_db_t& py_db) {
        if (py_db.native)
            return;
        auto error = py_db.native.open(py_db.config);
        error.throw_unhandled();
    });
    py_db.def("close", [](py_db_t& py_db) { py_db.native.close(); });

    py_db.def( //
        "__exit__",
        [](py_db_t& py_db, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
            py_db.native.close();
        });
    py_txn.def(
        "__exit__",
        [](py_txn_t& py_txn, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
            try {
                commit(py_txn);
            }
            catch (...) {
                // We must now propagate this exception upwards:
                // https://gist.github.com/YannickJadoul/f1fc8db711ed980cf02610277af058e4
            }
        });

    // Operator overaloads used to edit entries
    py_db.def("__contains__", [](py_db_t& py_db, ukv_key_t key) {
        return contains_item(py_db.native, nullptr, nullptr, py_db.session.tape(), key);
    });
    py_db.def("__getitem__", [](py_db_t& py_db, ukv_key_t key) {
        return get_item(py_db.native, nullptr, nullptr, py_db.session.tape(), key);
    });
    py_db.def("__setitem__", [](py_db_t& py_db, ukv_key_t key, py::bytes const& value) {
        return set_item(py_db.native, nullptr, nullptr, key, &value);
    });
    py_db.def("__delitem__",
              [](py_db_t& py_db, ukv_key_t key) { return set_item(py_db.native, nullptr, nullptr, key); });

    py_txn.def("__contains__", [](py_txn_t& py_txn, ukv_key_t key) {
        return contains_item(py_txn.db_ptr->native, py_txn.native, nullptr, py_txn.native.tape(), key);
    });
    py_txn.def("__getitem__", [](py_txn_t& py_txn, ukv_key_t key) {
        return get_item(py_txn.db_ptr->native, py_txn.native, nullptr, py_txn.native.tape(), key);
    });
    py_txn.def("__setitem__", [](py_txn_t& py_txn, ukv_key_t key, py::bytes const& value) {
        return set_item(py_txn.db_ptr->native, py_txn.native, nullptr, key, &value);
    });
    py_txn.def("__delitem__", [](py_txn_t& py_txn, ukv_key_t key) {
        return set_item(py_txn.db_ptr->native, py_txn.native, nullptr, key);
    });

    py_col.def("__contains__", [](py_col_t& py_col, ukv_key_t key) {
        return contains_item(py_col.db_ptr->native,
                             py_col.txn_ptr->native,
                             py_col.native,
                             py_col.txn_ptr ? py_col.txn_ptr->native.tape() : py_col.db_ptr->session.tape(),
                             key);
    });
    py_col.def("__getitem__", [](py_col_t& py_col, ukv_key_t key) {
        return get_item(py_col.db_ptr->native,
                        py_col.txn_ptr->native,
                        py_col.native,
                        py_col.txn_ptr ? py_col.txn_ptr->native.tape() : py_col.db_ptr->session.tape(),
                        key);
    });
    py_col.def("__setitem__", [](py_col_t& py_col, ukv_key_t key, py::bytes const& value) {
        return set_item(py_col.db_ptr->native, py_col.txn_ptr->native, py_col.native, key, &value);
    });
    py_col.def("__delitem__", [](py_col_t& py_col, ukv_key_t key) {
        return set_item(py_col.db_ptr->native, py_col.txn_ptr->native, py_col.native, key);
    });

    // Operator overaloads used to access collections
    py_db.def("__getitem__", [](py_db_t& py_db, std::string const& collection) {
        auto maybe_col = py_db.native[collection];
        maybe_col.throw_unhandled();

        auto py_col = std::make_shared<py_col_t>();
        py_col->name = collection;
        py_col->native = *std::move(maybe_col);
        py_col->db_ptr = py_db.shared_from_this();
        return py_col;
    });
    py_txn.def("__getitem__", [](py_txn_t& py_txn, std::string const& collection) {
        auto maybe_col = py_txn.db_ptr->native[collection];
        maybe_col.throw_unhandled();

        auto py_col = std::make_shared<py_col_t>();
        py_col->name = collection;
        py_col->native = *std::move(maybe_col);
        py_col->db_ptr = py_txn.db_ptr;
        py_col->txn_ptr = py_txn.shared_from_this();
        return py_col;
    });
    py_db.def("__delitem__", [](py_db_t& py_db, std::string const& collection) {
        auto error = py_db.native.remove(collection);
        error.throw_unhandled();
    });

    // Batch Matrix Operations
    py_db.def(
        "fill_matrix",
        [](py_db_t& py_db,
           py::handle keys,
           py::handle values,
           py::handle values_lengths,
           std::uint8_t padding_char = 0) {
            return export_matrix(py_db.native,
                                 nullptr,
                                 nullptr,
                                 py_db.session.tape(),
                                 keys,
                                 values,
                                 values_lengths,
                                 padding_char);
        },
        py::arg("keys"),
        py::arg("values"),
        py::arg("values_lengths"),
        py::arg("padding") = 0);
    py_col.def(
        "fill_matrix",
        [](py_col_t& py_col,
           py::handle keys,
           py::handle values,
           py::handle values_lengths,
           std::uint8_t padding_char = 0) {
            return export_matrix(py_col.db_ptr->native,
                                 py_col.txn_ptr->native,
                                 py_col.native,
                                 py_col.txn_ptr ? py_col.txn_ptr->native.tape() : py_col.db_ptr->session.tape(),
                                 keys,
                                 values,
                                 values_lengths,
                                 padding_char);
        },
        py::arg("keys"),
        py::arg("values"),
        py::arg("values_lengths"),
        py::arg("padding") = 0);
    py_txn.def(
        "fill_matrix",
        [](py_txn_t& py_txn,
           py::handle keys,
           py::handle values,
           py::handle values_lengths,
           std::uint8_t padding_char = 0) {
            return export_matrix(py_txn.db_ptr->native,
                                 py_txn.native,
                                 nullptr,
                                 py_txn.native.tape(),
                                 keys,
                                 values,
                                 values_lengths,
                                 padding_char);
        },
        py::arg("keys"),
        py::arg("values"),
        py::arg("values_lengths"),
        py::arg("padding") = 0);
}
