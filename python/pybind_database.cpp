/**
 * @brief Python bindings for UKV.
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
 *      * fill_tensor(collection?, keys, max_length: int, padding: byte)
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
 * to it's internal entries in a member @p `ob_item`. So we forward that list directly
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

#include <vector>
#include <utility>
#include <algorithm>

#include <Python.h>

#include "pybind.hpp"

using namespace unum::ukv;
using namespace unum;

std::shared_ptr<py_txn_t> begin_if_needed(py_txn_t& py_txn) {
    if (py_txn.native)
        return py_txn.shared_from_this();

    [[maybe_unused]] py::gil_scoped_release release;
    py_txn.native.reset().throw_unhandled();
    return py_txn.shared_from_this();
}

void commit_txn(py_txn_t& py_txn) {

    [[maybe_unused]] py::gil_scoped_release release;
    py_txn.native.commit().throw_unhandled();
}

struct py_bin_req_t {
    ukv_key_t key = ukv_key_unknown_k;
    ukv_str_view_t field = nullptr;
    ukv_val_ptr_t ptr = nullptr;
    ukv_val_len_t off = 0;
    ukv_val_len_t len = 0;
};

template <typename at>
at& at_growing(std::vector<at>& vec, std::size_t i) {
    if (i > vec.size())
        vec.resize(i + 1);
    return vec[i];
}

void populate_key(PyObject* obj, py_bin_req_t& req) {
    if (PyLong_Check(obj))
        req.key = static_cast<ukv_key_t>(PyLong_AsUnsignedLong(obj));
    else
        throw std::invalid_argument("Keys must be integers");
}

void populate_val(PyObject* obj, py_bin_req_t& req) {
    if (PyBytes_Check(obj)) {
        char* buffer = nullptr;
        Py_ssize_t length = 0;
        PyBytes_AsStringAndSize(obj, &buffer, &length);
        req.ptr = reinterpret_cast<ukv_val_ptr_t>(buffer);
        req.len = static_cast<ukv_val_len_t>(length);
    }
    else if (obj == Py_None) {
        // Means the object must be deleted
        req.ptr = nullptr;
        req.len = 0;
    }
    else
        throw std::invalid_argument("Value must be representable as a byte array");
}

/**
 * @brief Exports Python-native keys into what we can use.
 * @param obj Must have multiple keys in it, not just an integer.
 */
void populate_keys(PyObject* obj, std::vector<py_bin_req_t>& reqs) {

    bool is_buffer = PyObject_CheckBuffer(obj);
    bool is_seq = is_pyseq(obj);
    if (!is_buffer && !is_seq)
        throw std::invalid_argument("Keys must be a `tuple`, `list`, an iterable object or a 1D Buffer-protocol");

    std::size_t i = 0;
    if (is_seq)
        return scan_pyseq(obj, [&](PyObject* obj) { populate_key(obj, at_growing(reqs, i)), ++i; });

    if (is_buffer) {
        auto [buf, range] = strided_array<void const>(obj);
        auto fmt_len = std::strlen(buf.py.format);
        if (fmt_len != 1)
            throw std::invalid_argument("Unsupported keys scalar type");

        auto export_as = [&](auto type_example) {
            using scalar_t = decltype(type_example);
            auto begin = reinterpret_cast<scalar_t const*>(range.data());
            auto typed = strided_range_gt<scalar_t const>(begin, range.stride(), range.size());
            for (; i != typed.size(); ++i)
                at_growing(reqs, i).key = static_cast<ukv_key_t>(typed[i]);
        };

        switch (buf.py.format[0]) {
        case format_code_gt<short>::value: export_as((short)(0)); break;
        case format_code_gt<unsigned short>::value: export_as((unsigned short)(0)); break;
        case format_code_gt<int>::value: export_as((int)(0)); break;
        case format_code_gt<unsigned int>::value: export_as((unsigned int)(0)); break;
        case format_code_gt<long>::value: export_as((long)(0)); break;
        case format_code_gt<unsigned long>::value: export_as((unsigned long)(0)); break;
        case format_code_gt<long long>::value: export_as((long long)(0)); break;
        case format_code_gt<unsigned long long>::value: export_as((unsigned long long)(0)); break;
        default: throw std::invalid_argument("Unsupported keys scalar type"); break;
        }
    }
}

/**
 * @brief Exports Python-native values into what we can use.
 * @param obj Must have multiple binary values in it, not just one.
 */
void populate_vals(PyObject* obj, std::vector<py_bin_req_t>& reqs) {

    bool is_buffer = PyObject_CheckBuffer(obj);
    bool is_seq = is_pyseq(obj);
    if (!is_buffer && !is_seq)
        throw std::invalid_argument("Values must be a `tuple`, `list`, an iterable object or a 2D Buffer-protocol");

    std::size_t i = 0;
    if (is_seq)
        return scan_pyseq(obj, [&](PyObject* obj) { populate_val(obj, at_growing(reqs, i)), ++i; });

    if (is_buffer) {
        // auto [buf, range] = strided_matrix<void const>(obj);
        // throw_not_implemented();
    }
}

#pragma region Writes

/**
 * @param key_py Must be a `PyLong`.
 * @param val_py Can be anything.
 */
void py_write_one( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* key_py,
    PyObject* val_py) {

    status_t status;
    py_bin_req_t req;
    populate_key(key_py, req);
    populate_val(val_py, req);
    ukv_options_t options = ukv_options_default_k;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(db_ptr,
              txn_ptr,
              1,
              &col_ptr,
              0,
              &req.key,
              0,
              &req.ptr,
              0,
              &req.off,
              0,
              &req.len,
              0,
              options,
              arena.member_ptr(),
              status.member_ptr());
    status.throw_unhandled();
}

void py_write_many( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* keys_py,
    PyObject* vals_py) {

    std::vector<py_bin_req_t> reqs;
    populate_keys(keys_py, reqs);
    if (vals_py != Py_None)
        populate_vals(vals_py, reqs);

    status_t status;
    ukv_size_t step = sizeof(py_bin_req_t);
    ukv_options_t options = ukv_options_default_k;

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(db_ptr,
              txn_ptr,
              static_cast<ukv_size_t>(reqs.size()),
              &col_ptr,
              0,
              &reqs[0].key,
              step,
              &reqs[0].ptr,
              step,
              &reqs[0].off,
              step,
              &reqs[0].len,
              step,
              options,
              arena.member_ptr(),
              status.member_ptr());
    status.throw_unhandled();
}

void py_write( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* key_py,
    PyObject* val_py = Py_None) {

    auto is_single = PyLong_Check(key_py);
    auto func = is_single ? &py_write_one : &py_write_many;
    return func(db_ptr, txn_ptr, col_ptr, arena, key_py, val_py);
}

void py_update( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* dict_py) {

    status_t status;
    ukv_size_t step = sizeof(py_bin_req_t);
    ukv_options_t options = ukv_options_default_k;

    std::vector<py_bin_req_t> reqs;
    reqs.reserve(PyDict_Size(dict_py));

    std::size_t i = 0;
    scan_pydict(dict_py, [&](PyObject* key_obj, PyObject* val_obj) {
        py_bin_req_t& req = reqs[i];
        populate_key(key_obj, req);
        populate_val(val_obj, req);
        ++i;
    });

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(db_ptr,
              txn_ptr,
              static_cast<ukv_size_t>(reqs.size()),
              &col_ptr,
              0,
              &reqs[0].key,
              step,
              &reqs[0].ptr,
              step,
              &reqs[0].off,
              step,
              &reqs[0].len,
              step,
              options,
              arena.member_ptr(),
              status.member_ptr());
    status.throw_unhandled();
}

#pragma region Reads

py::object py_read_one( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* key_py) {

    status_t status;
    ukv_key_t key = static_cast<ukv_key_t>(PyLong_AsUnsignedLong(key_py));
    ukv_options_t options = ukv_options_default_k;
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_lengths = nullptr;

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(db_ptr,
                 txn_ptr,
                 1,
                 &col_ptr,
                 0,
                 &key,
                 0,
                 options,
                 &found_lengths,
                 &found_values,
                 arena.member_ptr(),
                 status.member_ptr());
        status.throw_unhandled();
    }

    // To fetch data without copies, there is a hacky way:
    // https://github.com/pybind/pybind11/issues/1236#issuecomment-527730864
    // But in that case we can't guarantee memory alignment, so doing a copy
    // is hard to avoid in Python.
    // https://github.com/pybind/pybind11/blob/a05bc3d2359d12840ef2329d68f613f1a7df9c5d/include/pybind11/pytypes.h#L1474
    // https://docs.python.org/3/c-api/bytes.html
    // https://github.com/python/cpython/blob/main/Objects/bytesobject.c
    tape_iterator_t tape_it {found_lengths, found_values};
    value_view_t val = *tape_it;
    PyObject* obj_ptr = val ? PyBytes_FromStringAndSize(val.c_str(), val.size()) : Py_None;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

py::object py_read_many( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* keys_py) {

    status_t status;
    std::vector<py_bin_req_t> reqs;
    ukv_options_t options = ukv_options_default_k;
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    populate_keys(keys_py, reqs);
    ukv_size_t step = sizeof(py_bin_req_t);

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(db_ptr,
                 txn_ptr,
                 1,
                 &col_ptr,
                 0,
                 &reqs[0].key,
                 step,
                 options,
                 &found_lengths,
                 &found_values,
                 arena.member_ptr(),
                 status.member_ptr());
        status.throw_unhandled();
    }

    tape_iterator_t tape_it {found_lengths, found_values};
    PyObject* tuple_ptr = PyTuple_New(reqs.size());
    for (std::size_t i = 0; i != reqs.size(); ++i, ++tape_it) {
        value_view_t val = *tape_it;
        PyObject* obj_ptr = val ? PyBytes_FromStringAndSize(val.c_str(), val.size()) : Py_None;
        PyTuple_SetItem(tuple_ptr, i, obj_ptr);
    }
    return py::reinterpret_borrow<py::object>(tuple_ptr);
}

py::object py_measure_many( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* keys_py) {
    return {};
}

py::object py_measure_one( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* key_py) {
    return {};
}

py::object py_read( //
    ukv_t db_ptr,
    ukv_txn_t txn_ptr,
    ukv_collection_t col_ptr,
    arena_t& arena,
    PyObject* key_py) {

    auto is_single = PyLong_Check(key_py);
    auto func = is_single ? &py_read_one : &py_read_many;
    return func(db_ptr, txn_ptr, col_ptr, arena, key_py);
}

py::object punned_collection( //
    py_db_t* py_db_ptr,
    py_txn_t* py_txn_ptr,
    std::string const& collection,
    ukv_format_t format) {
    db_t& db = py_db_ptr->native;
    txn_t& txn = py_txn_ptr->native;

    auto maybe_col = !py_txn_ptr ? db.collection(collection.c_str()) : txn.collection(collection.c_str());
    maybe_col.throw_unhandled();
    maybe_col->as(format);

    if (format == ukv_format_graph_k) {
        auto py_graph = std::make_shared<py_graph_t>();
        py_graph->db_ptr = py_db_ptr->shared_from_this();
        py_graph->index = *std::move(maybe_col);
        return py::cast(py_graph);
    }
    else {
        auto py_col = std::make_shared<py_col_t>();
        py_col->name = collection;
        py_col->db_ptr = py_db_ptr->shared_from_this();
        py_col->native = *std::move(maybe_col);
        return py::cast(py_col);
    }
}

void ukv::wrap_database(py::module& m) {
    // Define our primary classes: `DataBase`, `Collection`, `Transaction`
    auto py_db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase", py::module_local());
    auto py_col = py::class_<py_col_t, std::shared_ptr<py_col_t>>(m, "Collection", py::module_local());
    auto py_txn = py::class_<py_txn_t, std::shared_ptr<py_txn_t>>(m, "Transaction", py::module_local());

    py::enum_<ukv_format_t>(m, "Format", py::module_local())
        .value("Binary", ukv_format_binary_k)
        .value("Graph", ukv_format_graph_k)
        .value("MsgPack", ukv_format_msgpack_k)
        .value("JSON", ukv_format_json_k)
        .value("BSON", ukv_format_bson_k)
        .value("CBOR", ukv_format_cbor_k)
        .value("UBJSON", ukv_format_ubjson_k);

    // Define `DataBase`
    py_db.def( //
        py::init([](std::string const& config, bool open) {
            db_t db;
            if (open)
                db.open(config).throw_unhandled();
            auto py_db_ptr = std::make_shared<py_db_t>(std::move(db), config);
            return py_db_ptr;
        }),
        py::arg("config") = "",
        py::arg("open") = true);

    // Redirect `Collection`-level calls from `DataBase` and `Transaction` to the default collection
    // py_db.def("__getattr__", [](py_db_t& py_db, PyObject* attr_name) {});
    // Redirect `Collection`-level calls from `DataBase` and `Transaction` to the default collection
    // py_txn.def("__getattr__", [](py_txn_t& py_db, PyObject* attr_name) {});
    // Redirect some of the "double-under" methods to named methods
    // py_col.def("__getattr__", [](py_txn_t& py_db, PyObject* attr_name) {});

    // Define `Collection`s member method, without defining any external constructors
    py_col.def(
        "get",
        [](py_col_t& py_col, py::handle keys_py, bool track) {
            return py_read(py_col.db_ptr->native,
                           py_col.txn_ptr ? py_col.txn_ptr->native : ukv_txn_t(nullptr),
                           py_col.native,
                           py_col.txn_ptr ? py_col.txn_ptr->arena : py_col.db_ptr->arena,
                           keys_py.ptr());
        },
        py::arg(),
        py::arg("track") = false);

    py_col.def(
        "set",
        [](py_col_t& py_col, py::handle keys_py, py::handle vals_py, bool flush) {
            return py_write(py_col.db_ptr->native,
                            py_col.txn_ptr ? py_col.txn_ptr->native : ukv_txn_t(nullptr),
                            py_col.native,
                            py_col.txn_ptr ? py_col.txn_ptr->arena : py_col.db_ptr->arena,
                            keys_py.ptr(),
                            vals_py.ptr());
        },
        py::arg(),
        py::arg(),
        py::arg("flush") = false);

    py_col.def("clear", [](py_col_t& py_col) {
        db_t& db = py_col.db_ptr->native;
        db.remove(py_col.name.c_str()).throw_unhandled();
        auto maybe_col = db.collection(py_col.name.c_str());
        maybe_col.throw_unhandled();
        py_col.native = *std::move(maybe_col);
    });

    // `Transaction`:
    py_txn.def( //
        py::init([](py_db_t& py_db, bool begin) {
            auto db_ptr = py_db.shared_from_this();
            auto maybe_txn = py_db.native.transact();
            maybe_txn.throw_unhandled();
            return std::make_shared<py_txn_t>(std::move(db_ptr), *std::move(maybe_txn));
        }),
        py::arg("db"),
        py::arg("begin") = true);

    // Resource management
    py_txn.def("__enter__", &begin_if_needed);
    py_txn.def("commit", &commit_txn);

    py_db.def("__enter__", [](py_db_t& py_db) {
        if (!py_db.native)
            py_db.native.open(py_db.config).throw_unhandled();
        return py_db.shared_from_this();
    });
    py_db.def("close", [](py_db_t& py_db) { py_db.native.close(); });

    py_db.def( //
        "__exit__",
        [](py_db_t& py_db, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
            py_db.native.close();
            return false;
        });
    py_txn.def(
        "__exit__",
        [](py_txn_t& py_txn, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
            try {
                commit_txn(py_txn);
            }
            catch (...) {
                // We must now propagate this exception upwards:
                // https://stackoverflow.com/a/35483461
                // https://gist.github.com/YannickJadoul/f1fc8db711ed980cf02610277af058e4
                // https://github.com/pybind/pybind11/commit/5a7d17ff16a01436f7228a688c62511ab8c3efde
            }
            return false;
        });

    // Operator overloads used to access collections
    py_db.def(
        "__contains__",
        [](py_db_t& py_db, std::string const& collection) {
            auto maybe = py_db.native.contains(collection.c_str());
            maybe.throw_unhandled();
            return *maybe;
        },
        py::arg("collection"));
    py_db.def(
        "main",
        [](py_db_t& py_db, ukv_format_t format) -> py::object {
            return punned_collection(&py_db, nullptr, "", format);
        },
        py::arg("format") = ukv_format_binary_k);
    py_db.def(
        "__getitem__",
        [](py_db_t& py_db, std::string const& collection, ukv_format_t format) -> py::object {
            return punned_collection(&py_db, nullptr, collection, format);
        },
        py::arg("collection"),
        py::arg("format") = ukv_format_binary_k);
    py_txn.def(
        "__getitem__",
        [](py_txn_t& py_txn, std::string const& collection, ukv_format_t format) {
            return punned_collection(py_txn.db_ptr.get(), &py_txn, collection, format);
        },
        py::arg("collection"),
        py::arg("format") = ukv_format_binary_k);
    py_db.def("__delitem__", [](py_db_t& py_db, std::string const& collection) { //
        py_db.native.remove(collection.c_str()).throw_unhandled();
    });
}
