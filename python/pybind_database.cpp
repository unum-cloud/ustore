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
 * https://python-reference.readthedocs.io/en/latest/docs/dict/
 * https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
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
    if (i >= vec.size())
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
        req.off = 0;
    }
    else if (obj == Py_None) {
        // Means the object must be deleted
        req.ptr = nullptr;
        req.len = 0;
        req.off = 0;
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
        auto buf = py_strided_buffer(obj);
        auto fmt_len = std::strlen(buf.py.format);
        if (fmt_len != 1)
            throw std::invalid_argument("Unsupported keys scalar type");

        auto export_as = [&](auto type_example) {
            using scalar_t = decltype(type_example);
            auto typed = py_strided_range<scalar_t const>(buf);
            for (; i != typed.size(); ++i)
                at_growing(reqs, i).key = static_cast<ukv_key_t>(typed[i]);
        };

        switch (buf.py.format[0]) {
        case format_code_gt<short>::value[0]: export_as((short)(0)); break;
        case format_code_gt<unsigned short>::value[0]: export_as((unsigned short)(0)); break;
        case format_code_gt<int>::value[0]: export_as((int)(0)); break;
        case format_code_gt<unsigned int>::value[0]: export_as((unsigned int)(0)); break;
        case format_code_gt<long>::value[0]: export_as((long)(0)); break;
        case format_code_gt<unsigned long>::value[0]: export_as((unsigned long)(0)); break;
        case format_code_gt<long long>::value[0]: export_as((long long)(0)); break;
        case format_code_gt<unsigned long long>::value[0]: export_as((unsigned long long)(0)); break;
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
        scan_pyseq(obj, [&](PyObject* obj) { populate_val(obj, at_growing(reqs, i)), ++i; });

    if (is_buffer) {
        auto buf = py_strided_buffer(obj);
        auto fmt_len = std::strlen(buf.py.format);
        if (fmt_len != 1)
            throw std::invalid_argument("Unsupported keys scalar type");

        auto export_as = [&](auto type_example) {
            using scalar_t = decltype(type_example);
            auto typed = py_strided_matrix<scalar_t const>(buf);
            for (; i != typed.rows(); ++i) {
                auto row = typed.row(i);
                at_growing(reqs, i).key = value_view_t(reinterpret_cast<byte_t const*>(row.begin()),
                                                       reinterpret_cast<byte_t const*>(row.end()));
            }
        };

        switch (buf.py.format[0]) {
        case format_code_gt<char>::value[0]: export_as((char)(0)); break;
        case format_code_gt<signed char>::value[0]: export_as((signed char)(0)); break;
        case format_code_gt<unsigned char>::value[0]: export_as((unsigned char)(0)); break;

        case format_code_gt<short>::value[0]: export_as((short)(0)); break;
        case format_code_gt<unsigned short>::value[0]: export_as((unsigned short)(0)); break;
        case format_code_gt<int>::value[0]: export_as((int)(0)); break;
        case format_code_gt<unsigned int>::value[0]: export_as((unsigned int)(0)); break;
        case format_code_gt<long>::value[0]: export_as((long)(0)); break;
        case format_code_gt<unsigned long>::value[0]: export_as((unsigned long)(0)); break;
        case format_code_gt<long long>::value[0]: export_as((long long)(0)); break;
        case format_code_gt<unsigned long long>::value[0]: export_as((unsigned long long)(0)); break;
        default: throw std::invalid_argument("Unsupported keys scalar type"); break;
        }
    }
}

#pragma region Writes

/**
 * @param key_py Must be a `PyLong`.
 * @param val_py Can be anything.
 */
void py_write_one(py_task_ctx_t ctx, py::handle key_py, py::handle val_py) {

    status_t status;
    py_bin_req_t req;
    populate_key(key_py.ptr(), req);
    populate_val(val_py.ptr(), req);

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(ctx.db,
              ctx.txn,
              1,
              ctx.col,
              0,
              &req.key,
              0,
              &req.ptr,
              0,
              &req.off,
              0,
              &req.len,
              0,
              ctx.options,
              ctx.arena,
              status.member_ptr());
    status.throw_unhandled();
}

void py_write_many(py_task_ctx_t ctx, py::handle keys_py, py::handle vals_py) {

    std::vector<py_bin_req_t> reqs;
    populate_keys(keys_py.ptr(), reqs);
    if (vals_py.ptr() != Py_None)
        populate_vals(vals_py.ptr(), reqs);

    status_t status;
    ukv_size_t step = sizeof(py_bin_req_t);

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(ctx.db,
              ctx.txn,
              static_cast<ukv_size_t>(reqs.size()),
              ctx.col,
              0,
              &reqs[0].key,
              step,
              &reqs[0].ptr,
              step,
              &reqs[0].off,
              step,
              &reqs[0].len,
              step,
              ctx.options,
              ctx.arena,
              status.member_ptr());
    status.throw_unhandled();
}

#pragma region Reads

py::object py_read_one(py_task_ctx_t ctx, py::handle key_py) {

    status_t status;
    ukv_key_t key = static_cast<ukv_key_t>(PyLong_AsUnsignedLong(key_py.ptr()));
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_lengths = nullptr;

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(ctx.db,
                 ctx.txn,
                 1,
                 ctx.col,
                 0,
                 &key,
                 0,
                 ctx.options,
                 &found_lengths,
                 &found_values,
                 ctx.arena,
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

py::object py_read_many(py_task_ctx_t ctx, py::handle keys_py) {

    status_t status;
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    std::vector<py_bin_req_t> reqs;
    populate_keys(keys_py.ptr(), reqs);
    ukv_size_t step = sizeof(py_bin_req_t);

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(ctx.db,
                 ctx.txn,
                 static_cast<ukv_size_t>(reqs.size()),
                 ctx.col,
                 0,
                 &reqs[0].key,
                 step,
                 ctx.options,
                 &found_lengths,
                 &found_values,
                 ctx.arena,
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
    return py::reinterpret_steal<py::object>(tuple_ptr);
}

py::object py_has_many(py_task_ctx_t ctx, py::handle keys_py) {

    status_t status;
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_lengths = nullptr;

    std::vector<py_bin_req_t> reqs;
    populate_keys(keys_py.ptr(), reqs);
    ukv_size_t step = sizeof(py_bin_req_t);
    ctx.options = static_cast<ukv_options_t>(ctx.options | ukv_option_read_lengths_k);

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(ctx.db,
                 ctx.txn,
                 static_cast<ukv_size_t>(reqs.size()),
                 ctx.col,
                 0,
                 &reqs[0].key,
                 step,
                 ctx.options,
                 &found_lengths,
                 &found_values,
                 ctx.arena,
                 status.member_ptr());
        status.throw_unhandled();
    }

    PyObject* tuple_ptr = PyTuple_New(reqs.size());
    for (std::size_t i = 0; i != reqs.size(); ++i) {
        PyObject* obj_ptr = found_lengths[i] != ukv_val_len_missing_k ? Py_True : Py_False;
        PyTuple_SetItem(tuple_ptr, i, obj_ptr);
    }
    return py::reinterpret_steal<py::object>(tuple_ptr);
}

py::object py_has_one(py_task_ctx_t ctx, py::handle key_py) {

    status_t status;
    ukv_key_t key = static_cast<ukv_key_t>(PyLong_AsUnsignedLong(key_py.ptr()));
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    ctx.options = static_cast<ukv_options_t>(ctx.options | ukv_option_read_lengths_k);

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(ctx.db,
                 ctx.txn,
                 1,
                 ctx.col,
                 0,
                 &key,
                 0,
                 ctx.options,
                 &found_lengths,
                 &found_values,
                 ctx.arena,
                 status.member_ptr());
        status.throw_unhandled();
    }

    PyObject* obj_ptr = *found_lengths != ukv_val_len_missing_k ? Py_True : Py_False;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

#pragma region CRUD Entry Points

template <typename py_wrap_at>
py::object py_has(py_wrap_at& wrap, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &py_has_one : &py_has_many;
    return func(wrap, key_py);
}

template <typename py_wrap_at>
py::object py_read(py_wrap_at& wrap, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &py_read_one : &py_read_many;
    return func(wrap, key_py);
}

template <typename py_wrap_at>
void py_write(py_wrap_at& wrap, py::object key_py, py::object val_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &py_write_one : &py_write_many;
    return func(wrap, key_py, val_py);
}

template <typename py_wrap_at>
void py_remove(py_wrap_at& wrap, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &py_write_one : &py_write_many;
    return func(wrap, key_py, Py_None);
}

template <typename py_wrap_at>
void py_update(py_wrap_at& wrap, py::object dict_py) {
    py_task_ctx_t ctx = wrap;
    status_t status;
    ukv_size_t step = sizeof(py_bin_req_t);

    std::vector<py_bin_req_t> reqs;
    reqs.reserve(PyDict_Size(dict_py.ptr()));

    std::size_t i = 0;
    scan_pydict(dict_py.ptr(), [&](PyObject* key_obj, PyObject* val_obj) {
        py_bin_req_t& req = reqs[i];
        populate_key(key_obj, req);
        populate_val(val_obj, req);
        ++i;
    });

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(ctx.db,
              ctx.txn,
              static_cast<ukv_size_t>(reqs.size()),
              ctx.col,
              0,
              &reqs[0].key,
              step,
              &reqs[0].ptr,
              step,
              &reqs[0].off,
              step,
              &reqs[0].len,
              step,
              ctx.options,
              ctx.arena,
              status.member_ptr());
    status.throw_unhandled();
}

template <typename py_wrap_at>
py::array_t<ukv_key_t> py_scan( //
    py_wrap_at& wrap,
    ukv_key_t min_key,
    ukv_size_t scan_length) {

    py_task_ctx_t ctx = wrap;
    ukv_key_t* found_keys = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    status_t status;

    ukv_scan( //
        ctx.db,
        ctx.txn,
        1,
        ctx.col,
        0,
        &min_key,
        0,
        &scan_length,
        0,
        ctx.options,
        &found_keys,
        &found_lengths,
        ctx.arena,
        status.member_ptr());

    status.throw_unhandled();
    return py::array_t<ukv_key_t>(scan_length, found_keys);
}

#pragma region Helper API

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
    auto py_keys_range = py::class_<py_keys_range_t, std::shared_ptr<py_keys_range_t>>(m, "Range", py::module_local());
    auto py_keys_stream =
        py::class_<py_keys_stream_t, std::shared_ptr<py_keys_stream_t>>(m, "Stream", py::module_local());

    py::enum_<ukv_format_t>(m, "Format", py::module_local())
        .value("Binary", ukv_format_binary_k)
        .value("Graph", ukv_format_graph_k)
        .value("MsgPack", ukv_format_msgpack_k)
        .value("JSON", ukv_format_json_k)
        .value("BSON", ukv_format_bson_k)
        .value("CBOR", ukv_format_cbor_k)
        .value("UBJSON", ukv_format_ubjson_k);

    // Define keys_range
    py_keys_range.def("__iter__", [](py_keys_range_t& keys_range) {
        keys_stream_t stream = keys_range.native.begin();
        stream.seek(keys_range.min_key);
        return std::make_shared<py_keys_stream_t>(std::move(stream), keys_range.max_key);
    });

    py_keys_range.def("__getitem__", [](py_keys_range_t& keys_range, py::slice slice) {
        Py_ssize_t start, stop, step;
        if (PySlice_Unpack(slice.ptr(), &start, &stop, &step) || step != 1 || start >= stop)
            throw std::invalid_argument("Invalid Slice");
        keys_stream_t stream = keys_range.native.begin(stop);
        auto keys = stream.keys_batch();
        return py::array(std::min(stop - start, Py_ssize_t(keys.size()) - start), keys.begin() + start);
    });

    py_keys_range.def("since", [](py_keys_range_t& keys_range, ukv_key_t key) {
        keys_range.min_key = key;
        return std::move(keys_range);
    });

    py_keys_range.def("to", [](py_keys_range_t& keys_range, ukv_key_t key) {
        keys_range.max_key = key;
        return std::move(keys_range);
    });

    // Define keys_stream
    py_keys_stream.def("__next__", [](py_keys_stream_t& keys_stream) {
        ukv_key_t key = keys_stream.native.key();
        if (keys_stream.native.is_end() || keys_stream.last)
            throw py::stop_iteration();
        if (key == keys_stream.stop_point)
            keys_stream.last = true;
        ++keys_stream.native;
        return key;
    });

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

    // Define `Collection`s member method, without defining any external constructors
    py_col.def("set", &py_write<py_col_t>);
    py_col.def("pop", &py_remove<py_col_t>);  // Unlike Python, won't return the result
    py_col.def("has_key", &py_has<py_col_t>); // Similar to Python 2
    py_col.def("get", &py_read<py_col_t>);
    py_col.def("update", &py_update<py_col_t>);
    py_col.def("scan", &py_scan<py_col_t>);

    py_col.def("clear", [](py_col_t& py_col) {
        db_t& db = py_col.db_ptr->native;
        db.remove(py_col.name.c_str()).throw_unhandled();
        auto maybe_col = db.collection(py_col.name.c_str());
        maybe_col.throw_unhandled();
        py_col.native = *std::move(maybe_col);
    });

    // `Transaction`:
    py_txn.def( //
        py::init([](py_db_t& py_db, bool begin, bool track_reads, bool flush_writes, bool snapshot) {
            auto db_ptr = py_db.shared_from_this();
            auto maybe_txn = py_db.native.transact(snapshot);
            maybe_txn.throw_unhandled();
            auto py_txn_ptr = std::make_shared<py_txn_t>(std::move(db_ptr), *std::move(maybe_txn));
            py_txn_ptr->track_reads = track_reads;
            py_txn_ptr->flush_writes = flush_writes;
            return py_txn_ptr;
        }),
        py::arg("db"),
        py::arg("begin") = true,
        py::arg("track_reads") = false,
        py::arg("flush_writes") = false,
        py::arg("snapshot") = false);

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

    py_db.def_property_readonly("keys", [](py_db_t& py_db) {
        keys_range_t range(py_db.native);
        return py::cast(std::make_shared<py_keys_range_t>(std::move(range)));
    });

    py_col.def_property_readonly("keys", [](py_col_t& py_col) {
        keys_range_t range(py_col.db_ptr->native, nullptr, py_col.native);
        return py::cast(std::make_shared<py_keys_range_t>(std::move(range)));
    });

    // Shortcuts for main collection
    py_db.def("set", &py_write<py_db_t>);
    py_db.def("pop", &py_remove<py_db_t>);  // Unlike Python, won't return the result
    py_db.def("has_key", &py_has<py_db_t>); // Similar to Python 2
    py_db.def("get", &py_read<py_db_t>);
    py_db.def("update", &py_update<py_db_t>);
    py_db.def("scan", &py_scan<py_db_t>);

    py_txn.def("set", &py_write<py_txn_t>);
    py_txn.def("pop", &py_remove<py_txn_t>);  // Unlike Python, won't return the result
    py_txn.def("has_key", &py_has<py_txn_t>); // Similar to Python 2
    py_txn.def("get", &py_read<py_txn_t>);
    py_txn.def("update", &py_update<py_txn_t>);
    py_txn.def("scan", &py_scan<py_txn_t>);

    // Additional operator overloads
    py_col.def("__setitem__", &py_write<py_col_t>);
    py_col.def("__delitem__", &py_remove<py_col_t>);
    py_col.def("__contains__", &py_has<py_col_t>);
    py_col.def("__getitem__", &py_read<py_col_t>);

    py_db.def("__setitem__", &py_write<py_db_t>);
    py_db.def("__delitem__", &py_remove<py_db_t>);
    py_db.def("__contains__", &py_has<py_db_t>);
    py_db.def("__getitem__", &py_read<py_db_t>);

    py_txn.def("__setitem__", &py_write<py_txn_t>);
    py_txn.def("__delitem__", &py_remove<py_txn_t>);
    py_txn.def("__contains__", &py_has<py_txn_t>);
    py_txn.def("__getitem__", &py_read<py_txn_t>);
}
