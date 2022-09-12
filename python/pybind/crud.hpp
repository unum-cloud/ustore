
#pragma once
#include <vector> // `std::vector`

#include <pybind11/pybind11.h> // `gil_scoped_release`
#include <Python.h>            // `PyObject`

#include "ukv/ukv.h"
#include "cast.hpp"

namespace unum::ukv::pyb {

struct py_bin_req_t {
    ukv_key_t key = ukv_key_unknown_k;
    ukv_str_view_t field = nullptr;
    ukv_bytes_ptr_t ptr = nullptr;
    ukv_length_t off = 0;
    ukv_length_t len = 0;
};

#pragma region Writes

/**
 * @param key_py Must be a `PyLong`.
 * @param val_py Can be anything.
 */
static void write_one_binary(py_collection_t& collection, PyObject* key_py, PyObject* val_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    value_view_t val = py_to_bytes(val_py);

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write( //
        collection.db(),
        collection.txn(),
        1,
        collection.member_collection(),
        0,
        &key,
        0,
        nullptr,
        nullptr,
        0,
        val.member_length(),
        0,
        val.member_ptr(),
        0,
        collection.options(),
        collection.member_arena(),
        status.member_ptr());
    status.throw_unhandled();
}

static void write_many_binaries(py_collection_t& collection, PyObject* keys_py, PyObject* vals_py) {

    status_t status;
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));

    if (vals_py == Py_None) {
        [[maybe_unused]] py::gil_scoped_release release;

        ukv_write( //
            collection.db(),
            collection.txn(),
            static_cast<ukv_size_t>(keys.size()),
            collection.member_collection(),
            0,
            keys.data(),
            sizeof(ukv_key_t),
            nullptr,
            nullptr,
            0,
            nullptr,
            0,
            nullptr,
            0,
            collection.options(),
            collection.member_arena(),
            status.member_ptr());
    }
    else {
        std::vector<value_view_t> vals(keys.size());
        py_transform_n(vals_py, &py_to_bytes, vals.begin(), vals.size());

        [[maybe_unused]] py::gil_scoped_release release;
        ukv_write( //
            collection.db(),
            collection.txn(),
            static_cast<ukv_size_t>(keys.size()),
            collection.member_collection(),
            0,
            keys.data(),
            sizeof(ukv_key_t),
            nullptr,
            nullptr,
            0,
            vals[0].member_length(),
            sizeof(value_view_t),
            vals[0].member_ptr(),
            sizeof(value_view_t),
            collection.options(),
            collection.member_arena(),
            status.member_ptr());
    }

    status.throw_unhandled();
}

static void broadcast_binary(py_collection_t& collection, PyObject* keys_py, PyObject* vals_py) {

    status_t status;
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    value_view_t val = py_to_bytes(vals_py);

    [[maybe_unused]] py::gil_scoped_release release;

    ukv_write( //
        collection.db(),
        collection.txn(),
        static_cast<ukv_size_t>(keys.size()),
        collection.member_collection(),
        0,
        keys.data(),
        sizeof(ukv_key_t),
        nullptr,
        nullptr,
        0,
        val.member_length(),
        0,
        val.member_ptr(),
        0,
        collection.options(),
        collection.member_arena(),
        status.member_ptr());

    status.throw_unhandled();
}

#pragma region Reads

static py::object has_one_binary(py_collection_t& collection, PyObject* key_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    ukv_octet_t* found_indicators = nullptr;

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read( //
            collection.db(),
            collection.txn(),
            1,
            collection.member_collection(),
            0,
            &key,
            0,
            collection.options(),
            &found_indicators,
            nullptr,
            nullptr,
            nullptr,
            collection.member_arena(),
            status.member_ptr());
        status.throw_unhandled();
    }

    strided_iterator_gt<ukv_octet_t> indicators {found_indicators, sizeof(ukv_octet_t)};
    // if (collection.export_into_arrow()) {
    //     auto shared = std::make_shared<arrow::BooleanScalar>(indicators[0]);
    //     PyObject* obj_ptr = arrow::py::wrap_scalar(shared);
    //     return py::reinterpret_steal<py::object>(obj_ptr);
    // }

    PyObject* obj_ptr = indicators[0] ? Py_True : Py_False;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

static py::object read_one_binary(py_collection_t& collection, PyObject* key_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    ukv_bytes_ptr_t found_values = nullptr;
    ukv_length_t* found_offsets = nullptr;
    ukv_length_t* found_lengths = nullptr;

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read( //
            collection.db(),
            collection.txn(),
            1,
            collection.member_collection(),
            0,
            &key,
            0,
            collection.options(),
            nullptr,
            &found_offsets,
            &found_lengths,
            &found_values,
            collection.member_arena(),
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
    embedded_bins_iterator_t tape_it {found_values, found_offsets, found_lengths};
    value_view_t val = *tape_it;
    // if (collection.export_into_arrow()) {
    //     auto shared_buffer =
    //         std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(val.data()),
    //         static_cast<int64_t>(val.size()));
    //     auto shared = std::make_shared<arrow::BinaryScalar>(shared_buffer);
    //     PyObject* obj_ptr = arrow::py::wrap_scalar(shared);
    //     return py::reinterpret_steal<py::object>(obj_ptr);
    // }

    PyObject* obj_ptr = val ? PyBytes_FromStringAndSize(val.c_str(), val.size()) : Py_None;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

static py::object has_many_binaries(py_collection_t& collection, PyObject* keys_py) {

    status_t status;
    ukv_octet_t* found_indicators = nullptr;

    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read( //
            collection.db(),
            collection.txn(),
            static_cast<ukv_size_t>(keys.size()),
            collection.member_collection(),
            0,
            keys.data(),
            sizeof(ukv_key_t),
            collection.options(),
            &found_indicators,
            nullptr,
            nullptr,
            nullptr,
            collection.member_arena(),
            status.member_ptr());
        status.throw_unhandled();
    }

    strided_iterator_gt<ukv_octet_t> indicators {found_indicators, sizeof(ukv_octet_t)};
    PyObject* tuple_ptr = PyTuple_New(keys.size());
    for (std::size_t i = 0; i != keys.size(); ++i) {
        PyObject* obj_ptr = indicators[i] ? Py_True : Py_False;
        PyTuple_SetItem(tuple_ptr, i, obj_ptr);
    }
    return py::reinterpret_steal<py::object>(tuple_ptr);
}

static py::object read_many_binaries(py_collection_t& collection, PyObject* keys_py) {

    status_t status;
    ukv_bytes_ptr_t found_values = nullptr;
    ukv_length_t* found_offsets = nullptr;
    ukv_length_t* found_lengths = nullptr;

    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read( //
            collection.db(),
            collection.txn(),
            static_cast<ukv_size_t>(keys.size()),
            collection.member_collection(),
            0,
            keys.data(),
            sizeof(ukv_key_t),
            collection.options(),
            nullptr,
            &found_offsets,
            &found_lengths,
            &found_values,
            collection.member_arena(),
            status.member_ptr());
        status.throw_unhandled();
    }

    embedded_bins_iterator_t tape_it {found_values, found_offsets, found_lengths};
    if (collection.export_into_arrow()) {
    }

    PyObject* tuple_ptr = PyTuple_New(keys.size());
    for (std::size_t i = 0; i != keys.size(); ++i, ++tape_it) {
        value_view_t val = *tape_it;
        PyObject* obj_ptr = val ? PyBytes_FromStringAndSize(val.c_str(), val.size()) : Py_None;
        PyTuple_SetItem(tuple_ptr, i, obj_ptr);
    }
    return py::reinterpret_steal<py::object>(tuple_ptr);
}

static py::object has_binary(py_collection_t& collection, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &has_one_binary : &has_many_binaries;
    return func(collection, key_py.ptr());
}

static py::object read_binary(py_collection_t& collection, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &read_one_binary : &read_many_binaries;
    return func(collection, key_py.ptr());
}

static void write_binary(py_collection_t& collection, py::object key_py, py::object val_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &write_one_binary : &write_many_binaries;
    return func(collection, key_py.ptr(), val_py.ptr());
}

static void remove_binary(py_collection_t& collection, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &write_one_binary : &write_many_binaries;
    return func(collection, key_py.ptr(), Py_None);
}

static void update_binary(py_collection_t& collection, py::object dict_py) {
    status_t status;
    ukv_size_t step = sizeof(py_bin_req_t);

    std::vector<py_bin_req_t> keys;
    keys.reserve(PyDict_Size(dict_py.ptr()));

    std::size_t i = 0;
    py_scan_dict(dict_py.ptr(), [&](PyObject* key_obj, PyObject* val_obj) {
        auto val = py_to_bytes(val_obj);
        py_bin_req_t& req = keys[i];
        req.key = py_to_scalar<ukv_key_t>(key_obj);
        req.ptr = ukv_bytes_ptr_t(val.begin());
        req.len = static_cast<ukv_length_t>(val.size());
        ++i;
    });

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(collection.db(),
              collection.txn(),
              static_cast<ukv_size_t>(keys.size()),
              collection.member_collection(),
              0,
              &keys[0].key,
              step,
              nullptr,
              &keys[0].off,
              step,
              &keys[0].len,
              step,
              &keys[0].ptr,
              step,
              collection.options(),
              collection.member_arena(),
              status.member_ptr());
    status.throw_unhandled();
}

static py::array_t<ukv_key_t> scan_binary( //
    py_collection_t& collection,
    ukv_key_t min_key,
    ukv_length_t scan_limit) {

    ukv_key_t* found_keys = nullptr;
    ukv_length_t* found_lengths = nullptr;
    status_t status;

    ukv_scan( //
        collection.db(),
        collection.txn(),
        1,
        collection.member_collection(),
        0,
        &min_key,
        0,
        nullptr,
        0,
        &scan_limit,
        0,
        collection.options(),
        nullptr,
        &found_lengths,
        &found_keys,
        collection.member_arena(),
        status.member_ptr());

    status.throw_unhandled();
    return py::array_t<ukv_key_t>(*found_lengths, found_keys);
}
#if 0
/**
 * @brief Exports values into preallocated multi-dimensional NumPy-like buffers.
 *        The most performant batch-reading method, ideal for ML.
 *
 * Contrary to most data types exposed by the Python interpreter,
 * buffers are not @c `PyObject` pointers but rather simple C structures.
 * This allows them to be created and copied very simply.
 * When a generic wrapper around a buffer is needed, a @c `memoryview`
 * object can be created.
 * https://docs.python.org/3/c-api/buffer.html#buffer-structure
 *
 * @param keys       A NumPy array of keys.
 * @param values_arr A buffer-protocol object, whose `shape` has 2 dims
 *                   the `itemsize` is just one byte. The first dimensions
 *                   must match with `len(keys)` and the second one must be
 *                   at least 8 for us be able to efficiently reuse that memory.
 *
 * @param values_lengths May be nullptr.
 *
 * https://pybind11.readthedocs.io/en/stable/advanced/pycpp/numpy.html#buffer-protocol
 * https://pybind11.readthedocs.io/en/stable/advanced/cast/overview.html#list-of-all-builtin-conversions
 * https://docs.python.org/3/c-api/buffer.html
 */
void fill_tensor( //
    ukv_database_t db_ptr,
    ukv_transaction_t txn_ptr,
    ukv_collection_t collection_ptr,
    managed_arena_t& arena,
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
    py_received_buffer_t keys, values, values_lengths;
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
        throw std::invalid_argument("Keys must be placed in a continuous 1 dimensional array");
    if (keys.py.strides[0] != sizeof(ukv_key_t))
        throw std::invalid_argument("Keys can't be strided");
    ukv_size_t const tasks_count = static_cast<ukv_size_t>(keys.py.len / keys.py.itemsize);
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
    if (tasks_count != static_cast<ukv_size_t>(values.py.shape[0]))
        throw std::invalid_argument("Number of input keys and output slots doesn't match");
    auto outputs_bytes = reinterpret_cast<std::uint8_t*>(values.py.buf);
    auto outputs_bytes_stride = static_cast<std::size_t>(values.py.strides[0]);
    auto output_bytes_cap = static_cast<ukv_length_t>(values.py.shape[1]);

    // Validate the format of `values_lengths`
    if (values_lengths.py.ndim != 1)
        throw std::invalid_argument("Lengths tensor must have rank 1");
    if (values_lengths.py.itemsize != sizeof(ukv_length_t))
        throw std::invalid_argument("Lengths tensor must have 4-byte entries");
    if (values_lengths.py.shape[0] <= 0)
        throw std::invalid_argument("Lengths tensor sides can't be zero");
    if (values_lengths.py.strides[0] <= 0)
        throw std::invalid_argument("Lengths tensor strides can't be negative");
    if (tasks_count != static_cast<ukv_size_t>(values_lengths.py.shape[0]))
        throw std::invalid_argument("Number of input keys and output slots doesn't match");
    auto outputs_lengths_bytes = reinterpret_cast<std::uint8_t*>(values_lengths.py.buf);
    auto outputs_lengths_bytes_stride = static_cast<std::size_t>(values_lengths.py.strides[0]);

    // Perform the read
    [[maybe_unused]] py::gil_scoped_release release;
    status_t status;
    ukv_length_t* found_lengths = nullptr;
    ukv_bytes_ptr_t found_values = nullptr;
    ukv_options_t options = ukv_options_default_k;

    ukv_read( //
        db_ptr,
        txn_ptr,
        tasks_count,
        &collection_ptr,
        0,
        keys_ptr,
        sizeof(ukv_key_t),
        options,
        &found_lengths,
        &found_values,
        arena.internal_cptr(),
        status.internal_cptr());

    status.throw_unhandled();

    // Export the data into the matrix
    taped_values_view_t inputs {found_lengths, found_values, tasks_count};
    tape_iterator_t input_it = inputs.begin();
    for (ukv_size_t i = 0; i != tasks_count; ++i, ++input_it) {
        value_view_t input = *input_it;
        auto input_bytes = reinterpret_cast<std::uint8_t const*>(input.begin());
        auto input_length = static_cast<ukv_length_t const>(input.size());
        std::uint8_t* output_bytes = outputs_bytes + outputs_bytes_stride * i;
        ukv_length_t& output_length =
            *reinterpret_cast<ukv_length_t*>(outputs_lengths_bytes + outputs_lengths_bytes_stride * i);

        std::size_t count_copy = std::min(output_bytes_cap, input_length);
        std::size_t count_pads = output_bytes_cap - count_copy;
        std::memcpy(output_bytes, input_bytes, count_copy);
        std::memset(output_bytes + count_copy, padding_char, count_pads);

        output_length = count_copy;
    }
}
#endif

} // namespace unum::ukv::pyb