
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
    ukv_val_ptr_t ptr = nullptr;
    ukv_val_len_t off = 0;
    ukv_val_len_t len = 0;
};

#pragma region Writes

/**
 * @param key_py Must be a `PyLong`.
 * @param val_py Can be anything.
 */
static void write_one_binary(py_col_t& col, PyObject* key_py, PyObject* val_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    value_view_t val = py_to_bytes(val_py);

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(col.db(),
              col.txn(),
              1,
              col.member_col(),
              0,
              &key,
              0,
              val.member_ptr(),
              0,
              nullptr,
              0,
              val.member_length(),
              0,
              col.options(),
              col.member_arena(),
              status.member_ptr());
    status.throw_unhandled();
}

static void write_many_binaries(py_col_t& col, PyObject* keys_py, PyObject* vals_py) {

    status_t status;
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));

    if (vals_py == Py_None) {
        [[maybe_unused]] py::gil_scoped_release release;

        ukv_write(col.db(),
                  col.txn(),
                  static_cast<ukv_size_t>(keys.size()),
                  col.member_col(),
                  0,
                  keys.data(),
                  sizeof(ukv_key_t),
                  nullptr,
                  0,
                  nullptr,
                  0,
                  nullptr,
                  0,
                  col.options(),
                  col.member_arena(),
                  status.member_ptr());
    }
    else {
        std::vector<value_view_t> vals(keys.size());
        py_transform_n(vals_py, &py_to_bytes, vals.begin(), vals.size());

        [[maybe_unused]] py::gil_scoped_release release;
        ukv_write(col.db(),
                  col.txn(),
                  static_cast<ukv_size_t>(keys.size()),
                  col.member_col(),
                  0,
                  keys.data(),
                  sizeof(ukv_key_t),
                  vals[0].member_ptr(),
                  sizeof(value_view_t),
                  nullptr,
                  0,
                  vals[0].member_length(),
                  sizeof(value_view_t),
                  col.options(),
                  col.member_arena(),
                  status.member_ptr());
    }

    status.throw_unhandled();
}

static void broadcast_binary(py_col_t& col, PyObject* keys_py, PyObject* vals_py) {

    status_t status;
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    value_view_t val = py_to_bytes(vals_py);

    [[maybe_unused]] py::gil_scoped_release release;

    ukv_write(col.db(),
              col.txn(),
              static_cast<ukv_size_t>(keys.size()),
              col.member_col(),
              0,
              keys.data(),
              sizeof(ukv_key_t),
              val.member_ptr(),
              0,
              nullptr,
              0,
              val.member_length(),
              0,
              col.options(),
              col.member_arena(),
              status.member_ptr());

    status.throw_unhandled();
}

#pragma region Reads

static py::object has_one_binary(py_col_t& col, PyObject* key_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    ukv_1x8_t* presenses = nullptr;

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(col.db(),
                 col.txn(),
                 1,
                 col.member_col(),
                 0,
                 &key,
                 0,
                 col.options(),
                 nullptr,
                 nullptr,
                 nullptr,
                 &presenses,
                 col.member_arena(),
                 status.member_ptr());
        status.throw_unhandled();
    }

    strided_iterator_gt<ukv_1x8_t> presenses_it {presenses, sizeof(ukv_1x8_t)};
    PyObject* obj_ptr = presenses_it[0] ? Py_True : Py_False;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

static py::object read_one_binary(py_col_t& col, PyObject* key_py) {

    status_t status;
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_offsets = nullptr;
    ukv_val_len_t* found_lengths = nullptr;

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(col.db(),
                 col.txn(),
                 1,
                 col.member_col(),
                 0,
                 &key,
                 0,
                 col.options(),
                 &found_values,
                 &found_offsets,
                 &found_lengths,
                 col.member_arena(),
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
    joined_values_iterator_t tape_it {found_values, found_offsets, found_lengths};
    value_view_t val = *tape_it;
    PyObject* obj_ptr = val ? PyBytes_FromStringAndSize(val.c_str(), val.size()) : Py_None;
    return py::reinterpret_borrow<py::object>(obj_ptr);
}

static py::object has_many_binaries(py_col_t& col, PyObject* keys_py) {

    status_t status;
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_offsets = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    auto options = static_cast<ukv_options_t>(col.options() /* | ukv_option_read_lengths_k */);

    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(col.db(),
                 col.txn(),
                 static_cast<ukv_size_t>(keys.size()),
                 col.member_col(),
                 0,
                 keys.data(),
                 sizeof(ukv_key_t),
                 options,
                 &found_values,
                 &found_offsets,
                 &found_lengths,
                 col.member_arena(),
                 status.member_ptr());
        status.throw_unhandled();
    }

    PyObject* tuple_ptr = PyTuple_New(keys.size());
    for (std::size_t i = 0; i != keys.size(); ++i) {
        PyObject* obj_ptr = found_lengths[i] != ukv_val_len_missing_k ? Py_True : Py_False;
        PyTuple_SetItem(tuple_ptr, i, obj_ptr);
    }
    return py::reinterpret_steal<py::object>(tuple_ptr);
}

static py::object read_many_binaries(py_col_t& col, PyObject* keys_py) {

    status_t status;
    ukv_val_ptr_t found_values = nullptr;
    ukv_val_len_t* found_offsets = nullptr;
    ukv_val_len_t* found_lengths = nullptr;

    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));

    {
        [[maybe_unused]] py::gil_scoped_release release;
        ukv_read(col.db(),
                 col.txn(),
                 static_cast<ukv_size_t>(keys.size()),
                 col.member_col(),
                 0,
                 keys.data(),
                 sizeof(ukv_key_t),
                 col.options(),
                 &found_values,
                 &found_offsets,
                 &found_lengths,
                 col.member_arena(),
                 status.member_ptr());
        status.throw_unhandled();
    }

    joined_values_iterator_t tape_it {found_values, found_offsets, found_lengths};
    PyObject* tuple_ptr = PyTuple_New(keys.size());
    for (std::size_t i = 0; i != keys.size(); ++i, ++tape_it) {
        value_view_t val = *tape_it;
        PyObject* obj_ptr = val ? PyBytes_FromStringAndSize(val.c_str(), val.size()) : Py_None;
        PyTuple_SetItem(tuple_ptr, i, obj_ptr);
    }
    return py::reinterpret_steal<py::object>(tuple_ptr);
}

static py::object has_binary(py_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &has_one_binary : &has_many_binaries;
    return func(col, key_py.ptr());
}

static py::object read_binary(py_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &read_one_binary : &read_many_binaries;
    return func(col, key_py.ptr());
}

static void write_binary(py_col_t& col, py::object key_py, py::object val_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &write_one_binary : &write_many_binaries;
    return func(col, key_py.ptr(), val_py.ptr());
}

static void remove_binary(py_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &write_one_binary : &write_many_binaries;
    return func(col, key_py.ptr(), Py_None);
}

static void update_binary(py_col_t& col, py::object dict_py) {
    status_t status;
    ukv_size_t step = sizeof(py_bin_req_t);

    std::vector<py_bin_req_t> keys;
    keys.reserve(PyDict_Size(dict_py.ptr()));

    std::size_t i = 0;
    py_scan_dict(dict_py.ptr(), [&](PyObject* key_obj, PyObject* val_obj) {
        auto val = py_to_bytes(val_obj);
        py_bin_req_t& req = keys[i];
        req.key = py_to_scalar<ukv_key_t>(key_obj);
        req.ptr = ukv_val_ptr_t(val.begin());
        req.len = static_cast<ukv_val_len_t>(val.size());
        ++i;
    });

    [[maybe_unused]] py::gil_scoped_release release;
    ukv_write(col.db(),
              col.txn(),
              static_cast<ukv_size_t>(keys.size()),
              col.member_col(),
              0,
              &keys[0].key,
              step,
              &keys[0].ptr,
              step,
              &keys[0].off,
              step,
              &keys[0].len,
              step,
              col.options(),
              col.member_arena(),
              status.member_ptr());
    status.throw_unhandled();
}

static py::array_t<ukv_key_t> scan_binary( //
    py_col_t& col,
    ukv_key_t min_key,
    ukv_size_t scan_length) {

    ukv_key_t* found_keys = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    status_t status;

    ukv_scan( //
        col.db(),
        col.txn(),
        1,
        col.member_col(),
        0,
        &min_key,
        0,
        &scan_length,
        0,
        col.options(),
        &found_keys,
        &found_lengths,
        col.member_arena(),
        status.member_ptr());

    status.throw_unhandled();
    return py::array_t<ukv_key_t>(scan_length, found_keys);
}

} // namespace unum::ukv::pyb