
#pragma once
#include <utility> // `std::pair`

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include "ukv.hpp"

namespace unum::ukv {

namespace py = pybind11;

struct py_db_t;
struct py_txn_t;
struct py_col_t;

/**
 * @brief Wrapper for `ukv::db_t`.
 * Assumes that the Python client won't use more than one
 * concurrent session, as multithreading in Pyhton is
 * prohibitively expensive.
 * We need to preserve the `config`, to allow re-opening.
 */
struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    db_t native;
    session_t session;
    std::string config;

    py_db_t(db_t&& n, session_t&& s, std::string const& c) : native(std::move(n)), session(std::move(s)), config(c) {}
    py_db_t(py_db_t const&) = delete;
    py_db_t(py_db_t&& other) noexcept
        : native(std::move(other.native)), session(std::move(other.session)), config(std::move(config)) {}
};

/**
 * @brief Only adds reference counting to the native C++ interface.
 */
struct py_txn_t : public std::enable_shared_from_this<py_txn_t> {
    std::shared_ptr<py_db_t> db_ptr;
    txn_t native;

    py_txn_t(std::shared_ptr<py_db_t>&& d, txn_t&& t) : db_ptr(std::move(d)), native(std::move(t)) {}
    py_txn_t(py_txn_t const&) = delete;
    py_txn_t(py_txn_t&& other) noexcept : db_ptr(std::move(other.db_ptr)), native(std::move(other.native)) {}
};

/**
 * @brief Wrapper for `ukv::collection_t`.
 * We need to preserve the `name`, to upsert again, after removing it in `clear`.
 * We also keep the transaction pointer, to persist the context of operation.
 */
struct py_col_t : public std::enable_shared_from_this<py_col_t> {
    std::shared_ptr<py_db_t> db_ptr;
    std::shared_ptr<py_txn_t> txn_ptr;
    collection_t native;
    std::string name;

    py_col_t() {}
    py_col_t(py_col_t const&) = delete;
    py_col_t(py_col_t&& other) noexcept
        : db_ptr(std::move(other.db_ptr)), txn_ptr(std::move(other.txn_ptr)), native(std::move(other.native)),
          name(std::move(other.name)) {}
};

/**
 * @brief RAII object for `py::handle` parsing purposes,
 * which releases the buffer in the destructor.
 */
struct py_received_buffer_t {
    ~py_received_buffer_t() {
        if (initialized)
            PyBuffer_Release(&py);
        initialized = false;
    }
    py_received_buffer_t() = default;
    py_received_buffer_t(py_received_buffer_t const&) = delete;
    py_received_buffer_t(py_received_buffer_t&& other) noexcept
        : py(other.py), initialized(std::exchange(other.initialized, false)) {}

    Py_buffer py;
    bool initialized = false;
};

template <typename scalar_at>
std::pair<py_received_buffer_t, strided_range_gt<scalar_at>> strided_array(py::handle handle) {
    PyObject* obj = handle.ptr();
    if (!PyObject_CheckBuffer(obj))
        throw std::invalid_argument("Buffer protocol unsupported");

    auto flags = PyBUF_ANY_CONTIGUOUS | PyBUF_STRIDED;
    if constexpr (std::is_const_v<scalar_at>)
        flags |= PyBUF_WRITABLE;

    py_received_buffer_t raii;
    raii.initialized = PyObject_GetBuffer(obj, &raii.py, flags) == 0;
    if (!raii.initialized)
        throw std::invalid_argument("Couldn't obtain buffer overviews");
    if (raii.py.ndim != 1)
        throw std::invalid_argument("Expecting tensor rank 1");
    if (!raii.py.shape)
        throw std::invalid_argument("Shape wasn't inferred");
    if (raii.py.itemsize != sizeof(scalar_at))
        throw std::invalid_argument("Scalar type mismatch");

    strided_range_gt<scalar_at> result {
        reinterpret_cast<scalar_at*>(raii.py.buf),
        static_cast<ukv_size_t>(raii.py.strides[0]),
        static_cast<ukv_size_t>(raii.py.shape[0]),
    };
    return std::make_pair<py_received_buffer_t, strided_range_gt<scalar_at>>(std::move(raii), std::move(result));
}

template <typename scalar_at>
std::pair<py_received_buffer_t, strided_matrix_gt<scalar_at>> strided_matrix(py::handle handle) {
    PyObject* obj = handle.ptr();
    if (!PyObject_CheckBuffer(obj))
        throw std::invalid_argument("Buffer protocol unsupported");

    auto flags = PyBUF_ANY_CONTIGUOUS | PyBUF_STRIDED;
    if constexpr (std::is_const_v<scalar_at>)
        flags |= PyBUF_WRITABLE;

    py_received_buffer_t raii;
    raii.initialized = PyObject_GetBuffer(obj, &raii.py, flags) == 0;
    if (!raii.initialized)
        throw std::invalid_argument("Couldn't obtain buffer overviews");
    if (raii.py.ndim != 2)
        throw std::invalid_argument("Expecting tensor rank 2");
    if (!raii.py.shape)
        throw std::invalid_argument("Shape wasn't inferred");
    if (raii.py.itemsize != sizeof(scalar_at))
        throw std::invalid_argument("Scalar type mismatch");

    strided_matrix_gt<scalar_at> result {
        reinterpret_cast<scalar_at*>(raii.py.buf),
        static_cast<ukv_size_t>(raii.py.shape[0]),
        static_cast<ukv_size_t>(raii.py.shape[1]),
        static_cast<ukv_size_t>(raii.py.strides[0]),
    };
    return std::make_pair<py_received_buffer_t, strided_matrix_gt<scalar_at>>(std::move(raii), std::move(result));
}

inline void throw_not_implemented() {
    // https://github.com/pybind/pybind11/issues/1125#issuecomment-691552571
    throw std::runtime_error("Not Implemented!");
}

void wrap_database(py::module&);
void wrap_dataframe(py::module&);
void wrap_network(py::module&);

} // namespace unum::ukv