
#pragma once
#include <utility> // `std::pair`

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include "ukv/ukv.hpp"

namespace unum::ukv {

namespace py = pybind11;

struct py_db_t;
struct py_txn_t;
struct py_col_t;

struct py_graph_t;
struct py_frame_t;

struct py_task_ctx_t;
struct py_received_buffer_t;

/**
 * @brief Python tasks are generally called for a single collection.
 * That greatly simplifies the implementation.
 */
struct py_task_ctx_t {
    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_collection_t* col = nullptr;
    ukv_arena_t* arena = nullptr;
    ukv_options_t options = ukv_options_default_k;
};

/**
 * @brief Wrapper for `ukv::db_t`.
 * Assumes that the Python client won't use more than one
 * concurrent session, as multithreading in Python is
 * prohibitively expensive.
 * We need to preserve the `config`, to allow re-opening.
 */

template <typename stream_t>
struct py_stream_gt : public std::enable_shared_from_this<py_stream_gt<stream_t>> {
    stream_t native;
    ukv_key_t stop_point;
    bool last = false;

    py_stream_gt(stream_t&& stream, ukv_key_t max) : native(std::move(stream)), stop_point(max) {}
    py_stream_gt(py_stream_gt const&) = delete;
    py_stream_gt(py_stream_gt&& other) noexcept
        : native(std::move(other.native)), stop_point(std::move(other.stop_point)) {}
};

template <typename range_t>
struct py_range_gt : public std::enable_shared_from_this<py_range_gt<range_t>> {
    range_t native;
    ukv_key_t min_key;
    ukv_key_t max_key;

    py_range_gt(range_t&& range,
                ukv_key_t min = std::numeric_limits<ukv_key_t>::min(),
                ukv_key_t max = ukv_key_unknown_k)
        : native(std::move(range)), min_key(min), max_key(max) {}
    py_range_gt(py_range_gt const&) = delete;
    py_range_gt(py_range_gt&& other) noexcept
        : native(std::move(other.native)), min_key(std::move(other.min_key)), max_key(std::move(other.max_key)) {}
};

struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    db_t native;
    arena_t arena;
    std::string config;

    py_db_t(db_t&& n, std::string const& c) : native(std::move(n)), arena(native), config(c) {}
    py_db_t(py_db_t const&) = delete;
    py_db_t(py_db_t&& other) noexcept
        : native(std::move(other.native)), arena(std::move(other.arena)), config(std::move(config)) {}

    operator py_task_ctx_t() & noexcept {
        return {native, nullptr, nullptr, arena.member_ptr(), ukv_options_default_k};
    }
};

/**
 * @brief Only adds reference counting to the native C++ interface.
 */
struct py_txn_t : public std::enable_shared_from_this<py_txn_t> {
    std::shared_ptr<py_db_t> db_ptr;
    txn_t native;
    arena_t arena;
    bool track_reads = false;
    bool flush_writes = false;

    py_txn_t(std::shared_ptr<py_db_t>&& d, txn_t&& t) noexcept
        : db_ptr(std::move(d)), native(std::move(t)), arena(db_ptr->native) {}
    py_txn_t(py_txn_t const&) = delete;
    py_txn_t(py_txn_t&& other) noexcept
        : db_ptr(std::move(other.db_ptr)), native(std::move(other.native)), arena(std::move(other.arena)) {}

    operator py_task_ctx_t() & noexcept {
        auto options = static_cast<ukv_options_t>( //
            ukv_options_default_k |                //
            (track_reads ? ukv_option_read_track_k : ukv_options_default_k) |
            (flush_writes ? ukv_option_write_flush_k : ukv_options_default_k));
        return {db_ptr->native, native, nullptr, arena.member_ptr(), options};
    }
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

    operator py_task_ctx_t() & noexcept {
        py_task_ctx_t result = txn_ptr ? py_task_ctx_t(*txn_ptr) : py_task_ctx_t(*db_ptr);
        result.col = native.member_ptr();
        return result;
    }
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

/**
 * @brief A generalization of the graph supported by NetworkX.
 *
 * Sources and targets can match.
 * Relations attrs can be banned all together.
 *
 * Example for simple non-attributed undirected graphs:
 * > relations_name: ".graph"
 * > attrs_name: ""
 * > sources_name: ""
 * > targets_name: ""
 *
 * Example for recommender systems
 * > relations_name: "views.graph"
 * > attrs_name: "views.docs"
 * > sources_name: "people.docs"
 * > targets_name: "movies.docs"
 */
struct py_graph_t : public std::enable_shared_from_this<py_graph_t> {

    std::shared_ptr<py_db_t> db_ptr;
    collection_t index;
    collection_t sources_attrs;
    collection_t targets_attrs;
    collection_t relations_attrs;

    bool is_directed_ = false;
    bool is_multi_ = false;
    bool allow_self_loops_ = false;

    Py_buffer last_buffer;
    Py_ssize_t last_buffer_shape[3];
    Py_ssize_t last_buffer_strides[3];

    py_graph_t() {}
    py_graph_t(py_graph_t&&) = delete;
    py_graph_t(py_graph_t const&) = delete;
    ~py_graph_t() {}

    graph_ref_t ref() { return index.as_graph(); }
};

struct py_col_name_t {
    std::string owned;
    ukv_str_view_t view;
};

struct py_col_keys_range_t {
    ukv_collection_t col = ukv_default_collection_k;
    ukv_key_t min = std::numeric_limits<ukv_key_t>::min();
    ukv_key_t max = std::numeric_limits<ukv_key_t>::max();
    std::size_t limit = std::numeric_limits<std::size_t>::max();
};

/**
 * @brief Materialized view over a specific subset of documents
 * UIDs (potentially, in different collections) and column (field) names.
 *
 */
struct py_frame_t : public std::enable_shared_from_this<py_frame_t> {

    ukv_t db = NULL;

    std::variant<std::monostate, py_col_name_t, std::vector<py_col_name_t>> fields;
    std::variant<std::monostate, py_col_keys_range_t, std::vector<col_key_t>> docs;

    py_frame_t() = default;
    py_frame_t(py_frame_t&&) = delete;
    py_frame_t(py_frame_t const&) = delete;
};

#pragma region Helper Functions

inline py_received_buffer_t py_strided_buffer(PyObject* obj, bool const_ = true) {
    auto flags = PyBUF_ANY_CONTIGUOUS | PyBUF_STRIDED;
    if (!const_)
        flags |= PyBUF_WRITABLE;

    py_received_buffer_t raii;
    raii.initialized = PyObject_GetBuffer(obj, &raii.py, flags) == 0;
    if (!raii.initialized)
        throw std::invalid_argument("Couldn't obtain buffer overviews");
    if (!raii.py.shape)
        throw std::invalid_argument("Shape wasn't inferred");
    return raii;
}

/**
 * @brief Provides a typed view of 1D potentially-strided tensor.
 * @param obj Must implement the "Buffer protocol".
 */
template <typename scalar_at>
strided_range_gt<scalar_at> py_strided_range(py_received_buffer_t const& raii) {

    if (raii.py.ndim != 1)
        throw std::invalid_argument("Expecting tensor rank 1");
    if (raii.py.itemsize != sizeof(scalar_at))
        throw std::invalid_argument("Scalar type mismatch");

    strided_range_gt<scalar_at> result {
        reinterpret_cast<scalar_at*>(raii.py.buf),
        static_cast<ukv_size_t>(raii.py.strides[0]),
        static_cast<ukv_size_t>(raii.py.shape[0]),
    };
    return result;
}

/**
 * @brief Provides a typed view of 2D potentially-strided tensor.
 * @param obj Must implement the "Buffer protocol".
 */
template <typename scalar_at>
strided_matrix_gt<scalar_at> py_strided_matrix(py_received_buffer_t const& raii) {

    if (raii.py.ndim != 2)
        throw std::invalid_argument("Expecting tensor rank 2");
    if constexpr (!std::is_void_v<scalar_at>)
        if (raii.py.itemsize != sizeof(scalar_at))
            throw std::invalid_argument("Scalar type mismatch");
    if constexpr (!std::is_void_v<scalar_at>)
        if (raii.py.itemsize != raii.py.strides[1])
            throw std::invalid_argument("Rows are not continuous");

    strided_matrix_gt<scalar_at> result {
        reinterpret_cast<scalar_at*>(raii.py.buf),
        static_cast<ukv_size_t>(raii.py.shape[0]),
        static_cast<ukv_size_t>(raii.py.shape[1]),
        static_cast<ukv_size_t>(raii.py.strides[0]),
    };
    return result;
}

inline void throw_not_implemented() {
    // https://github.com/pybind/pybind11/issues/1125#issuecomment-691552571
    throw std::runtime_error("Not Implemented!");
}

inline bool is_pyseq(PyObject* obj) {
    return PyTuple_Check(obj) || PyList_Check(obj) || PyIter_Check(obj);
}

/**
 * @brief Iterates over Python `tuple`, `list`, or any `iter`.
 * @param call Callback for member `PyObject`s.
 */
template <typename member_callback_at>
void scan_pyseq(PyObject* obj, member_callback_at&& call) {

    if (PyTuple_Check(obj)) {
        size_t n = PyTuple_Size(obj);
        for (size_t i = 0; i != n; ++i)
            call(PyTuple_GetItem(obj, i));
    }
    else if (PyList_Check(obj)) {
        size_t n = PyList_Size(obj);
        for (size_t i = 0; i != n; ++i)
            call(PyList_GetItem(obj, i));
    }
    else if (PyIter_Check(obj)) {
        PyObject* item = nullptr;
        while ((item = PyIter_Next(obj))) {
            call(item);
            Py_DECREF(item);
        }
    }
}

/**
 * @brief Iterates over Python `dict`-like object.
 * @param call Callback for the key and value `PyObject`s.
 * @return true If a supported iterable type was detected.
 */
template <typename member_callback_at>
void scan_pydict(PyObject* obj, member_callback_at&& call) {

    PyObject* key = nullptr;
    PyObject* value = nullptr;
    Py_ssize_t pos = 0;

    while (PyDict_Next(obj, &pos, &key, &value))
        call(key, value);
}

#pragma region Type Conversions Guides

/**
 * @brief Defines Pythons type marker for primitive types.
 * @b All of them are only a single character long.
 */
template <typename element_at>
struct format_code_gt {};

// clang-format off
template <> struct format_code_gt<bool> { inline static constexpr char value[2] = "?"; };
template <> struct format_code_gt<char> { inline static constexpr char value[2] = "c"; };
template <> struct format_code_gt<signed char> { inline static constexpr char value[2] = "b"; };
template <> struct format_code_gt<unsigned char> { inline static constexpr char value[2] = "B"; };

template <> struct format_code_gt<short> { inline static constexpr char value[2] = "h"; };
template <> struct format_code_gt<unsigned short> { inline static constexpr char value[2] = "H"; };
template <> struct format_code_gt<int> { inline static constexpr char value[2] = "i"; };
template <> struct format_code_gt<unsigned int> { inline static constexpr char value[2] = "I"; };
template <> struct format_code_gt<long> { inline static constexpr char value[2] = "l"; };
template <> struct format_code_gt<unsigned long> { inline static constexpr char value[2] = "L"; };
template <> struct format_code_gt<long long> { inline static constexpr char value[2] = "q"; };
template <> struct format_code_gt<unsigned long long> { inline static constexpr char value[2] = "Q"; };

template <> struct format_code_gt<float> { inline static constexpr char value[2] = "f"; };
template <> struct format_code_gt<double> { inline static constexpr char value[2] = "d"; };
// clang-format on

void wrap_database(py::module&);
void wrap_pandas(py::module&);
void wrap_networkx(py::module&);

} // namespace unum::ukv