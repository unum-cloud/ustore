/**
 * @file cast.hpp
 * @brief Generic range-casting functions to avoid copies when exchange data.
 */
#pragma once
#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include "ukv/cpp/ranges.hpp" // `value_view_t`

namespace unum::ukv::pyb {

/**
 * @brief Defines Pythons type marker for primitive types.
 * @b All of them are only a single character long.
 */
template <typename element_at>
struct format_code_gt {};

#pragma region Zero Copy Buffer Protocol

/**
 * @brief RAII object for `Py_buffer` parsing purposes,
 * which releases the buffer in the destructor.
 */
struct py_buffer_t {
    py_buffer_t() = default;
    py_buffer_t(py_buffer_t const&) = delete;
    py_buffer_t(py_buffer_t&& other) noexcept : raw(std::exchange(other.raw, Py_buffer {})) {}
    ~py_buffer_t() { PyBuffer_Release(&raw); }

    Py_buffer raw;
};

inline py_buffer_t py_buffer(PyObject* obj, bool const_ = true) {
    auto flags = PyBUF_ANY_CONTIGUOUS | PyBUF_STRIDED;
    if (!const_)
        flags |= PyBUF_WRITABLE;

    py_buffer_t buf;
    bool initialized = PyObject_GetBuffer(obj, &buf.raw, flags) == 0;
    if (!initialized)
        throw std::invalid_argument("Couldn't obtain buffer overviews");
    if (!buf.raw.shape)
        throw std::invalid_argument("Shape wasn't inferred");
    return buf;
}

/**
 * @brief Provides a typed view of 1D potentially-strided tensor.
 * @param obj Must implement the "Buffer protocol".
 */
template <typename scalar_at>
strided_range_gt<scalar_at> py_strided_range(py_buffer_t const& buf) {

    if (buf.raw.ndim != 1)
        throw std::invalid_argument("Expecting tensor rank 1");
    if (buf.raw.itemsize != sizeof(scalar_at))
        throw std::invalid_argument("Scalar type mismatch");

    strided_range_gt<scalar_at> result {
        reinterpret_cast<scalar_at*>(buf.raw.buf),
        static_cast<ukv_size_t>(buf.raw.strides[0]),
        static_cast<ukv_size_t>(buf.raw.shape[0]),
    };
    return result;
}

/**
 * @brief Provides a typed view of 2D potentially-strided tensor.
 * @param obj Must implement the "Buffer protocol".
 */
template <typename scalar_at>
strided_matrix_gt<scalar_at> py_strided_matrix(py_buffer_t const& buf) {

    if (buf.raw.ndim != 2)
        throw std::invalid_argument("Expecting tensor rank 2");
    if (buf.raw.itemsize != sizeof(scalar_at))
        throw std::invalid_argument("Scalar type mismatch");

    strided_matrix_gt<scalar_at> result {
        reinterpret_cast<scalar_at*>(buf.raw.buf),
        static_cast<ukv_size_t>(buf.raw.shape[0]),
        static_cast<ukv_size_t>(buf.raw.shape[1]),
        static_cast<ukv_size_t>(buf.raw.strides[0]),
        static_cast<ukv_size_t>(buf.raw.strides[1]),
    };
    return result;
}

#pragma region Casting Python Objects

template <typename scalar_at>
scalar_at py_to_scalar(PyObject* obj) {
    if constexpr (std::is_integral_v<scalar_at>) {
        if (!PyLong_Check(obj))
            throw std::invalid_argument("Expects integer");
        return static_cast<scalar_at>(PyLong_AsUnsignedLong(obj));
    }
    else {
        if (!PyFloat_Check(obj))
            throw std::invalid_argument("Expects float");
        return static_cast<scalar_at>(PyFloat_AsDouble(obj));
    }
}

inline value_view_t py_to_bytes(PyObject* obj) {
    if (PyBytes_Check(obj)) {
        char* buffer = nullptr;
        Py_ssize_t length = 0;
        PyBytes_AsStringAndSize(obj, &buffer, &length);
        return {reinterpret_cast<ukv_val_ptr_t>(buffer), static_cast<ukv_val_len_t>(length)};
    }
    else if (obj == Py_None) {
        // Means the object must be deleted
        return value_view_t {};
    }

    throw std::invalid_argument("Value must be representable as a byte array");
    return {};
}

inline ukv_str_view_t py_to_str(PyObject* obj) {
    return py_to_bytes(obj).c_str();
}

inline bool py_is_sequence(PyObject* obj) {
    return PyTuple_Check(obj) || PyList_Check(obj) || PyIter_Check(obj);
}

inline std::optional<std::size_t> py_sequence_length(PyObject* obj) {
    if (PyTuple_Check(obj))
        return PyTuple_Size(obj);
    else if (PyList_Check(obj))
        return PyList_Size(obj);

    return std::nullopt;
}

/**
 * @brief Iterates over Python `tuple`, `list`, or any `iter`.
 * @param call Callback for member `PyObject`s.
 */
template <typename transform_at, typename output_iterator_at>
void py_transform_n(PyObject* obj,
                    transform_at&& transform,
                    output_iterator_at output_iterator,
                    std::size_t max_count = std::numeric_limits<std::size_t>::max()) {

    // TODO: Bring back the support of NumPy arrays
    // TODO: Add support for Arrow inputs

    if (PyTuple_Check(obj)) {
        max_count = std::min<std::size_t>(PyTuple_Size(obj), max_count);
        for (std::size_t i = 0; i != max_count; ++i, ++output_iterator)
            *output_iterator = transform(PyTuple_GetItem(obj, i));
    }
    else if (PyList_Check(obj)) {
        max_count = std::min<std::size_t>(PyList_Size(obj), max_count);
        for (std::size_t i = 0; i != max_count; ++i, ++output_iterator)
            *output_iterator = transform(PyList_GetItem(obj, i));
    }
    else if (PyIter_Check(obj)) {
        PyObject* item = nullptr;
        while ((item = PyIter_Next(obj))) {
            *output_iterator = transform(item);
            Py_DECREF(item);
            ++output_iterator;
        }
    }
}

/**
 * @brief Iterates over Python `dict`-like object.
 * @param call Callback for the key and value `PyObject`s.
 * @return true If a supported iterable type was detected.
 */
template <typename member_callback_at>
void py_scan_dict(PyObject* obj, member_callback_at&& call) {

    PyObject* key = nullptr;
    PyObject* value = nullptr;
    Py_ssize_t pos = 0;

    while (PyDict_Next(obj, &pos, &key, &value))
        call(key, value);
}

inline void throw_not_implemented() {
    // https://github.com/pybind/pybind11/issues/1125#issuecomment-691552571
    throw std::runtime_error("Not Implemented!");
}

#pragma region Type Conversions Guides

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

template <typename scalar_at>
bool can_cast_internal_scalars(py_buffer_t const& buf) {

    auto fmt_len = std::strlen(buf.raw.format);
    if (fmt_len != 1)
        throw std::invalid_argument("Unknown Python format string");

    if (buf.raw.itemsize != sizeof(scalar_at))
        return false;

    constexpr bool ii = std::is_integral_v<scalar_at>;
    constexpr bool is = std::is_signed_v<scalar_at>;

    switch (buf.raw.format[0]) {

        // Signed integral types
    case format_code_gt<short>::value[0]: return is && ii;
    case format_code_gt<int>::value[0]: return is && ii;
    case format_code_gt<long>::value[0]: return is && ii;
    case format_code_gt<long long>::value[0]:
        return is && ii;

        // Unsigned integral types
    case format_code_gt<unsigned short>::value[0]: return !is && ii;
    case format_code_gt<unsigned int>::value[0]: return !is && ii;
    case format_code_gt<unsigned long>::value[0]: return !is && ii;
    case format_code_gt<unsigned long long>::value[0]: return !is && ii;

    // Non-integral types
    case format_code_gt<float>::value[0]: return !ii;
    case format_code_gt<double>::value[0]: return !ii;

    // All following types are considered `byte_t`
    case format_code_gt<char>::value[0]: return true;
    case format_code_gt<signed char>::value[0]: return true;
    case format_code_gt<unsigned char>::value[0]: return true;

    // Special cases
    case format_code_gt<bool>::value[0]: return std::is_same_v<bool, scalar_at>;
    default: return false;
    }
}

template <typename scalar_at>
bool can_view_as_strided_range(py_buffer_t const& buf) {
    return buf.raw.ndim == 1 && can_cast_internal_scalars<scalar_at>(buf);
}

template <typename scalar_at>
bool can_view_as_strided_matrix(py_buffer_t const& buf) {
    return buf.raw.ndim == 2 && can_cast_internal_scalars<scalar_at>(buf);
}

/**
 * May view:
 * > NumPy (strided) column of `ukv_key_t` scalars.
 * > Apache Arrow array of `ukv_key_t` scalars.
 * > Apache Arrow table with "keys" column of `ukv_key_t` scalars
 *   and, optionally, "cols" column of IDs.
 * > Buffer-protocol 1D implementation with `ukv_key_t` scalars.
 *
 * May be copied from:
 * > list of any integer-convertible PyObjects.
 * > tuple of any integer-convertible PyObjects.
 * > iterable of any integer-convertible PyObjects.
 * > NumPy column of alternative integral type.
 * > Apache Arrow array of alternative integral type.
 *
 * No support for nested fields just yet.
 */
struct parsed_places_t {
    std::variant<std::monostate, places_arg_t, std::vector<col_key_field_t>> viewed_or_owned;
    operator places_arg_t() const noexcept {}
    parsed_places_t(PyObject* keys) {}
};

/**
 * May view:
 * > Apache Arrow array of binary or UTF8 strings.
 * > Apache Arrow table with "vals" column of binary or UTF8 strings.
 *
 * May allocate an array of `value_view_t` to reference:
 * > list of `bytes`-like PyObjects.
 * > tuple of `bytes`-like PyObjects.
 * > iterable of `bytes`-like PyObjects.
 * > Apache Arrow array of any objects.
 * > Apache Arrow array with "vals" column of any objects.
 */
struct parsed_contents_t {
    std::variant<std::monostate, contents_arg_t, std::vector<value_view_t>> viewed_or_owned;
    operator contents_arg_t() const noexcept {}
    parsed_contents_t(PyObject* contents) {}
};

/**
 * May view:
 * > NumPy (strided) column of `ukv_key_t` scalars.
 * > 3x Apache Arrow array of `ukv_key_t` scalars.
 * > Apache Arrow table with "source", "target", (optional) "edge" `ukv_key_t` columns.
 * > Buffer-protocol 2D implementation with 3x columns of `ukv_key_t` scalars.
 *
 * May be copied from:
 * > list/tuple of lists/tuples of any integer-convertible PyObjects.
 * > iterable of lists/tuples of any integer-convertible PyObjects.
 *
 * No support for nested fields just yet.
 */
struct parsed_adjacency_list_t {
    std::variant<std::monostate, edges_view_t, std::vector<edge_t>> viewed_or_owned;
    operator contents_arg_t() const noexcept {}

    parsed_adjacency_list_t(PyObject* adjacency_list) {
        // Check if we can do zero-copy
        if (PyObject_CheckBuffer(adjacency_list)) {
            py_buffer_t buf = py_buffer(adjacency_list);
            if (!can_cast_internal_scalars<ukv_key_t>(buf))
                throw std::invalid_argument("Expecting `ukv_key_t` scalars in zero-copy interface");
            auto mat = py_strided_matrix<ukv_key_t const>(buf);
            auto cols = mat.cols();
            if (cols != 2 && cols != 3)
                throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

            edges_view_t edges_view {
                mat.col(0),
                mat.col(1),
                cols == 3 ? mat.col(2) : strided_range_gt<ukv_key_t const>(&ukv_default_edge_id_k, 0, mat.rows()),
            };
            call(edges_view);
        }
        // Otherwise, we expect a sequence of 2-tuples or 3-tuples
        else {
            std::vector<edge_t> edges_vec;
            auto adj_len = py_sequence_length(adjacency_list);
            if (adj_len)
                edges_vec.reserve(*adj_len);

            auto to_edge = [](PyObject* obj) -> edge_t {
                if (!PyTuple_Check(obj))
                    throw std::invalid_argument("Each edge must be represented by a tuple");
                auto cols = PyTuple_Size(obj);
                if (cols != 2 && cols != 3)
                    throw std::invalid_argument("Expecting 2 or 3 columns: sources, targets, edge IDs");

                edge_t result;
                result.source_id = py_to_scalar<ukv_key_t>(PyTuple_GetItem(obj, 0));
                result.target_id = py_to_scalar<ukv_key_t>(PyTuple_GetItem(obj, 1));
                result.id = cols == 3 ? py_to_scalar<ukv_key_t>(PyTuple_GetItem(obj, 2)) : ukv_default_edge_id_k;
                return result;
            };
            py_transform_n(adjacency_list, to_edge, std::back_inserter(edges_vec));
            call(edges(edges_vec));
        }
    }

    parsed_adjacency_list_t(PyObject* source_ids, PyObject* target_ids, PyObject* edge_ids) {

        //
        auto source_ids_is_buf = PyObject_CheckBuffer(source_ids);
        auto target_ids_is_buf = PyObject_CheckBuffer(target_ids);
        auto edge_ids_is_buf = PyObject_CheckBuffer(edge_ids);
        auto all_same = source_ids_is_buf == target_ids_is_buf;
        if (edge_ids != Py_None)
            all_same &= source_ids_is_buf = edge_ids_is_buf;

        // Check if we can do zero-copy
        if (source_ids_is_buf) {
            if (!all_same)
                throw std::invalid_argument("Expecting `ukv_key_t` scalars in zero-copy interface");

            auto sources_handle = py_buffer(source_ids);
            auto sources = py_strided_range<ukv_key_t const>(sources_handle);
            auto targets_handle = py_buffer(target_ids);
            auto targets = py_strided_range<ukv_key_t const>(targets_handle);
            if (edge_ids != Py_None) {
                auto edge_ids_handle = py_buffer(edge_ids);
                auto edge_ids = py_strided_range<ukv_key_t const>(edge_ids_handle);
                edges_view_t edges_view {sources, targets, edge_ids};
                call(edges_view);
            }
            else {
                edges_view_t edges_view {sources, targets};
                call(edges_view);
            }
        }
        // Otherwise, we expect a sequence of 2-tuples or 3-tuples
        else {
            auto sources_n = py_sequence_length(source_ids);
            auto targets_n = py_sequence_length(target_ids);
            if (!sources_n || !targets_n || sources_n != targets_n)
                throw std::invalid_argument("Sequence lengths must match");

            auto n = *sources_n;
            std::vector<edge_t> edges_vec(n);
            edges_span_t edges_span = edges(edges_vec);

            py_transform_n(source_ids, &py_to_scalar<ukv_key_t>, edges_span.source_ids.begin(), n);
            py_transform_n(target_ids, &py_to_scalar<ukv_key_t>, edges_span.target_ids.begin(), n);
            if (edge_ids != Py_None)
                py_transform_n(edge_ids, &py_to_scalar<ukv_key_t>, edges_span.edge_ids.begin(), n);

            call(edges_span);
        }
    }
};

} // namespace unum::ukv::pyb