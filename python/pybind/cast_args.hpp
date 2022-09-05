/**
 * @file cast_args.hpp
 * @brief Implements function-specific casting mechanisms relying on @see "cast.hpp".
 */
#pragma once
#include "ukv/cpp/ranges_args.hpp" // `places_arg_t`
#include "pybind/cast.hpp"

namespace unum::ukv::pyb {

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
    using viewed_t = places_arg_t;
    using owned_t = std::vector<col_key_field_t>;
    std::variant<std::monostate, viewed_t, owned_t> viewed_or_owned;
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
    using viewed_t = contents_arg_t;
    using owned_t = std::vector<value_view_t>;
    std::variant<std::monostate, viewed_t, owned_t> viewed_or_owned;
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
    using viewed_t = edges_view_t;
    using owned_t = std::vector<edge_t>;
    std::variant<std::monostate, viewed_t, owned_t> viewed_or_owned;

    operator edges_view_t() const noexcept {
        if (std::holds_alternative<std::monostate>(viewed_or_owned))
            return {};
        else if (std::holds_alternative<owned_t>(viewed_or_owned))
            return edges(std::get<owned_t>(viewed_or_owned));
        else
            return std::get<viewed_t>(viewed_or_owned);
    }

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
            viewed_or_owned = edges_view;
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
            viewed_or_owned = std::move(edges_vec);
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
                viewed_or_owned = edges_view;
            }
            else {
                edges_view_t edges_view {sources, targets};
                viewed_or_owned = edges_view;
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

            viewed_or_owned = std::move(edges_vec);
        }
    }
};

} // namespace unum::ukv::pyb