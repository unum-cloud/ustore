/**
 * @file ranges_args.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 *
 * @brief Range-like argument resolvers for UKV.
 */

#pragma once
#include "ukv/cpp/ranges.hpp"

namespace unum::ukv {

using keys_view_t = strided_range_gt<ukv_key_t const>;
using fields_view_t = strided_range_gt<ukv_str_view_t const>;

struct place_t {
    ukv_col_t col;
    ukv_key_t const& key;
    ukv_str_view_t field;

    inline col_key_t col_key() const noexcept { return {col, key}; }
    inline col_key_field_t col_key_field() const noexcept { return {col, key, field}; }
};
/**
 * Working with batched data is ugly in C++.
 * This handle doesn't help in the general case,
 * but at least allow reusing the arguments.
 */
struct places_arg_t {
    using value_type = place_t;
    strided_iterator_gt<ukv_col_t const> cols_begin;
    strided_iterator_gt<ukv_key_t const> keys_begin;
    strided_iterator_gt<ukv_str_view_t const> fields_begin;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }
    inline place_t operator[](std::size_t i) const noexcept {
        ukv_col_t col = cols_begin ? cols_begin[i] : ukv_col_main_k;
        ukv_key_t const& key = keys_begin[i];
        ukv_str_view_t field = fields_begin ? fields_begin[i] : nullptr;
        return {col, key, field};
    }
};

/**
 * Working with batched data is ugly in C++.
 * This handle doesn't help in the general case,
 * but at least allow reusing the arguments.
 */
struct contents_arg_t {
    using value_type = value_view_t;
    strided_iterator_gt<ukv_val_ptr_t const> contents_begin;
    strided_iterator_gt<ukv_val_len_t const> offsets_begin;
    strided_iterator_gt<ukv_val_len_t const> lengths_begin;
    strided_range_gt<ukv_1x8_t const> nulls;

    inline std::size_t size() const noexcept { return nulls.size(); }
    inline value_view_t operator[](std::size_t i) const noexcept {
        if (!contents_begin || !contents_begin[i] || (nulls && nulls[i]))
            return {};

        auto begin = reinterpret_cast<byte_t const*>(contents_begin[i]);
        auto off = offsets_begin ? offsets_begin[i] : 0;
        auto len = lengths_begin //
                       ? lengths_begin[i]
                       : offsets_begin //
                             ? offsets_begin[i + 1] - off
                             : std::strlen((char const*)begin + off);

        return {begin + off, len};
    }

    inline bool is_arrow() const noexcept {
        return contents_begin.repeats() && nulls && offsets_begin && !lengths_begin;
    }
    inline bool is_continuous() const noexcept {
        auto last = operator[](0);
        for (std::size_t i = 1; i != size(); ++i) {
            auto value = operator[](i);
            if (value.begin() != last.end())
                return false;
            last = value;
        }
        return true;
    }
};

struct scan_t {
    ukv_col_t col;
    ukv_key_t const& min_key;
    ukv_size_t length;

    inline col_key_t col_key() const noexcept { return col_key_t {col, min_key}; }
};

/**
 * @brief Arguments of `ukv_scan` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct scans_arg_t {
    strided_iterator_gt<ukv_col_t const> cols;
    strided_iterator_gt<ukv_key_t const> min_keys;
    strided_iterator_gt<ukv_size_t const> lengths;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }
    inline scan_t operator[](std::size_t i) const noexcept {
        ukv_col_t col = cols ? cols[i] : ukv_col_main_k;
        ukv_key_t const& key = min_keys[i];
        ukv_size_t len = lengths[i];
        return {col, key, len};
    }
};

template <typename id_at>
struct edges_range_gt {

    using id_t = id_at;
    using tuple_t = std::conditional_t<std::is_const_v<id_t>, edge_t const, edge_t>;
    static_assert(sizeof(tuple_t) == 3 * sizeof(id_t));

    strided_range_gt<id_t> source_ids;
    strided_range_gt<id_t> target_ids;
    strided_range_gt<id_t> edge_ids;

    inline edges_range_gt() = default;
    inline edges_range_gt(edges_range_gt&&) = default;
    inline edges_range_gt(edges_range_gt const&) = default;
    inline edges_range_gt& operator=(edges_range_gt&&) = default;
    inline edges_range_gt& operator=(edges_range_gt const&) = default;

    inline edges_range_gt(strided_range_gt<id_t> sources,
                          strided_range_gt<id_t> targets,
                          strided_range_gt<id_t> edges = {&ukv_default_edge_id_k}) noexcept
        : source_ids(sources), target_ids(targets), edge_ids(edges) {}

    inline edges_range_gt(tuple_t* ptr, tuple_t* end) noexcept {
        auto strided = strided_range_gt<tuple_t>(ptr, end);
        source_ids = strided.members(&edge_t::source_id);
        target_ids = strided.members(&edge_t::target_id);
        edge_ids = strided.members(&edge_t::id);
    }

    inline std::size_t size() const noexcept { return std::min(source_ids.count(), target_ids.count()); }

    inline edge_t operator[](std::size_t i) const noexcept {
        edge_t result;
        result.source_id = source_ids[i];
        result.target_id = target_ids[i];
        result.id = edge_ids[i];
        return result;
    }

    inline operator edges_range_gt<id_at const>() const noexcept { return immutable(); }
    inline edges_range_gt<id_at const> immutable() const noexcept {
        return {source_ids.immutable(), target_ids.immutable(), edge_ids.immutable()};
    }
};

using edges_span_t = edges_range_gt<ukv_key_t>;
using edges_view_t = edges_range_gt<ukv_key_t const>;

template <typename tuples_at>
auto edges(tuples_at&& tuples) noexcept {
    using value_type = typename std::remove_reference_t<tuples_at>::value_type;
    using result_t = std::conditional_t<std::is_const_v<value_type>, edges_view_t, edges_span_t>;
    auto ptr = std::data(tuples);
    auto count = std::size(tuples);
    return result_t(ptr, ptr + count);
}

} // namespace unum::ukv
