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
    ukv_collection_t col;
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
    strided_iterator_gt<ukv_collection_t const> cols_begin;
    strided_iterator_gt<ukv_key_t const> keys_begin;
    strided_iterator_gt<ukv_str_view_t const> fields_begin;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }
    inline place_t operator[](std::size_t i) const noexcept {
        ukv_collection_t col = cols_begin ? cols_begin[i] : ukv_collection_main_k;
        ukv_key_t const& key = keys_begin[i];
        ukv_str_view_t field = fields_begin ? fields_begin[i] : nullptr;
        return {col, key, field};
    }

    bool same_collection() const noexcept {
        return !cols_begin || cols_begin.repeats() ||
               !transform_reduce_n(cols_begin, count, false, [=](ukv_collection_t col) {
                   return col != cols_begin[0];
               });
    }

    bool same_collections_are_named() const noexcept { return cols_begin && cols_begin[0] != ukv_collection_main_k; }
};

/**
 * Working with batched data is ugly in C++.
 * This handle doesn't help in the general case,
 * but at least allow reusing the arguments.
 */
struct contents_arg_t {
    using value_type = value_view_t;
    strided_iterator_gt<ukv_octet_t const> presences_begin;
    strided_iterator_gt<ukv_length_t const> offsets_begin;
    strided_iterator_gt<ukv_length_t const> lengths_begin;
    strided_iterator_gt<ukv_bytes_cptr_t const> contents_begin;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }
    inline value_view_t operator[](std::size_t i) const noexcept {
        if (!contents_begin || !contents_begin[i] || (presences_begin && !presences_begin[i]))
            return {};

        auto begin = reinterpret_cast<byte_t const*>(contents_begin[i]);
        auto off = offsets_begin ? offsets_begin[i] : 0;
        ukv_val_len_t len = 0;
        if (lengths_begin)
            len = lengths_begin[i];
        else if (offsets_begin)
            len = offsets_begin[i + 1] - off;
        else
            len = std::strlen(reinterpret_cast<char const*>(begin) + off);
        return {begin + off, len};
    }

    inline bool is_arrow() const noexcept {
        return contents_begin.repeats() && offsets_begin && !lengths_begin &&
               (!presences_begin || presences_begin.is_continuous());
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
    ukv_collection_t col;
    ukv_key_t min_key;
    ukv_key_t max_key;
    ukv_length_t limit;
};

/**
 * @brief Arguments of `ukv_scan` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct scans_arg_t {
    strided_iterator_gt<ukv_collection_t const> cols;
    strided_iterator_gt<ukv_key_t const> start_keys;
    strided_iterator_gt<ukv_key_t const> end_keys;
    strided_iterator_gt<ukv_length_t const> limits;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }
    inline scan_t operator[](std::size_t i) const noexcept {
        ukv_collection_t col = cols ? cols[i] : ukv_collection_main_k;
        ukv_key_t start_key = start_keys ? start_keys[i] : std::numeric_limits<ukv_key_t>::min();
        ukv_key_t end_key = end_keys ? end_keys[i] : std::numeric_limits<ukv_key_t>::max();
        ukv_length_t limit = limits[i];
        return {col, start_key, end_key, limit};
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
    using tuples_t = std::remove_reference_t<tuples_at>;
    using element_t = typename tuples_t::value_type;
    constexpr bool immutable_k = std::is_const_v<element_t> || std::is_const_v<tuples_t>;
    using result_t = std::conditional_t<immutable_k, edges_view_t, edges_span_t>;
    auto ptr = std::data(tuples);
    auto count = std::size(tuples);
    return result_t(ptr, ptr + count);
}

} // namespace unum::ukv
