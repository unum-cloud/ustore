/**
 * @file utility.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 *
 * @brief Smart Pointers, Monads and Range-like abstractions for C++ bindings.
 */

#pragma once
#include "ukv/cpp/types.hpp" // `value_view_t`

namespace unum::ukv {

/**
 * @brief A smart pointer type with customizable jump length for increments.
 * In other words, it allows a strided data layout, common to HPC apps.
 * Cool @b hint, you can use this to represent an infinite array of repeating
 * values with `stride` equal to zero.
 */
template <typename object_at>
class strided_iterator_gt {

    object_at* raw_ = nullptr;
    ukv_size_t stride_ = 0;

    object_at* upshift(std::ptrdiff_t bytes) const noexcept { return (object_at*)((char*)raw_ + bytes); }
    object_at* downshift(std::ptrdiff_t bytes) const noexcept { return (object_at*)((char*)raw_ - bytes); }

  public:
    using iterator_category = std::random_access_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = object_at;
    using pointer = value_type*;
    using reference = value_type&;

    inline strided_iterator_gt(object_at* raw = nullptr, ukv_size_t stride = 0) noexcept : raw_(raw), stride_(stride) {}
    inline strided_iterator_gt(strided_iterator_gt&&) noexcept = default;
    inline strided_iterator_gt(strided_iterator_gt const&) noexcept = default;
    inline strided_iterator_gt& operator=(strided_iterator_gt&&) noexcept = default;
    inline strided_iterator_gt& operator=(strided_iterator_gt const&) noexcept = default;

    inline object_at& operator[](ukv_size_t idx) const noexcept { return *upshift(stride_ * idx); }

    inline strided_iterator_gt& operator++() noexcept {
        raw_ = upshift(stride_);
        return *this;
    }

    inline strided_iterator_gt& operator--() noexcept {
        raw_ = downshift(stride_);
        return *this;
    }

    inline strided_iterator_gt operator++(int) const noexcept { return {upshift(stride_), stride_}; }
    inline strided_iterator_gt operator--(int) const noexcept { return {downshift(stride_), stride_}; }
    inline strided_iterator_gt operator+(std::ptrdiff_t n) const noexcept { return {upshift(n * stride_), stride_}; }
    inline strided_iterator_gt operator-(std::ptrdiff_t n) const noexcept { return {downshift(n * stride_), stride_}; }
    inline strided_iterator_gt& operator+=(std::ptrdiff_t n) noexcept {
        raw_ = upshift(n * stride_);
        return *this;
    }
    inline strided_iterator_gt& operator-=(std::ptrdiff_t n) noexcept {
        raw_ = downshift(n * stride_);
        return *this;
    }

    /**
     * ! Calling this function with "stride" different from zero or
     * ! non-zero `sizeof(object_at)` multiple will cause Undefined
     * ! Behaviour.
     */
    inline std::ptrdiff_t operator-(strided_iterator_gt other) const noexcept {
        return stride_ ? (raw_ - other.raw_) * sizeof(object_at) / stride_ : 0;
    }

    inline operator bool() const noexcept { return raw_ != nullptr; }
    inline bool repeats() const noexcept { return !stride_; }
    inline ukv_size_t stride() const noexcept { return stride_; }
    inline object_at& operator*() const noexcept { return *raw_; }
    inline object_at* operator->() const noexcept { return raw_; }
    inline object_at* get() const noexcept { return raw_; }

    inline bool operator==(strided_iterator_gt const& other) const noexcept { return raw_ == other.raw_; }
    inline bool operator!=(strided_iterator_gt const& other) const noexcept { return raw_ != other.raw_; }

    template <typename member_at, typename parent_at = object_at>
    inline auto members(member_at parent_at::*member_ptr) const noexcept {
        using parent_t = std::conditional_t<std::is_const_v<object_at>, parent_at const, parent_at>;
        using member_t = std::conditional_t<std::is_const_v<object_at>, member_at const, member_at>;
        parent_t& first = *raw_;
        member_t& first_member = first.*member_ptr;
        return strided_iterator_gt<member_t> {&first_member, stride()};
    }
};

template <typename object_at>
class strided_range_gt {
    static_assert(!std::is_void_v<object_at>);

    object_at* begin_ = nullptr;
    ukv_size_t stride_ = 0;
    ukv_size_t count_ = 0;

  public:
    using value_type = object_at;

    strided_range_gt() = default;
    strided_range_gt(object_at* single) noexcept : begin_(single), stride_(0), count_(1) {}
    strided_range_gt(object_at* begin, object_at* end) noexcept
        : begin_(begin), stride_(sizeof(object_at)), count_(end - begin) {}
    strided_range_gt(object_at* begin, std::size_t stride, std::size_t count) noexcept
        : begin_(begin), stride_(static_cast<ukv_size_t>(stride)), count_(static_cast<ukv_size_t>(count)) {}
    strided_range_gt(strided_iterator_gt<object_at> begin, std::size_t count) noexcept
        : strided_range_gt(begin.get(), begin.stride(), count) {}

    strided_range_gt(strided_range_gt&&) = default;
    strided_range_gt(strided_range_gt const&) = default;
    strided_range_gt& operator=(strided_range_gt&&) = default;
    strided_range_gt& operator=(strided_range_gt const&) = default;

    inline object_at* data() const noexcept { return begin_; }
    inline decltype(auto) begin() const noexcept { return strided_iterator_gt<object_at> {begin_, stride_}; }
    inline decltype(auto) end() const noexcept { return begin() + static_cast<std::ptrdiff_t>(count_); }
    inline decltype(auto) at(std::size_t i) const noexcept { return *(begin() + static_cast<std::ptrdiff_t>(i)); }
    inline decltype(auto) operator[](std::size_t i) const noexcept { return at(i); }

    inline auto immutable() const noexcept { return strided_range_gt<object_at const>(begin_, stride_, count_); }
    inline strided_range_gt subspan(std::size_t offset, std::size_t count) const noexcept {
        return {begin_ + offset * stride_, stride_, count};
    }

    inline bool empty() const noexcept { return !count_; }
    inline std::size_t size() const noexcept { return count_; }
    inline ukv_size_t stride() const noexcept { return stride_; }
    inline ukv_size_t count() const noexcept { return count_; }
    inline operator bool() const noexcept { return begin_ != nullptr; }

    template <typename member_at, typename parent_at = object_at>
    inline auto members(member_at parent_at::*member_ptr) const noexcept {
        auto begin_members = begin().members(member_ptr);
        using member_t = typename decltype(begin_members)::value_type;
        return strided_range_gt<member_t> {begin_members.get(), begin_members.stride(), count()};
    }
};

template <typename at, typename alloc_at = std::allocator<at>>
strided_range_gt<at> strided_range(std::vector<at, alloc_at>& vec) noexcept {
    return {vec.data(), sizeof(at), vec.size()};
}

template <typename at, typename alloc_at = std::allocator<at>>
strided_range_gt<at const> strided_range(std::vector<at, alloc_at> const& vec) noexcept {
    return {vec.data(), sizeof(at), vec.size()};
}

template <typename at, std::size_t count_ak>
strided_range_gt<at> strided_range(at (&c_array)[count_ak]) noexcept {
    return {&c_array[0], sizeof(at), count_ak};
}

template <typename at, std::size_t count_ak>
strided_range_gt<at const> strided_range(std::array<at, count_ak> const& array) noexcept {
    return {array.data(), sizeof(at), count_ak};
}

template <typename at>
strided_range_gt<at const> strided_range(std::initializer_list<at> list) noexcept {
    return {list.begin(), sizeof(at), list.size()};
}

/**
 * @brief Similar to `std::optional<std::span>`.
 * It's NULL state and "empty string" states are not identical.
 * The NULL state generally reflects missing values.
 */
template <typename pointer_at>
struct indexed_range_gt {
    pointer_at begin_ = nullptr;
    pointer_at end_ = nullptr;

    inline pointer_at begin() const noexcept { return begin_; }
    inline pointer_at end() const noexcept { return end_; }
    inline decltype(auto) operator[](std::size_t i) const noexcept { return begin_[i]; }
    inline decltype(auto) at(std::size_t i) const noexcept { return begin_[i]; }

    inline std::size_t size() const noexcept { return end_ - begin_; }
    inline bool empty() const noexcept { return end_ == begin_; }
    inline operator bool() const noexcept { return end_ != begin_; }
    inline auto strided() const noexcept {
        using element_t = std::remove_pointer_t<pointer_at>;
        using strided_t = strided_range_gt<element_t>;
        return strided_t {begin_, sizeof(element_t), static_cast<ukv_size_t>(size())};
    }
};

template <typename pointer_at>
struct range_gt {
    pointer_at begin_ = nullptr;
    pointer_at end_ = nullptr;

    inline pointer_at const& begin() const& noexcept { return begin_; }
    inline pointer_at const& end() const& noexcept { return end_; }
    inline pointer_at&& begin() && noexcept { return std::move(begin_); }
    inline pointer_at&& end() && noexcept { return std::move(end_); }
};

#pragma region Tapes

/**
 * @brief A read-only iterator for values packed into a
 * contiguous memory range. Doesn't own underlying memory.
 */
class tape_iterator_t {

    ukv_val_ptr_t contents_ = nullptr;
    ukv_val_len_t* offsets_ = nullptr;
    ukv_val_len_t* lengths_ = nullptr;

  public:
    using iterator_category = std::random_access_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = value_view_t;
    using pointer = void;
    using reference = void;

    inline tape_iterator_t(ukv_val_ptr_t vals, ukv_val_len_t* offs, ukv_val_len_t* lens) noexcept
        : contents_(vals), offsets_(offs), lengths_(lens) {}

    inline tape_iterator_t& operator++() noexcept {
        ++lengths_;
        ++offsets_;
        return *this;
    }

    inline tape_iterator_t operator++(int) const noexcept { return {contents_, lengths_ + 1, offsets_ + 1}; }
    inline tape_iterator_t operator--(int) const noexcept { return {contents_, lengths_ - 1, offsets_ - 1}; }
    inline value_view_t operator*() const noexcept { return {contents_ + *offsets_, *lengths_}; }

    inline bool operator==(tape_iterator_t const& other) const noexcept { return lengths_ == other.lengths_; }
    inline bool operator!=(tape_iterator_t const& other) const noexcept { return lengths_ != other.lengths_; }
};

class tape_view_t {
    ukv_val_ptr_t contents_ = nullptr;
    ukv_val_len_t* offsets_ = nullptr;
    ukv_val_len_t* lengths_ = nullptr;
    ukv_size_t count_ = 0;

  public:
    inline tape_view_t() = default;

    inline tape_view_t(ukv_val_ptr_t vals, ukv_val_len_t* offs, ukv_val_len_t* lens, ukv_size_t elements) noexcept
        : contents_(vals), offsets_(offs), lengths_(lens), count_(elements) {}

    inline tape_iterator_t begin() const noexcept { return {contents_, offsets_, lengths_}; }
    inline tape_iterator_t end() const noexcept { return {contents_, offsets_ + count_, lengths_ + count_}; }
    inline std::size_t size() const noexcept { return count_; }

    inline ukv_val_len_t* offsets() const noexcept { return offsets_; }
    inline ukv_val_len_t* lengths() const noexcept { return lengths_; }
    inline ukv_val_ptr_t contents() const noexcept { return contents_; }
};

#pragma region Multiple Dimensions

template <typename scalar_at>
class strided_matrix_gt {
  public:
    using scalar_t = scalar_at;
    static_assert(!std::is_void_v<scalar_t>);

  private:
    scalar_t* begin_ = nullptr;
    ukv_size_t bytes_between_rows_ = 0;
    ukv_size_t bytes_between_cols_ = 0;
    ukv_size_t rows_ = 0;
    ukv_size_t cols_ = 0;

  public:
    strided_matrix_gt() = default;
    strided_matrix_gt(scalar_t* begin,
                      std::size_t rows,
                      std::size_t cols,
                      std::size_t bytes_between_rows,
                      std::size_t col_stride = sizeof(scalar_t)) noexcept
        : begin_(begin), bytes_between_rows_(static_cast<ukv_size_t>(bytes_between_rows)),
          bytes_between_cols_(static_cast<ukv_size_t>(col_stride)), rows_(static_cast<ukv_size_t>(rows)),
          cols_(static_cast<ukv_size_t>(cols)) {}

    strided_matrix_gt(strided_matrix_gt&&) = default;
    strided_matrix_gt(strided_matrix_gt const&) = default;
    strided_matrix_gt& operator=(strided_matrix_gt&&) = default;
    strided_matrix_gt& operator=(strided_matrix_gt const&) = default;

    inline std::size_t size() const noexcept { return rows_ * cols_; }
    inline decltype(auto) operator()(std::size_t i, std::size_t j) noexcept { return row(i)[j]; }
    inline decltype(auto) operator()(std::size_t i, std::size_t j) const noexcept { return row(i)[j]; }
    inline strided_range_gt<scalar_t const> col(std::size_t j) const noexcept {
        auto begin = begin_ + j * bytes_between_cols_ / sizeof(scalar_t);
        return {begin, bytes_between_rows_, rows_};
    }
    inline strided_range_gt<scalar_t const*> row(std::size_t i) const noexcept {
        auto begin = begin_ + i * bytes_between_rows_ / sizeof(scalar_t);
        return {begin, bytes_between_cols_, cols_};
    }
    inline std::size_t rows() const noexcept { return rows_; }
    inline std::size_t cols() const noexcept { return cols_; }
    inline scalar_t const* data() const noexcept { return begin_; }
};

#pragma region Algorithms

/**
 * @brief Unlike the `std::accumulate` and `std::transform_reduce` takes an integer `n`
 * instead of the end iterator. This helps with zero-strided iterators.
 */
template <typename element_at, typename transform_at, typename iterator_at>
element_at transform_reduce_n(iterator_at begin, std::size_t n, element_at init, transform_at transform) {
    for (std::size_t i = 0; i != n; ++i, ++begin)
        init += transform(*begin);
    return init;
}

template <typename transform_at, typename output_iterator_at, typename iterator_at>
output_iterator_at transform_n(iterator_at begin, std::size_t n, output_iterator_at output, transform_at transform) {
    for (std::size_t i = 0; i != n; ++i, ++begin, ++output)
        *output = *begin;
    return output;
}

template <typename element_at, typename iterator_at>
element_at reduce_n(iterator_at begin, std::size_t n, element_at init) {
    return transform_reduce_n(begin, n, init, [](auto x) { return x; });
}

template <typename iterator_at>
bool all_ascending(iterator_at begin, std::size_t n) {
    auto previous = begin;
    ++begin;
    for (std::size_t i = 1; i != n; ++i, ++begin)
        if (*begin <= *std::exchange(previous, begin))
            return false;
    return true;
}

#pragma region Aliases and Packs

using keys_view_t = strided_range_gt<ukv_key_t const>;
using fields_view_t = strided_range_gt<ukv_str_view_t const>;

/**
 * Working with batched data is ugly in C++.
 * This handle doesn't help in the general case,
 * but at least allow reusing the arguments.
 */
struct keys_arg_t {
    using value_type = col_key_field_t;
    strided_iterator_gt<ukv_col_t const> cols_begin;
    strided_iterator_gt<ukv_key_t const> keys_begin;
    strided_iterator_gt<ukv_str_view_t const> fields_begin;
    ukv_size_t count = 0;
};

/**
 * Working with batched data is ugly in C++.
 * This handle doesn't help in the general case,
 * but at least allow reusing the arguments.
 */
struct values_arg_t {
    using value_type = value_view_t;
    strided_iterator_gt<ukv_val_ptr_t const> contents_begin;
    strided_iterator_gt<ukv_val_len_t const> offsets_begin;
    strided_iterator_gt<ukv_val_len_t const> lengths_begin;
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

/**
 * @brief Iterates through a predetermined number of NULL-delimited
 * strings joined one after another in continuous memory.
 * Can be used for `ukv_docs_gist` or `ukv_col_list`.
 */
class strings_tape_iterator_t {
    ukv_size_t remaining_count_ = 0;
    ukv_str_view_t current_ = nullptr;

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::string_view;
    using pointer = ukv_str_view_t*;
    using reference = std::string_view;

    strings_tape_iterator_t(ukv_size_t remaining = 0, ukv_str_view_t current = nullptr)
        : remaining_count_(remaining), current_(current) {}

    strings_tape_iterator_t(strings_tape_iterator_t&&) = default;
    strings_tape_iterator_t& operator=(strings_tape_iterator_t&&) = default;

    strings_tape_iterator_t(strings_tape_iterator_t const&) = default;
    strings_tape_iterator_t& operator=(strings_tape_iterator_t const&) = default;

    strings_tape_iterator_t& operator++() noexcept {
        current_ += std::strlen(current_) + 1;
        --remaining_count_;
        return *this;
    }

    strings_tape_iterator_t operator++(int) noexcept {
        return {remaining_count_ - 1, current_ + std::strlen(current_) + 1};
    }

    ukv_str_view_t operator*() const noexcept { return current_; }
    bool is_end() const noexcept { return !remaining_count_; }
    ukv_size_t size() const noexcept { return remaining_count_; }
};

} // namespace unum::ukv
