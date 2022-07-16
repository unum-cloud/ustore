/**
 * @file utility.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 * @brief Smart Pointers, Monads and Range-like abstractions for C++ bindings.
 */

#pragma once
#include <vector>
#include <functional> // `std::hash`
#include <optional>   // `std::optional`
#include <stdexcept>  // `std::runtime_error`

#include "ukv/ukv.h"

namespace unum::ukv {

using key_t = ukv_key_t;
using val_len_t = ukv_val_len_t;
using tape_ptr_t = ukv_val_ptr_t;
using size_t = ukv_size_t;

enum class byte_t : uint8_t {};

/**
 * @brief An OOP-friendly location representation for objects in the DB.
 * Should be used with `stride` set to `sizeof(located_key_t)`.
 */
struct located_key_t {

    ukv_collection_t collection = ukv_default_collection_k;
    ukv_key_t key = 0;

    located_key_t() = default;
    located_key_t(located_key_t const&) = default;
    located_key_t& operator=(located_key_t const&) = default;

    inline located_key_t(ukv_collection_t c, ukv_key_t k) noexcept : collection(c), key(k) {}
    inline located_key_t(ukv_key_t k) noexcept : key(k) {}
    inline located_key_t in(ukv_collection_t col) noexcept { return {col, key}; }

    inline bool operator==(located_key_t const& other) const noexcept {
        return (collection == other.collection) & (key == other.key);
    }
    inline bool operator!=(located_key_t const& other) const noexcept {
        return (collection != other.collection) | (key != other.key);
    }
    inline bool operator<(located_key_t const& other) const noexcept { return key < other.key; }
    inline bool operator>(located_key_t const& other) const noexcept { return key > other.key; }
    inline bool operator<=(located_key_t const& other) const noexcept { return key <= other.key; }
    inline bool operator>=(located_key_t const& other) const noexcept { return key >= other.key; }
};

class [[nodiscard]] status_t {
    ukv_error_t raw_ = nullptr;

  public:
    status_t(ukv_error_t err = nullptr) noexcept : raw_(err) {}
    operator bool() const noexcept { return !raw_; }

    status_t(status_t const&) = delete;
    status_t& operator=(status_t const&) = delete;

    status_t(status_t&& other) noexcept { raw_ = std::exchange(other.raw_, nullptr); }
    status_t& operator=(status_t&& other) noexcept {
        std::swap(raw_, other.raw_);
        return *this;
    }
    ~status_t() {
        if (raw_)
            ukv_error_free(raw_);
        raw_ = nullptr;
    }

    std::runtime_error release_exception() {
        std::runtime_error result(raw_);
        ukv_error_free(std::exchange(raw_, nullptr));
        return result;
    }

    void throw_unhandled() {
        if (__builtin_expect(*this, 0)) // C++20: [[unlikely]]
            throw release_exception();
    }

    ukv_error_t* internal_cptr() noexcept { return &raw_; }
    ukv_error_t release_error() noexcept { return std::exchange(raw_, nullptr); }
};

template <typename object_at>
class [[nodiscard]] expected_gt {
    status_t status_;
    object_at object_;

  public:
    expected_gt() = default;
    expected_gt(object_at&& object) : object_(std::move(object)) {}
    expected_gt(status_t&& status, object_at&& default_object = object_at {})
        : status_(std::move(status)), object_(std::move(default_object)) {}

    operator bool() const noexcept { return status_; }
    object_at&& operator*() && noexcept { return std::move(object_); }
    object_at const& operator*() const& noexcept { return object_; }
    object_at const* operator->() const& noexcept { return &object_; }
    operator std::optional<object_at>() && {
        return status_ ? std::nullopt : std::optional<object_at> {std::move(object_)};
    }

    void throw_unhandled() {
        if (!__builtin_expect(status_, 1)) // C++20: [[unlikely]]
            throw status_.release_exception();
    }
    inline status_t release_status() { return std::exchange(status_, status_t {}); }
};

/**
 * @brief A smart pointer type with customizable jump length for increments.
 * In other words, it allows a strided data layout, common to HPC apps.
 * Cool @b hint, you can use this to represent an inifinite array of repeating
 * values with `stride` equal to zero.
 */
template <typename object_at>
class strided_iterator_gt {

    object_at* raw_ = nullptr;
    ukv_size_t stride_ = 0;

    object_at* upshift(std::ptrdiff_t bytes) const noexcept { return (object_at*)((byte_t*)raw_ + bytes); }
    object_at* downshift(std::ptrdiff_t bytes) const noexcept { return (object_at*)((byte_t*)raw_ - bytes); }

  public:
    using iterator_category = std::random_access_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = object_at;
    using pointer = value_type*;
    using reference = value_type&;

    inline strided_iterator_gt(object_at* raw = nullptr, ukv_size_t stride = 0) noexcept : raw_(raw), stride_(stride) {}
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
};

template <typename object_at>
class strided_range_gt {

    object_at* begin_ = nullptr;
    ukv_size_t stride_ = 0;
    ukv_size_t count_ = 0;

  public:
    strided_range_gt() = default;
    strided_range_gt(object_at& single, std::size_t repeats = 1) noexcept
        : begin_(&single), stride_(0), count_(static_cast<ukv_size_t>(repeats)) {}
    strided_range_gt(object_at* begin, object_at* end) noexcept
        : begin_(begin), stride_(sizeof(object_at)), count_(end - begin) {}
    strided_range_gt(object_at* begin, std::size_t stride, std::size_t count) noexcept
        : begin_(begin), stride_(static_cast<ukv_size_t>(stride)), count_(static_cast<ukv_size_t>(count)) {}

    strided_range_gt(std::vector<object_at>& vec) noexcept
        : begin_(vec.data()), stride_(sizeof(object_at)), count_(vec.size()) {}
    strided_range_gt(std::vector<std::remove_const_t<object_at>> const& vec) noexcept
        : begin_(vec.data()), stride_(sizeof(object_at)), count_(vec.size()) {}

    strided_range_gt(strided_range_gt&&) = default;
    strided_range_gt(strided_range_gt const&) = default;
    strided_range_gt& operator=(strided_range_gt&&) = default;
    strided_range_gt& operator=(strided_range_gt const&) = default;

    inline strided_iterator_gt<object_at> begin() const noexcept { return {begin_, stride_}; }
    inline strided_iterator_gt<object_at> end() const noexcept { return begin() + static_cast<std::ptrdiff_t>(count_); }
    inline object_at& operator[](std::size_t i) const noexcept { return *(begin() + static_cast<std::ptrdiff_t>(i)); }

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
        using parent_t = std::conditional_t<std::is_const_v<object_at>, parent_at const, parent_at>;
        using member_t = std::conditional_t<std::is_const_v<object_at>, member_at const, member_at>;
        parent_t& first = *begin_;
        member_t& first_member = first.*member_ptr;
        return strided_range_gt<member_t> {&first_member, stride(), count()};
    }
};

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

template <typename object_at>
class strided_matrix_gt {

    object_at* begin_ = nullptr;
    ukv_size_t stride_ = 0;
    ukv_size_t rows_ = 0;
    ukv_size_t cols_ = 0;

  public:
    strided_matrix_gt() = default;
    strided_matrix_gt(object_at* begin, std::size_t rows, std::size_t cols, std::size_t stride) noexcept
        : begin_(begin), stride_(static_cast<ukv_size_t>(stride)), rows_(static_cast<ukv_size_t>(rows)),
          cols_(static_cast<ukv_size_t>(cols)) {}

    strided_matrix_gt(strided_matrix_gt&&) = default;
    strided_matrix_gt(strided_matrix_gt const&) = default;
    strided_matrix_gt& operator=(strided_matrix_gt&&) = default;
    strided_matrix_gt& operator=(strided_matrix_gt const&) = default;

    inline std::size_t size() const noexcept { return rows_ * cols_; }
    inline object_at& operator()(std::size_t i, std::size_t j) noexcept { return row(i)[j]; }
    inline object_at const& operator()(std::size_t i, std::size_t j) const noexcept { return row(i)[j]; }
    inline strided_range_gt<object_at> col(std::size_t j) const noexcept { return {begin_ + j, stride_, rows_}; }
    inline indexed_range_gt<object_at> row(std::size_t i) const noexcept {
        return {begin_ + i * stride_ / sizeof(object_at), cols_};
    }
    inline std::size_t rows() const noexcept { return rows_; }
    inline std::size_t cols() const noexcept { return cols_; }
};

/**
 * @brief Similar to `std::optional<std::string_view>`.
 * It's NULL state and "empty string" states are not identical.
 * The NULL state generally reflects missing values.
 * Unlike `indexed_range_gt<byte_t const*>`, this classes layout allows
 * easily passing it to the internals of UKV implementations
 * without additional bit-twiddling.
 */
class value_view_t {

    ukv_val_ptr_t ptr_ = nullptr;
    ukv_val_len_t length_ = 0;

  public:
    inline value_view_t() = default;
    inline value_view_t(ukv_val_ptr_t ptr, ukv_val_len_t length) noexcept {
        if (length == ukv_val_len_missing_k) {
            ptr_ = nullptr;
            length_ = 0;
        }
        else {
            ptr_ = ptr;
            length_ = length;
        }
    }
    inline value_view_t(byte_t const* begin, byte_t const* end) noexcept
        : ptr_(ukv_val_ptr_t(begin)), length_(static_cast<ukv_val_len_t>(end - begin)) {}

    inline byte_t const* begin() const noexcept { return reinterpret_cast<byte_t const*>(ptr_); }
    inline byte_t const* end() const noexcept { return begin() + length_; }
    inline std::size_t size() const noexcept { return length_; }
    inline bool empty() const noexcept { return !length_; }
    inline operator bool() const noexcept { return ptr_ != nullptr; }
};

struct collections_view_t : public strided_range_gt<ukv_collection_t const> {
    using parent_t = strided_range_gt<ukv_collection_t const>;

    collections_view_t() noexcept : parent_t(ukv_default_collection_k, 1) {}
    collections_view_t(strided_range_gt<ukv_collection_t const> r) noexcept : parent_t(r) {}
};

using keys_view_t = strided_range_gt<ukv_key_t const>;

using located_keys_view_t = strided_range_gt<located_key_t const>;

struct disjoint_values_view_t {
    strided_range_gt<ukv_val_ptr_t> contents;
    strided_range_gt<ukv_val_len_t> offsets;
    strided_range_gt<ukv_val_len_t> lengths;
};

/**
 * @brief A read-only iterator for values packed into a
 * contiguous memory range. Doesn't own underlying memory.
 */
class tape_iterator_t {

    ukv_val_len_t const* lengths_ = nullptr;
    ukv_val_ptr_t contents_ = nullptr;

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = value_view_t;
    using pointer = void;
    using reference = void;

    inline tape_iterator_t(ukv_val_ptr_t ptr, ukv_size_t elements) noexcept
        : lengths_(reinterpret_cast<ukv_val_len_t*>(ptr)), contents_(ptr + sizeof(ukv_val_len_t) * elements) {}

    inline tape_iterator_t(ukv_val_len_t const* lens, ukv_val_ptr_t vals) noexcept : lengths_(lens), contents_(vals) {}

    inline tape_iterator_t& operator++() noexcept {
        if (*lengths_ != ukv_val_len_missing_k)
            contents_ += *lengths_;
        ++lengths_;
        return *this;
    }

    inline tape_iterator_t operator++(int) const noexcept { return {lengths_ + 1, contents_ + *lengths_}; }
    inline operator bool() const noexcept { return *lengths_; }
    inline value_view_t operator*() const noexcept { return {contents_, *lengths_}; }

    inline bool operator==(tape_iterator_t const& other) const noexcept { return lengths_ == other.lengths_; }
    inline bool operator!=(tape_iterator_t const& other) const noexcept { return lengths_ != other.lengths_; }
};

class taped_values_view_t {
    ukv_val_len_t const* lengths_ = nullptr;
    ukv_val_ptr_t contents_ = nullptr;
    ukv_size_t count_ = 0;

  public:
    inline taped_values_view_t() = default;
    inline taped_values_view_t(ukv_val_len_t const* lens, ukv_val_ptr_t vals, ukv_size_t elements) noexcept
        : lengths_(lens), contents_(vals), count_(elements) {}

    inline tape_iterator_t begin() const noexcept { return {lengths_, contents_}; }
    inline tape_iterator_t end() const noexcept { return {lengths_ + count_, contents_}; }
    inline std::size_t size() const noexcept { return count_; }

    ukv_val_len_t const* lengths() const noexcept { return lengths_; }
    ukv_val_ptr_t contents() const noexcept { return contents_; }
};

/**
 * @brief A view of a tape received from the DB.
 * Allocates no memory, but is responsible for the cleanup.
 */
class managed_arena_t {

    ukv_t db_ = nullptr;
    ukv_arena_t memory_ = nullptr;

  public:
    managed_arena_t(ukv_t db) noexcept : db_(db) {}
    managed_arena_t(managed_arena_t const&) = delete;

    ~managed_arena_t() {
        if (memory_)
            ukv_arena_free(db_, memory_);
        memory_ = nullptr;
    }

    inline managed_arena_t(managed_arena_t&& other) noexcept
        : db_(other.db_), memory_(std::exchange(other.memory_, nullptr)) {}

    inline managed_arena_t& operator=(managed_arena_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(memory_, other.memory_);
        return *this;
    }

    inline ukv_arena_t* internal_cptr() noexcept { return &memory_; }
};

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

template <typename element_at, typename iterator_at>
element_at reduce_n(iterator_at begin, std::size_t n, element_at init) {
    return transform_reduce_n(begin, n, init, [](auto x) { return x; });
}

/**
 * @brief Trivial hash-mixing scheme from Boost.
 * @see https://www.boost.org/doc/libs/1_37_0/doc/html/hash/reference.html#boost.hash_combine
 */
template <typename hashable_at>
inline void hash_combine(std::size_t& seed, hashable_at const& v) {
    std::hash<hashable_at> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

struct located_key_hash_t {
    inline std::size_t operator()(located_key_t const& located) const noexcept {
        std::size_t result = SIZE_MAX;
        hash_combine(result, located.key);
        hash_combine(result, located.collection);
        return result;
    }
};

} // namespace unum::ukv
