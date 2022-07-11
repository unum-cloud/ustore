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

#include "ukv.h"

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

struct located_key_hash_t {
    inline std::size_t operator()(located_key_t const& located) const noexcept {
        return std::hash<ukv_key_t> {}(located.key);
    }
};

class error_t {
    ukv_error_t raw_ = nullptr;

  public:
    error_t(ukv_error_t err = nullptr) noexcept : raw_(err) {}
    operator bool() const noexcept { return raw_; }

    error_t(error_t const&) = delete;
    error_t& operator=(error_t const&) = delete;

    error_t(error_t&& other) noexcept { raw_ = std::exchange(other.raw_, nullptr); }
    error_t& operator=(error_t&& other) noexcept {
        raw_ = std::exchange(other.raw_, nullptr);
        return *this;
    }
    ~error_t() {
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
};

template <typename object_at>
class expected_gt {
    error_t error_;
    object_at object_;

  public:
    expected_gt() = default;
    expected_gt(object_at&& object) : object_(std::move(object)) {}
    expected_gt(error_t&& error, object_at&& default_object = object_at {})
        : error_(std::move(error)), object_(std::move(default_object)) {}

    operator bool() const noexcept { return !error_; }
    object_at&& operator*() && noexcept { return std::move(object_); }
    object_at const& operator*() const& noexcept { return object_; }
    object_at const* operator->() const& noexcept { return &object_; }
    operator std::optional<object_at>() && {
        return error_ ? std::nullopt : std::optional<object_at> {std::move(object_)};
    }

    void throw_unhandled() {
        if (__builtin_expect(error_, 0)) // C++20: [[unlikely]]
            throw error_.release_exception();
    }
};

/**
 * @brief A smart pointer type with customizable jump length for increments.
 * In other words, it allows a strided data layout, common to HPC apps.
 * Cool @b hint, you can use this to represent an inifinite array of repeating
 * values with `stride` equal to zero.
 */
template <typename object_at>
class strided_ptr_gt {

    object_at* raw_ = nullptr;
    ukv_size_t stride_ = 0;

  public:
    inline strided_ptr_gt(object_at* raw, ukv_size_t stride = 0) noexcept : raw_(raw), stride_(stride) {}

    inline object_at& operator[](ukv_size_t idx) const noexcept {
        auto raw_bytes = (byte_t*)raw_ + stride_ * idx;
        return *reinterpret_cast<object_at*>(raw_bytes);
    }

    inline strided_ptr_gt& operator++() noexcept {
        raw_ += stride_;
        return *this;
    }

    inline strided_ptr_gt& operator--() noexcept {
        raw_ -= stride_;
        return *this;
    }

    inline strided_ptr_gt operator++(int) const noexcept { return {raw_ + stride_, stride_}; }
    inline strided_ptr_gt operator--(int) const noexcept { return {raw_ - stride_, stride_}; }
    inline strided_ptr_gt operator+(std::size_t n) const noexcept { return {raw_ + stride_ * n, stride_}; }
    inline strided_ptr_gt operator-(std::size_t n) const noexcept { return {raw_ - stride_ * n, stride_}; }

    inline operator bool() const noexcept { return raw_ != nullptr; }
    inline bool repeats() const noexcept { return !stride_; }
    inline object_at& operator*() const noexcept { return *raw_; }
    inline object_at* operator->() const noexcept { return raw_; }
    inline object_at* get() const noexcept { return raw_; }

    inline bool operator==(strided_ptr_gt const& other) const noexcept { return raw_ == other.raw_; }
    inline bool operator!=(strided_ptr_gt const& other) const noexcept { return raw_ != other.raw_; }
};

template <typename object_at>
class strided_range_gt {

    object_at* begin_ = nullptr;
    ukv_size_t stride_ = 0;
    ukv_size_t count_ = 0;

  public:
    strided_range_gt() = default;
    strided_range_gt(object_at& single, ukv_size_t repeats = 1) noexcept
        : begin_(&single), stride_(0), count_(repeats) {}
    strided_range_gt(object_at* begin, object_at* end) noexcept
        : begin_(begin), stride_(sizeof(object_at)), count_(end - begin) {}
    strided_range_gt(std::vector<object_at>& vec) noexcept
        : begin_(vec.data()), stride_(sizeof(object_at)), count_(vec.size()) {}
    strided_range_gt(object_at* begin, ukv_size_t stride, ukv_size_t count) noexcept
        : begin_(begin), stride_(stride), count_(count) {}

    strided_range_gt(strided_range_gt&&) = default;
    strided_range_gt(strided_range_gt const&) = default;
    strided_range_gt& operator=(strided_range_gt&&) = default;
    strided_range_gt& operator=(strided_range_gt const&) = default;

    inline strided_ptr_gt<object_at> begin() const noexcept { return {begin_, stride_}; }
    inline strided_ptr_gt<object_at> end() const noexcept { return begin() + count_; }
    inline object_at& operator[](std::size_t i) const noexcept { return *(begin() + i); }

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
struct range_gt {
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

/**
 * @brief Similar to `std::optional<std::string_view>`.
 * It's NULL state and "empty string" states are not identical.
 * The NULL state generally reflects missing values.
 * Unlike `range_gt<byte_t>`, this classes layout allows
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
    inline byte_t const* end() const noexcept { return begin(); }
    inline std::size_t size() const noexcept { return length_; }
    inline bool empty() const noexcept { return !length_; }
    inline operator bool() const noexcept { return ptr_ != nullptr; }
};

struct collections_view_t : public strided_range_gt<ukv_collection_t> {
    using parent_t = strided_range_gt<ukv_collection_t>;

    collections_view_t() noexcept : parent_t(ukv_default_collection_k, 1) {}
    collections_view_t(strided_range_gt<ukv_collection_t> r) noexcept : parent_t(r) {}
};

using keys_view_t = strided_range_gt<ukv_key_t>;

using located_keys_view_t = strided_range_gt<located_key_t>;

struct disjoint_values_view_t {
    strided_range_gt<ukv_val_ptr_t> values_range;
    strided_range_gt<ukv_val_len_t> offsets_range;
    strided_range_gt<ukv_val_len_t> lengths_range;
};

/**
 * @brief A read-only iterator for values packed into a
 * contiguous memory range.
 *
 */
class tape_iterator_t {

    ukv_val_len_t const* lengths_ = nullptr;
    ukv_val_ptr_t contents_ = nullptr;

  public:
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

    inline ukv_arena_t* internal_cptr() noexcept { return &memory_; }
};

} // namespace unum::ukv
