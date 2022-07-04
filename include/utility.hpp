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
using tape_ptr_t = ukv_tape_ptr_t;
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
        if (*this) [[unlikely]]
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
    operator std::optional<object_at>() && {
        return error_ ? std::nullopt : std::optional<object_at> {std::move(object_)};
    }

    void throw_unhandled() {
        if (error_) [[unlikely]]
            throw error_.release_exception();
    }
};

template <typename pointer_at>
struct range_gt {
    pointer_at begin_ = nullptr;
    pointer_at end_ = nullptr;

    pointer_at begin() const noexcept { return begin_; }
    pointer_at end() const noexcept { return end_; }
    std::size_t size() const noexcept { return end_ - begin_; }
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

    inline strided_ptr_gt<object_at> begin() const noexcept { return {begin_, stride_}; }
    inline strided_ptr_gt<object_at> end() const noexcept { return begin() + count_; }
    inline std::size_t size() const noexcept { return count_; }
    inline operator bool() const noexcept { return count_; }
    ukv_size_t stride() const noexcept { return stride_; }
    ukv_size_t count() const noexcept { return count_; }
};

class value_view_t {

    ukv_tape_ptr_t ptr_ = nullptr;
    ukv_val_len_t length_ = 0;

  public:
    inline value_view_t(ukv_tape_ptr_t ptr, ukv_val_len_t length) noexcept : ptr_(ptr), length_(length) {}
    inline value_view_t(byte_t const* begin, byte_t const* end) noexcept
        : ptr_(ukv_tape_ptr_t(begin)), length_(static_cast<ukv_val_len_t>(end - begin)) {}

    inline byte_t const* begin() const noexcept { return reinterpret_cast<byte_t const*>(ptr_); }
    inline byte_t const* end() const noexcept { return begin() + length_; }
    inline std::size_t size() const noexcept { return length_; }
    inline operator bool() const noexcept { return length_; }
};

struct collections_view_t {
    strided_range_gt<ukv_collection_t> range;

    collections_view_t() noexcept : range(ukv_default_collection_k, 1) {}
    collections_view_t(strided_range_gt<ukv_collection_t> r) noexcept : range(r) {}
};

struct keys_view_t {
    strided_range_gt<ukv_key_t> range;
};

struct located_keys_view_t {
    strided_range_gt<located_key_t> range;

    keys_view_t keys() const noexcept {
        return {
            strided_range_gt<ukv_key_t> {&range.begin()->key, range.stride() * sizeof(located_key_t), range.count()}};
    }

    collections_view_t collections() const noexcept {
        return {strided_range_gt<ukv_collection_t> {&range.begin()->collection,
                                                    range.stride() * sizeof(located_key_t),
                                                    range.count()}};
    }
};

struct disjoint_values_view_t {
    strided_range_gt<ukv_tape_ptr_t> values_range;
    strided_range_gt<ukv_val_len_t> offsets_range;
    strided_range_gt<ukv_val_len_t> lengths_range;
};

/**
 * @brief A read-only iterator for values packed into a
 * contiguous memory range.
 *
 */
struct tape_iterator_t {

    ukv_val_len_t const* lengths = nullptr;
    ukv_tape_ptr_t values = nullptr;

    inline tape_iterator_t(ukv_tape_ptr_t ptr, ukv_size_t elements) noexcept
        : lengths(reinterpret_cast<ukv_val_len_t*>(ptr)), values(ptr + sizeof(ukv_val_len_t) * elements) {}

    inline tape_iterator_t(ukv_val_len_t const* lens, ukv_tape_ptr_t vals) noexcept : lengths(lens), values(vals) {}

    inline tape_iterator_t& operator++() noexcept {
        values += *lengths;
        ++lengths;
        return *this;
    }

    inline tape_iterator_t operator++(int) const noexcept { return {lengths + 1, values + *lengths}; }
    inline operator bool() const noexcept { return *lengths; }
    inline value_view_t operator*() const noexcept { return {values, *lengths}; }

    inline bool operator==(tape_iterator_t const& other) const noexcept { return lengths == other.lengths; }
    inline bool operator!=(tape_iterator_t const& other) const noexcept { return lengths != other.lengths; }
};

struct taped_values_view_t {
    ukv_val_len_t const* lengths = nullptr;
    ukv_tape_ptr_t values = nullptr;
    ukv_size_t count = 0;

    inline taped_values_view_t() = default;
    inline taped_values_view_t(ukv_tape_ptr_t ptr, ukv_size_t elements) noexcept
        : lengths(reinterpret_cast<ukv_val_len_t*>(ptr)), values(ptr + sizeof(ukv_val_len_t) * elements),
          count(elements) {}

    inline tape_iterator_t begin() const noexcept { return {lengths, values}; }
    inline tape_iterator_t end() const noexcept { return {lengths + count, values}; }
    inline std::size_t size() const noexcept { return count; }
};

/**
 * @brief A view of a tape received from the DB.
 * Allocates no memory, but is responsible for the cleanup.
 */
class managed_tape_t {

    ukv_t db_ = nullptr;
    ukv_tape_ptr_t memory_ = nullptr;
    ukv_size_t capacity_ = 0;

  public:
    managed_tape_t(ukv_t db) noexcept : db_(db) {}
    managed_tape_t(managed_tape_t const&) = delete;

    ~managed_tape_t() {
        if (memory_)
            ukv_tape_free(db_, memory_, capacity_);
        memory_ = nullptr;
        capacity_ = 0;
    }

    inline managed_tape_t(managed_tape_t&& other) noexcept
        : db_(other.db_), memory_(std::exchange(other.memory_, nullptr)),
          capacity_(std::exchange(other.capacity_, 0u)) {}

    inline operator taped_values_view_t() const noexcept { return {memory_, capacity_}; }
    inline ukv_tape_ptr_t* internal_memory() noexcept { return &memory_; }
    inline ukv_size_t* internal_capacity() noexcept { return &capacity_; }
};

} // namespace unum::ukv
