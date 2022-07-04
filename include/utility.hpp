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

    ukv_collection_t collection = nullptr;
    ukv_key_t key = 0;

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
  public:
    ukv_error_t raw = nullptr;

    error_t(ukv_error_t err = nullptr) noexcept : raw(err) {}
    operator bool() const noexcept { return raw; }

    error_t(error_t const&) = delete;
    error_t& operator=(error_t const&) = delete;

    error_t(error_t&& other) noexcept { raw = std::exchange(other.raw, nullptr); }
    error_t& operator=(error_t&& other) noexcept {
        raw = std::exchange(other.raw, nullptr);
        return *this;
    }
    ~error_t() {
        if (raw)
            ukv_error_free(raw);
        raw = nullptr;
    }

    std::runtime_error release_exception() {
        std::runtime_error result(raw);
        ukv_error_free(std::exchange(raw, nullptr));
        return result;
    }
};

template <typename object_at>
class expected_gt {
    error_t error_;
    object_at object_;

  public:
    expected_gt() = default;
    expected_gt(object_at&& object) : object_(std::move(object)) {}
    expected_gt(error_t&& error) : error_(std::move(error)) {}

    operator bool() const noexcept { return !error_; }
    object_at&& operator*() && noexcept { return std::move(object_); }
    object_at const& operator*() const& noexcept { return object_; }
    operator std::optional<object_at>() && noexcept {
        return error_ ? std::nullopt : std::optional<object_at> {std::move(object_)};
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
struct strided_ptr_gt {

    object_at* raw = nullptr;
    ukv_size_t stride = 0;

    inline object_at& operator[](ukv_size_t idx) const noexcept {
        auto raw_bytes = (byte_t*)raw + stride * idx;
        return *reinterpret_cast<object_at*>(raw_bytes);
    }

    inline strided_ptr_gt& operator++() noexcept {
        raw += stride;
        return *this;
    }

    inline strided_ptr_gt& operator--() noexcept {
        raw -= stride;
        return *this;
    }

    inline strided_ptr_gt operator++(int) const noexcept { return {raw + stride, stride}; }
    inline strided_ptr_gt operator--(int) const noexcept { return {raw - stride, stride}; }
    inline operator bool() const noexcept { return raw != nullptr; }
    inline bool repeats() const noexcept { return !stride; }
    inline object_at& operator*() const noexcept { return *raw; }

    inline bool operator==(strided_ptr_gt const& other) const noexcept { return raw == other.raw; }
    inline bool operator!=(strided_ptr_gt const& other) const noexcept { return raw != other.raw; }
};

template <typename object_at>
struct strided_range_gt {

    object_at* raw = nullptr;
    ukv_size_t stride = 0;
    ukv_size_t count = 0;

    strided_range_gt() = default;
    strided_range_gt(object_at& single, ukv_size_t repeats = 1) noexcept : raw(&single), stride(0), count(repeats) {}
    strided_range_gt(object_at* begin, object_at* end) noexcept
        : raw(begin), stride(sizeof(object_at)), count(end - begin) {}
    strided_range_gt(std::vector<object_at>& vec) noexcept
        : raw(vec.data()), stride(sizeof(object_at)), count(vec.size()) {}

    inline strided_ptr_gt<object_at> begin() const noexcept { return {raw, stride}; }
    inline strided_ptr_gt<object_at> end() const noexcept { return begin() + count; }
    inline std::size_t size() const noexcept { return count; }
    inline operator bool() const noexcept { return count; }
};

struct value_view_t {

    ukv_tape_ptr_t ptr = nullptr;
    ukv_val_len_t length = 0;

    inline byte_t const* begin() const noexcept { return reinterpret_cast<byte_t const*>(ptr); }
    inline byte_t const* end() const noexcept { return begin() + length; }
    inline std::size_t size() const noexcept { return length; }
    inline operator bool() const noexcept { return length; }
};

struct collections_t {
    strided_range_gt<ukv_collection_t> range;

    collections_t() noexcept {
        range.raw = &ukv_default_collection_k;
        range.stride = 0;
        range.count = 1;
    }

    collections_t(strided_range_gt<ukv_collection_t> r) noexcept : range(r) {}
};

struct keys_t {
    strided_range_gt<ukv_key_t> range;
};

struct located_keys_t {
    strided_range_gt<located_key_t> range;

    keys_t keys() const noexcept {
        keys_t result;
        result.range.raw = &range.raw->key;
        result.range.count = range.count;
        result.range.stride = sizeof(located_key_t) * range.stride;
        return result;
    }

    collections_t collections() const noexcept {
        collections_t result;
        result.range.raw = &range.raw->collection;
        result.range.count = range.count;
        result.range.stride = sizeof(located_key_t) * range.stride;
        return result;
    }
};

struct disjoint_values_t {
    strided_range_gt<ukv_tape_ptr_t> values_range;
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

struct taped_values_t {
    ukv_val_len_t const* lengths = nullptr;
    ukv_tape_ptr_t values = nullptr;
    ukv_size_t count = 0;

    inline taped_values_t() = default;
    inline taped_values_t(ukv_tape_ptr_t ptr, ukv_size_t elements) noexcept
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
    ~managed_tape_t() {
        if (memory_)
            ukv_tape_free(db_, memory_, capacity_);
        memory_ = nullptr;
        capacity_ = 0;
    }

    operator taped_values_t() const noexcept { return {memory_, capacity_}; }
    ukv_tape_ptr_t* memory() noexcept { return &memory_; }
    ukv_size_t* capacity() noexcept { return &capacity_; }
};

} // namespace unum::ukv
