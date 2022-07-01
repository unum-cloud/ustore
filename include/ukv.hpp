/**
 * @file ukv.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings built on top of @see "ukv.h" with
 * two primary purposes:
 * > @b RAII controls for non-trivial & potentially heavy objects.
 * > syntactic @b sugar, iterators, containers and other C++  stuff.
 */

#pragma once
#include <vector>
#include <string_view>
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
    ukv_error_t raw_ = nullptr;

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
};

/**
 * @brief Append-only datastructure for variable length blobs.
 * Owns the underlying memory and is external to the underlying DB.
 * Is suited for data preparation before passing to the C API.
 */
class appendable_tape_t {
  public:
    void push_back(value_view_t value) {
        lengths_.push_back(static_cast<ukv_val_len_t>(value.size()));
        data_.insert(data_.end(), value.begin(), value.end());
    }

    void clear() {
        lengths_.clear();
        data_.clear();
    }

    operator taped_values_t() const noexcept {
        taped_values_t view;
        view.lengths = lengths_.data();
        view.values = ukv_tape_ptr_t(data_.data());
        view.count = data_.size();
        return view;
    }

  private:
    std::vector<ukv_val_len_t> lengths_;
    std::vector<byte_t> data_;
};

/**
 * @brief A view of a tape received from the DB.
 * Allocates no memory, but is responsible for the cleanup.
 */
struct managed_tape_t {

    ukv_t db = nullptr;
    ukv_tape_ptr_t memory = nullptr;
    ukv_size_t capacity = 0;

    ~managed_tape_t() {
        if (memory)
            ukv_tape_free(db, memory, capacity);
        memory = nullptr;
        capacity = 0;
    }

    operator taped_values_t() const noexcept { return {memory, capacity}; }
};

/**
 * @brief A proxy object, that allows both lookups and writes
 * with `[]` and assignment operators for a batch of keys
 * simultaneously.
 */
struct sample_handle_t {

    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_tape_ptr_t* memory = nullptr;
    ukv_size_t* capacity = 0;
    ukv_options_t options = ukv_options_default_k;

    collections_t cols;
    keys_t keys;

    [[nodiscard]] expected_gt<taped_values_t> get() const noexcept {
        error_t error;
        ukv_read(db,
                 txn,
                 cols.range.raw,
                 cols.range.stride,
                 keys.range.raw,
                 keys.range.count,
                 keys.range.stride,
                 options,
                 memory,
                 capacity,
                 &error.raw_);
        if (error)
            return {std::move(error)};

        return {taped_values_t {*memory, *capacity}};
    }

    [[nodiscard]] error_t set(disjoint_values_t vals) noexcept {
        error_t error;
        ukv_write(db,
                  txn,
                  cols.range.raw,
                  cols.range.stride,
                  keys.range.raw,
                  keys.range.count,
                  keys.range.stride,
                  vals.values_range.raw,
                  vals.values_range.stride,
                  vals.lengths_range.raw,
                  vals.lengths_range.stride,
                  options,
                  &error.raw_);
        return error;
    }

    operator expected_gt<taped_values_t>() const noexcept { return get(); }
    sample_handle_t& operator=(disjoint_values_t vals) noexcept(false) {
        auto error = set(vals);
        if (error)
            throw error;
        return *this;
    }
};

struct db_t;
struct session_t;
struct collection_handle_t;

struct collection_handle_t {
    ukv_t db = nullptr;
    ukv_collection_t raw = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_tape_ptr_t* memory = nullptr;
    ukv_size_t* capacity = 0;
    ukv_options_t options = ukv_options_default_k;

    collection_handle_t(ukv_t db_ptr) noexcept : db(db_ptr) {}
    ~collection_handle_t() noexcept { ukv_collection_free(db, raw); }

    void drop();
};

struct txn_t {

    collection_handle_t operator[](std::string const& name);

    sample_handle_t operator[](located_keys_t) noexcept;
    expected_gt<taped_values_t> operator[](located_keys_t) const noexcept;

    sample_handle_t operator[](keys_t) noexcept;
    expected_gt<taped_values_t> operator[](keys_t) const noexcept;

    error_t rollback();
    error_t commit();
};

struct session_t {
    db_t& db;
    managed_tape_t read_tape;
    std::vector<located_key_t> planned_lookups;

    session_t(db_t& db) noexcept : db(db) {}
    expected_gt<txn_t> transact();

    collection_handle_t operator[](std::string const& name);

    sample_handle_t operator[](located_keys_t) noexcept;
    expected_gt<taped_values_t> operator[](located_keys_t) const noexcept;

    sample_handle_t operator[](keys_t) noexcept;
    expected_gt<taped_values_t> operator[](keys_t) const noexcept;
};

struct db_t {
    ukv_t raw = nullptr;

    error_t open(std::string const& config) {
        error_t error;
        ukv_open(config.c_str(), &raw, &error.raw_);
        return error;
    }

    void close() {
        ukv_free(raw);
        raw = nullptr;
    }

    ~db_t() {
        if (raw)
            close();
    }
};

} // namespace unum::ukv

// auto db = db_t {};
// auto txn =
// auto col = db["bank"];
// auto val = col[alice_id];
//