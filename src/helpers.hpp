/**
 * @file helpers.htpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions for the C++ backend implementations.
 */
#pragma once
#include <memory>    // `std::allocator`
#include <cstring>   // `std::memcpy`
#include <stdexcept> // `std::runtime_error`

#include "ukv.hpp"

namespace unum::ukv {

using allocator_t = std::allocator<byte_t>;
using buffer_t = std::vector<byte_t, allocator_t>;
using sequence_t = std::int64_t;

/**
 * @brief An `std::vector`-like class, with open layout,
 * friendly to our C API. Can't grow, but can shrink.
 * Just like humans after puberty :)
 */
struct value_t {
    ukv_tape_ptr_t ptr = nullptr;
    ukv_val_len_t length = 0;
    ukv_val_len_t cap = 0;

    value_t(value_t const&) = delete;
    value_t& operator=(value_t const&) = delete;

    value_t(value_t&& v) noexcept
        : ptr(std::exchange(v.ptr, nullptr)), length(std::exchange(v.length, 0)), cap(std::exchange(v.cap, 0)) {}

    value_t& operator=(value_t&& v) noexcept {
        ptr = std::exchange(v.ptr, nullptr);
        length = std::exchange(v.length, 0);
        cap = std::exchange(v.cap, 0);
        return *this;
    }

    value_t() = default;

    value_t(std::size_t size) {
        auto new_ptr = allocator_t {}.allocate(size);
        if (!new_ptr)
            throw std::bad_alloc();
        ptr = reinterpret_cast<ukv_tape_ptr_t>(new_ptr);
        cap = length = size;
    }

    value_t(value_view_t view) : value_t(view.size()) { std::memcpy(ptr, view.begin(), view.size()); }

    ~value_t() {
        if (ptr)
            allocator_t {}.deallocate(reinterpret_cast<byte_t*>(ptr), cap);
        ptr = nullptr;
        length = 0;
        cap = 0;
    }

    void resize(std::size_t size) {
        if (size > cap)
            throw std::runtime_error("Only shrinking is currently supported");
        length = size;
    }

    inline byte_t* begin() const noexcept { return reinterpret_cast<byte_t*>(ptr); }
    inline byte_t* end() const noexcept { return begin() + length; }
    inline std::size_t size() const noexcept { return length; }
    inline operator bool() const noexcept { return length; }
    inline operator value_view_t() const noexcept { return {ptr, length}; }
    inline void clear() noexcept { length = 0; }
};

/**
 * @brief Solves the problem of modulo arithmetic and `sequence_t` overflow.
 * Still works correctly, when `max` has overflown, but `min` hasn't yet,
 * so `min` can be bigger than `max`.
 */
inline bool entry_was_overwritten(sequence_t entry_sequence,
                                  sequence_t transaction_sequence,
                                  sequence_t youngest_sequence) noexcept {

    return transaction_sequence <= youngest_sequence
               ? ((entry_sequence >= transaction_sequence) & (entry_sequence <= youngest_sequence))
               : ((entry_sequence >= transaction_sequence) | (entry_sequence <= youngest_sequence));
}

inline byte_t* reserve_tape(ukv_tape_ptr_t* tape_ptr,
                            ukv_size_t* tape_length,
                            ukv_size_t new_length,
                            ukv_error_t* c_error) {

    byte_t* tape = *reinterpret_cast<byte_t**>(tape_ptr);
    if (new_length >= *tape_length) {
        try {
            if (tape)
                allocator_t {}.deallocate(tape, *tape_length);
            tape = allocator_t {}.allocate(new_length);
            *tape_ptr = reinterpret_cast<ukv_tape_ptr_t>(tape);
            *tape_length = new_length;
            return tape;
        }
        catch (...) {
            *c_error = "Failed to allocate memory for exports!";
            return nullptr;
        }
    }
    else
        return tape;
}

struct read_task_t {
    ukv_collection_t collection;
    ukv_key_t key;

    inline located_key_t location() const noexcept { return located_key_t {collection, key}; }
};

/**
 * @brief Arguments of `ukv_read` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct read_tasks_soa_t {
    strided_ptr_gt<ukv_collection_t> cols;
    strided_ptr_gt<ukv_key_t const> keys;

    inline read_task_t operator[](ukv_size_t i) const noexcept {
        ukv_collection_t col = cols && cols[i] ? cols[i] : ukv_default_collection_k;
        ukv_key_t key = keys[i];
        return {col, key};
    }
};

struct write_task_t {
    ukv_collection_t collection;
    ukv_key_t key;
    byte_t const* begin;
    ukv_val_len_t offset;
    ukv_val_len_t length;

    inline located_key_t location() const noexcept { return located_key_t {collection, key}; }
    value_view_t view() const { return {begin + offset, begin + offset + length}; }
    buffer_t buffer() const { return {begin + offset, begin + offset + length}; }
};

/**
 * @brief Arguments of `ukv_write` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct write_tasks_soa_t {
    strided_ptr_gt<ukv_collection_t> cols;
    strided_ptr_gt<ukv_key_t const> keys;
    strided_ptr_gt<ukv_tape_ptr_t const> vals;
    strided_ptr_gt<ukv_val_len_t const> offs;
    strided_ptr_gt<ukv_val_len_t const> lens;

    inline write_task_t operator[](ukv_size_t i) const noexcept {
        ukv_collection_t col = cols && cols[i] ? cols[i] : ukv_default_collection_k;
        ukv_key_t key = keys[i];
        byte_t const* begin;
        ukv_val_len_t off;
        ukv_val_len_t len;
        if (vals) {
            begin = reinterpret_cast<byte_t const*>(vals[i]);
            off = offs ? offs[i] : 0u;
            len = lens ? lens[i] : std::strlen(reinterpret_cast<char const*>(begin + off));
        }
        else {
            begin = nullptr;
            off = 0u;
            len = 0u;
        }
        return {col, key, begin, len};
    }
};

} // namespace unum::ukv