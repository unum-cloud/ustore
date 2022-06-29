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

#include "ukv.h"

namespace unum::ukv {

enum class byte_t : uint8_t {};

using allocator_t = std::allocator<byte_t>;
using buffer_t = std::vector<byte_t, allocator_t>;
using sequence_t = std::int64_t;

/**
 * @brief Calculates the offset of entry in a strided memory span.
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

    inline strided_ptr_gt operator++(int) const noexcept { return {raw + stride, stride}; }
    inline operator bool() const noexcept { return raw != nullptr; }
    inline bool repeats() const noexcept { return !stride; }
    inline object_at& operator*() const noexcept { return *raw; }
};

struct value_view_t {
    ukv_tape_ptr_t ptr = nullptr;
    ukv_val_len_t length = 0;

    inline byte_t const* begin() const noexcept { return reinterpret_cast<byte_t const*>(ptr); }
    inline byte_t const* end() const noexcept { return begin() + length; }
    inline std::size_t size() const noexcept { return length; }
    inline operator bool() const noexcept { return length; }
};

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
        : ptr(std::exchange(v.ptr, nullptr)), length(std::exchange(v.length, nullptr)),
          cap(std::exchange(v.cap, nullptr)) {}

    value_t& operator=(value_t&& v) noexcept {
        ptr = std::exchange(v.ptr, nullptr);
        length = std::exchange(v.length, nullptr);
        cap = std::exchange(v.cap, nullptr);
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
};

struct tape_iterator_t {

    ukv_val_len_t const* lengths = nullptr;
    ukv_tape_ptr_t values = nullptr;

    tape_iterator_t(ukv_tape_ptr_t ptr, ukv_size_t elements) noexcept {
        lengths = reinterpret_cast<ukv_val_len_t*>(ptr);
        values = ptr + sizeof(ukv_val_len_t) * elements;
    }

    inline tape_iterator_t& operator++() noexcept {
        values += *lengths;
        ++lengths;
        return *this;
    }

    inline tape_iterator_t operator++(int) const noexcept { return {values + *lengths, lengths + 1}; }
    inline operator bool() const noexcept { return *lengths; }
    inline value_view_t operator*() const noexcept { return {values, *lengths}; }
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

byte_t* reserve_tape(ukv_tape_ptr_t* tape_ptr, ukv_size_t* tape_length, ukv_size_t new_length, ukv_error_t* c_error) {

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

} // namespace unum::ukv