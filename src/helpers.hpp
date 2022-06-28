/**
 * @file helpers.htpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions for the C++ backend implementations.
 */
#pragma once
#include <memory>  // `std::allocator`
#include <cstring> // `std::memcpy`

#include "ukv.h"

namespace unum::ukv {

using key_t = ukv_key_t;
enum class byte_t : uint8_t {};

using allocator_t = std::allocator<byte_t>;
using value_t = std::vector<byte_t, allocator_t>;
using sequence_t = std::ssize_t;

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

/**
 * @brief Calculates the offset of entry in a strided memory span.
 */
template <typename object_at>
inline object_at& strided_ref(object_at* begin, ukv_size_t stride, ukv_size_t idx) noexcept {
    auto begin_bytes = reinterpret_cast<byte_t*>(begin) + stride * idx;
    return *reinterpret_cast<object_at*>(begin_bytes);
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