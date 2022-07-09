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
class value_t {
    ukv_tape_ptr_t ptr_ = nullptr;
    ukv_val_len_t length_ = 0;
    ukv_val_len_t cap_ = 0;

  public:
    value_t() = default;
    value_t(value_t const&) = delete;
    value_t& operator=(value_t const&) = delete;

    value_t(value_t&& v) noexcept
        : ptr_(std::exchange(v.ptr_, nullptr)), length_(std::exchange(v.length_, 0)), cap_(std::exchange(v.cap_, 0)) {}

    value_t& operator=(value_t&& v) noexcept {
        std::swap(v.ptr_, ptr_);
        std::swap(v.length_, length_);
        std::swap(v.cap_, cap_);
        return *this;
    }

    value_t(std::size_t size) {
        if (!size)
            return;
        auto new_ptr = allocator_t {}.allocate(size);
        if (!new_ptr)
            throw std::bad_alloc();
        ptr_ = reinterpret_cast<ukv_tape_ptr_t>(new_ptr);
        cap_ = length_ = size;
    }

    value_t(value_view_t view) : value_t(view.size()) { std::memcpy(ptr_, view.begin(), view.size()); }

    ~value_t() {
        if (ptr_)
            allocator_t {}.deallocate(reinterpret_cast<byte_t*>(ptr_), cap_);
        ptr_ = nullptr;
        length_ = 0;
        cap_ = 0;
    }

    void resize(std::size_t size) {
        if (size > cap_)
            throw std::runtime_error("Only shrinking is currently supported");
        length_ = size;
    }

    void insert(std::size_t offset, byte_t const* inserted_begin, byte_t const* inserted_end) {
        auto inserted_len = static_cast<ukv_val_len_t>(inserted_end - inserted_begin);
        auto new_size = length_ + inserted_len;
        if (new_size > cap_) {
            auto new_ptr = allocator_t {}.allocate(new_size);
            if (!new_ptr)
                throw std::bad_alloc();
            std::memcpy(new_ptr, ptr_, offset);
            std::memcpy(new_ptr + offset, inserted_begin, inserted_len);
            std::memcpy(new_ptr + offset + inserted_len, ptr_ + offset, length_ - offset);
            if (ptr_)
                allocator_t {}.deallocate(reinterpret_cast<byte_t*>(ptr_), cap_);

            ptr_ = reinterpret_cast<ukv_tape_ptr_t>(new_ptr);
            cap_ = length_ = new_size;
        }
        else {
            std::memmove(ptr_ + offset + inserted_len, ptr_ + offset, inserted_len);
            std::memcpy(ptr_ + offset, inserted_begin, inserted_len);
            length_ = new_size;
        }
    }

    void erase(std::size_t offset, std::size_t length) noexcept {
        std::memmove(ptr_ + offset, ptr_ + offset + length, length_ - offset - length);
        length_ -= length;
    }

    inline byte_t* begin() const noexcept { return reinterpret_cast<byte_t*>(ptr_); }
    inline byte_t* end() const noexcept { return begin() + length_; }
    inline std::size_t size() const noexcept { return length_; }
    inline operator bool() const noexcept { return length_; }
    inline operator value_view_t() const noexcept { return {ptr_, length_}; }
    inline void clear() noexcept { length_ = 0; }

    inline ukv_tape_ptr_t* internal_cptr() noexcept { return &ptr_; }
    inline ukv_val_len_t* internal_length() noexcept { return &length_; }
    inline ukv_val_len_t* internal_cap() noexcept { return &cap_; }
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
            if (!tape) {
                *c_error = "Failed to allocate memory!";
                return nullptr;
            }
            *tape_ptr = reinterpret_cast<ukv_tape_ptr_t>(tape);
            *tape_length = new_length;
            return tape;
        }
        catch (...) {
            *c_error = "Failed to allocate memory!";
            return nullptr;
        }
    }
    else
        return tape;
}

struct read_task_t {
    ukv_collection_t collection;
    ukv_key_t& key;

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
        ukv_key_t& key = keys[i];
        return {col, key};
    }
};

struct write_task_t {
    ukv_collection_t collection;
    ukv_key_t& key;
    byte_t const* begin;
    ukv_val_len_t offset;
    ukv_val_len_t length;

    inline located_key_t location() const noexcept { return located_key_t {collection, key}; }
    inline bool is_deleted() const noexcept { return begin == nullptr; }
    value_view_t view() const noexcept { return {begin + offset, begin + offset + length}; }
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
        ukv_key_t& key = keys[i];
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
        return {col, key, begin, off, len};
    }
};

} // namespace unum::ukv