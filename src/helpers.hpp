/**
 * @file helpers.htpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions for the C++ backend implementations.
 */
#pragma once
#include <limits.h>        // `CHAR_BIT`
#include <cstring>         // `std::memcpy`
#include <stdexcept>       // `std::runtime_error`
#include <memory>          // `std::allocator`
#include <vector>          // `std::vector`
#include <algorithm>       // `std::sort`
#include <memory_resource> // `std::pmr::vector`

#include "ukv/ukv.hpp"

namespace unum::ukv {

using allocator_t = std::allocator<byte_t>;
using buffer_t = std::vector<byte_t, allocator_t>;
using generation_t = std::int64_t;

constexpr std::size_t arrow_extra_offsets_k = 1;

inline std::size_t next_power_of_two(std::size_t x) {
    return 1ull << (sizeof(std::size_t) * CHAR_BIT - __builtin_clzll(x));
}

/**
 * @brief An `std::vector`-like class, with open layout,
 * friendly to our C API. Can't grow, but can shrink.
 * Just like humans after puberty :)
 */
class value_t {
    ukv_val_ptr_t ptr_ = nullptr;
    ukv_val_len_t length_ = 0;
    ukv_val_len_t cap_ = 0;

  public:
    value_t() noexcept = default;
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
        ptr_ = reinterpret_cast<ukv_val_ptr_t>(new_ptr);
        cap_ = length_ = size;
    }

    value_t(value_view_t view) : value_t(view.size()) { std::memcpy(ptr_, view.begin(), view.size()); }
    ~value_t() { reset(); }

    void reset() {
        if (ptr_)
            allocator_t {}.deallocate(reinterpret_cast<byte_t*>(ptr_), cap_);
        ptr_ = nullptr;
        length_ = 0;
        cap_ = 0;
    }

    void resize(std::size_t size) {
        if (size > cap_)
            throw std::invalid_argument("Only shrinking is currently supported");
        length_ = size;
    }

    void push_back(byte_t byte) {
        auto new_size = length_ + 1;
        if (new_size > cap_) {
            auto new_cap = next_power_of_two(new_size);
            auto new_ptr = allocator_t {}.allocate(new_cap);
            if (!new_ptr)
                throw std::bad_alloc();
            std::memcpy(new_ptr, ptr_, length_);
            if (ptr_)
                allocator_t {}.deallocate(reinterpret_cast<byte_t*>(ptr_), cap_);

            ptr_ = reinterpret_cast<ukv_val_ptr_t>(new_ptr);
            cap_ = new_cap;
        }

        reinterpret_cast<byte_t*>(ptr_)[length_] = byte;
        length_ = new_size;
    }

    void insert(std::size_t offset, byte_t const* inserted_begin, byte_t const* inserted_end) {
        if (offset > size())
            throw std::out_of_range("Can't insert");

        auto inserted_len = static_cast<ukv_val_len_t>(inserted_end - inserted_begin);
        auto following_len = static_cast<ukv_val_len_t>(length_ - offset);
        auto new_size = length_ + inserted_len;
        if (new_size > cap_) {
            auto new_ptr = allocator_t {}.allocate(new_size);
            if (!new_ptr)
                throw std::bad_alloc();
            std::memcpy(new_ptr, ptr_, offset);
            std::memcpy(new_ptr + offset, inserted_begin, inserted_len);
            std::memcpy(new_ptr + offset + inserted_len, ptr_ + offset, following_len);
            if (ptr_)
                allocator_t {}.deallocate(reinterpret_cast<byte_t*>(ptr_), cap_);

            ptr_ = reinterpret_cast<ukv_val_ptr_t>(new_ptr);
            cap_ = length_ = new_size;
        }
        else {
            std::memmove(ptr_ + offset + inserted_len, ptr_ + offset, following_len);
            std::memcpy(ptr_ + offset, inserted_begin, inserted_len);
            length_ = new_size;
        }
    }

    void erase(std::size_t offset, std::size_t length) {
        if (offset + length > size())
            throw std::out_of_range("Can't erase");

        std::memmove(ptr_ + offset, ptr_ + offset + length, length_ - (offset + length));
        length_ -= length;
    }

    inline byte_t* begin() const noexcept { return reinterpret_cast<byte_t*>(ptr_); }
    inline byte_t* end() const noexcept { return begin() + length_; }
    inline std::size_t size() const noexcept { return length_; }
    inline operator bool() const noexcept { return length_; }
    inline operator value_view_t() const noexcept { return {ptr_, length_}; }
    inline void clear() noexcept { length_ = 0; }

    inline ukv_val_ptr_t* member_ptr() noexcept { return &ptr_; }
    inline ukv_val_len_t* member_length() noexcept { return &length_; }
    inline ukv_val_len_t* member_cap() noexcept { return &cap_; }
};

/**
 * @brief Append-only data-structure for variable length blobs.
 * Owns the underlying arena and is external to the underlying DB.
 * Is suited for data preparation before passing to the C API.
 */
class growing_tape_t {
    std::pmr::vector<ukv_val_len_t> offsets_;
    std::pmr::vector<ukv_val_len_t> lengths_;
    std::pmr::vector<byte_t> contents_;

  public:
    growing_tape_t(std::pmr::memory_resource* resource) : offsets_(resource), lengths_(resource), contents_(resource) {}

    void push_back(value_view_t value) {
        offsets_.push_back(static_cast<ukv_val_len_t>(contents_.size()));
        lengths_.push_back(static_cast<ukv_val_len_t>(value.size()));
        contents_.insert(contents_.end(), value.begin(), value.end());
    }

    void clear() {
        offsets_.clear();
        lengths_.clear();
        contents_.clear();
    }

    strided_range_gt<ukv_val_len_t> offsets() noexcept { return strided_range(offsets_); }
    strided_range_gt<ukv_val_len_t> lengths() noexcept { return strided_range(lengths_); }
    strided_range_gt<byte_t> contents() noexcept { return strided_range(contents_); }

    operator tape_view_t() noexcept {
        return {ukv_val_ptr_t(contents_.data()), offsets_.data(), lengths_.data(), lengths_.size()};
    }
};

class monotonic_resource_t : public std::pmr::memory_resource {
    bool borrowed_;
    std::pmr::memory_resource* upstream_;
    void* begin_;
    size_t alignment_;
    size_t total_memory_;
    size_t available_memory_;

  public:
    explicit monotonic_resource_t(monotonic_resource_t* upstream) noexcept
        : borrowed_(true), upstream_(upstream), begin_(nullptr), alignment_(upstream->alignment_), total_memory_(0),
          available_memory_(0) {};

    monotonic_resource_t(size_t buffer_size,
                         size_t alignment,
                         std::pmr::memory_resource* upstream = std::pmr::get_default_resource())
        : borrowed_(false), upstream_(upstream), begin_(upstream->allocate(buffer_size, alignment)),
          alignment_(alignment), total_memory_(buffer_size), available_memory_(buffer_size) {}

    ~monotonic_resource_t() noexcept override {
        if (begin_ && !borrowed_) {
            release();
            upstream_->deallocate(begin_, total_memory_, alignment_);
        }
    }

    void release() noexcept {
        begin_ = (uint8_t*)(begin_) - (total_memory_ - available_memory_);
        available_memory_ = total_memory_;
    }

  private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        void* result = nullptr;

        if (borrowed_)
            result = upstream_->allocate(bytes, alignment);
        else
            result = std::align(alignment, bytes, begin_, available_memory_);

        if (result != nullptr) {
            begin_ = (uint8_t*)begin_ + bytes;
            available_memory_ -= bytes;
        }
        return result;
    }

    void do_deallocate(void*, std::size_t, std::size_t) noexcept override {}

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};

template <typename at>
struct span_gt {
    span_gt() : ptr_(nullptr), size_(0) {}
    span_gt(at* ptr, size_t sz) noexcept : ptr_(ptr), size_(sz) {}

    constexpr at* begin() const noexcept { return ptr_; }
    constexpr at* end() const noexcept { return ptr_ + size_; }
    at const* cbegin() const noexcept { return ptr_; }
    at const* cend() const noexcept { return ptr_ + size_; }

    at& operator[](size_t i) { return ptr_[i]; }
    at& operator[](size_t i) const { return ptr_[i]; }

    template <typename another_at>
    span_gt<another_at> cast() const noexcept {
        return {reinterpret_cast<another_at*>(ptr_), size_ * sizeof(at) / sizeof(another_at)};
    }

    span_gt<byte_t const> span_bytes() const noexcept {
        return {reinterpret_cast<byte_t const*>(ptr_), size_ * sizeof(at)};
    }

    size_t size_bytes() const noexcept { return size_ * sizeof(at); }
    size_t size() const noexcept { return size_; }

  private:
    at* ptr_;
    size_t size_;
};

struct stl_arena_t {
    explicit stl_arena_t(monotonic_resource_t* mem_resource) : resource(mem_resource) {}

    explicit stl_arena_t(size_t buffer_size = 1024 * 1024,
                         std::pmr::memory_resource* upstream = std::pmr::get_default_resource())
        : resource(buffer_size, 16ul, upstream) {}

    template <typename at>
    span_gt<at> alloc(size_t size, ukv_error_t* c_error, size_t alignment = sizeof(at)) {
        void* result = resource.allocate(sizeof(at) * size, alignment);
        if (result == nullptr) {
            *c_error = "Failed to allocate memory!";
            return {};
        }
        return {reinterpret_cast<at*>(result), size};
    }

    template <typename at>
    span_gt<at> grow(span_gt<at> span, size_t additional_size, ukv_error_t* c_error, size_t alignment = sizeof(at)) {
        void* result = resource.allocate(sizeof(at) * additional_size, alignment);
        if (result == nullptr) {
            *c_error = "Failed to allocate memory!";
            return result;
        }
        std::memcpy(result, span.begin(), span.size_bytes());
        return {reinterpret_cast<at*>(result), span.size + additional_size};
    }

    monotonic_resource_t resource;
};

inline stl_arena_t clean_arena(ukv_arena_t* c_arena, ukv_error_t* c_error) noexcept {
    try {
        if (!*c_arena)
            *c_arena = new stl_arena_t;
        stl_arena_t* arena = *reinterpret_cast<stl_arena_t**>(c_arena);
        arena->resource.release();
        return stl_arena_t(&arena->resource);
    }
    catch (...) {
        *c_error = "Failed to allocate memory!";
        return stl_arena_t(nullptr);
    }
}

/**
 * @brief Solves the problem of modulo arithmetic and `generation_t` overflow.
 * Still works correctly, when `max` has overflown, but `min` hasn't yet,
 * so `min` can be bigger than `max`.
 */
inline bool entry_was_overwritten(generation_t entry_generation,
                                  generation_t transaction_generation,
                                  generation_t youngest_generation) noexcept {

    return transaction_generation <= youngest_generation
               ? ((entry_generation >= transaction_generation) & (entry_generation <= youngest_generation))
               : ((entry_generation >= transaction_generation) | (entry_generation <= youngest_generation));
}

struct read_task_t {
    ukv_col_t col;
    ukv_key_t const& key;

    inline col_key_t location() const noexcept { return col_key_t {col, key}; }
};

/**
 * @brief Arguments of `ukv_read` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct read_tasks_soa_t {
    strided_iterator_gt<ukv_col_t const> cols;
    strided_iterator_gt<ukv_key_t const> keys;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }

    inline read_task_t operator[](ukv_size_t i) const noexcept {
        ukv_col_t col = cols ? cols[i] : ukv_col_main_k;
        ukv_key_t const& key = keys[i];
        return {col, key};
    }
};

struct scan_task_t {
    ukv_col_t col;
    ukv_key_t const& min_key;
    ukv_size_t length;

    inline col_key_t location() const noexcept { return col_key_t {col, min_key}; }
};

/**
 * @brief Arguments of `ukv_scan` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct scan_tasks_soa_t {
    strided_iterator_gt<ukv_col_t const> cols;
    strided_iterator_gt<ukv_key_t const> min_keys;
    strided_iterator_gt<ukv_size_t const> lengths;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }

    inline scan_task_t operator[](ukv_size_t i) const noexcept {
        ukv_col_t col = cols ? cols[i] : ukv_col_main_k;
        ukv_key_t const& key = min_keys[i];
        ukv_size_t len = lengths[i];
        return {col, key, len};
    }
};

struct write_task_t {
    ukv_col_t col;
    ukv_key_t const& key;
    byte_t const* begin;
    ukv_val_len_t offset;
    ukv_val_len_t length;

    inline col_key_t location() const noexcept { return col_key_t {col, key}; }
    inline bool is_deleted() const noexcept { return begin == nullptr; }
    value_view_t view() const noexcept { return {begin + offset, begin + offset + length}; }
    buffer_t buffer() const { return {begin + offset, begin + offset + length}; }
};

/**
 * @brief Arguments of `ukv_write` aggregated into a Structure-of-Arrays.
 * Is used to validate various combinations of arguments, strides, NULLs, etc.
 */
struct write_tasks_soa_t {
    strided_iterator_gt<ukv_col_t const> cols;
    strided_iterator_gt<ukv_key_t const> keys;
    strided_iterator_gt<ukv_val_ptr_t const> vals;
    strided_iterator_gt<ukv_val_len_t const> offs;
    strided_iterator_gt<ukv_val_len_t const> lens;
    ukv_size_t count = 0;

    inline std::size_t size() const noexcept { return count; }

    inline write_task_t operator[](ukv_size_t i) const noexcept {
        ukv_col_t col = cols ? cols[i] : ukv_col_main_k;
        ukv_key_t const& key = keys[i];
        byte_t const* begin;
        ukv_val_len_t off;
        ukv_val_len_t len;
        if (vals) {
            begin = reinterpret_cast<byte_t const*>(vals[i]);
            // We have separate entries at different start points.
            if (!offs && lens) {
                off = 0u;
                len = lens[i];
            }
            // We are working with a densely packed tape with `count + 1` offsets.
            else if (offs && !lens) {
                off = offs[i];
                len = offs[i + 1] - off;
            }
            // All the info is provided.
            else if (offs && lens) {
                off = offs[i];
                len = lens[i];
            }
            // We are just given C-strings, we have to guess the length.
            else {
                off = 0u;
                len = std::strlen(reinterpret_cast<char const*>(begin));
            }
        }
        // An entry just has to be deleted.
        else {
            begin = nullptr;
            off = 0u;
            len = 0u;
        }
        return {col, key, begin, off, len};
    }
};

class file_handle_t {
    std::FILE* handle_ = nullptr;

  public:
    status_t open(char const* path, char const* mode) {
        if (handle_)
            return "Close previous file before opening the new one!";
        handle_ = std::fopen(path, mode);
        if (!handle_)
            return "Failed to open a file";
        return {};
    }

    status_t close() {
        if (!handle_)
            return {};
        if (std::fclose(handle_) == EOF)
            return "Couldn't close the file after write.";
        else
            handle_ = nullptr;
        return {};
    }

    ~file_handle_t() {
        if (handle_)
            std::fclose(handle_);
    }

    operator std::FILE *() const noexcept { return handle_; }
};

template <typename range_at, typename comparable_at>
inline range_at equal_subrange(range_at range, comparable_at&& comparable) {
    auto p = std::equal_range(range.begin(), range.end(), comparable);
    return range_at {p.first, p.second};
}

template <typename element_at, typename alloc_at = std::allocator<element_at>>
void sort_and_deduplicate(std::vector<element_at, alloc_at>& elems) {
    std::sort(elems.begin(), elems.end());
    elems.erase(std::unique(elems.begin(), elems.end()), elems.end());
}

template <typename element_at, typename alloc_at = std::allocator<element_at>>
std::size_t offset_in_sorted(std::vector<element_at, alloc_at> const& elems, element_at const& wanted) {
    return std::lower_bound(elems.begin(), elems.end(), wanted) - elems.begin();
}

template <typename element_at>
void inplace_inclusive_prefix_sum(element_at* begin, element_at* const end) {
    element_at sum = 0;
    for (; begin != end; ++begin)
        sum += std::exchange(*begin, *begin + sum);
}

} // namespace unum::ukv
