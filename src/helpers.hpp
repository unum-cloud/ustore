/**
 * @file helpers.hpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions for the C++ backend implementations.
 */
#pragma once
#include <sys/mman.h> // `mmap`
#include <limits.h>   // `CHAR_BIT`
#include <cstring>    // `std::memcpy`
#include <stdexcept>  // `std::runtime_error`
#include <memory>     // `std::allocator`
#include <vector>     // `std::vector`
#include <algorithm>  // `std::sort`
#include <numeric>    // `std::accumulate`
#include <forward_list>

#if __APPLE__
#include <experimental/memory_resource> // `std::pmr::vector`
#else
#include <memory_resource> // `std::pmr::vector`
#endif

#include "ukv/ukv.hpp"

#if __APPLE__
namespace std::pmr {
template <typename at>
using polymorphic_allocator = ::std::experimental::pmr::polymorphic_allocator<at>;
template <typename at>
using vector = std::vector<at, ::std::experimental::pmr::polymorphic_allocator<at>>;
using memory_resource = ::std::experimental::pmr::memory_resource;
inline auto get_default_resource() {
    return ::std::experimental::pmr::get_default_resource();
}
} // namespace std::pmr
#endif

namespace unum::ukv {

using allocator_t = std::allocator<byte_t>;
using buffer_t = std::vector<byte_t, allocator_t>;
using generation_t = std::int64_t;

constexpr std::size_t arrow_extra_offsets_k = 1;
constexpr std::size_t arrow_bytes_alignment_k = 64;

inline thread_local std::pmr::memory_resource* thrlocal_memres = std::pmr::get_default_resource();

class monotonic_resource_t final : public std::pmr::memory_resource {
  public:
    enum type_t { capped_k, growing_k, borrowed_k };

  private:
    static constexpr size_t growth_factor_k = 2;

    struct buffer_t {
        void* begin = nullptr;
        size_t total_memory = 0;
        size_t available_memory = 0;

        buffer_t() = default;
        buffer_t(void* bg, size_t tm, size_t am) : begin(bg), total_memory(tm), available_memory(am) {};
    };

    std::forward_list<buffer_t> buffers_;
    std::pmr::memory_resource* upstream_;
    size_t alignment_;
    type_t type_;

  public:
    monotonic_resource_t(monotonic_resource_t const&) = delete;
    monotonic_resource_t& operator=(monotonic_resource_t const&) = delete;

    explicit monotonic_resource_t(std::pmr::memory_resource* upstream) noexcept
        : buffers_(), upstream_(upstream), alignment_(0), type_(type_t::borrowed_k) {};

    monotonic_resource_t(size_t buffer_size,
                         size_t alignment,
                         type_t type = type_t::capped_k,
                         std::pmr::memory_resource* upstream = std::pmr::get_default_resource())
        : buffers_(), upstream_(upstream), alignment_(alignment), type_(type) {
        buffers_.emplace_front(upstream->allocate(buffer_size, alignment), buffer_size, buffer_size);
    }

    ~monotonic_resource_t() noexcept override {
        for (auto buffer : buffers_) {
            release_one(buffer);
            upstream_->deallocate(buffer.begin, buffer.total_memory, alignment_);
        }
    }

    void release() noexcept {
        if (buffers_.empty())
            return;

        buffer_t buf = buffers_.front();
        buffers_.pop_front();
        while (!buffers_.empty()) {
            release_one(buf);
            upstream_->deallocate(buf.begin, buf.total_memory, alignment_);
            buf = buffers_.front();
            buffers_.pop_front();
        }

        buffers_.push_front(buf);
        release_one(buf);
    }

    std::size_t capacity() const noexcept {
        return type_ == borrowed_k ? reinterpret_cast<monotonic_resource_t*>(upstream_)->capacity()
                                   : std::accumulate(buffers_.begin(), buffers_.end(), 0, [](size_t sum, auto& buf) {
                                         return sum + buf.total_memory;
                                     });
    }

    std::size_t used() const noexcept {
        return type_ == borrowed_k //
                   ? reinterpret_cast<monotonic_resource_t*>(upstream_)->used()
                   : capacity() - buffers_.front().available_memory;
    }

  private:
    void release_one(buffer_t& buffer) noexcept {
        buffer.begin = (uint8_t*)(buffer.begin) - (buffer.total_memory - buffer.available_memory);
        buffer.available_memory = buffer.total_memory;
    }

    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        if (type_ == type_t::borrowed_k)
            return upstream_->allocate(bytes, alignment);

        void* result = std::align(alignment, bytes, buffers_.front().begin, buffers_.front().available_memory);
        if (result != nullptr) {
            buffers_.front().begin = (uint8_t*)buffers_.front().begin + bytes;
            buffers_.front().available_memory -= bytes;
        }
        else if (type_ == type_t::growing_k) {
            size_t new_size = buffers_.front().total_memory * growth_factor_k;
            if (new_size < (bytes + alignment))
                new_size = next_power_of_two(bytes + alignment);
            buffers_.emplace_front(upstream_->allocate(new_size, alignment_), new_size, new_size);
            result = do_allocate(bytes, alignment);
        }
        return result;
    }

    void do_deallocate(void*, std::size_t, std::size_t) noexcept override {}
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};

class shared_resource_t final : public std::pmr::memory_resource {
    void* do_allocate(std::size_t bytes, std::size_t) override {
        return mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    }

    void do_deallocate(void* ptr, std::size_t, std::size_t bytes) noexcept override { munmap(ptr, bytes); }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

  public:
    static shared_resource_t* get_default_resource() {
        static shared_resource_t default_resource;
        return &default_resource;
    }
};

template <typename at>
class polymorphic_allocator_gt {
  public:
    using value_type = at;
    using size_type = std::size_t;

    void deallocate(at* ptr, std::size_t size) { thrlocal_memres->deallocate(ptr, sizeof(at) * size); }
    at* allocate(std::size_t size) { return reinterpret_cast<at*>(thrlocal_memres->allocate(sizeof(at) * size)); }
};

template <typename at>
struct span_gt {
    span_gt() noexcept : ptr_(nullptr), size_(0) {}
    span_gt(at* ptr, std::size_t sz) noexcept : ptr_(ptr), size_(sz) {}

    constexpr at* begin() const noexcept { return ptr_; }
    constexpr at* end() const noexcept { return ptr_ + size_; }
    at const* cbegin() const noexcept { return ptr_; }
    at const* cend() const noexcept { return ptr_ + size_; }

    at& operator[](std::size_t i) noexcept { return ptr_[i]; }
    at& operator[](std::size_t i) const noexcept { return ptr_[i]; }

    template <typename another_at>
    span_gt<another_at> cast() const noexcept {
        return {reinterpret_cast<another_at*>(ptr_), size_ * sizeof(at) / sizeof(another_at)};
    }

    span_gt<byte_t const> span_bytes() const noexcept {
        return {reinterpret_cast<byte_t const*>(ptr_), size_ * sizeof(at)};
    }

    std::size_t size_bytes() const noexcept { return size_ * sizeof(at); }
    std::size_t size() const noexcept { return size_; }

    strided_range_gt<at> strided() const noexcept { return {ptr_, ptr_ + size_}; }

  private:
    at* ptr_;
    std::size_t size_;
};

struct stl_arena_t {
    explicit stl_arena_t(monotonic_resource_t* mem_resource) noexcept
        : resource(mem_resource), using_shared_memory(false) {}
    explicit stl_arena_t( //
        std::size_t initial_buffer_size = 1024ul * 1024ul,
        monotonic_resource_t::type_t type = monotonic_resource_t::growing_k,
        bool use_shared_memory = false)
        : resource(initial_buffer_size,
                   64ul,
                   type,
                   use_shared_memory ? shared_resource_t::get_default_resource() : std::pmr::get_default_resource()),
          using_shared_memory(use_shared_memory) {
        thrlocal_memres = &resource;
    }
    ~stl_arena_t() { thrlocal_memres = std::pmr::get_default_resource(); }

    template <typename at>
    span_gt<at> alloc(std::size_t size, ukv_error_t* c_error, std::size_t alignment = sizeof(at)) noexcept {
        void* result = resource.allocate(sizeof(at) * size, alignment);
        log_if_error(result, c_error, out_of_memory_k, "");
        return {reinterpret_cast<at*>(result), size};
    }

    template <typename at>
    span_gt<at> grow( //
        span_gt<at> span,
        std::size_t additional_size,
        ukv_error_t* c_error,
        std::size_t alignment = sizeof(at)) noexcept {

        auto new_size = span.size() + additional_size;
        void* result = resource.allocate(sizeof(at) * new_size, alignment);
        if (result)
            std::memcpy(result, span.begin(), span.size_bytes());
        else
            log_error(c_error, out_of_memory_k, "");
        return {reinterpret_cast<at*>(result), new_size};
    }

    template <typename at>
    strided_range_or_dummy_gt<at> alloc_or_dummy( //
        std::size_t size,
        ukv_error_t* c_error,
        at** output,
        std::size_t alignment = sizeof(at)) noexcept {

        using strided_t = strided_range_gt<at>;
        auto strided = output //
                           ? strided_t {((*output) = alloc<at>(size, c_error, alignment).begin()), sizeof(at), size}
                           : strided_t {nullptr, 0, size};
        return {strided, {}};
    }

    monotonic_resource_t resource;
    bool using_shared_memory;
};

template <typename dangerous_at>
void safe_section(ukv_str_view_t name, ukv_error_t* c_error, dangerous_at&& dangerous) {
    try {
        dangerous();
    }
    catch (std::bad_alloc const&) {
        log_error(c_error, out_of_memory_k, name);
    }
    catch (...) {
        log_error(c_error, error_unknown_k, name);
    }
}

inline stl_arena_t prepare_arena(ukv_arena_t* c_arena, ukv_options_t options, ukv_error_t* c_error) noexcept {
    try {
        stl_arena_t** arena = reinterpret_cast<stl_arena_t**>(c_arena);

        if (!*arena || ((options & ukv_option_read_shared_k) && !(*arena)->using_shared_memory)) {
            delete *arena;
            *arena =
                new stl_arena_t(1024ul * 1024ul, monotonic_resource_t::growing_k, options & ukv_option_read_shared_k);
        }

        if (!(options & ukv_option_nodiscard_k))
            (*arena)->resource.release();
        return stl_arena_t(&(*arena)->resource);
    }
    catch (...) {
        log_error(c_error, out_of_memory_k, "");
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

    operator std::FILE*() const noexcept { return handle_; }
};

template <typename range_at, typename comparable_at>
inline range_at equal_subrange(range_at range, comparable_at&& comparable) {
    auto p = std::equal_range(range.begin(), range.end(), comparable);
    return range_at {p.first, p.second};
}

template <typename iterator_at>
std::size_t sort_and_deduplicate(iterator_at begin, iterator_at end) {
    std::sort(begin, end);
    return std::unique(begin, end) - begin;
}

template <typename element_at, typename alloc_at = std::allocator<element_at>>
void sort_and_deduplicate(std::vector<element_at, alloc_at>& elems) {
    elems.erase(elems.begin() + sort_and_deduplicate(elems.begin(), elems.end()), elems.end());
}

template <typename container_at, typename comparable_at>
std::size_t offset_in_sorted(container_at const& elems, comparable_at const& wanted) {
    return std::lower_bound(elems.begin(), elems.end(), wanted) - elems.begin();
}

template <typename element_at>
element_at inplace_inclusive_prefix_sum(element_at* begin, element_at* const end) {
    element_at sum = 0;
    for (; begin != end; ++begin)
        sum += std::exchange(*begin, *begin + sum);
    return sum;
}

/**
 * @brief An `std::vector`-like class, with open layout,
 * friendly to our C API. Can't grow, but can shrink.
 * Just like humans after puberty :)
 */

template <typename element_at>
class safe_vector_gt {
    using element_t = element_at;
    using elementc_t = element_t const;
    using ptrc_t = elementc_t*;
    using ptr_t = element_t*;

  private:
    ptr_t ptr_ = nullptr;
    ukv_length_t length_ = 0;
    ukv_length_t cap_ = 0;

    stl_arena_t* arena_ptr_ = nullptr;

  public:
    safe_vector_gt() noexcept = default;
    safe_vector_gt(safe_vector_gt const&) = delete;
    safe_vector_gt& operator=(safe_vector_gt const&) = delete;

    safe_vector_gt(safe_vector_gt&& v) noexcept
        : ptr_(std::exchange(v.ptr_, nullptr)), length_(std::exchange(v.length_, 0)), cap_(std::exchange(v.cap_, 0)),
          arena_ptr_(std::exchange(v.arena_ptr_, nullptr)) {}

    safe_vector_gt& operator=(safe_vector_gt&& v) noexcept {
        std::swap(v.ptr_, ptr_);
        std::swap(v.length_, length_);
        std::swap(v.cap_, cap_);
        std::swap(v.arena_ptr_, arena_ptr_);

        return *this;
    }
    safe_vector_gt(stl_arena_t* arena_ptr) : arena_ptr_(arena_ptr) {}
    safe_vector_gt(std::size_t size, stl_arena_t* arena_ptr, ukv_error_t* c_error) : arena_ptr_(arena_ptr) {
        if (!size)
            return;
        auto tape = arena_ptr_->alloc<element_t>(size, c_error);
        ptr_ = tape.begin();
        cap_ = length_ = size;
    }

    safe_vector_gt(value_view_t view) : safe_vector_gt(view.size()) { std::memcpy(ptr_, view.begin(), view.size()); }
    ~safe_vector_gt() { reset(); }

    void reset() {
        ptr_ = nullptr;
        length_ = 0;
        cap_ = 0;
    }

    void resize(std::size_t size, ukv_error_t* c_error) {
        return_if_error(cap_ >= size, c_error, args_wrong_k, "Only shrinking is currently supported");
        length_ = size;
    }

    void reserve(size_t new_cap, ukv_error_t* c_error) {
        if (new_cap <= cap_)
            return;
        auto tape = ptr_ ? arena_ptr_->grow<element_t>({ptr_, cap_}, new_cap - cap_, c_error)
                         : arena_ptr_->alloc<element_t>(new_cap, c_error);
        return_on_error(c_error);

        ptr_ = tape.begin();
        cap_ = new_cap;
    }

    void push_back(element_t val, ukv_error_t* c_error) {
        auto new_size = length_ + 1;
        reserve(new_size, c_error);
        return_on_error(c_error);

        ptr_[length_] = val;
        length_ = new_size;
    }

    void insert(std::size_t offset, ptrc_t inserted_begin, ptrc_t inserted_end, ukv_error_t* c_error) {
        return_if_error(size() >= offset, c_error, out_of_range_k, "Can't insert");

        auto inserted_len = static_cast<ukv_length_t>(inserted_end - inserted_begin);
        auto following_len = static_cast<ukv_length_t>(length_ - offset);
        auto new_size = length_ + inserted_len;

        if (new_size > cap_) {
            auto tape = arena_ptr_->grow<element_t>({ptr_, cap_}, new_size - cap_, c_error);
            return_on_error(c_error);
            ptr_ = tape.begin();
            cap_ = length_ = new_size;
        }
        else
            length_ = new_size;

        std::memmove(ptr_ + offset + inserted_len, ptr_ + offset, following_len);
        std::memcpy(ptr_ + offset, inserted_begin, inserted_len);
    }

    void erase(std::size_t offset, std::size_t length, ukv_error_t* c_error) {
        return_if_error(size() >= offset + length, c_error, out_of_range_k, "Can't erase");

        std::memmove(ptr_ + offset, ptr_ + offset + length, length_ - (offset + length));
        length_ -= length;
    }

    inline ptrc_t data() const noexcept { return ptr_; }
    inline ptr_t data() noexcept { return ptr_; }
    inline ptr_t begin() const noexcept { return reinterpret_cast<ptr_t>(ptr_); }
    inline ptr_t end() const noexcept { return begin() + length_; }
    inline element_t& operator[](std::size_t i) noexcept { return ptr_[i]; }
    inline std::size_t size() const noexcept { return length_; }
    inline operator bool() const noexcept { return length_; }
    inline operator value_view_t() const noexcept { return {ptr_, length_}; }
    inline void clear() noexcept { length_ = 0; }

    inline ptr_t* member_ptr() noexcept { return &ptr_; }
    inline ukv_length_t* member_length() noexcept { return &length_; }
    inline ukv_length_t* member_cap() noexcept { return &cap_; }
};

/**
 * @brief Append-only data-structure for variable length blobs.
 * Owns the underlying arena and is external to the underlying DB.
 * Is suited for data preparation before passing to the C API.
 */
class growing_tape_t {
    safe_vector_gt<ukv_length_t> offsets_;
    safe_vector_gt<ukv_length_t> lengths_;
    safe_vector_gt<byte_t> contents_;

  public:
    growing_tape_t(stl_arena_t& arena) : offsets_(&arena), lengths_(&arena), contents_(&arena) {}

    void push_back(value_view_t value, ukv_error_t* c_error) {
        offsets_.push_back(static_cast<ukv_length_t>(contents_.size()), c_error);
        lengths_.push_back(static_cast<ukv_length_t>(value.size()), c_error);
        contents_.insert(contents_.size(), value.begin(), value.end(), c_error);
    }

    void reserve(size_t new_cap, ukv_error_t* c_error) {
        offsets_.reserve(new_cap + 1, c_error);
        lengths_.reserve(new_cap, c_error);
    }

    void clear() {
        offsets_.clear();
        lengths_.clear();
        contents_.clear();
    }

    strided_range_gt<ukv_length_t> offsets() noexcept {
        auto n = lengths_.size();
        offsets_[n] = offsets_[n - 1] + lengths_[n - 1];
        return strided_range<ukv_length_t>(offsets_.begin(), offsets_.end());
    }
    strided_range_gt<ukv_length_t> lengths() noexcept {
        return strided_range<ukv_length_t>(lengths_.begin(), lengths_.end());
    }
    strided_range_gt<byte_t> contents() noexcept { return strided_range<byte_t>(contents_.begin(), contents_.end()); }

    operator joined_bins_t() noexcept { return {ukv_bytes_ptr_t(contents_.data()), offsets_.data(), lengths_.size()}; }

    operator embedded_bins_t() noexcept {
        return {ukv_bytes_ptr_t(contents_.data()), offsets_.data(), lengths_.data(), lengths_.size()};
    }
};

} // namespace unum::ukv
