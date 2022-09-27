/**
 * @file pmr.hpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions Polymorphic Memory Allocators.
 */
#pragma once
#include <sys/mman.h> // `mmap`
#include <limits.h>   // `CHAR_BIT`
#include <cstring>    // `std::memcpy`
#include <stdexcept>  // `std::runtime_error`
#include <memory>     // `std::allocator`
#include <vector>     // `std::vector`
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

    strided_range_gt<at> strided() const noexcept { return {{ptr_, sizeof(at)}, size_}; }

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
                           ? strided_t {{((*output) = alloc<at>(size, c_error, alignment).begin()), sizeof(at)}, size}
                           : strided_t {{nullptr, 0}, size};
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

} // namespace unum::ukv
