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

#define std_memory_resource_m 0

#if std_memory_resource_m
#if __APPLE__
#include <experimental/memory_resource> // `std::pmr::vector`
#else
#include <memory_resource> // `std::pmr::vector`
#endif
#endif // std_memory_resource_m

#include "ukv/cpp/types.hpp"  // `byte_t`, `next_power_of_two`
#include "ukv/cpp/ranges.hpp" // `strided_range_gt`
#include "ukv/cpp/status.hpp" // `out_of_memory_k`

#if std_memory_resource_m
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
#endif // std_memory_resource_m

#if !std_memory_resource_m
namespace std::pmr {
class memory_resource {
  public:
    // https://en.cppreference.com/w/cpp/types/max_align_t
    static const size_t max_align_k = alignof(max_align_t);

  public:
    virtual ~memory_resource() = default;

    void* allocate(size_t bytes, size_t align = max_align_k) { return do_allocate(bytes, align); }
    void deallocate(void* ptr, size_t bytes, size_t align = max_align_k) { do_deallocate(ptr, bytes, align); }
    bool is_equal(memory_resource const& other) const noexcept { return do_is_equal(other); }

  private:
    virtual void* do_allocate(size_t, size_t) = 0;
    virtual void do_deallocate(void*, size_t, size_t) = 0;
    virtual bool do_is_equal(memory_resource const&) const noexcept = 0;
};

class new_delete_memory_resource : public memory_resource {
  public:
    ~new_delete_memory_resource() override = default;

  private:
    void* do_allocate(size_t size, size_t align = max_align_k) override { return aligned_alloc(align, size); }
    void do_deallocate(void* ptr,
                       [[maybe_unused]] size_t size = 0,
                       [[maybe_unused]] size_t align = max_align_k) override {
        free(ptr);
    }
    bool do_is_equal(memory_resource const& other) const noexcept override { return &other == this; }
};

static new_delete_memory_resource default_memory_resource;

inline memory_resource* get_default_resource() {
    return &default_memory_resource;
}

} // namespace std::pmr
#endif // std_memory_resource_m

namespace unum::ukv {

inline thread_local std::pmr::memory_resource* local_memory = std::pmr::get_default_resource();

class monotonic_resource_t final : public std::pmr::memory_resource {
  public:
    enum type_t { capped_k, growing_k, borrowed_k };

  private:
    static constexpr std::size_t growth_factor_k = 2;

    struct buffer_t {
        void* begin = nullptr;
        std::size_t total_memory = 0;
        std::size_t available_memory = 0;

        buffer_t() = default;
        buffer_t(void* bg, std::size_t tm, std::size_t am) : begin(bg), total_memory(tm), available_memory(am) {};
    };

    std::forward_list<buffer_t> buffers_;
    std::pmr::memory_resource* upstream_;
    std::size_t alignment_;
    type_t type_;

  public:
    monotonic_resource_t(monotonic_resource_t const&) = delete;
    monotonic_resource_t& operator=(monotonic_resource_t const&) = delete;

    explicit monotonic_resource_t(std::pmr::memory_resource* upstream) noexcept
        : buffers_(), upstream_(upstream), alignment_(0), type_(type_t::borrowed_k) {};

    monotonic_resource_t(std::size_t buffer_size,
                         std::size_t alignment,
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
        return type_ == borrowed_k
                   ? reinterpret_cast<monotonic_resource_t*>(upstream_)->capacity()
                   : std::accumulate(buffers_.begin(), buffers_.end(), 0, [](std::size_t sum, auto& buf) {
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
            std::size_t new_size = buffers_.front().total_memory * growth_factor_k;
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

    void deallocate(at* ptr, std::size_t size) { local_memory->deallocate(ptr, sizeof(at) * size); }
    at* allocate(std::size_t size) { return reinterpret_cast<at*>(local_memory->allocate(sizeof(at) * size)); }
};

template <typename range_at>
struct range_or_dummy_gt {
    using range_t = range_at;
    using value_type = typename range_t::value_type;
    using reference = typename range_t::reference;

    range_t range_;
    value_type dummy_;

    reference operator[](std::size_t i) & noexcept { return range_ ? reference(range_[i]) : reference(dummy_); }
    reference operator[](std::size_t i) const& noexcept { return range_ ? reference(range_[i]) : reference(dummy_); }
    std::size_t size() const noexcept { return range_.size(); }
    explicit operator bool() const noexcept { return range_; }
};

struct stl_arena_t {
    static constexpr std::size_t initial_size_k = 1024ul * 1024ul;

    explicit stl_arena_t(monotonic_resource_t* resource) noexcept : resource_(resource), is_shared_memory_(false) {}

    explicit stl_arena_t( //
        std::size_t initial_size = initial_size_k,
        monotonic_resource_t::type_t type = monotonic_resource_t::growing_k,
        bool is_shared_memory = false)
        : resource_(initial_size,
                    64ul,
                    type,
                    is_shared_memory ? shared_resource_t::get_default_resource() : std::pmr::get_default_resource()),
          is_shared_memory_(is_shared_memory) {
        local_memory = &resource_;
    }

    ~stl_arena_t() noexcept { local_memory = std::pmr::get_default_resource(); }

    template <typename at>
    ptr_range_gt<at> alloc(std::size_t size, ukv_error_t* c_error, std::size_t alignment = sizeof(at)) noexcept {
        void* result = resource_.allocate(sizeof(at) * size, alignment);
        log_if_error(result, c_error, out_of_memory_k, "");
        return {reinterpret_cast<at*>(result), size};
    }

    template <typename at>
    ptr_range_gt<at> grow( //
        ptr_range_gt<at> span,
        std::size_t additional_size,
        ukv_error_t* c_error,
        std::size_t alignment = sizeof(at)) noexcept {

        auto new_size = span.size() + additional_size;
        void* result = resource_.allocate(sizeof(at) * new_size, alignment);
        if (result)
            std::memcpy(result, span.begin(), span.size_bytes());
        else
            log_error(c_error, out_of_memory_k, "");
        return {reinterpret_cast<at*>(result), new_size};
    }

    range_or_dummy_gt<bits_span_t> alloc_or_dummy( //
        std::size_t size,
        ukv_error_t* c_error,
        ukv_octet_t** output,
        std::size_t alignment = sizeof(ukv_octet_t)) noexcept {

        using range_t = bits_span_t;
        auto slots = divide_round_up(size, bits_in_byte_k);
        auto range = output //
                         ? range_t {(*output = alloc<ukv_octet_t>(slots, c_error, alignment).begin())}
                         : range_t {nullptr};
        return {range, {}};
    }

    template <typename at>
    range_or_dummy_gt<ptr_range_gt<at>> alloc_or_dummy( //
        std::size_t size,
        ukv_error_t* c_error,
        at** output,
        std::size_t alignment = sizeof(at)) noexcept {

        static_assert(!std::is_same<at, ukv_octet_t>());
        using range_t = ptr_range_gt<at>;
        auto range = output //
                         ? range_t {(*output = alloc<at>(size, c_error, alignment).begin()), size}
                         : range_t {nullptr, nullptr};
        return {range, {}};
    }

    monotonic_resource_t resource_;
    bool is_shared_memory_;
};

template <typename dangerous_at>
void safe_section(ukv_str_view_t name, ukv_error_t* c_error, dangerous_at&& dangerous) noexcept {
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

inline stl_arena_t make_stl_arena(ukv_arena_t* c_arena, ukv_options_t options, ukv_error_t* c_error) noexcept {
    try {
        stl_arena_t** arena_output = reinterpret_cast<stl_arena_t**>(c_arena);
        stl_arena_t*& arena = *arena_output;
        bool is_shared_memory = options & ukv_option_read_shared_memory_k;
        if (!arena || (is_shared_memory && !arena->is_shared_memory_)) {
            delete arena;
            arena = new stl_arena_t(stl_arena_t::initial_size_k, monotonic_resource_t::growing_k, is_shared_memory);
        }

        bool keep_old_data = options & ukv_option_dont_discard_memory_k;
        if (!keep_old_data)
            arena->resource_.release();
        return stl_arena_t(&arena->resource_);
    }
    catch (...) {
        log_error(c_error, out_of_memory_k, "");
        return stl_arena_t(nullptr);
    }
}

} // namespace unum::ukv
