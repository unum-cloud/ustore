/**
 * @file limited_priority_queue.hpp
 * @author Ashot Vardanian
 *
 * @brief Building size-constrained priority-queues over managed memory.
 */
#pragma once
#include <algorithm> // `std::destroy_n`

namespace unum::ukv {

/**
 * @brief Max-heap or Priority-Queue-like structure, keeping the biggest
 *        element in front, but having a fixed-size capacity. Can't reallocate,
 *        so beyond a certain size, either the `push()`-es will be failing,
 *        or the older entries will get evicted.
 */
template <typename element_at, typename comparator_at = std::less<void>>
class limited_priority_queue_gt {
    using element_t = element_at;
    using elementc_t = element_t const;
    using ptrc_t = elementc_t*;
    using ptr_t = element_t*;
    using comparator_t = comparator_at;

  private:
    ptr_t ptr_ = nullptr;
    std::size_t length_ = 0;
    std::size_t capacity_ = 0;

    static bool less_important(element_t const& a, element_t const& b) noexcept { return comparator_t {}(a, b); }

  public:
    limited_priority_queue_gt(limited_priority_queue_gt const&) = delete;
    limited_priority_queue_gt& operator=(limited_priority_queue_gt const&) = delete;

    limited_priority_queue_gt(limited_priority_queue_gt&& v) noexcept
        : ptr_(std::exchange(v.ptr_, nullptr)), length_(std::exchange(v.length_, 0)),
          capacity_(std::exchange(v.capacity_, 0)) {}

    limited_priority_queue_gt& operator=(limited_priority_queue_gt&& v) noexcept {
        std::swap(v.ptr_, ptr_);
        std::swap(v.length_, length_);
        std::swap(v.capacity_, capacity_);
        return *this;
    }

    limited_priority_queue_gt(ptr_t begin, ptr_t end, std::size_t length_populated = 0) noexcept
        : ptr_(begin), length_(length_populated), capacity_(end - begin) {}

    void clear() {
        std::destroy_n(ptr_, length_);
        length_ = 0;
    }

    ptrc_t data() const noexcept { return ptr_; }
    ptr_t data() noexcept { return ptr_; }
    ptr_t begin() const noexcept { return reinterpret_cast<ptr_t>(ptr_); }
    ptr_t end() const noexcept { return begin() + length_; }
    element_t& operator[](std::size_t i) noexcept { return ptr_[i]; }
    std::size_t size() const noexcept { return length_; }
    std::size_t capacity() const noexcept { return capacity_; }
    bool empty() const noexcept { return !length_; }

    /**
     * @brief
     *
     * @return true If insertion succeeded.
     * @return false If the priority of the input was to low to keep it.
     */
    bool push(element_t element) {
        if (length_ >= capacity_) {
            // Find the optimal place within the container,
            // where this entry belongs.
            auto element_ptr = std::lower_bound(ptr_, ptr_ + length_, element, &less_important);
            if (element_ptr == end())
                return false;

            auto source_ptr = element_ptr;
            for (; source_ptr + 1 != end(); ++source_ptr)
                source_ptr[1] = std::move(source_ptr[0]);
            *element_ptr = std::move(element);
            return true;
        }

        new (ptr_ + length_) element_t(std::move(element));
        ++length_;
        return true;
    }
};

} // namespace unum::ukv
