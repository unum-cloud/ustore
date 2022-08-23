/**
 * @file utility.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 *
 * @brief Smart Pointers, Monads and Range-like abstractions for C++ bindings.
 */

#pragma once
#include <optional>  // `std::optional`
#include <stdexcept> // `std::runtime_error`

#include "ukv/cpp/types.hpp"

namespace unum::ukv {

class [[nodiscard]] status_t {
    ukv_error_t raw_ = nullptr;

  public:
    status_t(ukv_error_t err = nullptr) noexcept : raw_(err) {}
    operator bool() const noexcept { return !raw_; }

    status_t(status_t const&) = delete;
    status_t& operator=(status_t const&) = delete;

    status_t(status_t&& other) noexcept { raw_ = std::exchange(other.raw_, nullptr); }
    status_t& operator=(status_t&& other) noexcept {
        std::swap(raw_, other.raw_);
        return *this;
    }
    ~status_t() {
        if (raw_)
            ukv_error_free(raw_);
        raw_ = nullptr;
    }

    std::runtime_error release_exception() {
        std::runtime_error result(raw_);
        ukv_error_free(std::exchange(raw_, nullptr));
        return result;
    }

    void throw_unhandled() {
        if (raw_ != nullptr) // C++20: [[unlikely]]
            throw release_exception();
    }

    ukv_error_t* member_ptr() noexcept { return &raw_; }
    ukv_error_t release_error() noexcept { return std::exchange(raw_, nullptr); }
    ukv_error_t message() const noexcept { return raw_; }
};

/**
 * @brief Extends `std::optional` to support a status, describing empty state.
 */
template <typename object_at>
class [[nodiscard]] expected_gt {
  protected:
    status_t status_;
    object_at object_;

  public:
    expected_gt() = default;
    expected_gt(object_at&& object) : object_(std::move(object)) {}
    expected_gt(status_t&& status, object_at&& default_object = object_at {})
        : status_(std::move(status)), object_(std::move(default_object)) {}

    expected_gt(expected_gt&& other) noexcept : status_(std::move(other.status_)), object_(std::move(other.object_)) {}

    expected_gt& operator=(expected_gt&& other) noexcept {
        std::swap(status_, other.status_);
        std::swap(object_, other.object_);
        return *this;
    }

    operator bool() const noexcept { return status_; }
    object_at operator*() && noexcept { return std::move(object_); }
    object_at const& operator*() const& noexcept { return object_; }
    object_at* operator->() noexcept { return &object_; }
    object_at const* operator->() const noexcept { return &object_; }
    operator std::optional<object_at>() && {
        return !status_ ? std::nullopt : std::optional<object_at> {std::move(object_)};
    }

    void throw_unhandled() { return status_.throw_unhandled(); }
    status_t release_status() { return std::exchange(status_, status_t {}); }
    object_at& throw_or_ref() & {
        status_.throw_unhandled();
        return object_;
    }
    object_at throw_or_release() && {
        status_.throw_unhandled();
        return std::move(object_);
    }

    template <typename hetero_at>
    bool operator==(expected_gt<hetero_at> const& other) const noexcept {
        return status_ == other.status_ && object_ == other.object_;
    }

    template <typename hetero_at>
    bool operator!=(expected_gt<hetero_at> const& other) const noexcept {
        return status_ != other.status_ || object_ != other.object_;
    }

    template <typename hetero_at>
    bool operator==(hetero_at const& other) const noexcept {
        return status_ && object_ == other;
    }

    template <typename hetero_at>
    bool operator!=(hetero_at const& other) const noexcept {
        return status_ || object_ != other;
    }
};

/**
 * @brief Wraps a potentially non-trivial type, like "optional",
 * often controlling the underlying memory of the object.
 */
template <typename object_at>
class [[nodiscard]] given_gt : public expected_gt<object_at> {
  protected:
    using base_t = expected_gt<object_at>;
    using base_t::object_;
    using base_t::status_;
    arena_t arena_;

  public:
    // given_gt() : an;
    given_gt(object_at&& object, arena_t&& arena) : base_t(std::move(object)), arena_(std::move(arena)) {}

    given_gt(status_t&& status, object_at&& default_object = object_at {}, arena_t&& arena = {nullptr})
        : base_t(std::move(status), std::move(default_object)), arena_(std::move(arena)) {}

    given_gt(given_gt&& other) noexcept
        : base_t(std::move(other.status_, other.object_)), arena_(std::move(other.arena_)) {}

    given_gt& operator=(given_gt&& other) noexcept {
        std::swap(status_, other.status_);
        std::swap(object_, other.object_);
        std::swap(arena_, other.arena_);
        return *this;
    }

    using base_t::operator bool;
    using base_t::operator==;
    using base_t::operator!=;
    using base_t::release_status;
    using base_t::throw_unhandled;

    object_at const& operator*() const& noexcept { return object_; }
    object_at const* operator->() const& noexcept { return &object_; }

    // operator std::optional<object_at>() && = delete;
    // object_at&& operator*() && noexcept = delete;
    // object_at* operator->() && noexcept = delete;

    inline arena_t release_arena() noexcept { return std::exchange(arena_, {arena_.db()}); }
    inline base_t release_expected() noexcept { return {std::move(status_), std::move(object_)}; }
};

enum error_code_t {
    out_of_memory_k,
    args_combo_k,
    args_wrong_k,
    uninitialized_state_k,
    network_k,
    missing_feature_k,
    error_unknown_k
};

} // namespace unum::ukv

#define log_error(c_error, code, message) \
    { *c_error = message; }

#define log_if_error(must_be_true, c_error, code, message) \
    if (!(must_be_true)) {                                 \
        *c_error = message;                                \
    }

#define return_if_error(must_be_true, c_error, code, message) \
    if (!(must_be_true)) {                                    \
        *c_error = message;                                   \
        return;                                               \
    }

#define return_on_error(c_error) \
    {                            \
        if (*c_error)            \
            return;              \
    }