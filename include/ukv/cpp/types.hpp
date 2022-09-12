/**
 * @file utility.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 *
 * @brief Smart Pointers, Monads and Range-like abstractions for C++ bindings.
 */

#pragma once
#include <functional> // `std::hash`
#include <utility>    // `std::exchange`

#include "ukv/ukv.h"

namespace unum::ukv {

enum class byte_t : std::uint8_t {};

/**
 * @brief An OOP-friendly location representation for objects in the DB.
 * Should be used with `stride` set to `sizeof(col_key_t)`.
 */
struct col_key_t {

    ukv_collection_t col = ukv_collection_main_k;
    ukv_key_t key = 0;

    col_key_t() = default;
    col_key_t(col_key_t const&) = default;
    col_key_t& operator=(col_key_t const&) = default;

    inline col_key_t(ukv_collection_t c, ukv_key_t k) noexcept : col(c), key(k) {}
    inline col_key_t(ukv_key_t k) noexcept : key(k) {}
    inline col_key_t in(ukv_collection_t col) noexcept { return {col, key}; }

    inline bool operator==(col_key_t const& other) const noexcept { return (col == other.col) & (key == other.key); }
    inline bool operator!=(col_key_t const& other) const noexcept { return (col != other.col) | (key != other.key); }
    inline bool operator<(col_key_t const& other) const noexcept {
        return (col < other.col) | ((col == other.col) & (key < other.key));
    }
    inline bool operator>(col_key_t const& other) const noexcept {
        return (col > other.col) | ((col == other.col) & (key > other.key));
    }
    inline bool operator<=(col_key_t const& other) const noexcept { return operator<(other) | operator==(other); }
    inline bool operator>=(col_key_t const& other) const noexcept { return operator>(other) | operator==(other); }
};

struct col_key_field_t {
    ukv_collection_t col = 0;
    ukv_key_t key = ukv_key_unknown_k;
    ukv_str_view_t field = nullptr;

    col_key_field_t() = default;
    col_key_field_t(ukv_key_t key) noexcept : col(ukv_collection_main_k), key(key), field(nullptr) {}
    col_key_field_t(ukv_collection_t col, ukv_key_t key, ukv_str_view_t field = nullptr) noexcept
        : col(col), key(key), field(field) {}
    col_key_field_t(ukv_key_t key, ukv_str_view_t field) noexcept
        : col(ukv_collection_main_k), key(key), field(field) {}
};

template <typename... args_at>
inline col_key_field_t ckf(args_at&&... args) {
    return {std::forward<args_at>(args)...};
}

/**
 * @brief Graph Edge, or in DBMS terms - a relationship.
 */
struct edge_t {
    ukv_key_t source_id;
    ukv_key_t target_id;
    ukv_key_t id = ukv_default_edge_id_k;

    inline bool operator==(edge_t const& other) const noexcept {
        return (source_id == other.source_id) & (target_id == other.target_id) & (id == other.id);
    }
    inline bool operator!=(edge_t const& other) const noexcept {
        return (source_id != other.source_id) | (target_id != other.target_id) | (id != other.id);
    }
};

/**
 * @brief An asymmetric slice of a bond/relation.
 * Every vertex stores a list of such @c `neighborship_t`s
 * in a sorted order.
 */
struct neighborship_t {
    ukv_key_t neighbor_id = 0;
    ukv_key_t edge_id = 0;

    friend inline bool operator<(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id < b.neighbor_id) | ((a.neighbor_id == b.neighbor_id) & (a.edge_id < b.edge_id));
    }
    friend inline bool operator==(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id == b.neighbor_id) & (a.edge_id == b.edge_id);
    }
    friend inline bool operator!=(neighborship_t a, neighborship_t b) noexcept {
        return (a.neighbor_id != b.neighbor_id) | (a.edge_id != b.edge_id);
    }

    friend inline bool operator<(ukv_key_t a_vertex_id, neighborship_t b) noexcept {
        return a_vertex_id < b.neighbor_id;
    }
    friend inline bool operator<(neighborship_t a, ukv_key_t b_vertex_id) noexcept {
        return a.neighbor_id < b_vertex_id;
    }
    friend inline bool operator==(ukv_key_t a_vertex_id, neighborship_t b) noexcept {
        return a_vertex_id == b.neighbor_id;
    }
    friend inline bool operator==(neighborship_t a, ukv_key_t b_vertex_id) noexcept {
        return a.neighbor_id == b_vertex_id;
    }
};

#pragma region Variable Length Views

/**
 * @brief Similar to `std::optional<std::string_view>`.
 * It's NULL state and "empty string" states are not identical.
 * The NULL state generally reflects missing values.
 * Unlike `indexed_range_gt<byte_t const*>`, this classes layout allows
 * easily passing it to the internals of UKV implementations
 * without additional bit-twiddling.
 */
class value_view_t {

    ukv_bytes_cptr_t ptr_ = nullptr;
    ukv_length_t length_ = ukv_length_missing_k;

  public:
    using value_type = byte_t;

    inline value_view_t() = default;
    inline value_view_t(ukv_bytes_cptr_t ptr, ukv_length_t length) noexcept {
        ptr_ = ptr;
        length_ = length;
    }

    inline value_view_t(byte_t const* begin, byte_t const* end) noexcept
        : ptr_(ukv_bytes_cptr_t(begin)), length_(static_cast<ukv_length_t>(end - begin)) {}

    /// Compatibility with `std::basic_string_view` in C++17.
    inline value_view_t(byte_t const* begin, std::size_t n) noexcept
        : ptr_(ukv_bytes_cptr_t(begin)), length_(static_cast<ukv_length_t>(n)) {}

    inline value_view_t(char const* c_str) noexcept
        : ptr_(ukv_bytes_cptr_t(c_str)), length_(static_cast<ukv_length_t>(std::strlen(c_str))) {}

    inline operator bool() const noexcept { return length_ != ukv_length_missing_k; }
    inline std::size_t size() const noexcept { return length_ == ukv_length_missing_k ? 0 : length_; }
    inline byte_t const* data() const noexcept {
        return length_ != ukv_length_missing_k ? reinterpret_cast<byte_t const*>(ptr_) : nullptr;
    }

    inline char const* c_str() const noexcept { return reinterpret_cast<char const*>(ptr_); }
    inline byte_t const* begin() const noexcept { return data(); }
    inline byte_t const* end() const noexcept { return data() + size(); }
    inline bool empty() const noexcept { return !size(); }

    ukv_bytes_cptr_t const* member_ptr() const noexcept { return &ptr_; }
    ukv_length_t const* member_length() const noexcept { return &length_; }

    bool operator==(value_view_t other) const noexcept {
        return size() == other.size() && std::equal(begin(), end(), other.begin());
    }
    bool operator!=(value_view_t other) const noexcept {
        return size() != other.size() || !std::equal(begin(), end(), other.begin());
    }
};

class value_ref_t {

    ukv_byte_t* ptr_ = nullptr;
    ukv_length_t* offset_ = nullptr;
    ukv_length_t* length_ = nullptr;

  public:
    using value_type = byte_t;

    inline value_ref_t() = default;
    inline value_ref_t(ukv_byte_t* ptr, ukv_length_t& offset, ukv_length_t& length) noexcept {
        ptr_ = ptr;
        offset_ = &offset;
        length_ = &length;
    }

    inline byte_t const* begin() const noexcept { return reinterpret_cast<byte_t const*>(ptr_); }
    inline byte_t const* end() const noexcept { return begin() + size(); }
    inline char const* c_str() const noexcept { return reinterpret_cast<char const*>(ptr_); }
    inline std::size_t size() const noexcept { return *length_ == ukv_length_missing_k ? 0 : *length_; }
    inline bool empty() const noexcept { return !size(); }
    inline operator bool() const noexcept { return *length_ != ukv_length_missing_k; }

    ukv_byte_t* const* member_ptr() const noexcept { return &ptr_; }
    ukv_length_t const* member_offset() const noexcept { return offset_; }
    ukv_length_t const* member_length() const noexcept { return length_; }

    bool operator==(value_ref_t other) const noexcept {
        return size() == other.size() && std::equal(begin(), end(), other.begin());
    }
    bool operator!=(value_ref_t other) const noexcept {
        return size() != other.size() || !std::equal(begin(), end(), other.begin());
    }

    void swap(value_ref_t& other) {
        std::swap(*ptr_, *other.ptr_);
        std::swap(*offset_, *other.offset_);
        std::swap(*length_, *other.length_);
    }
};

template <typename container_at>
value_view_t value_view(container_at&& container) {
    using element_t = typename std::remove_reference_t<container_at>::value_type;
    return {reinterpret_cast<byte_t const*>(container.data()), container.size() * sizeof(element_t)};
}

#pragma region Memory Management

/**
 * @brief A view of a tape received from the DB.
 * Allocates no memory, but is responsible for the cleanup.
 */
class arena_t {

    ukv_database_t db_ = nullptr;
    ukv_arena_t memory_ = nullptr;

  public:
    arena_t(ukv_database_t db) noexcept : db_(db) {}
    arena_t(arena_t const&) = delete;
    arena_t& operator=(arena_t const&) = delete;

    ~arena_t() {
        ukv_arena_free(db_, memory_);
        memory_ = nullptr;
    }

    inline arena_t(arena_t&& other) noexcept : db_(other.db_), memory_(std::exchange(other.memory_, nullptr)) {}

    inline arena_t& operator=(arena_t&& other) noexcept {
        std::swap(db_, other.db_);
        std::swap(memory_, other.memory_);
        return *this;
    }

    inline ukv_arena_t* member_ptr() noexcept { return &memory_; }
    inline operator ukv_arena_t*() & noexcept { return member_ptr(); }
    inline ukv_database_t db() const noexcept { return db_; }
};

class any_arena_t {

    arena_t owned_;
    arena_t* accessible_ = nullptr;

  public:
    any_arena_t(ukv_database_t db) noexcept : owned_(db), accessible_(nullptr) {}
    any_arena_t(arena_t& accessible) noexcept : owned_(nullptr), accessible_(&accessible) {}

    any_arena_t(any_arena_t&&) = default;
    any_arena_t& operator=(any_arena_t&&) = default;

    any_arena_t(any_arena_t const&) = delete;
    any_arena_t& operator=(any_arena_t const&) = delete;

    inline arena_t& arena() noexcept { return accessible_ ? *accessible_ : owned_; }
    inline ukv_arena_t* member_ptr() noexcept { return arena().member_ptr(); }
    inline operator ukv_arena_t*() & noexcept { return member_ptr(); }
    inline arena_t release_owned() noexcept { return std::exchange(owned_, arena_t {owned_.db()}); }
};

#pragma region Adapters

/**
 * @brief Trivial hash-mixing scheme from Boost.
 * @see https://www.boost.org/doc/libs/1_37_0/doc/html/hash/reference.html#boost.hash_combine
 */
template <typename hashable_at>
inline void hash_combine(std::size_t& seed, hashable_at const& v) {
    std::hash<hashable_at> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

struct sub_key_hash_t {
    inline std::size_t operator()(col_key_t const& sub) const noexcept {
        std::size_t result = SIZE_MAX;
        hash_combine(result, sub.key);
        hash_combine(result, sub.col);
        return result;
    }
};

struct edge_hash_t {
    inline std::size_t operator()(edge_t const& edge) const noexcept {
        std::size_t result = SIZE_MAX;
        hash_combine(result, edge.source_id);
        hash_combine(result, edge.target_id);
        hash_combine(result, edge.id);
        return result;
    }
};

inline ukv_vertex_role_t invert(ukv_vertex_role_t role) {
    switch (role) {
    case ukv_vertex_source_k: return ukv_vertex_target_k;
    case ukv_vertex_target_k: return ukv_vertex_source_k;
    case ukv_vertex_role_any_k: return ukv_vertex_role_unknown_k;
    case ukv_vertex_role_unknown_k: return ukv_vertex_role_any_k;
    }
    __builtin_unreachable();
}

} // namespace unum::ukv

namespace std {
template <>
inline void swap(unum::ukv::value_ref_t& a, unum::ukv::value_ref_t& b) noexcept {
    a.swap(b);
}
} // namespace std