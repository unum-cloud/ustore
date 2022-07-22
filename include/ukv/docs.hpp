/**
 * @file docs.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 * @brief C++ bindings for @see "ukv/docs.h".
 *
 * Most field-level operations are still accessible through normal @c `value_refs_t`.
 * This interface mostly helps with tabular and SoA <-> AoS operations involving:
 * > ukv_docs_gist(...)
 * > ukv_docs_gather_scalars(...)
 * > ukv_docs_gather_strings(...)
 */

#pragma once
#include "ukv/docs.h"
#include "ukv/ukv.hpp"

namespace unum::ukv {

/**
 * @brief Iterates through a predetermined number of NULL-delimited
 * strings joined one after another in continuous memory.
 * Can be used for `ukv_docs_gist` or `ukv_collection_list`.
 */
class strings_tape_iterator_t {
    ukv_size_t remaining_count_ = 0;
    ukv_str_view_t current_ = nullptr;

  public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::string_view;
    using pointer = ukv_str_view_t*;
    using reference = std::string_view;

    strings_tape_iterator_t(ukv_size_t remaining = 0, ukv_str_view_t current = nullptr)
        : remaining_count_(remaining), current_(current) {}

    strings_tape_iterator_t(strings_tape_iterator_t&&) = default;
    strings_tape_iterator_t& operator=(strings_tape_iterator_t&&) = default;

    strings_tape_iterator_t(strings_tape_iterator_t const&) = default;
    strings_tape_iterator_t& operator=(strings_tape_iterator_t const&) = default;

    strings_tape_iterator_t& operator++() noexcept {
        current_ += std::strlen(current_) + 1;
        --remaining_count_;
        return *this;
    }

    strings_tape_iterator_t operator++(int) noexcept {
        return {remaining_count_ - 1, current_ + std::strlen(current_) + 1};
    }

    bool is_end() const noexcept { return !remaining_count_; }
};

template <typename scalar_at>
class field_column_view_gt {
    std::uint8_t* validities_ = nullptr;
    std::uint8_t* conversions_ = nullptr;
    std::uint8_t* collisions_ = nullptr;
    scalar_at* scalars_ = nullptr;
};

template <typename scalar_at>
class doc_row_view_gt {
    std::uint8_t* validities_ = nullptr;
    std::uint8_t* conversions_ = nullptr;
    std::uint8_t* collisions_ = nullptr;
    scalar_at* scalars_ = nullptr;
    std::uint8_t mask_ = 0;
};

class docs_table_t {

    ukv_size_t docs_count;
    ukv_size_t fields_count;

    strided_iterator_gt<ukv_collection_t> cols;
    strided_iterator_gt<ukv_key_t> keys;
    strided_iterator_gt<ukv_str_view_t> fields;
    strided_iterator_gt<ukv_type_t> types;

    template <typename scalar_at>
    field_column_view_gt<scalar_at> column(std::size_t column_idx) {}

    template <typename scalar_at>
    field_column_view_gt<scalar_at> column(std::size_t column_idx) {}
};

} // namespace unum::ukv
