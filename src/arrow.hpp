/**
 * @file arrow.hpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions for Apache Arrow interoperability.
 */
#pragma once
#include "helpers.hpp"

namespace unum::ukv {

/**
 * We have a different methodology of marking NULL entries, than Arrow.
 * We can reuse the `column_lengths` to put-in some NULL markers.
 * Bitmask would use 32x less memory.
 */
inline ukv_1x8_t* convert_lengths_into_bitmap(ukv_val_len_t* lengths, ukv_size_t n) {
    size_t count_slots = (n + (CHAR_BIT - 1)) / CHAR_BIT;
    ukv_1x8_t* slots = (ukv_1x8_t*)lengths;
    for (size_t slot_idx = 0; slot_idx != count_slots; ++slot_idx) {
        ukv_1x8_t slot_value = 0;
        size_t first_idx = slot_idx * CHAR_BIT;
        size_t remaining_count = count_slots - first_idx;
        size_t remaining_in_slot = remaining_count > CHAR_BIT ? CHAR_BIT : remaining_count;
        for (size_t bit_idx = 0; bit_idx != remaining_in_slot; ++bit_idx) {
            slot_value |= 1 << bit_idx;
        }
        slots[slot_idx] = slot_value;
    }
    // Cleanup the following memory
    std::memset(slots + count_slots + 1, 0, n * sizeof(ukv_val_len_t) - count_slots);
    return slots;
}

/**
 * @brief Replaces "lengths" with `ukv_val_len_missing_k` is matching NULL indicator is set.
 */
inline ukv_val_len_t* normalize_lengths_with_bitmap(ukv_1x8_t const* slots, ukv_val_len_t* lengths, ukv_size_t n) {
    size_t count_slots = (n + (CHAR_BIT - 1)) / CHAR_BIT;
    for (size_t slot_idx = 0; slot_idx != count_slots; ++slot_idx) {
        size_t first_idx = slot_idx * CHAR_BIT;
        size_t remaining_count = count_slots - first_idx;
        size_t remaining_in_slot = remaining_count > CHAR_BIT ? CHAR_BIT : remaining_count;
        for (size_t bit_idx = 0; bit_idx != remaining_in_slot; ++bit_idx) {
            if (slots[slot_idx] & (1 << bit_idx))
                lengths[first_idx + bit_idx] = ukv_val_len_missing_k;
        }
    }
    return lengths;
}

} // namespace unum::ukv
