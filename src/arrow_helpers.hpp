/**
 * @file arrow_helpers.hpp
 * @author Ashot Vardanian
 *
 * @brief Helper functions for Apache Arrow interoperability.
 */
#pragma once
#include <string_view>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#include <arrow/type.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/memory_pool.h>
#include <arrow/c/bridge.h>
#pragma GCC diagnostic pop

#include "helpers.hpp" // `stl_arena_t`

namespace unum::ukv {

namespace arf = arrow::flight;
namespace ar = arrow;

class arrow_mem_pool_t final : public ar::MemoryPool {
    monotonic_resource_t resource_;

  public:
    arrow_mem_pool_t(stl_arena_t& arena) : resource_(&arena.resource) {}
    ~arrow_mem_pool_t() {}

    ar::Status Allocate(int64_t size, uint8_t** ptr) override {
        auto new_ptr = resource_.allocate(size);
        if (!new_ptr)
            return ar::Status::OutOfMemory("");

        *ptr = reinterpret_cast<uint8_t*>(new_ptr);
        return ar::Status::OK();
    }
    ar::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
        auto new_ptr = resource_.allocate(new_size);
        if (!new_ptr)
            return ar::Status::OutOfMemory("");

        std::memcpy(new_ptr, *ptr, old_size);
        resource_.deallocate(*ptr, old_size);
        *ptr = reinterpret_cast<uint8_t*>(new_ptr);
        return ar::Status::OK();
    }
    void Free(uint8_t* buffer, int64_t size) override { resource_.deallocate(buffer, size); }
    void ReleaseUnused() override {}
    int64_t bytes_allocated() const override { return static_cast<int64_t>(resource_.used()); }
    int64_t max_memory() const override { return static_cast<int64_t>(resource_.capacity()); }
    std::string backend_name() const { return "ukv"; }
};

ar::ipc::IpcReadOptions arrow_read_options(arrow_mem_pool_t& pool) {
    ar::ipc::IpcReadOptions options;
    options.memory_pool = &pool;
    options.use_threads = false;
    options.max_recursion_depth = 1;
    return options;
}

ar::ipc::IpcWriteOptions arrow_write_options(arrow_mem_pool_t& pool) {
    ar::ipc::IpcWriteOptions options;
    options.memory_pool = &pool;
    options.use_threads = false;
    options.max_recursion_depth = 1;
    return options;
}

ar::Status unpack_table( //
    ar::Result<std::shared_ptr<ar::Table>> const& maybe_table,
    ArrowSchema& schema_c,
    ArrowArray& batch_c) {

    if (!maybe_table.ok())
        return maybe_table.status();

    std::shared_ptr<ar::Table> const& table = maybe_table.ValueUnsafe();
    ar::Status ar_status = ar::ExportSchema(*table->schema(), &schema_c);
    if (!ar_status.ok())
        return ar_status;

    // Join all the chunks to form a single table
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = table->CombineChunksToBatch();
    if (!maybe_batch.ok())
        return maybe_batch.status();

    std::shared_ptr<ar::RecordBatch> const& batch_ptr = maybe_batch.ValueUnsafe();
    ar_status = ar::ExportRecordBatch(*batch_ptr, &batch_c, nullptr);
    return ar_status;
}

inline expected_gt<std::size_t> column_idx(ArrowSchema* schema_c, std::string_view name) {
    auto begin = schema_c->children;
    auto end = begin + schema_c->n_children;
    auto it = std::find_if(begin, end, [=](ArrowSchema* column_schema) {
        return std::string_view {column_schema->name} == name;
    });
    if (it == end)
        return status_t {"Column not found!"};
    return static_cast<std::size_t>(it - begin);
}

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
 * @brief Replaces "lengths" with `ukv_val_len_missing_k` if matching NULL indicator is set.
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
