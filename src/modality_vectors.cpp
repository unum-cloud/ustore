/**
 * @file modality_vectors.cpp
 * @author Ashot Vardanian
 *
 * @brief Vectors compatibility layer.
 * Sits on top of any @see "ukv.h"-compatible system.
 * Prefers integer quantized representations for faster compute.
 */

#include "ukv/vectors.h"
#include "ukv/cpp/ranges_args.hpp" // `places_arg_t`

#include "helpers/linked_memory.hpp"          // `linked_memory_lock_t`
#include "helpers/algorithm.hpp"              // `sort_and_deduplicate`
#include "helpers/vector.hpp"                 // `uninitialized_vector_gt`
#include "helpers/full_scan.hpp"              // `full_scan_collection`
#include "helpers/limited_priority_queue.hpp" // `limited_priority_queue_gt`

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

static constexpr std::int8_t float_scaling_k = 100;

struct metric_dot_t {
    float operator()(std::int8_t const* a, std::int8_t const* b, std::size_t dims) const noexcept {
        constexpr std::int64_t component_normalizer_k = float_scaling_k * float_scaling_k;
        std::int64_t sum = 0;
        for (std::size_t i = 0; i != dims; ++i)
            sum += std::int16_t(a[i]) * std::int16_t(b[i]);
        return float(sum) / component_normalizer_k;
    }
};

struct metric_cos_t {
    float operator()(std::int8_t const* a, std::int8_t const* b, std::size_t dims) const noexcept { return 0; }
};

struct metric_l2_t {
    float operator()(std::int8_t const* a, std::int8_t const* b, std::size_t dims) const noexcept { return 0; }
};

struct entry_t {
    collection_key_t collection_key;
    value_view_t value;
};

template <typename float_at = float>
void quantize(float_at const* original, std::int8_t* quant, std::size_t dims) noexcept {
    for (std::size_t i = 0; i != dims; ++i)
        quant[i] = static_cast<std::int8_t>(original[i] * float_scaling_k);
}

ukv_length_t size_bytes(ukv_vector_scalar_t scalar_type) noexcept {
    switch (scalar_type) {
    case ukv_vector_scalar_f32_k: return sizeof(float);
    case ukv_vector_scalar_f64_k: return sizeof(double);
    case ukv_vector_scalar_f16_k: return sizeof(std::int16_t);
    case ukv_vector_scalar_i8_k: return sizeof(std::int8_t);
    }
}

struct vectors_arg_t {
    strided_iterator_gt<ukv_bytes_cptr_t const> contents;
    strided_iterator_gt<ukv_length_t const> offsets;
    ukv_size_t vectors_stride;
    ukv_vector_scalar_t scalar_type;
    ukv_length_t dimensions;
    ukv_size_t tasks_count = 1;

    value_view_t operator[](std::size_t i) const noexcept {
        if (!contents)
            return {};
        ukv_bytes_cptr_t begin = contents[i];
        begin += offsets ? offsets[i] : 0u;
        begin += vectors_stride * i;
        return {begin, dimensions * size_bytes(scalar_type)};
    }
};

void ukv_vectors_write(ukv_vectors_write_t* c_ptr) {

    ukv_vectors_write_t& c = *c_ptr;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    places_arg_t places_args {collections, keys, {}, c.tasks_count};

    strided_iterator_gt<ukv_bytes_cptr_t const> starts {c.vectors_starts, c.vectors_starts_stride};
    strided_iterator_gt<ukv_length_t const> offs {c.offsets, c.offsets_stride};
    vectors_arg_t vectors_args {starts, offs, c.vectors_stride, c.scalar_type, c.dimensions, c.tasks_count};

    // For each input key we must get its
    auto quantized_entries = arena.alloc<entry_t>(c.tasks_count * 2u, c.error);
    return_on_error(c.error);

    auto quantized_vectors = arena.alloc<std::int8_t>(c.tasks_count * c.dimensions, c.error);
    return_on_error(c.error);

    // Add the original entries
    for (std::size_t task_idx = 0; task_idx != c.tasks_count; ++task_idx) {
        entry_t& entry = quantized_entries[task_idx];
        entry.collection_key.collection = places_args[task_idx].collection;
        entry.collection_key.key = places_args[task_idx].key;
        entry.value = vectors_args[task_idx];
    }

    // Add the mirror tasks for quantized copies
    for (std::size_t task_idx = 0; task_idx != c.tasks_count; ++task_idx) {
        auto quantized_begin = quantized_vectors.begin() + task_idx * c.dimensions;
        entry_t& entry = quantized_entries[task_idx];
        entry.collection_key.collection = places_args[task_idx].collection;
        entry.collection_key.key = -places_args[task_idx].key;
        entry.value = value_view_t {(ukv_bytes_cptr_t)quantized_begin, c.dimensions};
    }

    // Quantize the original vectors into new buckets
    for (std::size_t task_idx = 0; task_idx != c.tasks_count; ++task_idx) {
        auto original_begin = vectors_args[task_idx].begin();
        auto quantized_begin = quantized_vectors.begin() + task_idx * c.dimensions;

        switch (c.scalar_type) {
        case ukv_vector_scalar_f32_k: quantize((float const*)original_begin, quantized_begin, c.dimensions);
        case ukv_vector_scalar_f64_k: quantize((double const*)original_begin, quantized_begin, c.dimensions);
        case ukv_vector_scalar_f16_k: quantize((std::int16_t const*)original_begin, quantized_begin, c.dimensions);
        case ukv_vector_scalar_i8_k: quantize((std::int8_t const*)original_begin, quantized_begin, c.dimensions);
        }
    }

    // Submit both original and quantized entries
    entry_t& first = quantized_entries[0];
    ukv_write_t write;
    write.db = c.db;
    write.error = c.error;
    write.transaction = c.transaction;
    write.arena = c.arena;
    write.options = c.options;
    write.tasks_count = c.tasks_count * 2u;
    write.collections = &first.collection_key.collection;
    write.collections_stride = sizeof(entry_t);
    write.keys = &first.collection_key.key;
    write.keys_stride = sizeof(entry_t);
    write.lengths = first.value.member_length();
    write.lengths_stride = sizeof(entry_t);
    write.values = first.value.member_ptr();
    write.values_stride = sizeof(entry_t);
    ukv_write(&write);
}

void ukv_vectors_read(ukv_vectors_read_t* c_ptr) {

    ukv_vectors_read_t& c = *c_ptr;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    places_arg_t places_args {collections, keys, {}, c.tasks_count};

    auto vector_size = c.dimensions * size_bytes(c.scalar_type);

    // Read from disk, but potentially re-layout the data response.
    ukv_read_t read;
    read.db = c.db;
    read.error = c.error;
    read.transaction = c.transaction;
    read.arena = arena;
    read.options = c.options;
    read.tasks_count = c.tasks_count;
    read.collections = c.collections;
    read.collections_stride = c.collections_stride;
    read.keys = keys.get();
    read.keys_stride = keys.stride();
    read.offsets = c.offsets;
    read.presences = c.presences;
    read.values = c.vectors;
    ukv_read(&read);
    return_on_error(c.error);

    // From here on, if we have the offsets don't form identical-length chunks,
    // we must compact the range:
}

struct match_t {
    ukv_key_t key;
    ukv_float_t metric;
};

struct lower_similarity_t {
    bool operator()(match_t const& a, match_t const& b) const noexcept { return a.metric < b.metric; }
};

void ukv_vectors_search(ukv_vectors_search_t* c_ptr) {

    ukv_vectors_search_t const& c = *c_ptr;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    strided_iterator_gt<ukv_bytes_cptr_t const> starts {c.queries_starts, c.queries_starts_stride};
    strided_iterator_gt<ukv_length_t const> offs {c.queries_offsets, c.queries_offsets_stride};
    vectors_arg_t queries_args {starts, offs, c.queries_stride, c.scalar_type, c.dimensions, c.tasks_count};

    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_range_gt<ukv_length_t const> count_limits {{c.match_counts_limits, c.match_counts_limits_stride},
                                                       c.tasks_count};

    auto count_limits_max = ukv_length_t {0};
    auto count_limits_sum = transform_reduce_n(count_limits.begin(), c.tasks_count, 0ul, [&](ukv_length_t l) {
        count_limits_max = std::max(count_limits_max, l);
        return l;
    });

    auto found_counts = arena.alloc<ukv_length_t>(c.tasks_count, c.error);
    return_on_error(c.error);
    auto found_offsets = arena.alloc<ukv_length_t>(c.tasks_count, c.error);
    return_on_error(c.error);
    auto found_keys = arena.alloc<ukv_key_t>(count_limits_sum, c.error);
    return_on_error(c.error);
    auto found_metrics = arena.alloc<ukv_float_t>(count_limits_sum, c.error);
    return_on_error(c.error);
    auto temp_matches = arena.alloc<match_t>(count_limits_max, c.error);
    return_on_error(c.error);

    ukv_length_t total_exported_matches = 0;
    for (std::size_t i = 0; i != c.tasks_count && !*c.error; ++i) {
        auto col = collections ? collections[i] : ukv_collection_main_k;
        auto query = queries_args[i];
        auto limit = count_limits[i];

        using pq_t = limited_priority_queue_gt<match_t, lower_similarity_t>;
        pq_t pq {temp_matches.begin(), temp_matches.begin() + limit};
        auto callback = [&](ukv_key_t key, value_view_t vector) noexcept {
            metric_dot_t metric;
            match_t match;
            match.key = key;
            match.metric = metric((std::int8_t*)query.data(), (std::int8_t*)vector.data(), c.dimensions);
            if (match.metric < c.metric_threshold)
                return true;

            pq.push(match);
            return true;
        };

        auto min_key = std::numeric_limits<ukv_key_t>::min();
        full_scan_collection(c.db, c.transaction, col, c.options, min_key, limit, arena, c.error, callback);
        auto count = pq.size();

        found_counts[i] = count;
        found_offsets[i] = total_exported_matches;

        for (std::size_t j = 0; j != count; ++j)
            found_keys[total_exported_matches + j] = temp_matches[j].key, //
                found_metrics[total_exported_matches + j] = temp_matches[j].metric;

        total_exported_matches += count;
        pq.clear();
    }
}