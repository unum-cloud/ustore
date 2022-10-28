/**
 * @file modality_vectors.cpp
 * @author Ashot Vardanian
 *
 * @brief Vectors compatibility layer.
 * Sits on top of any @see "ukv.h"-compatible system.
 * Prefers integer quantized representations for faster compute.
 */

#include "ukv/vectors.h"
#include "helpers/linked_memory.hpp" // `linked_memory_lock_t`
#include "helpers/algorithm.hpp"     // `sort_and_deduplicate`
#include "helpers/vector.hpp"        // `uninitialized_vector_gt`
#include "ukv/cpp/ranges_args.hpp"   // `places_arg_t`

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
    float operator()(std::int8_t const* a, std::int8_t const* b, std::size_t dims) const noexcept {}
};

struct metric_l2_t {
    float operator()(std::int8_t const* a, std::int8_t const* b, std::size_t dims) const noexcept {}
};

struct entry_t {
    collection_key_t collection_key;
    value_view_t value;
};

void quantize_f32(float* original, std::int8_t* quant, std::size_t dims) noexcept {
    for (std::size_t i = 0; i != dims; ++i)
        quant[i] = static_cast<std::int8_t>(original[i]*float_scaling_k);
}

std::size_t size_bytes(ukv_vector_scalar_t scalar_type) noexcept {
    switch (scalar_type) {
        
    }
}

struct vectors_arg_t {
    strided_iterator_gt<ukv_bytes_cptr_t const> contents;
    strided_iterator_gt<ukv_length_t const> offsets;
    ukv_size_t vectors_stride;
    ukv_vector_scalar_t scalar_type;
    ukv_length_t dimension;
    ukv_size_t tasks_count = 1;

    value_view_t operator [] (std::size_t i) const noexcept {
        if (!contents)
        return {};
        ukv_bytes_cptr_t begin = contents[i];
        begin += offsets ? offsets[i] : 0;
        begin += vectors_stride * i;
        return {begin, dimension + i * };
    }
};

void ukv_vectors_write(ukv_vectors_write_t* c_ptr) {

    ukv_vectors_write_t& c = *c_ptr;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    strided_iterator_gt<ukv_collection_t const> collections {c.collections, c.collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c.keys, c.keys_stride};
    places_arg_t places {collections, keys, {}, c.tasks_count};

    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c.values, c.values_stride};
    strided_iterator_gt<ukv_length_t const> offs {c.offsets, c.offsets_stride};
    strided_iterator_gt<ukv_length_t const> lens {c.lengths, c.lengths_stride};
    bits_view_t presences {c.presences};
    contents_arg_t contents {presences, offs, lens, vals, c.tasks_count};

    // For each input key we must get its
    auto quantized_entries = arena.alloc<entry_t>(c.tasks_count * 2, c.error);
    return_on_error(c.error);

    auto quantized_vecs = arena.alloc<std::uint8_t>(c.tasks_count * c.dimension, c.error);
    return_on_error(c.error);

    // Add the orginal entries
    for (std::size_t task_idx = 0; task_idx != c.tasks_count; ++task_idx) {
        entry_t& entry = quantized_entries[task_idx];
        entry.collection_key.collection = places[task_idx].collection;
        entry.collection_key.key = places[task_idx].key;
        entry.value = contents[task_idx];
    }

    // Add the mirror tasks for quantized copies
    for (std::size_t task_idx = 0; task_idx != c.tasks_count; ++task_idx) {
        auto quantized_begin = quantized_vecs.begin() + task_idx * c.dimension;
        entry_t& entry = quantized_entries[task_idx];
        entry.collection_key.collection = places[task_idx].collection;
        entry.collection_key.key = -places[task_idx].key;
        entry.value = value_view_t {quantized_begin, c.dimension};
    }

    // Quantize the original vectors into new buckets
    for (std::size_t task_idx = 0; task_idx != c.tasks_count; ++task_idx) {
        auto original_begin = 
        auto quantized_begin = quantized_vecs.begin() + task_idx * c.dimension;
    }

    entry_t& first = quantized_entries.front();
    ukv_write_t write {
        .db = c.db,
        .error = c.error,
        .transaction = c.transaction,
        .arena = c.arena,
        .options = c.options,
        .tasks_count = c.tasks_count * 2,
        .collections = first.collection_key.collection,
        .collections_stride = sizeof(entry_t),
        .keys = first.collection_key.key,
        .collections_stride = sizeof(entry_t),
        .lengths = first.value.member_length(),
        .collections_stride = sizeof(entry_t),
        .values = first.value.member_ptr(),
        .collections_stride = sizeof(entry_t),
    };
    ukv_write(&write);
}

void ukv_vectors_read(ukv_vectors_read_t* c_ptr) {

    ukv_vectors_read_t& c = *c_ptr;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    contents_arg_t keys_str_args;
    keys_str_args.offsets_begin = {c.vectors_offsets, c.vectors_offsets_stride};
    keys_str_args.lengths_begin = {c.vectors_lengths, c.vectors_lengths_stride};
    keys_str_args.contents_begin = {(ukv_bytes_cptr_t const*)c.vectors, c.vectors_stride};
    keys_str_args.count = c.tasks_count;

    // Getting hash-collisions is such a rare case, that we will not
    // optimize for it in the current implementation. Sorting and
    // deduplicating the IDs will cost more overall, than a repeated
    // read every once in a while.
    auto buckets_keys = arena.alloc<ukv_key_t>(c.tasks_count, c.error);
    return_on_error(c.error);

    // Parse and hash input string buckets_keys
    hash_t hash;
    for (std::size_t i = 0; i != c.tasks_count; ++i)
        buckets_keys[i] = hash(keys_str_args[i]);

    // Read from disk
    // We don't need:
    // > presences: zero length buckets are impossible here.
    // > lengths: value lengths are always smaller than buckets.
    // We can infer those and export differently.
    ukv_length_t* buckets_offsets = nullptr;
    ukv_byte_t* buckets_values = nullptr;
    ukv_read_t read {
        .db = c.db,
        .error = c.error,
        .transaction = c.transaction,
        .arena = arena,
        .options = c.options,
        .tasks_count = c.tasks_count,
        .collections = c.collections,
        .collections_stride = c.collections_stride,
        .keys = buckets_keys.begin(),
        .keys_stride = sizeof(ukv_key_t),
        .offsets = &buckets_offsets,
        .values = &buckets_values,
    };

    ukv_read(&read);
    return_on_error(c.error);

    // Some of the entries will contain more then one key-value pair in case of collisions.
    ukv_length_t exported_volume = 0;
    joined_blobs_t buckets {c.tasks_count, buckets_offsets, buckets_values};
    auto presences =
        arena.alloc_or_dummy(divide_round_up<std::size_t>(c.tasks_count, bits_in_byte_k), c.error, c.presences);
    auto lengths = arena.alloc_or_dummy(c.tasks_count, c.error, c.lengths);
    auto offsets = arena.alloc_or_dummy(c.tasks_count, c.error, c.offsets);

    for (std::size_t i = 0; i != c.tasks_count; ++i) {
        std::string_view key_str = keys_str_args[i];
        value_view_t bucket = buckets[i];

        // Now that we have found our match - clamp everything else.
        value_view_t val = find_in_bucket(bucket, key_str).value;
        if (val) {
            presences[i] = true;
            offsets[i] = exported_volume;
            lengths[i] = static_cast<ukv_length_t>(val.size());
            if (c.values)
                std::memmove(buckets_values + exported_volume, val.data(), val.size());
            buckets_values[exported_volume + val.size()] = ukv_byte_t {0};
            exported_volume += static_cast<ukv_length_t>(val.size()) + 1;
        }
        else {
            presences[i] = false;
            offsets[i] = exported_volume;
            lengths[i] = ukv_length_missing_k;
        }
    }

    offsets[c.tasks_count] = exported_volume;
    if (c.values)
        *c.values = buckets_values;
}

/**
 * - Same collection
 * - One scan request
 * - May have previous results
 */
template <typename predicate_at>
void scan_predicate( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_transaction,
    ukv_collection_t c_collection,
    std::string_view previous_path,
    ukv_length_t c_count_limit,
    ukv_options_t const c_options,
    ukv_length_t& count,
    growing_tape_t& vectors,
    linked_memory_lock_t& arena,
    ukv_error_t* c_error,
    predicate_at predicate) {

    hash_t hash;
    ukv_length_t found_vectors = 0;
    bool has_reached_previous = previous_path.empty();
    ukv_key_t start_key = !previous_path.empty() ? hash(previous_path) : std::numeric_limits<ukv_key_t>::min();
    while (found_vectors < c_count_limit && !*c_error) {
        ukv_length_t const scan_length = std::max<ukv_length_t>(c_count_limit, 2u);
        ukv_length_t* found_buckets_count = nullptr;
        ukv_key_t* found_buckets_keys = nullptr;
        ukv_scan_t scan {
            .db = c_db,
            .error = c_error,
            .transaction = c_transaction,
            .arena = arena,
            .options = c_options,
            .collections = &c_collection,
            .start_keys = &start_key,
            .count_limits = &scan_length,
            .counts = &found_buckets_count,
            .keys = &found_buckets_keys,
        };

        ukv_scan(&scan);
        if (*c_error)
            break;

        if (found_buckets_count[0] <= 1)
            // We have reached the end of c_collection
            break;

        ukv_length_t* found_buckets_offsets = nullptr;
        ukv_byte_t* found_buckets_data = nullptr;
        ukv_read_t read {
            .db = c_db,
            .error = c_error,
            .transaction = c_transaction,
            .arena = arena,
            .options = ukv_options_t(c_options | ukv_option_dont_discard_memory_k),
            .tasks_count = found_buckets_count[0],
            .collections = &c_collection,
            .collections_stride = 0,
            .keys = found_buckets_keys,
            .keys_stride = sizeof(ukv_key_t),
            .offsets = &found_buckets_offsets,
            .values = &found_buckets_data,
        };
        ukv_read(&read);
        if (*c_error)
            break;

        joined_blobs_iterator_t found_buckets {found_buckets_offsets, found_buckets_data};
        for (std::size_t i = 0; i != found_buckets_count[0]; ++i, ++found_buckets) {
            value_view_t bucket = *found_buckets;
            for_each_in_bucket(bucket, [&](bucket_member_t const& member) {
                if (!predicate(member.key))
                    // Skip irrelevant entries
                    return;
                if (member.key == previous_path) {
                    // We may have reached the boundary between old results and new ones
                    has_reached_previous = true;
                    return;
                }
                if (!has_reached_previous)
                    // Skip the results we have already seen
                    return;
                if (found_vectors >= c_count_limit)
                    // We have more than we need
                    return;

                // All the matches in this section should be exported
                vectors.push_back(member.key, c_error);
                return_on_error(c_error);
                vectors.add_terminator(byte_t {0}, c_error);
                return_on_error(c_error);
                ++found_vectors;
            });
        }

        auto count_buckets = found_buckets_count[0];
        start_key = found_buckets_keys[count_buckets - 1] + 1;
    }

    count = found_vectors;
}

void scan_prefix( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_transaction,
    ukv_collection_t c_collection,
    std::string_view prefix,
    std::string_view previous_path,
    ukv_length_t c_count_limit,
    ukv_options_t const c_options,
    ukv_length_t& count,
    growing_tape_t& vectors,
    linked_memory_lock_t& arena,
    ukv_error_t* c_error) {

    scan_predicate( //
        c_db,
        c_transaction,
        c_collection,
        previous_path,
        c_count_limit,
        c_options,
        count,
        vectors,
        arena,
        c_error,
        [=](std::string_view body) { return starts_with(body, prefix); });
}

struct pcre2_ctx_t {
    linked_memory_lock_t& arena;
    ukv_error_t* c_error;
};

static void* pcre2_malloc(PCRE2_SIZE length, void* ctx_ptr) noexcept {
    pcre2_ctx_t& ctx = *reinterpret_cast<pcre2_ctx_t*>(ctx_ptr);
    return ctx.arena.alloc<byte_t>(static_cast<std::size_t>(length), ctx.c_error).begin();
}

static void pcre2_free(void*, void*) noexcept {
    // Our arenas only grow, we don't dealloc!
}

void scan_regex( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_transaction,
    ukv_collection_t c_collection,
    std::string_view pattern,
    std::string_view previous_path,
    ukv_length_t c_count_limit,
    ukv_options_t const c_options,
    ukv_length_t& count,
    growing_tape_t& vectors,
    linked_memory_lock_t& arena,
    ukv_error_t* c_error) {

    pcre2_ctx_t ctx {arena, c_error};

    // https://www.pcre.org/current/doc/html/pcre2_compile.html
    pcre2_general_context* pcre2_context = pcre2_general_context_create(&pcre2_malloc, &pcre2_free, &ctx);
    pcre2_compile_context* pcre2_compile_context = pcre2_compile_context_create(pcre2_context);
    int pcre2_pattern_error_code = 0;
    PCRE2_SIZE pcre2_pattern_error_offset = 0;
    pcre2_code* pcre2_code = pcre2_compile( //
        PCRE2_SPTR8(pattern.data()),
        PCRE2_SIZE(pattern.size()),
        PCRE2_MATCH_INVALID_UTF,
        &pcre2_pattern_error_code,
        &pcre2_pattern_error_offset,
        pcre2_compile_context);

    // https://www.pcre.org/current/doc/html/pcre2_jit_compile.html
    auto jit_status = pcre2_jit_compile(pcre2_code, PCRE2_JIT_COMPLETE);
    if (jit_status != 0)
        *c_error = "Failed to JIT-compile the RegEx query";

    pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(pcre2_code, pcre2_context);
    if (!match_data)
        *c_error = "Failed to allocate memory for RegEx pattern matches";

    if (!*c_error)
        scan_predicate( //
            c_db,
            c_transaction,
            c_collection,
            previous_path,
            c_count_limit,
            c_options,
            count,
            vectors,
            arena,
            c_error,
            [=](std::string_view body) {
                // https://www.pcre.org/current/doc/html/pcre2_jit_match.html
                // pcre2_match_data match_data;
                // pcre2_match_context match_context;
                auto found_matches = pcre2_jit_match( //
                    pcre2_code,
                    PCRE2_SPTR(body.data()),
                    PCRE2_SIZE(body.size()),
                    PCRE2_SIZE(0), // start offset
                    PCRE2_NO_UTF_CHECK,
                    match_data,
                    NULL);
                return found_matches > 0;
            });

    pcre2_match_data_free(match_data);
    pcre2_code_free(pcre2_code);
    pcre2_compile_context_free(pcre2_compile_context);
    pcre2_general_context_free(pcre2_context);
}

void ukv_vectors_match(ukv_vectors_match_t* c_ptr) {

    ukv_vectors_match_t const& c = *c_ptr;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    return_on_error(c.error);

    contents_arg_t patterns_args;
    patterns_args.offsets_begin = {c.patterns_offsets, c.patterns_offsets_stride};
    patterns_args.lengths_begin = {c.patterns_lengths, c.patterns_lengths_stride};
    patterns_args.contents_begin = {(ukv_bytes_cptr_t const*)c.patterns, c.patterns_stride};
    patterns_args.count = c.tasks_count;

    contents_arg_t previous_args;
    previous_args.offsets_begin = {c.previous_offsets, c.previous_offsets_stride};
    previous_args.lengths_begin = {c.previous_lengths, c.previous_lengths_stride};
    previous_args.contents_begin = {(ukv_bytes_cptr_t const*)c.previous, c.previous_stride};
    previous_args.count = c.tasks_count;

    strided_range_gt<ukv_collection_t const> collections {{c.collections, c.collections_stride}, c.tasks_count};
    strided_range_gt<ukv_length_t const> count_limits {{c.match_counts_limits, c.match_counts_limits_stride},
                                                       c.tasks_count};

    auto count_limits_sum = transform_reduce_n(count_limits.begin(), c.tasks_count, 0ul);
    auto found_counts = arena.alloc<ukv_length_t>(c.tasks_count, c.error);
    auto found_vectors = growing_tape_t(arena);
    found_vectors.reserve(count_limits_sum, c.error);
    return_on_error(c.error);

    for (std::size_t i = 0; i != c.tasks_count && !*c.error; ++i) {
        auto col = collections ? collections[i] : ukv_collection_main_k;
        auto pattern = patterns_args[i];
        auto previous = previous_args[i];
        auto limit = count_limits[i];
        auto func = is_prefix(pattern) ? &scan_prefix : &scan_regex;
        func(c.db,
             c.transaction,
             col,
             pattern,
             previous,
             limit,
             c.options,
             found_counts[i],
             found_vectors,
             arena,
             c.error);
    }

    // Export the results
    if (c.match_counts)
        *c.match_counts = found_counts.begin();
    if (c.vectors_offsets)
        *c.vectors_offsets = found_vectors.offsets().begin().get();
    if (c.vectors_strings)
        *c.vectors_strings = (ukv_char_t*)found_vectors.contents().begin().get();
}