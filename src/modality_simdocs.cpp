/**
 * @file modality_simdocs.cpp
 * @author Ashot Vardanian
 *
 * @brief Document storage using "nlohmann/JSON" lib.
 * Sits on top of any @see "ukv.h"-compatible system.
 */

#include <vector>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <charconv> // `std::to_chars`
#include <cstdio>   // `std::snprintf`

#include <yyjson.h>
#include <fmt/core.h>

#include "helpers.hpp"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

struct json_t {
    yyjson_doc* handle = nullptr;
    yyjson_mut_doc* mut_handle = nullptr;

    ~json_t() {
        if (mut_handle)
            yyjson_mut_doc_free(mut_handle);
        if (handle)
            yyjson_doc_free(handle);
    }

    json_t() = default;
    json_t(json_t const&) = delete;
    json_t& operator=(json_t const&) = delete;

    json_t(json_t&& other) noexcept
        : handle(std::exchange(other.handle, nullptr)), mut_handle(std::exchange(other.mut_handle, nullptr)) {}
    json_t& operator=(json_t&& other) noexcept {
        std::swap(handle, other.handle);
        std::swap(mut_handle, other.mut_handle);
        return *this;
    }
};

constexpr ukv_format_t internal_format_k = ukv_format_json_k;
ukv_format_t ukv_format_docs_internal_k = internal_format_k;

static constexpr char const* true_k = "true";
static constexpr char const* false_k = "false";

/// The length of buffer to be used to convert/format/print numerical values into strings.
constexpr std::size_t printed_number_length_limit_k = 32;
constexpr std::size_t field_path_len_limit_k = 512;

using field_path_buffer_t = char[field_path_len_limit_k];

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

static void* callback_yy_malloc(void* ctx, size_t size) {
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(ctx);
    return arena.alloc<byte_t>(size, nullptr).begin();
}

static void* callback_yy_realloc(void* ctx, void* ptr, size_t size) {
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(ctx);
    return arena.alloc<byte_t>(size, nullptr).begin();
}

static void callback_yy_free(void*, void*) {
}

yyjson_alc wrap_allocator(stl_arena_t& arena) {
    yyjson_alc allocator;
    allocator.malloc = callback_yy_malloc;
    allocator.realloc = callback_yy_realloc;
    allocator.free = callback_yy_free;
    allocator.ctx = &arena;
    return allocator;
}

json_t parse_any( //
    value_view_t bytes,
    ukv_format_t const,
    stl_arena_t& arena,
    ukv_error_t* c_error) noexcept {

    json_t result;
    yyjson_alc allocator = wrap_allocator(arena);
    yyjson_read_flag flg = YYJSON_READ_ALLOW_COMMENTS | YYJSON_READ_ALLOW_INF_AND_NAN;
    result.handle = yyjson_read_opts((char*)bytes.data(), (size_t)bytes.size(), flg, &allocator, NULL);
    if (!result.handle)
        *c_error = "Failed to parse document!";
    return result;
}

yyjson_val* lookup_field(yyjson_val* json, ukv_str_view_t field) noexcept {
    return !field ? json : field[0] == '/' ? yyjson_get_pointer(json, field) : yyjson_obj_get(json, field);
}

void dump_any( //
    yyjson_val* json,
    ukv_format_t const,
    stl_arena_t& arena,
    growing_tape_t& output,
    ukv_error_t* c_error) noexcept {

    size_t result_length = 0;
    yyjson_write_flag flg = 0;
    yyjson_alc allocator = wrap_allocator(arena);
    char* result_begin = yyjson_val_write_opts(json, flg, &allocator, &result_length, NULL);
    if (!result_begin)
        *c_error = "Failed to serialize the document!";

    auto result = value_view_t {reinterpret_cast<byte_t const*>(result_begin), result_length};
    output.push_back(result, c_error);
}

void dump_any( //
    yyjson_mut_val* json,
    ukv_format_t const,
    stl_arena_t& arena,
    growing_tape_t& output,
    ukv_error_t* c_error) noexcept {

    size_t result_length = 0;
    yyjson_write_flag flg = 0;
    yyjson_alc allocator = wrap_allocator(arena);
    char* result_begin = yyjson_mut_val_write_opts(json, flg, &allocator, &result_length, NULL);
    if (!result_begin)
        *c_error = "Failed to serialize the modified document!";

    auto result = value_view_t {reinterpret_cast<byte_t const*>(result_begin), result_length};
    output.push_back(result, c_error);
}

template <typename at>
std::from_chars_result from_chars(char const* begin, char const* end, at& result) {
    if constexpr (std::is_same_v<at, float>) {
        char* end = nullptr;
        result = std::strtof(begin, &end);
        return {end, begin == end ? std::errc::invalid_argument : std::errc()};
    }
    else if constexpr (std::is_same_v<at, double>) {
        char* end = nullptr;
        result = std::strtod(begin, &end);
        return {end, begin == end ? std::errc::invalid_argument : std::errc()};
    }
    else if constexpr (std::is_same_v<at, bool>) {
        bool is_true = end - begin == 4 && std::equal(begin, end, true_k);
        bool is_false = end - begin == 5 && std::equal(begin, end, false_k);
        if (is_true | is_false) {
            result = is_true;
            return {end, std::errc()};
        }
        else
            return {end, std::errc::invalid_argument};
    }
    else
        return std::from_chars(begin, end, result);
}

template <typename at>
std::to_chars_result to_chars(char* begin, char* end, at scalar) {
    if constexpr (std::is_floating_point_v<at>) {
        // Parsing and dumping floating-point numbers is still not fully implemented in STL:
        //  std::to_chars_result result = std::to_chars(&print_buf[0], print_buf + printed_number_length_limit_k,
        //  scalar); bool fits_terminator = result.ec != std::errc() && result.ptr < print_buf +
        //  printed_number_length_limit_k;
        // Using FMT would cause an extra dependency:
        //  auto end_ptr = fmt::format_to(print_buf, "{}", scalar);
        //  bool fits_terminator = end_ptr < print_buf + printed_number_length_limit_k;
        // If we use `std::snprintf`, the result would be NULL-terminated:
        auto result = std::snprintf(begin, end - begin, "%f", scalar);
        return result >= 0 ? std::to_chars_result {begin + result - 1, std::errc()}
                           : std::to_chars_result {begin, std::errc::invalid_argument};
    }
    else
        return std::to_chars(begin, end, scalar);
}

template <typename callback_at>
void read_unique_docs( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    ukv_options_t const c_options,
    stl_arena_t& arena,
    places_arg_t& unique_places,
    safe_vector_gt<json_t>&,
    ukv_error_t* c_error,
    callback_at callback) noexcept {

    ukv_arena_t arena_ptr = &arena;
    ukv_byte_t* found_binary_begin = nullptr;
    ukv_length_t* found_binary_offs = nullptr;
    ukv_read( //
        c_db,
        c_txn,
        places.count,
        places.collections_begin.get(),
        places.collections_begin.stride(),
        places.keys_begin.get(),
        places.keys_begin.stride(),
        c_options,
        nullptr,
        &found_binary_offs,
        nullptr,
        &found_binary_begin,
        &arena_ptr,
        c_error);

    auto found_binaries = joined_bins_t(places.count, found_binary_offs, found_binary_begin);
    auto found_binary_it = found_binaries.begin();

    for (std::size_t task_idx = 0; task_idx != places.size(); ++task_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t parsed = parse_any(binary_doc, internal_format_k, arena, c_error);

        // This error is extremely unlikely, as we have previously accepted the data into the store.
        return_on_error(c_error);

        ukv_str_view_t field = places.fields_begin ? places.fields_begin[task_idx] : nullptr;
        callback(task_idx, field, parsed);
    }

    unique_places = places;
}

/**
 * ! Returned object may not contain any fields, if multiple fields are requested from the same doc.
 */
template <typename callback_at>
void read_docs( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    ukv_options_t const c_options,
    stl_arena_t& arena,
    places_arg_t& unique_places,
    safe_vector_gt<json_t>& unique_docs,
    ukv_error_t* c_error,
    callback_at callback) {

    // Handle the common case of requesting the non-colliding
    // all-ascending input sequences of document IDs received
    // during scans without the sort and extra memory.
    if (all_ascending(places.keys_begin, places.count))
        return read_unique_docs(c_db, c_txn, places, c_options, arena, unique_places, unique_docs, c_error, callback);

    // If it's not one of the trivial consecutive lookups, we want
    // to sort & deduplicate the entries to minimize the random reads
    // from disk.
    auto unique_col_keys = arena.alloc<collection_key_t>(places.count, c_error);
    return_on_error(c_error);

    transform_n(places, places.count, unique_col_keys, std::mem_fn(&place_t::collection_key));
    unique_col_keys = {unique_col_keys.begin(), sort_and_deduplicate(unique_col_keys.begin(), unique_col_keys.end())};

    // There is a chance, all the entries are unique.
    // In such case, let's free-up the memory.
    if (unique_col_keys.size() == places.count)
        return read_unique_docs(c_db, c_txn, places, c_options, arena, unique_places, unique_docs, c_error, callback);

    // Otherwise, let's retrieve the sublist of unique docs,
    // which may be in a very different order from original.
    ukv_arena_t arena_ptr = &arena;
    ukv_byte_t* found_binary_begin = nullptr;
    ukv_length_t* found_binary_offs = nullptr;
    auto unique_col_keys_strided = strided_range(unique_col_keys.begin(), unique_col_keys.end()).immutable();
    unique_places.collections_begin = unique_col_keys_strided.members(&collection_key_t::collection).begin();
    unique_places.keys_begin = unique_col_keys_strided.members(&collection_key_t::key).begin();
    unique_places.fields_begin = {};
    unique_places.count = static_cast<ukv_size_t>(unique_col_keys.size());
    ukv_read( //
        c_db,
        c_txn,
        unique_places.count,
        unique_places.collections_begin.get(),
        unique_places.collections_begin.stride(),
        unique_places.keys_begin.get(),
        unique_places.keys_begin.stride(),
        c_options,
        nullptr,
        &found_binary_offs,
        nullptr,
        &found_binary_begin,
        &arena_ptr,
        c_error);
    return_on_error(c_error);

    // We will later need to locate the data for every separate request.
    // Doing it in O(N) tape iterations every time is too slow.
    // Once we transform to inclusive sums, it will be O(1).
    //      inplace_inclusive_prefix_sum(found_binary_lens, found_binary_lens + found_binary_count);
    // Alternatively we can compensate it with additional memory:
    unique_docs.resize(unique_places.count, c_error);
    return_on_error(c_error);

    // Parse all the unique documents
    auto found_binaries = joined_bins_t(places.count, found_binary_offs, found_binary_begin);
    auto found_binary_it = found_binaries.begin();
    for (ukv_size_t doc_idx = 0; doc_idx != unique_places.count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t& parsed = unique_docs[doc_idx];
        parsed = parse_any(binary_doc, internal_format_k, arena, c_error);

        // This error is extremely unlikely, as we have previously accepted the data into the store.
        return_on_error(c_error);
    }

    // Join docs and fields with binary search
    for (std::size_t task_idx = 0; task_idx != places.size(); ++task_idx) {
        auto place = places[task_idx];
        auto parsed_idx = offset_in_sorted(unique_col_keys, place.collection_key());
        json_t& parsed = unique_docs[parsed_idx];
        callback(task_idx, place.field, parsed);
    }
}

void replace_docs( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    contents_arg_t const& contents,
    ukv_options_t const c_options,
    ukv_format_t const c_format,
    stl_arena_t& arena,
    ukv_error_t* c_error) noexcept {
}

void read_modify_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    contents_arg_t const& contents,
    ukv_options_t const c_options,
    ukv_format_t const c_format,
    stl_arena_t& arena,
    ukv_error_t* c_error) noexcept {

    yyjson_alc allocator = wrap_allocator(arena);
    auto safe_callback = [&](ukv_size_t task_idx, ukv_str_view_t field, json_t& parsed) {
        if (!parsed.mut_handle)
            parsed.mut_handle = yyjson_doc_mut_copy(parsed.handle, &allocator);
        if (!parsed.mut_handle)
            return;

        json_t parsed_task = parse_any(contents[task_idx], c_format, arena, c_error);
        return_on_error(c_error);

        // Perform modifications
    };

    places_arg_t unique_places;
    safe_vector_gt<json_t> unique_docs;
    read_docs(c_db, c_txn, places, c_options, arena, unique_places, unique_docs, c_error, safe_callback);
    return_on_error(c_error);

    // Export all those modified documents
    growing_tape_t growing_tape {arena};
    for (auto const& doc : unique_docs) {
        yyjson_mut_val* root = yyjson_mut_doc_get_root(doc.mut_handle);
        dump_any(root, internal_format_k, arena, growing_tape, c_error);
        return_on_error(c_error);
    }

    // By now, the tape contains concatenated updates docs:
    ukv_byte_t* tape_begin = reinterpret_cast<ukv_byte_t*>(growing_tape.contents().begin().get());
    ukv_arena_t arena_ptr = &arena;
    ukv_write( //
        c_db,
        c_txn,
        unique_places.count,
        unique_places.collections_begin.get(),
        unique_places.collections_begin.stride(),
        unique_places.keys_begin.get(),
        unique_places.keys_begin.stride(),
        nullptr,
        growing_tape.offsets().begin().get(),
        growing_tape.offsets().stride(),
        growing_tape.lengths().begin().get(),
        growing_tape.lengths().stride(),
        &tape_begin,
        0,
        c_options,
        &arena_ptr,
        c_error);
}

void ukv_docs_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_str_view_t const* c_fields,
    ukv_size_t const c_fields_stride,

    ukv_octet_t const* c_presences,

    ukv_length_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_length_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_bytes_cptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_options_t const c_options,
    ukv_format_t const c_format,
    ukv_type_t const,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;

    // If user wants the entire doc in the same format, as the one we use internally,
    // this request can be passed entirely to the underlying Key-Value store.
    strided_iterator_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
    auto has_fields = fields && (!fields.repeats() || *fields);
    if (!has_fields && c_format == internal_format_k)
        return ukv_write( //
            c_db,
            c_txn,
            c_tasks_count,
            c_collections,
            c_collections_stride,
            c_keys,
            c_keys_stride,
            c_presences,
            c_offs,
            c_offs_stride,
            c_lens,
            c_lens_stride,
            c_vals,
            c_vals_stride,
            c_options,
            &new_arena,
            c_error);

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_length_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_lens, c_lens_stride};
    strided_iterator_gt<ukv_octet_t const> presences {c_presences, sizeof(ukv_octet_t)};

    places_arg_t places {collections, keys, fields, c_tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c_tasks_count};

    auto func = has_fields || c_format == ukv_format_json_patch_k || c_format == ukv_format_json_merge_patch_k
                    ? &read_modify_write
                    : &replace_docs;

    func(c_db, c_txn, places, contents, c_options, c_format, arena, c_error);
}

void ukv_docs_read( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_str_view_t const* c_fields,
    ukv_size_t const c_fields_stride,

    ukv_options_t const c_options,
    ukv_format_t const c_format,
    ukv_type_t const,

    ukv_octet_t** c_found_presences,
    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_lengths,
    ukv_byte_t** c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;

    // If user wants the entire doc in the same format, as the one we use internally,
    // this request can be passed entirely to the underlying Key-Value store.
    strided_iterator_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
    auto has_fields = fields && (!fields.repeats() || *fields);
    if (!has_fields && c_format == internal_format_k)
        return ukv_read( //
            c_db,
            c_txn,
            c_tasks_count,
            c_collections,
            c_collections_stride,
            c_keys,
            c_keys_stride,
            c_options,
            c_found_presences,
            c_found_offsets,
            c_found_lengths,
            c_found_values,
            &new_arena,
            c_error);

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {collections, keys, fields, c_tasks_count};

    // Now, we need to parse all the entries to later export them into a target format.
    // Potentially sampling certain sub-fields again along the way.
    growing_tape_t growing_tape {arena};
    auto safe_callback = [&](ukv_size_t, ukv_str_view_t field, json_t const& doc) {
        yyjson_val* root = yyjson_doc_get_root(doc.handle);
        auto branch = lookup_field(root, field);
        dump_any(branch, c_format, arena, growing_tape, c_error);
        return_on_error(c_error);
    };
    places_arg_t unique_places;
    safe_vector_gt<json_t> unique_docs;
    read_docs(c_db, c_txn, places, c_options, arena, unique_places, unique_docs, c_error, safe_callback);

    if (c_found_offsets)
        *c_found_offsets = growing_tape.offsets().begin().get();
    if (c_found_lengths)
        *c_found_lengths = growing_tape.lengths().begin().get();
    if (c_found_values)
        *c_found_values = reinterpret_cast<ukv_byte_t*>(growing_tape.contents().begin().get());
}

/*********************************************************/
/*****************	 Tabular Exports	  ****************/
/*********************************************************/

void gist_recursively(yyjson_val* node,
                      field_path_buffer_t& path,
                      safe_vector_gt<std::string_view>& sorted_paths,
                      growing_tape_t& exported_paths,
                      ukv_error_t* c_error) {

    auto path_len = std::strlen(path);
    auto constexpr slash_len = 1;
    auto constexpr terminator_len = 1;

    if (yyjson_is_obj(node)) {
        yyjson_val *key, *val;
        yyjson_obj_iter iter;
        yyjson_obj_iter_init(node, &iter);
        while ((key = yyjson_obj_iter_next(&iter)) && !*c_error) {
            val = yyjson_obj_iter_get_val(key);
            const char* key_name = yyjson_get_str(key);
            size_t key_len = yyjson_get_len(key);
            if (path_len + slash_len + key_len + terminator_len >= field_path_len_limit_k) {
                *c_error = "Path is too long!";
                return;
            }

            path[path_len] = '/';
            std::memcpy(path + path_len + slash_len, key_name, key_len);
            path[path_len + slash_len + key_len] = 0;
            gist_recursively(val, path, sorted_paths, exported_paths, c_error);
        }
        path[path_len] = 0;
    }
    else if (yyjson_is_arr(node)) {
        std::size_t idx = 0;
        yyjson_val* val;
        yyjson_arr_iter iter;
        yyjson_arr_iter_init(node, &iter);
        while ((val = yyjson_arr_iter_next(&iter)) && !*c_error) {

            path[path_len] = '/';
            auto result = to_chars(path + path_len + slash_len, path + field_path_len_limit_k, idx);
            bool fits_terminator =
                result.ec == std::errc() && result.ptr + terminator_len < path + field_path_len_limit_k;
            if (!fits_terminator) {
                *c_error = "Path is too long!";
                return;
            }

            result.ptr[0] = 0;
            gist_recursively(val, path, sorted_paths, exported_paths, c_error);
            ++idx;
        }
        path[path_len] = 0;
    }
    else {

        std::string_view path_str = std::string_view(path, path_len);
        std::size_t idx = std::lower_bound(sorted_paths.begin(), sorted_paths.end(), path_str) - sorted_paths.begin();
        if (idx != sorted_paths.size() && sorted_paths[idx] == path_str)
            // This same path is already exported
            return;

        auto exported_path = exported_paths.push_back(path, c_error);
        return_on_error(c_error);
        exported_paths.add_terminator(byte_t {0}, c_error);
        return_on_error(c_error);

        path_str = std::string_view(exported_path.c_str(), exported_path.size());
        sorted_paths.insert(idx, &path_str, &path_str + 1, c_error);
    }
}

void ukv_docs_gist( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_docs_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_size_t* c_found_fields_count,
    ukv_length_t** c_found_offsets,
    ukv_char_t** c_found_fields,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;

    ukv_byte_t* found_binary_begin = nullptr;
    ukv_length_t* found_binary_offs = nullptr;
    ukv_read( //
        c_db,
        c_txn,
        c_docs_count,
        c_collections,
        c_collections_stride,
        c_keys,
        c_keys_stride,
        c_options,
        nullptr,
        &found_binary_offs,
        nullptr,
        &found_binary_begin,
        &new_arena,
        c_error);
    return_on_error(c_error);

    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};

    joined_bins_t found_binaries {c_docs_count, found_binary_offs, found_binary_begin};
    joined_bins_iterator_t found_binary_it = found_binaries.begin();

    // Export all the elements into a heap-allocated hash-set, keeping only unique entries
    field_path_buffer_t field_name = {0};
    safe_vector_gt<std::string_view> sorted_paths(&arena);
    growing_tape_t exported_paths(arena);
    for (ukv_size_t doc_idx = 0; doc_idx != c_docs_count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        if (!binary_doc)
            continue;

        json_t doc = parse_any(binary_doc, internal_format_k, arena, c_error);
        return_on_error(c_error);
        yyjson_val* root = yyjson_doc_get_root(doc.handle);
        gist_recursively(root, field_name, sorted_paths, exported_paths, c_error);
        return_on_error(c_error);
    }

    if (c_found_fields_count)
        *c_found_fields_count = static_cast<ukv_size_t>(sorted_paths.size());
    if (c_found_offsets)
        *c_found_offsets = exported_paths.offsets().begin().get();
    if (c_found_fields)
        *c_found_fields = reinterpret_cast<ukv_char_t*>(exported_paths.contents().begin().get());
}

std::size_t min_memory_usage(ukv_type_t type) {
    switch (type) {
    default: return 0;
    case ukv_type_null_k: return 0;
    case ukv_type_bool_k: return 1;
    case ukv_type_uuid_k: return 16;

    case ukv_type_i8_k: return 1;
    case ukv_type_i16_k: return 2;
    case ukv_type_i32_k: return 4;
    case ukv_type_i64_k: return 8;

    case ukv_type_u8_k: return 1;
    case ukv_type_u16_k: return 2;
    case ukv_type_u32_k: return 4;
    case ukv_type_u64_k: return 8;

    case ukv_type_f16_k: return 2;
    case ukv_type_f32_k: return 4;
    case ukv_type_f64_k: return 8;

    // Offsets and lengths:
    case ukv_type_bin_k: return 8;
    case ukv_type_str_k: return 8;
    }
}

struct column_begin_t {
    ukv_octet_t* validities;
    ukv_octet_t* conversions;
    ukv_octet_t* collisions;
    ukv_byte_t* scalars;
    ukv_length_t* str_offsets;
    ukv_length_t* str_lengths;
};

template <typename scalar_at>
void export_scalar_column(yyjson_val* value, std::size_t doc_idx, column_begin_t column) {

    yyjson_type const type = yyjson_get_type(value);
    yyjson_subtype const subtype = yyjson_get_subtype(value);

    // Bitmaps are indexed from the last bit within every byte
    // https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    ukv_octet_t mask_bitmap = static_cast<ukv_octet_t>(1 << (doc_idx % CHAR_BIT));
    ukv_octet_t& ref_valid = column.validities[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_convert = column.conversions[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_collide = column.collisions[doc_idx / CHAR_BIT];
    scalar_at& ref_scalar = reinterpret_cast<scalar_at*>(column.scalars)[doc_idx];

    switch (type) {
    case YYJSON_TYPE_NULL:
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid &= ~mask_bitmap;
        break;
    case YYJSON_TYPE_NONE:
    case YYJSON_TYPE_OBJ:
    case YYJSON_TYPE_ARR:
        ref_convert &= ~mask_bitmap;
        ref_collide |= mask_bitmap;
        ref_valid &= ~mask_bitmap;
        break;

    case YYJSON_TYPE_BOOL:
        ref_scalar = yyjson_is_true(value);
        if constexpr (std::is_same_v<scalar_at, bool>)
            ref_convert &= ~mask_bitmap;
        else
            ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;

    case YYJSON_TYPE_STR: {
        const char* str_begin = yyjson_get_str(value);
        size_t str_len = yyjson_get_len(value);
        std::from_chars_result result = from_chars(str_begin, str_begin + str_len, ref_scalar);
        bool entire_string_is_number = result.ec == std::errc() && result.ptr == str_begin + str_len;
        if (entire_string_is_number) {
            ref_convert |= mask_bitmap;
            ref_collide &= ~mask_bitmap;
            ref_valid |= mask_bitmap;
        }
        else {
            ref_convert &= ~mask_bitmap;
            ref_collide |= mask_bitmap;
            ref_valid &= ~mask_bitmap;
        }
        break;
    }

    case YYJSON_TYPE_NUM: {

        switch (subtype) {
        case YYJSON_SUBTYPE_UINT:
            ref_scalar = static_cast<scalar_at>(yyjson_get_uint(value));
            if constexpr (std::is_unsigned_v<scalar_at>)
                ref_convert &= ~mask_bitmap;
            else
                ref_convert |= mask_bitmap;
            ref_collide &= ~mask_bitmap;
            ref_valid |= mask_bitmap;
            break;

        case YYJSON_SUBTYPE_SINT:
            ref_scalar = static_cast<scalar_at>(yyjson_get_sint(value));
            if constexpr (std::is_integral_v<scalar_at> && std::is_signed_v<scalar_at>)
                ref_convert &= ~mask_bitmap;
            else
                ref_convert |= mask_bitmap;
            ref_collide &= ~mask_bitmap;
            ref_valid |= mask_bitmap;
            break;

        case YYJSON_SUBTYPE_REAL:
            ref_scalar = static_cast<scalar_at>(yyjson_get_real(value));
            if constexpr (std::is_floating_point_v<scalar_at>)
                ref_convert &= ~mask_bitmap;
            else
                ref_convert |= mask_bitmap;
            ref_collide &= ~mask_bitmap;
            ref_valid |= mask_bitmap;
            break;
        }
    }
    }
}

template <typename scalar_at>
ukv_length_t print_scalar(scalar_at scalar, safe_vector_gt<char>& output, ukv_error_t* c_error) {

    /// The on-stack buffer to be used to convert/format/print numerical values into strings.
    char print_buf[printed_number_length_limit_k];

    std::to_chars_result result = to_chars(print_buf, print_buf + printed_number_length_limit_k, scalar);
    bool fits_terminator = result.ec == std::errc() && result.ptr + 1 < print_buf + printed_number_length_limit_k;
    if (fits_terminator) {
        *result.ptr = '\0';
        auto length = result.ptr + 1 - print_buf;
        output.insert(output.size(), print_buf, print_buf + length, c_error);
        return static_cast<ukv_length_t>(length);
    }
    else
        return ukv_length_missing_k;
}

void export_string_column(
    yyjson_val* value, std::size_t doc_idx, column_begin_t column, safe_vector_gt<char>& output, ukv_error_t* c_error) {

    yyjson_type const type = yyjson_get_type(value);
    yyjson_subtype const subtype = yyjson_get_subtype(value);

    // Bitmaps are indexed from the last bit within every byte
    // https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    ukv_octet_t mask_bitmap = static_cast<ukv_octet_t>(1 << (doc_idx % CHAR_BIT));
    ukv_octet_t& ref_valid = column.validities[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_convert = column.conversions[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_collide = column.collisions[doc_idx / CHAR_BIT];
    ukv_length_t& ref_off = column.str_offsets[doc_idx];
    ukv_length_t& ref_len = column.str_lengths[doc_idx];

    ref_off = static_cast<ukv_length_t>(output.size());

    switch (type) {
    case YYJSON_TYPE_NULL:
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid &= ~mask_bitmap;
        ref_off = ref_len = ukv_length_missing_k;
        break;
    case YYJSON_TYPE_NONE:
    case YYJSON_TYPE_OBJ:
    case YYJSON_TYPE_ARR:
        ref_convert &= ~mask_bitmap;
        ref_collide |= mask_bitmap;
        ref_valid &= ~mask_bitmap;
        ref_off = ref_len = ukv_length_missing_k;
        break;

    case YYJSON_TYPE_BOOL: {
        if (yyjson_is_true(value)) {
            ref_len = 5;
            output.insert(output.size(), true_k, true_k + 5, c_error);
        }
        else {
            ref_len = 6;
            output.insert(output.size(), false_k, false_k + 6, c_error);
        }
        ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    }

    case YYJSON_TYPE_STR: {
        const char* str_begin = yyjson_get_str(value);
        size_t str_len = yyjson_get_len(value);

        ref_len = static_cast<ukv_length_t>(str_len);
        output.insert(output.size(), str_begin, str_begin + str_len + 1, c_error);
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    }

    case YYJSON_TYPE_NUM: {

        switch (subtype) {
        case YYJSON_SUBTYPE_UINT:
            ref_len = print_scalar(yyjson_get_uint(value), output, c_error);
            ref_convert |= mask_bitmap;
            ref_collide = ref_len != ukv_length_missing_k ? (ref_collide & ~mask_bitmap) : (ref_collide | mask_bitmap);
            ref_valid = ref_len == ukv_length_missing_k ? (ref_valid & ~mask_bitmap) : (ref_valid | mask_bitmap);
            break;

        case YYJSON_SUBTYPE_SINT:
            ref_len = print_scalar(yyjson_get_sint(value), output, c_error);
            ref_convert |= mask_bitmap;
            ref_collide = ref_len != ukv_length_missing_k ? (ref_collide & ~mask_bitmap) : (ref_collide | mask_bitmap);
            ref_valid = ref_len == ukv_length_missing_k ? (ref_valid & ~mask_bitmap) : (ref_valid | mask_bitmap);
            break;

        case YYJSON_SUBTYPE_REAL:
            ref_len = print_scalar(yyjson_get_real(value), output, c_error);
            ref_convert |= mask_bitmap;
            ref_collide = ref_len != ukv_length_missing_k ? (ref_collide & ~mask_bitmap) : (ref_collide | mask_bitmap);
            ref_valid = ref_len == ukv_length_missing_k ? (ref_valid & ~mask_bitmap) : (ref_valid | mask_bitmap);
            break;
        }
    }
    }
}

void ukv_docs_gather( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_docs_count,
    ukv_size_t const c_fields_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_str_view_t const* c_fields,
    ukv_size_t const c_fields_stride,

    ukv_type_t const* c_types,
    ukv_size_t const c_types_stride,

    ukv_options_t const c_options,

    ukv_octet_t*** c_result_bitmap_valid,
    ukv_octet_t*** c_result_bitmap_converted,
    ukv_octet_t*** c_result_bitmap_collision,
    ukv_byte_t*** c_result_scalars,
    ukv_length_t*** c_result_strs_offsets,
    ukv_length_t*** c_result_strs_lengths,
    ukv_byte_t** c_result_strs_contents,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;
    // Validate the input arguments

    // Retrieve the entire documents before we can sample internal fields
    ukv_byte_t* found_binary_begin = nullptr;
    ukv_length_t* found_binary_offs = nullptr;
    ukv_read( //
        c_db,
        c_txn,
        c_docs_count,
        c_collections,
        c_collections_stride,
        c_keys,
        c_keys_stride,
        c_options,
        nullptr,
        &found_binary_offs,
        nullptr,
        &found_binary_begin,
        &new_arena,
        c_error);
    return_on_error(c_error);

    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
    strided_iterator_gt<ukv_type_t const> types {c_types, c_types_stride};

    joined_bins_t found_binaries {c_docs_count, found_binary_offs, found_binary_begin};
    joined_bins_iterator_t found_binary_it = found_binaries.begin();

    // Estimate the amount of memory needed to store at least scalars and columns addresses
    // TODO: Align offsets of bitmaps to 64-byte boundaries for Arrow
    // https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
    // TODO: Align offsets of bitmaps to 64-byte boundaries for Arrow
    // https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
    bool wants_conversions = c_result_bitmap_converted;
    bool wants_collisions = c_result_bitmap_collision;
    std::size_t slots_per_bitmap = c_docs_count / 8 + (c_docs_count % 8 != 0);
    std::size_t count_bitmaps = 1ul + wants_conversions + wants_collisions;
    std::size_t bytes_per_bitmap = sizeof(ukv_octet_t) * slots_per_bitmap;
    std::size_t bytes_per_addresses_row = sizeof(void*) * c_fields_count;
    std::size_t bytes_for_addresses = bytes_per_addresses_row * 6;
    std::size_t bytes_for_bitmaps = bytes_per_bitmap * count_bitmaps * c_fields_count * c_fields_count;
    std::size_t bytes_per_scalars_row = transform_reduce_n(types, c_fields_count, 0ul, &min_memory_usage);
    std::size_t bytes_for_scalars = bytes_per_scalars_row * c_docs_count;

    // Preallocate at least a minimum amount of memory.
    // It will be organized in the following way:
    // 1. validity bitmaps for all fields
    // 2. optional conversion bitmaps for all fields
    // 3. optional collision bitmaps for all fields
    // 4. offsets of all strings
    // 5. lengths of all strings
    // 6. scalars for all fields

    span_gt<byte_t> tape = arena.alloc<byte_t>(bytes_for_addresses + bytes_for_bitmaps + bytes_for_scalars, c_error);
    byte_t* const tape_ptr = tape.begin();

    // If those pointers were not provided, we can reuse the validity bitmap
    // It will allow us to avoid extra checks later.
    // ! Still, in every sequence of updates, validity is the last bit to be set,
    // ! to avoid overwriting.
    auto first_collection_validities = reinterpret_cast<ukv_octet_t*>(tape_ptr + bytes_for_addresses);
    auto first_collection_conversions = wants_conversions //
                                            ? first_collection_validities + slots_per_bitmap * c_fields_count
                                            : first_collection_validities;
    auto first_collection_collisions = wants_collisions //
                                           ? first_collection_conversions + slots_per_bitmap * c_fields_count
                                           : first_collection_validities;
    auto first_collection_scalars = reinterpret_cast<ukv_byte_t*>(tape_ptr + bytes_for_addresses + bytes_for_bitmaps);

    // 1, 2, 3. Export validity maps addresses
    std::size_t tape_progress = 0;
    {
        auto addresses = *c_result_bitmap_valid = reinterpret_cast<ukv_octet_t**>(tape_ptr + tape_progress);
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx)
            addresses[field_idx] = first_collection_validities + field_idx * slots_per_bitmap;
        tape_progress += bytes_per_addresses_row;
    }
    if (wants_conversions) {
        auto addresses = *c_result_bitmap_converted = reinterpret_cast<ukv_octet_t**>(tape_ptr + tape_progress);
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx)
            addresses[field_idx] = first_collection_conversions + field_idx * slots_per_bitmap;
        tape_progress += bytes_per_addresses_row;
    }
    if (wants_collisions) {
        auto addresses = *c_result_bitmap_collision = reinterpret_cast<ukv_octet_t**>(tape_ptr + tape_progress);
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx)
            addresses[field_idx] = first_collection_collisions + field_idx * slots_per_bitmap;
        tape_progress += bytes_per_addresses_row;
    }

    // 4, 5, 6. Export addresses for scalars, strings offsets and strings lengths
    {
        auto addresses_offs = *c_result_strs_offsets =
            reinterpret_cast<ukv_length_t**>(tape_ptr + tape_progress + bytes_per_addresses_row * 0);
        auto addresses_lens = *c_result_strs_lengths =
            reinterpret_cast<ukv_length_t**>(tape_ptr + tape_progress + bytes_per_addresses_row * 1);
        auto addresses_scalars = *c_result_scalars =
            reinterpret_cast<ukv_byte_t**>(tape_ptr + tape_progress + bytes_per_addresses_row * 2);

        auto scalars_tape = first_collection_scalars;
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx) {
            ukv_type_t type = types[field_idx];
            switch (type) {
            case ukv_type_str_k:
            case ukv_type_bin_k:
                addresses_offs[field_idx] = reinterpret_cast<ukv_length_t*>(scalars_tape);
                addresses_lens[field_idx] = addresses_offs[field_idx] + c_docs_count;
                addresses_scalars[field_idx] = nullptr;
                break;
            default:
                addresses_offs[field_idx] = nullptr;
                addresses_lens[field_idx] = nullptr;
                addresses_scalars[field_idx] = reinterpret_cast<ukv_byte_t*>(scalars_tape);
                break;
            }
            scalars_tape += min_memory_usage(type) * c_docs_count;
        }
    }

    // Go though all the documents extracting and type-checking the relevant parts
    safe_vector_gt<char> string_tape(&arena);
    for (ukv_size_t doc_idx = 0; doc_idx != c_docs_count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t doc = parse_any(binary_doc, internal_format_k, arena, c_error);
        return_on_error(c_error);
        yyjson_val* root = yyjson_doc_get_root(doc.handle);

        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx) {

            // Find this field within document
            ukv_type_t type = types[field_idx];
            ukv_str_view_t field = fields[field_idx];
            yyjson_val* found_value = lookup_field(root, field);

            column_begin_t column {
                .validities = (*c_result_bitmap_valid)[field_idx],
                .conversions = (*(c_result_bitmap_converted ?: c_result_bitmap_valid))[field_idx],
                .collisions = (*(c_result_bitmap_collision ?: c_result_bitmap_valid))[field_idx],
                .scalars = (*c_result_scalars)[field_idx],
                .str_offsets = (*c_result_strs_offsets)[field_idx],
                .str_lengths = (*c_result_strs_lengths)[field_idx],
            };

            // Export the types
            switch (type) {

            case ukv_type_bool_k: export_scalar_column<bool>(found_value, doc_idx, column); break;

            case ukv_type_i8_k: export_scalar_column<std::int8_t>(found_value, doc_idx, column); break;
            case ukv_type_i16_k: export_scalar_column<std::int16_t>(found_value, doc_idx, column); break;
            case ukv_type_i32_k: export_scalar_column<std::int32_t>(found_value, doc_idx, column); break;
            case ukv_type_i64_k: export_scalar_column<std::int64_t>(found_value, doc_idx, column); break;

            case ukv_type_u8_k: export_scalar_column<std::uint8_t>(found_value, doc_idx, column); break;
            case ukv_type_u16_k: export_scalar_column<std::uint16_t>(found_value, doc_idx, column); break;
            case ukv_type_u32_k: export_scalar_column<std::uint32_t>(found_value, doc_idx, column); break;
            case ukv_type_u64_k: export_scalar_column<std::uint64_t>(found_value, doc_idx, column); break;

            case ukv_type_f32_k: export_scalar_column<float>(found_value, doc_idx, column); break;
            case ukv_type_f64_k: export_scalar_column<double>(found_value, doc_idx, column); break;

            case ukv_type_str_k: export_string_column(found_value, doc_idx, column, string_tape, c_error); break;
            case ukv_type_bin_k: export_string_column(found_value, doc_idx, column, string_tape, c_error); break;

            default: break;
            }
        }
    }

    *c_result_strs_contents = reinterpret_cast<ukv_byte_t*>(string_tape.data());
}
