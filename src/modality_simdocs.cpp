/**
 * @file modality_simdocs.cpp
 * @author Ashot Vardanian
 *
 * @brief Document storage using "nlohmann/JSON" lib.
 * Sits on top of any @see "ukv.h"-compatible system.
 */

#include <cstdio>      // `std::snprintf`
#include <charconv>    // `std::to_chars`
#include <string_view> // `std::string_view`

#include <yyjson.h> // Primary internal JSON representation
#include <bson.h>   // Converting from/to BSON

#include "helpers/pmr.hpp"
#include "helpers/algorithm.hpp"
#include "helpers/vector.hpp" // `growing_tape_t`

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

constexpr ukv_doc_field_type_t internal_format_k = ukv_doc_field_json_k;

static constexpr char const* true_k = "true";
static constexpr char const* false_k = "false";

/// The length of buffer to be used to convert/format/print numerical values into strings.
constexpr std::size_t printed_number_length_limit_k = 32;
constexpr std::size_t field_path_len_limit_k = 512;

using printed_number_buffer_t = char[printed_number_length_limit_k];
using field_path_buffer_t = char[field_path_len_limit_k];

/*********************************************************/
/*****************	 STL Compatibility	  ****************/
/*********************************************************/

/**
 * @brief Parses `float`, `double`, `bool` or any integral type from string.
 * @return true If not the entire string was recognized as a number.
 */
template <typename at>
bool parse_entire_number(char const* begin, char const* end, at& result) {
    // Floats:
    if constexpr (std::is_same_v<at, float>) {
        char* number_end = nullptr;
        result = std::strtof(begin, &number_end);
        return end == number_end;
    }
    // Doubles:
    else if constexpr (std::is_same_v<at, double>) {
        char* number_end = nullptr;
        result = std::strtod(begin, &number_end);
        return end == number_end;
    }
    // Booleans:
    else if constexpr (std::is_same_v<at, bool>) {
        bool is_true = (end - begin) == 4 && std::equal(begin, end, true_k);
        bool is_false = (end - begin) == 5 && std::equal(begin, end, false_k);
        if (is_true | is_false) {
            result = is_true;
            return true;
        }
        else
            return false;
    }
    // Integers:
    else {
        return std::from_chars(begin, end, result).ptr == end;
    }
}

/**
 * @brief Prints a number into a string buffer. Terminates with zero character.
 * @return The string-vew until the termination character. Empty string on failure.
 */
template <typename at>
std::string_view print_number(char* begin, char* end, at scalar) {
    if constexpr (std::is_floating_point_v<at>) {
        // Parsing and dumping floating-point numbers is still not fully implemented in STL:
        //  std::to_chars_result result = std::to_chars(&print_buffer[0], print_buffer + printed_number_length_limit_k,
        //  scalar); bool fits_terminator = result.ec != std::errc() && result.ptr < print_buffer +
        //  printed_number_length_limit_k;
        // Using FMT would cause an extra dependency:
        //  auto end_ptr = fmt::format_to(print_buffer, "{}", scalar);
        //  bool fits_terminator = end_ptr < print_buffer + printed_number_length_limit_k;
        // If we use `std::snprintf`, the result will @b already be NULL-terminated:
        auto result = std::snprintf(begin, end - begin, "%f", scalar);
        return result > 0 //
                   ? std::string_view {begin, static_cast<std::size_t>(result - 1)}
                   : std::string_view {};
    }
    else {
        // `std::to_chars` won't NULL-terminate the string, but we should.
        auto result = std::to_chars(begin, end, scalar);
        if (result.ec != std::errc() || result.ptr == end)
            return {};

        *result.ptr = '\0';
        return {begin, result.ptr - begin};
    }
}

/*********************************************************/
/*****************	 Working with JSONs	  ****************/
/*********************************************************/

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

    explicit operator bool() const noexcept { return handle || mut_handle; }
};

struct json_branch_t {
    yyjson_val* handle = nullptr;
    yyjson_mut_val* mut_handle = nullptr;

    explicit operator bool() const noexcept { return handle || mut_handle; }
    yyjson_val* punned() const noexcept { return mut_handle ? (yyjson_val*)(mut_handle) : handle; }
};

static void* json_yy_malloc(void* ctx, size_t size) noexcept {
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(ctx);
    return arena.alloc<byte_t>(size, nullptr).begin();
}

static void* json_yy_realloc(void* ctx, void* ptr, size_t size) noexcept {
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(ctx);
    return arena.alloc<byte_t>(size, nullptr).begin();
}

static void json_yy_free(void*, void*) noexcept {
}

yyjson_alc wrap_allocator(stl_arena_t& arena) {
    yyjson_alc allocator;
    allocator.malloc = json_yy_malloc;
    allocator.realloc = json_yy_realloc;
    allocator.free = json_yy_free;
    allocator.ctx = &arena;
    return allocator;
}

yyjson_val* json_lookup(yyjson_val* json, ukv_str_view_t field) noexcept {
    return !field ? json : field[0] == '/' ? yyjson_get_pointer(json, field) : yyjson_obj_get(json, field);
}

yyjson_mut_val* json_lookup(yyjson_mut_val* json, ukv_str_view_t field) noexcept {
    return !field ? json : field[0] == '/' ? yyjson_mut_get_pointer(json, field) : yyjson_mut_obj_get(json, field);
}

json_t json_parse(value_view_t bytes, stl_arena_t& arena, ukv_error_t* c_error) noexcept {

    if (bytes.empty())
        return {};

    json_t result;
    yyjson_alc allocator = wrap_allocator(arena);
    yyjson_read_flag flg = YYJSON_READ_ALLOW_COMMENTS | YYJSON_READ_ALLOW_INF_AND_NAN;
    result.handle = yyjson_read_opts((char*)bytes.data(), (size_t)bytes.size(), flg, &allocator, NULL);
    if (!result.handle)
        *c_error = "Failed to parse document!";
    return result;
}

value_view_t json_dump(json_branch_t json, stl_arena_t& arena, growing_tape_t& output, ukv_error_t* c_error) noexcept {

    if (!json)
        return output.push_back(value_view_t {}, c_error);

    size_t result_length = 0;
    yyjson_write_flag flg = 0;
    yyjson_alc allocator = wrap_allocator(arena);
    char* result_begin = json.mut_handle
                             ? yyjson_mut_val_write_opts(json.mut_handle, flg, &allocator, &result_length, NULL)
                             : yyjson_val_write_opts(json.handle, flg, &allocator, &result_length, NULL);
    if (!result_begin)
        *c_error = "Failed to serialize the document!";

    auto result = value_view_t {reinterpret_cast<byte_t const*>(result_begin), result_length};
    result = output.push_back(result, c_error);
    output.add_terminator(byte_t {0}, c_error);
    return result;
}

template <typename scalar_at>
void json_to_scalar(yyjson_val* value,
                    ukv_octet_t mask,
                    ukv_octet_t& valid,
                    ukv_octet_t& convert,
                    ukv_octet_t& collide,
                    scalar_at& scalar) noexcept {

    yyjson_type const type = yyjson_get_type(value);
    yyjson_subtype const subtype = yyjson_get_subtype(value);

    switch (type) {
    case YYJSON_TYPE_NULL:
        convert &= ~mask;
        collide &= ~mask;
        valid &= ~mask;
        break;
    case YYJSON_TYPE_NONE:
    case YYJSON_TYPE_OBJ:
    case YYJSON_TYPE_ARR:
        convert &= ~mask;
        collide |= mask;
        valid &= ~mask;
        break;

    case YYJSON_TYPE_BOOL:
        scalar = yyjson_is_true(value);
        if constexpr (std::is_same_v<scalar_at, bool>)
            convert &= ~mask;
        else
            convert |= mask;
        collide &= ~mask;
        valid |= mask;
        break;

    case YYJSON_TYPE_STR: {
        char const* str_begin = yyjson_get_str(value);
        size_t str_len = yyjson_get_len(value);
        if (parse_entire_number(str_begin, str_begin + str_len, scalar)) {
            convert |= mask;
            collide &= ~mask;
            valid |= mask;
        }
        else {
            convert &= ~mask;
            collide |= mask;
            valid &= ~mask;
        }
        break;
    }

    case YYJSON_TYPE_NUM: {

        switch (subtype) {
        case YYJSON_SUBTYPE_UINT:
            scalar = static_cast<scalar_at>(yyjson_get_uint(value));
            if constexpr (std::is_unsigned_v<scalar_at>)
                convert &= ~mask;
            else
                convert |= mask;
            collide &= ~mask;
            valid |= mask;
            break;

        case YYJSON_SUBTYPE_SINT:
            scalar = static_cast<scalar_at>(yyjson_get_sint(value));
            if constexpr (std::is_integral_v<scalar_at> && std::is_signed_v<scalar_at>)
                convert &= ~mask;
            else
                convert |= mask;
            collide &= ~mask;
            valid |= mask;
            break;

        case YYJSON_SUBTYPE_REAL:
            scalar = static_cast<scalar_at>(yyjson_get_real(value));
            if constexpr (std::is_floating_point_v<scalar_at>)
                convert &= ~mask;
            else
                convert |= mask;
            collide &= ~mask;
            valid |= mask;
            break;
        }
    }
    }
}

std::string_view json_to_string(yyjson_val* value,
                                ukv_octet_t mask,
                                ukv_octet_t& valid,
                                ukv_octet_t& convert,
                                ukv_octet_t& collide,
                                printed_number_buffer_t& print_buffer) noexcept {

    yyjson_type const type = yyjson_get_type(value);
    yyjson_subtype const subtype = yyjson_get_subtype(value);
    std::string_view result;

    switch (type) {
    case YYJSON_TYPE_NULL:
        convert &= ~mask;
        collide &= ~mask;
        valid &= ~mask;
        break;
    case YYJSON_TYPE_NONE:
    case YYJSON_TYPE_OBJ:
    case YYJSON_TYPE_ARR:
        convert &= ~mask;
        collide |= mask;
        valid &= ~mask;
        break;

    case YYJSON_TYPE_BOOL: {
        result = yyjson_is_true(value) ? std::string_view(true_k, 5) : std::string_view(false_k, 6);
        convert |= mask;
        collide &= ~mask;
        valid |= mask;
        break;
    }

    case YYJSON_TYPE_STR: {
        char const* str_begin = yyjson_get_str(value);
        size_t str_len = yyjson_get_len(value);

        result = std::string_view(str_begin, str_len);
        convert &= ~mask;
        collide &= ~mask;
        valid |= mask;
        break;
    }

    case YYJSON_TYPE_NUM: {

        switch (subtype) {
        case YYJSON_SUBTYPE_UINT:
            result = print_number(print_buffer, print_buffer + printed_number_length_limit_k, yyjson_get_uint(value));
            convert |= mask;
            collide = !result.empty() ? (collide & ~mask) : (collide | mask);
            valid = result.empty() ? (valid & ~mask) : (valid | mask);
            break;

        case YYJSON_SUBTYPE_SINT:
            result = print_number(print_buffer, print_buffer + printed_number_length_limit_k, yyjson_get_sint(value));
            convert |= mask;
            collide = !result.empty() ? (collide & ~mask) : (collide | mask);
            valid = result.empty() ? (valid & ~mask) : (valid | mask);
            break;

        case YYJSON_SUBTYPE_REAL:
            result = print_number(print_buffer, print_buffer + printed_number_length_limit_k, yyjson_get_real(value));
            convert |= mask;
            collide = !result.empty() ? (collide & ~mask) : (collide | mask);
            valid = result.empty() ? (valid & ~mask) : (valid | mask);
            break;
        }
    }
    }

    return result;
}

/*********************************************************/
/*****************	 Format Conversions	  ****************/
/*********************************************************/

static bool bson_visit_before(bson_iter_t const*, char const*, void*) {
    return false;
}
static bool bson_visit_after(bson_iter_t const*, char const*, void*) {
    return false;
}
static void bson_visit_corrupt(bson_iter_t const*, void*) {
}
static bool bson_visit_double(bson_iter_t const*, char const* key, double v_double, void* data);
static bool bson_visit_utf8(bson_iter_t const*, char const* key, size_t v_utf8_len, char const* v_utf8, void* data);
static bool bson_visit_document(bson_iter_t const*, char const* key, bson_t const* v_document, void* data);
static bool bson_visit_array(bson_iter_t const*, char const* key, bson_t const* v_array, void* data);
static bool bson_visit_binary(bson_iter_t const*,
                              char const* key,
                              bson_subtype_t v_subtype,
                              size_t v_binary_len,
                              const uint8_t* v_binary,
                              void* data);
static bool bson_visit_undefined(bson_iter_t const*, char const* key, void* data);
static bool bson_visit_oid(bson_iter_t const*, char const* key, const bson_oid_t* v_oid, void* data);
static bool bson_visit_bool(bson_iter_t const*, char const* key, bool v_bool, void* data);
static bool bson_visit_date_time(bson_iter_t const*, char const* key, int64_t msec_since_epoch, void* data);
static bool bson_visit_null(bson_iter_t const*, char const* key, void* data);
static bool bson_visit_regex(
    bson_iter_t const*, char const* key, char const* v_regex, char const* v_options, void* data);
static bool bson_visit_dbpointer(bson_iter_t const*,
                                 char const* key,
                                 size_t v_collection_len,
                                 char const* v_collection,
                                 bson_oid_t const* v_oid,
                                 void* data);
static bool bson_visit_code(bson_iter_t const*, char const* key, size_t v_code_len, char const* v_code, void* data);
static bool bson_visit_symbol(
    bson_iter_t const*, char const* key, size_t v_symbol_len, char const* v_symbol, void* data);
static bool bson_visit_codewscope(
    bson_iter_t const*, char const* key, size_t v_code_len, char const* v_code, bson_t const* v_scope, void* data);
static bool bson_visit_int32(bson_iter_t const*, char const* key, int32_t v_int32, void* data);
static bool bson_visit_timestamp(
    bson_iter_t const*, char const* key, uint32_t v_timestamp, uint32_t v_increment, void* data);
static bool bson_visit_int64(bson_iter_t const*, char const* key, int64_t v_int64, void* data);
static bool bson_visit_maxkey(bson_iter_t const*, char const* key, void* data);
static bool bson_visit_minkey(bson_iter_t const*, char const* key, void* data);
static void bson_visit_unsupported_type(bson_iter_t const*, char const* key, uint32_t type_code, void* data);
static bool bson_visit_decimal128(bson_iter_t const*,
                                  char const* key,
                                  bson_decimal128_t const* v_decimal128,
                                  void* data);

json_t any_parse(value_view_t bytes,
                 ukv_doc_field_type_t const field_type,
                 stl_arena_t& arena,
                 ukv_error_t* c_error) noexcept {

    if (field_type == ukv_doc_field_bson_k) {
        bson_t bson;
        bool success = bson_init_static(&bson, reinterpret_cast<uint8_t const*>(bytes.data()), bytes.size());
        // Using `bson_as_canonical_extended_json` is a bad idea, as it allocates dynamically.
        // Instead we will manually iterate over the document, using the "visitor" pattern.
        bson_visitor_t visitor = {0};
        bson_iter_t iter;
        safe_vector_gt<char> json(arena);

        if (!bson_iter_init(&iter, &bson)) {
            *c_error = "Failed to parse the BSON document!";
            return {};
        }

        if (!bson_iter_visit_all(&iter, &visitor, &json)) {
            *c_error = "Failed to iterate the BSON document!";
            return {};
        }
    }

    if (field_type == ukv_doc_field_json_k)
        return json_parse(bytes, arena, c_error);

    // Wrapping binary data into a JSON object
    yyjson_alc allocator = wrap_allocator(arena);
    yyjson_mut_doc* doc = yyjson_mut_doc_new(&allocator);
    yyjson_mut_val* root = nullptr;
    switch (field_type) {
    case ukv_doc_field_null_k:
    case ukv_doc_field_uuid_k:
    case ukv_doc_field_f16_k:
    case ukv_doc_field_bin_k: *c_error = "Input type not supported";
    case ukv_doc_field_str_k: root = yyjson_mut_strn(doc, bytes.c_str(), bytes.size()); break;
    case ukv_doc_field_u8_k: root = yyjson_mut_uint(doc, *reinterpret_cast<uint8_t const*>(bytes.data())); break;
    case ukv_doc_field_u16_k: root = yyjson_mut_uint(doc, *reinterpret_cast<uint16_t const*>(bytes.data())); break;
    case ukv_doc_field_u32_k: root = yyjson_mut_uint(doc, *reinterpret_cast<uint32_t const*>(bytes.data())); break;
    case ukv_doc_field_u64_k: root = yyjson_mut_uint(doc, *reinterpret_cast<uint64_t const*>(bytes.data())); break;
    case ukv_doc_field_i8_k: root = yyjson_mut_sint(doc, *reinterpret_cast<int8_t const*>(bytes.data())); break;
    case ukv_doc_field_i16_k: root = yyjson_mut_sint(doc, *reinterpret_cast<int16_t const*>(bytes.data())); break;
    case ukv_doc_field_i32_k: root = yyjson_mut_sint(doc, *reinterpret_cast<int32_t const*>(bytes.data())); break;
    case ukv_doc_field_i64_k: root = yyjson_mut_sint(doc, *reinterpret_cast<int64_t const*>(bytes.data())); break;
    case ukv_doc_field_f32_k: root = yyjson_mut_real(doc, *reinterpret_cast<float const*>(bytes.data())); break;
    case ukv_doc_field_f64_k: root = yyjson_mut_real(doc, *reinterpret_cast<double const*>(bytes.data())); break;
    case ukv_doc_field_bool_k: root = yyjson_mut_bool(doc, *reinterpret_cast<bool const*>(bytes.data())); break;
    }
    yyjson_mut_doc_set_root(doc, root);
    json_t result;
    result.mut_handle = doc;
    return result;
}

value_view_t any_dump(json_branch_t json,
                      ukv_doc_field_type_t const field_type,
                      stl_arena_t& arena,
                      growing_tape_t& output,
                      ukv_error_t* c_error) noexcept {

    if (field_type == ukv_doc_field_str_k) {
        ukv_octet_t dummy;
        printed_number_buffer_t print_buffer;
        auto str = json_to_string(json.punned(), 0, dummy, dummy, dummy, print_buffer);
        auto result = output.push_back(str, c_error);
        output.add_terminator(byte_t {0}, c_error);
        return result;
    }

    else if (field_type == ukv_doc_field_json_k)
        return json_dump(json, arena, output, c_error);

    *c_error = "Output type not supported!";
    return {};
}

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

yyjson_mut_val* modify_recursively( //
    yyjson_mut_doc* doc,
    yyjson_mut_val* branch,
    std::string_view remaining_path,
    ukv_doc_modification_t const c_modification,
    yyjson_val* new_content) {

    if (remaining_path.empty())
        return yyjson_val_mut_copy(doc, new_content);

    auto first_end = remaining_path.find('/');
    auto key_or_idx_str = remaining_path.substr(0, first_end);
    auto is_idx = std::all_of(key_or_idx_str.begin(), key_or_idx_str.end(), [](char c) { return std::isdigit(c); });
    auto build_missing = c_modification != ukv_doc_modify_update_k;

    if (is_idx) {
        size_t idx = 0;
        std::from_chars(key_or_idx_str.begin(), key_or_idx_str.end(), idx);
        yyjson_mut_val* old_child = yyjson_mut_arr_get(branch, idx);
        // auto child_replacement = modify_recursively(doc, old_child, remaining_path.substr(first_end), build_missing);
        // yyjson_mut_arr_replace(branch, idx, child_replacement);
        return branch;
    }
    else {

        yyjson_mut_val* old_child = yyjson_mut_obj_getn(branch, key_or_idx_str.data(), key_or_idx_str.size());
        if (old_child) {
        }

        yyjson_mut_val* new_key = yyjson_mut_strn(doc, key_or_idx_str.data(), key_or_idx_str.size());
        // bool yyjson_mut_obj_add(branch, new_key, yyjson_mut_val * val);
    }
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
        json_t parsed = any_parse(binary_doc, internal_format_k, arena, c_error);

        // This error is extremely unlikely, as we have previously accepted the data into the store.
        return_on_error(c_error);

        ukv_str_view_t field = places.fields_begin ? places.fields_begin[task_idx] : nullptr;
        callback(task_idx, field, parsed);
    }

    unique_places = places;
}

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
        parsed = any_parse(binary_doc, internal_format_k, arena, c_error);

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
    ukv_doc_modification_t const c_modification,
    ukv_doc_field_type_t const c_type,
    stl_arena_t& arena,
    ukv_error_t* c_error) noexcept {
}

void read_modify_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    contents_arg_t const& contents,
    ukv_options_t const c_options,
    ukv_doc_modification_t const c_modification,
    ukv_doc_field_type_t const c_type,
    stl_arena_t& arena,
    ukv_error_t* c_error) noexcept {

    yyjson_alc allocator = wrap_allocator(arena);
    auto safe_callback = [&](ukv_size_t task_idx, ukv_str_view_t field, json_t& parsed) {
        if (!parsed.mut_handle)
            parsed.mut_handle = yyjson_doc_mut_copy(parsed.handle, &allocator);
        if (!parsed.mut_handle)
            return;

        json_t parsed_task = any_parse(contents[task_idx], c_type, arena, c_error);
        return_on_error(c_error);

// Perform modifications
#if 0
        if (c_modification == ukv_doc_modify_merge_k) {
            yyjson_mut_val* root = yyjson_mut_doc_get_root(parsed.mut_handle);
            yyjson_mut_val* branch = json_lookup(root, field);
            yyjson_mut_merge_patch(parsed.mut_handle, branch, parsed_task.handle);
        }
        else if (c_modification == ukv_doc_modify_patch_k) {
            *c_error = "Patches aren't currently supported";
        }
        else {
            yyjson_mut_val* root = yyjson_mut_doc_get_root(parsed.mut_handle);
            auto new_root = modify_recursively(doc.mut_handle, root, field, c_modification, parsed_task.handle);
            yyjson_mut_doc_set_root(doc.mut_handle, new_root);
        }
#endif
    };

    places_arg_t unique_places;
    safe_vector_gt<json_t> unique_docs(arena);
    auto opts = ukv_options_t(c_options | (c_txn ? ukv_option_txn_watch_k : 0));
    read_docs(c_db, c_txn, places, opts, arena, unique_places, unique_docs, c_error, safe_callback);
    return_on_error(c_error);

    // Export all those modified documents
    growing_tape_t growing_tape {arena};
    growing_tape.reserve(unique_places.size(), c_error);
    return_on_error(c_error);

    for (auto const& doc : unique_docs) {
        yyjson_mut_val* root = yyjson_mut_doc_get_root(doc.mut_handle);
        any_dump({.mut_handle = root}, internal_format_k, arena, growing_tape, c_error);
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

    ukv_doc_modification_t const c_modification,
    ukv_doc_field_type_t const c_type,
    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_tasks_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;

    // If user wants the entire doc in the same format, as the one we use internally,
    // this request can be passed entirely to the underlying Key-Value store.
    strided_iterator_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
    auto has_fields = fields && (!fields.repeats() || *fields);
    if (!has_fields && c_type == internal_format_k)
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
    strided_iterator_gt<ukv_octet_t const> presences {c_presences, sizeof(ukv_octet_t)};
    strided_iterator_gt<ukv_length_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_lens, c_lens_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c_vals, c_vals_stride};

    places_arg_t places {collections, keys, fields, c_tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c_tasks_count};

    auto func = has_fields || c_modification == ukv_doc_modify_patch_k || c_modification == ukv_doc_modify_merge_k
                    ? &read_modify_write
                    : &replace_docs;

    func(c_db, c_txn, places, contents, c_options, c_modification, c_type, arena, c_error);
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

    ukv_doc_field_type_t const c_type,
    ukv_options_t const c_options,

    ukv_octet_t** c_found_presences,
    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_lengths,
    ukv_byte_t** c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_tasks_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;

    // If user wants the entire doc in the same format, as the one we use internally,
    // this request can be passed entirely to the underlying Key-Value store.
    strided_iterator_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
    auto has_fields = fields && (!fields.repeats() || *fields);
    if (!has_fields && c_type == internal_format_k)
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
    growing_tape.reserve(places.size(), c_error);
    return_on_error(c_error);

    auto safe_callback = [&](ukv_size_t, ukv_str_view_t field, json_t const& doc) {
        yyjson_val* root = yyjson_doc_get_root(doc.handle);
        auto branch = json_lookup(root, field);
        any_dump({.handle = branch}, c_type, arena, growing_tape, c_error);
        return_on_error(c_error);
    };
    places_arg_t unique_places;
    safe_vector_gt<json_t> unique_docs(arena);
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
            char const* key_name = yyjson_get_str(key);
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
            auto result = print_number(path + path_len + slash_len, path + field_path_len_limit_k, idx);
            if (result.empty()) {
                *c_error = "Path is too long!";
                return;
            }

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

    if (!c_docs_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
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
    safe_vector_gt<std::string_view> sorted_paths(arena);
    growing_tape_t exported_paths(arena);
    for (ukv_size_t doc_idx = 0; doc_idx != c_docs_count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        if (!binary_doc)
            continue;

        json_t doc = any_parse(binary_doc, internal_format_k, arena, c_error);
        return_on_error(c_error);
        if (!doc)
            continue;

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

std::size_t doc_field_size_bytes(ukv_doc_field_type_t type) {
    switch (type) {
    default: return 0;
    case ukv_doc_field_null_k: return 0;
    case ukv_doc_field_bool_k: return 1;
    case ukv_doc_field_uuid_k: return 16;

    case ukv_doc_field_i8_k: return 1;
    case ukv_doc_field_i16_k: return 2;
    case ukv_doc_field_i32_k: return 4;
    case ukv_doc_field_i64_k: return 8;

    case ukv_doc_field_u8_k: return 1;
    case ukv_doc_field_u16_k: return 2;
    case ukv_doc_field_u32_k: return 4;
    case ukv_doc_field_u64_k: return 8;

    case ukv_doc_field_f16_k: return 2;
    case ukv_doc_field_f32_k: return 4;
    case ukv_doc_field_f64_k: return 8;

    // Offsets and lengths:
    case ukv_doc_field_bin_k: return 8;
    case ukv_doc_field_str_k: return 8;
    }
}

struct column_begin_t {
    ukv_octet_t* validities;
    ukv_octet_t* conversions;
    ukv_octet_t* collisions;
    ukv_byte_t* scalars;
    ukv_length_t* str_offsets;
    ukv_length_t* str_lengths;

    template <typename scalar_at>
    inline void set(std::size_t doc_idx, yyjson_val* value) noexcept {

        ukv_octet_t mask = static_cast<ukv_octet_t>(1 << (doc_idx % CHAR_BIT));
        ukv_octet_t& valid = validities[doc_idx / CHAR_BIT];
        ukv_octet_t& convert = conversions[doc_idx / CHAR_BIT];
        ukv_octet_t& collide = collisions[doc_idx / CHAR_BIT];
        scalar_at& scalar = reinterpret_cast<scalar_at*>(scalars)[doc_idx];

        json_to_scalar(value, mask, valid, convert, collide, scalar);
    }

    inline void set_str(std::size_t doc_idx,
                        yyjson_val* value,
                        printed_number_buffer_t& print_buffer,
                        safe_vector_gt<char>& output,
                        ukv_error_t* c_error) noexcept {

        ukv_octet_t mask = static_cast<ukv_octet_t>(1 << (doc_idx % CHAR_BIT));
        ukv_octet_t& valid = validities[doc_idx / CHAR_BIT];
        ukv_octet_t& convert = conversions[doc_idx / CHAR_BIT];
        ukv_octet_t& collide = collisions[doc_idx / CHAR_BIT];
        ukv_length_t& off = str_offsets[doc_idx];
        ukv_length_t& len = str_lengths[doc_idx];

        auto str = json_to_string(value, mask, valid, convert, collide, print_buffer);
        off = static_cast<ukv_length_t>(output.size());
        len = static_cast<ukv_length_t>(str.size());
        output.insert(output.size(), str.begin(), str.end(), c_error);
        return_on_error(c_error);
        output.push_back('\0', c_error);
    }
};

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

    ukv_doc_field_type_t const* c_types,
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

    if (!c_docs_count || !c_fields_count)
        return;

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);
    ukv_arena_t new_arena = &arena;

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
    strided_iterator_gt<ukv_doc_field_type_t const> types {c_types, c_types_stride};

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
    std::size_t bytes_per_scalars_row = transform_reduce_n(types, c_fields_count, 0ul, &doc_field_size_bytes);
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
            ukv_doc_field_type_t type = types[field_idx];
            switch (type) {
            case ukv_doc_field_str_k:
            case ukv_doc_field_bin_k:
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
            scalars_tape += doc_field_size_bytes(type) * c_docs_count;
        }
    }

    // Go though all the documents extracting and type-checking the relevant parts
    printed_number_buffer_t print_buffer;
    safe_vector_gt<char> string_tape(arena);
    for (ukv_size_t doc_idx = 0; doc_idx != c_docs_count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t doc = any_parse(binary_doc, internal_format_k, arena, c_error);
        return_on_error(c_error);
        if (!doc)
            continue;
        yyjson_val* root = yyjson_doc_get_root(doc.handle);

        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx) {

            // Find this field within document
            ukv_doc_field_type_t type = types[field_idx];
            ukv_str_view_t field = fields[field_idx];
            yyjson_val* found_value = json_lookup(root, field);

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

            case ukv_doc_field_bool_k: column.set<bool>(doc_idx, found_value); break;

            case ukv_doc_field_i8_k: column.set<std::int8_t>(doc_idx, found_value); break;
            case ukv_doc_field_i16_k: column.set<std::int16_t>(doc_idx, found_value); break;
            case ukv_doc_field_i32_k: column.set<std::int32_t>(doc_idx, found_value); break;
            case ukv_doc_field_i64_k: column.set<std::int64_t>(doc_idx, found_value); break;

            case ukv_doc_field_u8_k: column.set<std::uint8_t>(doc_idx, found_value); break;
            case ukv_doc_field_u16_k: column.set<std::uint16_t>(doc_idx, found_value); break;
            case ukv_doc_field_u32_k: column.set<std::uint32_t>(doc_idx, found_value); break;
            case ukv_doc_field_u64_k: column.set<std::uint64_t>(doc_idx, found_value); break;

            case ukv_doc_field_f32_k: column.set<float>(doc_idx, found_value); break;
            case ukv_doc_field_f64_k: column.set<double>(doc_idx, found_value); break;

            case ukv_doc_field_str_k: column.set_str(doc_idx, found_value, print_buffer, string_tape, c_error); break;
            case ukv_doc_field_bin_k: column.set_str(doc_idx, found_value, print_buffer, string_tape, c_error); break;

            default: break;
            }
        }
    }

    *c_result_strs_contents = reinterpret_cast<ukv_byte_t*>(string_tape.data());
}
