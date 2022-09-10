/**
 * @file logic_docs.cpp
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

#include <nlohmann/json.hpp>

#include "helpers.hpp"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

using json_t =
    nlohmann::basic_json<std::map, std::vector, std::string, bool, int64_t, uint64_t, double, polymorphic_allocator_gt>;
using json_ptr_t = json_t::json_pointer;

constexpr ukv_format_t internal_format_k = ukv_format_msgpack_k;
ukv_format_t ukv_format_docs_internal_k = internal_format_k;

static constexpr char const* true_k = "true";
static constexpr char const* false_k = "false";

// Both the variant and the vector wouldn't have `noexcept` default constructors
// if we didn't ingest @c `std::monostate` into the first and wrapped the second
// into an @c `std::optional`.
using heapy_field_t = std::variant<std::monostate, json_t::string_t, json_ptr_t>;
using heapy_fields_t = std::optional<std::vector<heapy_field_t>>;

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

value_view_t to_view(char const* str, std::size_t len) noexcept {
    auto ptr = reinterpret_cast<byte_t const*>(str);
    return {ptr, ptr + len};
}

struct export_to_value_t final : public nlohmann::detail::output_adapter_protocol<char>,
                                 public std::enable_shared_from_this<export_to_value_t> {
    safe_vector_gt<byte_t>* value_ptr = nullptr;
    ukv_error_t* c_error = nullptr;

    export_to_value_t() = default;
    export_to_value_t(safe_vector_gt<byte_t>& value) noexcept : value_ptr(&value) {}
    void write_character(char c) override { value_ptr->push_back(static_cast<byte_t>(c), c_error); }
    void write_characters(char const* s, std::size_t length) override {
        auto ptr = reinterpret_cast<byte_t const*>(s);
        value_ptr->insert(value_ptr->size(), ptr, ptr + length, c_error);
    }

    template <typename at>
    void write_scalar(at scalar) {
        write_characters(reinterpret_cast<char const*>(&scalar), sizeof(at));
    }
};

json_t& lookup_field( //
    json_t& json,
    ukv_str_view_t field,
    json_t& default_json) noexcept(false) {

    if (!field)
        return json;

    if (field[0] == '/') {
        // This libraries doesn't implement `find` for JSON-Pointers:
        json_ptr_t field_ptr {field};
        return json.contains(field_ptr) ? json.at(field_ptr) : default_json;
    }
    else {
        auto it = json.find(field);
        return it != json.end() ? it.value() : default_json;
    }
}

json_t parse_any( //
    value_view_t bytes,
    ukv_format_t const c_format,
    ukv_error_t* c_error) noexcept {

    json_t result;
    safe_section("Parsing document", c_error, [&] {
        auto str = reinterpret_cast<char const*>(bytes.begin());
        auto len = bytes.size();
        switch (c_format) {
        case ukv_format_json_patch_k:
        case ukv_format_json_merge_patch_k:
        case ukv_format_json_k: result = json_t::parse(str, str + len, nullptr, false, true); break;
        case ukv_format_msgpack_k: result = json_t::from_msgpack(str, str + len, false, false); break;
        case ukv_format_bson_k: result = json_t::from_bson(str, str + len, false, false); break;
        case ukv_format_cbor_k: result = json_t::from_cbor(str, str + len, false, false); break;
        case ukv_format_ubjson_k: result = json_t::from_ubjson(str, str + len, false, false); break;
        case ukv_format_binary_k:
            result = json_t::binary({reinterpret_cast<std::int8_t const*>(bytes.begin()),
                                     reinterpret_cast<std::int8_t const*>(bytes.end())});
            break;
        default: log_error(c_error, missing_feature_k, "Unsupported document format");
        }
    });
    return result;
}

/**
 * The JSON package provides a number of simple interfaces, which only work with simplest STL types
 * and always allocate the output objects, without the ability to reuse previously allocated memory,
 * including: `dump`, `to_msgpack`, `to_bson`, `to_cbor`, `to_ubjson`.
 * They have more flexible alternatives in the form of `nlohmann::detail::serializer`s,
 * that will accept our custom adapter. Unfortunately, they require a bogus shared pointer. WHY?!
 */
void dump_any( //
    json_t const& json,
    ukv_format_t const c_format,
    std::shared_ptr<export_to_value_t> const& value,
    ukv_error_t* c_error) noexcept {

    using text_serializer_t = nlohmann::detail::serializer<json_t>;
    using binary_serializer_t = nlohmann::detail::binary_writer<json_t, char>;

    safe_section("Dumping document", c_error, [&] {
        switch (c_format) {
        case ukv_format_json_patch_k:
        case ukv_format_json_merge_patch_k:
        case ukv_format_json_k: return text_serializer_t(value, ' ').dump(json, false, false, 0, 0);
        case ukv_format_msgpack_k: return binary_serializer_t(value).write_msgpack(json);
        case ukv_format_bson_k: return binary_serializer_t(value).write_bson(json);
        case ukv_format_cbor_k: return binary_serializer_t(value).write_cbor(json);
        case ukv_format_ubjson_k: return binary_serializer_t(value).write_ubjson(json, true, true);
        case ukv_format_binary_k: {
            switch (json.type()) {
            case json_t::value_t::null: break;
            case json_t::value_t::discarded: break;
            case json_t::value_t::object:
                log_error(c_error, 0, "Can't export a nested dictionary in binary form!");
                break;
            case json_t::value_t::array:
                log_error(c_error, 0, "Can't export a nested dictionary in binary form!");
                break;
            case json_t::value_t::binary: {
                json_t::binary_t const& str = json.get_ref<json_t::binary_t const&>();
                value->write_characters(reinterpret_cast<char const*>(str.data()), str.size());
                break;
            }
            case json_t::value_t::string: {
                json_t::string_t const& str = json.get_ref<json_t::string_t const&>();
                value->write_characters(str.data(), str.size());
                break;
            }
            case json_t::value_t::boolean: value->write_character(json.get<json_t::boolean_t>()); break;
            case json_t::value_t::number_integer: value->write_scalar(json.get<json_t::number_integer_t>()); break;
            case json_t::value_t::number_unsigned: value->write_scalar(json.get<json_t::number_unsigned_t>()); break;
            case json_t::value_t::number_float: value->write_scalar(json.get<json_t::number_float_t>()); break;
            default: log_error(c_error, 0, "Unsupported member type"); break;
            }
            return;
        }
        default: log_error(c_error, 0, "Unsupported output format"); return;
        }
    });
}

struct serializing_tape_ref_t {

    serializing_tape_ref_t(stl_arena_t& a, ukv_error_t* c_error) noexcept
        : arena_(a), single_doc_buffer_(&a), growing_tape(arena_), c_error(c_error) {
        safe_section("Allocating doc exporter", c_error, [&] {
            using allocator_t = std::pmr::polymorphic_allocator<export_to_value_t>;
            shared_exporter_ = std::allocate_shared<export_to_value_t, allocator_t>(&arena_.resource);
            shared_exporter_->value_ptr = &single_doc_buffer_;
            shared_exporter_->c_error = c_error;
        });
    }

    void push_back(json_t const& doc, ukv_format_t c_format) noexcept {

        single_doc_buffer_.clear();
        dump_any(doc, c_format, shared_exporter_, c_error);
        return_on_error(c_error);

        if ((c_format == ukv_format_json_k) |       //
            (c_format == ukv_format_json_patch_k) | //
            (c_format == ukv_format_json_merge_patch_k)) {
            single_doc_buffer_.push_back(byte_t {0}, c_error);
            return_on_error(c_error);
        }

        growing_tape.push_back(single_doc_buffer_, c_error);
        return_on_error(c_error);
    }

    embedded_bins_t view() noexcept { return growing_tape; }

  private:
    stl_arena_t& arena_;
    std::shared_ptr<export_to_value_t> shared_exporter_;
    safe_vector_gt<byte_t> single_doc_buffer_;

  public:
    growing_tape_t growing_tape;
    ukv_error_t* c_error = nullptr;
};

template <typename callback_at>
places_arg_t const& read_unique_docs( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    ukv_options_t const c_options,
    stl_arena_t& arena,
    ukv_error_t* c_error,
    callback_at callback) noexcept {

    ukv_arena_t arena_ptr = &arena;
    ukv_byte_t* found_binary_begin = nullptr;
    ukv_length_t* found_binary_offs = nullptr;
    ukv_read( //
        c_db,
        c_txn,
        places.count,
        places.cols_begin.get(),
        places.cols_begin.stride(),
        places.keys_begin.get(),
        places.keys_begin.stride(),
        c_options,
        nullptr,
        &found_binary_offs,
        nullptr,
        &found_binary_begin,
        &arena_ptr,
        c_error);

    auto found_binaries = joined_bins_t(found_binary_begin, found_binary_offs, places.count);
    auto found_binary_it = found_binaries.begin();

    for (std::size_t task_idx = 0; task_idx != places.size(); ++task_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t parsed = parse_any(binary_doc, internal_format_k, c_error);

        // This error is extremely unlikely, as we have previously accepted the data into the store.
        if (*c_error)
            return places;

        ukv_str_view_t field = places.fields_begin[task_idx];
        callback(task_idx, field, parsed);
    }

    return places;
}

/**
 * ! Returned object may not contain any fields, if multiple fields are requested from the same doc.
 */
template <typename callback_at>
places_arg_t read_docs( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    places_arg_t const& places,
    ukv_options_t const c_options,
    stl_arena_t& arena,
    ukv_error_t* c_error,
    callback_at callback) {

    // Handle the common case of requesting the non-colliding
    // all-ascending input sequences of document IDs received
    // during scans without the sort and extra memory.
    if (all_ascending(places.keys_begin, places.count))
        return read_unique_docs(c_db, c_txn, places, c_options, arena, c_error, callback);

    // If it's not one of the trivial consecutive lookups, we want
    // to sort & deduplicate the entries to minimize the random reads
    // from disk.
    auto unique_places = arena.alloc<col_key_t>(places.count, c_error);
    if (*c_error)
        return {};

    transform_n(places, places.count, unique_places, std::mem_fn(&place_t::col_key));
    unique_places = {unique_places.begin(), sort_and_deduplicate(unique_places.begin(), unique_places.end())};

    // There is a chance, all the entries are unique.
    // In such case, let's free-up the memory.
    if (unique_places.size() == places.count)
        return read_unique_docs(c_db, c_txn, places, c_options, arena, c_error, callback);

    // Otherwise, let's retrieve the sublist of unique docs,
    // which may be in a very different order from original.
    ukv_arena_t arena_ptr = &arena;
    ukv_byte_t* found_binary_begin = nullptr;
    ukv_length_t* found_binary_offs = nullptr;
    ukv_size_t unique_places_count = static_cast<ukv_size_t>(unique_places.size());
    auto unique_places_strided = strided_range(unique_places.begin(), unique_places.end()).immutable();
    auto cols = unique_places_strided.members(&col_key_t::col);
    auto keys = unique_places_strided.members(&col_key_t::key);
    ukv_read( //
        c_db,
        c_txn,
        unique_places_count,
        cols.begin().get(),
        cols.begin().stride(),
        keys.begin().get(),
        keys.begin().stride(),
        c_options,
        nullptr,
        &found_binary_offs,
        nullptr,
        &found_binary_begin,
        &arena_ptr,
        c_error);
    if (*c_error)
        return {};

    // We will later need to locate the data for every separate request.
    // Doing it in O(N) tape iterations every time is too slow.
    // Once we transform to inclusive sums, it will be O(1).
    //      inplace_inclusive_prefix_sum(found_binary_lens, found_binary_lens + found_binary_count);
    // Alternatively we can compensate it with additional memory:
    std::optional<std::vector<json_t>> parsed_docs;
    try {
        parsed_docs = std::vector<json_t>(places.count);
    }
    catch (std::bad_alloc const&) {
        *c_error = "Out of memory!";
        return places;
    }

    // Parse all the unique documents
    auto found_binaries = joined_bins_t(found_binary_begin, found_binary_offs, places.count);
    auto found_binary_it = found_binaries.begin();
    for (ukv_size_t doc_idx = 0; doc_idx != unique_places_count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t& parsed = (*parsed_docs)[doc_idx];
        parsed = parse_any(binary_doc, internal_format_k, c_error);

        // This error is extremely unlikely, as we have previously accepted the data into the store.
        if (*c_error)
            return places;
    }

    // Join docs and fields with binary search
    for (std::size_t task_idx = 0; task_idx != places.size(); ++task_idx) {
        auto place = places[task_idx];
        auto parsed_idx = offset_in_sorted(unique_places, place.col_key());
        json_t& parsed = (*parsed_docs)[parsed_idx];
        callback(task_idx, place.field, parsed);
    }

    return {cols.begin(), keys.begin(), {}, unique_places_count};
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

    serializing_tape_ref_t serializing_tape {arena, c_error};
    return_on_error(c_error);
    auto& growing_tape = serializing_tape.growing_tape;
    growing_tape.reserve(places.count, c_error);
    return_on_error(c_error);

    for (std::size_t doc_idx = 0; doc_idx != places.size(); ++doc_idx) {
        auto place = places[doc_idx];
        auto content = contents[doc_idx];
        auto parsed = parse_any(content, c_format, c_error);
        return_on_error(c_error);

        if (parsed.is_discarded()) {
            *c_error = "Couldn't parse inputs";
            return;
        }

        serializing_tape.push_back(parsed, internal_format_k);
        return_on_error(c_error);
    }

    auto tape_begin = growing_tape.contents().begin().get();
    ukv_byte_t* tape_begin_punned = reinterpret_cast<ukv_byte_t*>(tape_begin);
    ukv_arena_t arena_ptr = &arena;
    ukv_write( //
        c_db,
        c_txn,
        places.count,
        places.cols_begin.get(),
        places.cols_begin.stride(),
        places.keys_begin.get(),
        places.keys_begin.stride(),
        nullptr,
        growing_tape.offsets().begin().get(),
        growing_tape.offsets().stride(),
        growing_tape.lengths().begin().get(),
        growing_tape.lengths().stride(),
        &tape_begin_punned,
        0,
        c_options,
        &arena_ptr,
        c_error);
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

    serializing_tape_ref_t serializing_tape {arena, c_error};
    auto safe_callback = [&](ukv_size_t task_idx, ukv_str_view_t field, json_t& parsed) {
        try {
            json_t parsed_task = parse_any(contents[task_idx], c_format, c_error);
            return_on_error(c_error);

            // Apply the patch
            json_t null_object;
            json_t& parsed_part = lookup_field(parsed, field, null_object);
            if (&parsed != &null_object) {
                switch (c_format) {
                case ukv_format_json_patch_k: parsed_part = parsed_part.patch(parsed_task); break;
                case ukv_format_json_merge_patch_k: parsed_part.merge_patch(parsed_task); break;
                default: parsed_part = parsed_task; break;
                }
            }
            else if (c_format != ukv_format_json_patch_k && c_format != ukv_format_json_merge_patch_k) {
                json_t::string_t heapy_field {field};
                parsed = parsed.flatten();
                parsed.emplace(heapy_field, parsed_task);
                parsed = parsed.unflatten();
            }

            // Save onto output tape
            serializing_tape.push_back(parsed_part, internal_format_k);
            return_on_error(c_error);
        }
        catch (std::bad_alloc const&) {
            *c_error = "Out of memory!";
        }
    };
    places_arg_t read_order = read_docs( //
        c_db,
        c_txn,
        places,
        c_options,
        arena,
        c_error,
        safe_callback);

    // By now, the tape contains concatenated updates docs:
    ukv_size_t unique_places_count = static_cast<ukv_size_t>(read_order.size());
    ukv_byte_t* found_binary_begin =
        reinterpret_cast<ukv_byte_t*>(serializing_tape.growing_tape.contents().begin().get());
    ukv_arena_t arena_ptr = &arena;
    ukv_write( //
        c_db,
        c_txn,
        unique_places_count,
        read_order.cols_begin.get(),
        read_order.cols_begin.stride(),
        read_order.keys_begin.get(),
        read_order.keys_begin.stride(),
        nullptr,
        serializing_tape.growing_tape.offsets().begin().get(),
        serializing_tape.growing_tape.offsets().stride(),
        serializing_tape.growing_tape.lengths().begin().get(),
        serializing_tape.growing_tape.lengths().stride(),
        &found_binary_begin,
        0,
        c_options,
        &arena_ptr,
        c_error);
}

void parse_fields( //
    strided_iterator_gt<ukv_str_view_t const> fields,
    ukv_size_t n,
    heapy_fields_t& fields_parsed,
    ukv_error_t* c_error) noexcept {

    try {
        fields_parsed = std::vector<heapy_field_t>(n);
        ukv_str_view_t joined_fields_ptr = *fields;
        for (ukv_size_t field_idx = 0; field_idx != n; ++field_idx) {
            ukv_str_view_t field = fields.repeats() ? joined_fields_ptr : fields[field_idx];
            if (!field && (*c_error = "NULL JSON-Pointers are not allowed!"))
                return;

            heapy_field_t& field_parsed = (*fields_parsed)[field_idx];
            if (field[0] == '/')
                field_parsed = json_ptr_t {field};
            else
                field_parsed = std::string {field};

            joined_fields_ptr += std::strlen(field) + 1;
        }
    }
    catch (nlohmann::json::parse_error const&) {
        log_error(c_error, args_wrong_k, "Inappropriate field path!");
    }
    catch (std::bad_alloc const&) {
        log_error(c_error, out_of_memory_k, "");
    }
}

void ukv_docs_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

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
            c_cols,
            c_cols_stride,
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

    strided_iterator_gt<ukv_collection_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_byte_t* const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_length_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_lens, c_lens_stride};
    strided_iterator_gt<ukv_octet_t const> presences {c_presences, sizeof(ukv_octet_t)};

    places_arg_t places {cols, keys, fields, c_tasks_count};
    contents_arg_t contents {vals, offs, lens, presences, c_tasks_count};

    auto func = has_fields || c_format == ukv_format_json_patch_k || c_format == ukv_format_json_merge_patch_k
                    ? &read_modify_write
                    : &replace_docs;

    func(c_db, c_txn, places, contents, c_options, c_format, arena, c_error);
}

void ukv_docs_read( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

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
            c_cols,
            c_cols_stride,
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

    strided_iterator_gt<ukv_collection_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {cols, keys, fields, c_tasks_count};

    // Now, we need to parse all the entries to later export them into a target format.
    // Potentially sampling certain sub-fields again along the way.
    serializing_tape_ref_t serializing_tape {arena, c_error};
    json_t null_object;

    auto safe_callback = [&](ukv_size_t, ukv_str_view_t field, json_t& parsed) {
        try {
            json_t& parsed_part = lookup_field(parsed, field, null_object);
            serializing_tape.push_back(parsed_part, c_format);
            return_on_error(c_error);
        }
        catch (std::bad_alloc const&) {
            *c_error = "Out of memory!";
        }
    };
    read_docs(c_db, c_txn, places, c_options, arena, c_error, safe_callback);

    auto serialized_view = serializing_tape.view();
    *c_found_values = reinterpret_cast<ukv_byte_t*>(serialized_view.contents());
    *c_found_offsets = serialized_view.offsets();
    *c_found_lengths = serialized_view.lengths();
}

/*********************************************************/
/*****************	 Tabular Exports	  ****************/
/*********************************************************/

void ukv_docs_gist( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_docs_count,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

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
        c_cols,
        c_cols_stride,
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

    strided_iterator_gt<ukv_collection_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};

    joined_bins_t found_binaries {found_binary_begin, found_binary_offs, c_docs_count};
    joined_bins_iterator_t found_binary_it = found_binaries.begin();

    // Export all the elements into a heap-allocated hash-set, keeping only unique entries
    std::optional<std::unordered_set<std::string>> paths;
    try {
        paths = std::unordered_set<std::string> {};
        for (ukv_size_t doc_idx = 0; doc_idx != c_docs_count; ++doc_idx, ++found_binary_it) {
            value_view_t binary_doc = *found_binary_it;
            json_t parsed = parse_any(binary_doc, internal_format_k, c_error);
            return_on_error(c_error);

            json_t parsed_flat = parsed.flatten();
            paths->reserve(paths->size() + parsed_flat.size());
            for (auto& pair : parsed_flat.items())
                paths->emplace(pair.key());
        }
    }
    catch (std::bad_alloc const&) {
        *c_error = "Out of memory!";
        return;
    }

    // Estimate the final memory consumption on-tape and export offsets
    span_gt<ukv_length_t> offs = arena.alloc<ukv_length_t>(paths->size() + 1, c_error);
    return_on_error(c_error);

    ukv_length_t total_length = 0;
    ukv_length_t* prefix_length = offs.begin();
    for (auto const& path : *paths) {
        *prefix_length = total_length;
        total_length += path.size() + 1;
        ++prefix_length;
    }
    *prefix_length = total_length;

    // Reserve memory
    span_gt<byte_t> tape = arena.alloc<byte_t>(total_length, c_error);
    return_on_error(c_error);

    // Export on to the tape
    byte_t* tape_ptr = tape.begin();
    *c_found_fields_count = static_cast<ukv_size_t>(paths->size());
    *c_found_offsets = reinterpret_cast<ukv_length_t*>(offs.begin());
    *c_found_fields = reinterpret_cast<ukv_str_view_t>(tape_ptr);
    for (auto const& path : *paths)
        std::memcpy(std::exchange(tape_ptr, tape_ptr + path.size() + 1), path.c_str(), path.size() + 1);
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
        //  std::to_chars_result result = std::to_chars(&print_buf[0], print_buf + print_buf_len_k,
        //  scalar); bool fits_null_terminated = result.ec != std::errc() && result.ptr < print_buf +
        //  print_buf_len_k;
        // Using FMT would cause an extra dependency:
        //  auto end_ptr = fmt::format_to(print_buf, "{}", scalar);
        //  bool fits_null_terminated = end_ptr < print_buf + print_buf_len_k;
        // If we use `std::snprintf`, the result would be NULL-terminated:
        auto result = std::snprintf(begin, end - begin, "%f", scalar);
        return result >= 0 ? std::to_chars_result {begin + result - 1, std::errc()}
                           : std::to_chars_result {begin, std::errc::invalid_argument};
    }
    else
        return std::to_chars(begin, end, scalar);
}

template <typename scalar_at>
void export_scalar_column(json_t const& value, size_t doc_idx, column_begin_t column) {

    // Bitmaps are indexed from the last bit within every byte
    // https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    ukv_octet_t mask_bitmap = static_cast<ukv_octet_t>(1 << (doc_idx % CHAR_BIT));
    ukv_octet_t& ref_valid = column.validities[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_convert = column.conversions[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_collide = column.collisions[doc_idx / CHAR_BIT];
    scalar_at& ref_scalar = reinterpret_cast<scalar_at*>(column.scalars)[doc_idx];

    switch (value.type()) {
    case json_t::value_t::null:
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid &= ~mask_bitmap;
        break;
    case json_t::value_t::discarded:
    case json_t::value_t::object:
    case json_t::value_t::array:
        ref_convert &= ~mask_bitmap;
        ref_collide |= mask_bitmap;
        ref_valid &= ~mask_bitmap;
        break;
    case json_t::value_t::binary: {
        json_t::binary_t const& str = value.get_ref<json_t::binary_t const&>();
        if (str.size() == sizeof(scalar_at)) {
            ref_convert |= mask_bitmap;
            ref_collide &= ~mask_bitmap;
            ref_valid |= mask_bitmap;
            std::memcpy(&ref_scalar, str.data(), sizeof(scalar_at));
        }
        else {
            ref_convert &= ~mask_bitmap;
            ref_collide |= mask_bitmap;
            ref_valid &= ~mask_bitmap;
        }
        break;
    }
    case json_t::value_t::string: {
        json_t::string_t const& str = value.get_ref<json_t::string_t const&>();
        std::from_chars_result result = from_chars(str.data(), str.data() + str.size(), ref_scalar);
        bool entire_string_is_number = result.ec == std::errc() && result.ptr == str.data() + str.size();
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
    case json_t::value_t::boolean:
        ref_scalar = value.get<json_t::boolean_t>();
        if constexpr (std::is_same_v<scalar_at, bool>)
            ref_convert &= ~mask_bitmap;
        else
            ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    case json_t::value_t::number_integer:
        ref_scalar = static_cast<scalar_at>(value.get<json_t::number_integer_t>());
        if constexpr (std::is_integral_v<scalar_at> && std::is_signed_v<scalar_at>)
            ref_convert &= ~mask_bitmap;
        else
            ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    case json_t::value_t::number_unsigned:
        ref_scalar = static_cast<scalar_at>(value.get<json_t::number_unsigned_t>());
        if constexpr (std::is_unsigned_v<scalar_at>)
            ref_convert &= ~mask_bitmap;
        else
            ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    case json_t::value_t::number_float:
        ref_scalar = static_cast<scalar_at>(value.get<json_t::number_float_t>());
        if constexpr (std::is_floating_point_v<scalar_at>)
            ref_convert &= ~mask_bitmap;
        else
            ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    }
}

template <typename scalar_at, typename alloc_at = std::allocator<scalar_at>>
ukv_length_t print_scalar(scalar_at scalar, std::vector<byte_t, alloc_at>& output) {

    /// The length of buffer to be used to convert/format/print numerical values into strings.
    constexpr std::size_t print_buf_len_k = 32;
    /// The on-stack buffer to be used to convert/format/print numerical values into strings.
    char print_buf[print_buf_len_k];

    std::to_chars_result result = to_chars(print_buf, print_buf + print_buf_len_k, scalar);
    bool fits_null_terminated = result.ec == std::errc() && result.ptr + 1 < print_buf + print_buf_len_k;
    if (fits_null_terminated) {
        *result.ptr = '\0';
        auto view = to_view(print_buf, result.ptr + 1 - print_buf);
        output.insert(output.end(), view.begin(), view.end());
        return static_cast<ukv_length_t>(view.size());
    }
    else
        return ukv_length_missing_k;
}

template <typename alloc_at = std::allocator<byte_t>>
void export_string_column(json_t const& value,
                          size_t doc_idx,
                          column_begin_t column,
                          std::vector<byte_t, alloc_at>& output) {

    // Bitmaps are indexed from the last bit within every byte
    // https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
    ukv_octet_t mask_bitmap = static_cast<ukv_octet_t>(1 << (doc_idx % CHAR_BIT));
    ukv_octet_t& ref_valid = column.validities[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_convert = column.conversions[doc_idx / CHAR_BIT];
    ukv_octet_t& ref_collide = column.collisions[doc_idx / CHAR_BIT];
    ukv_length_t& ref_off = column.str_offsets[doc_idx];
    ukv_length_t& ref_len = column.str_lengths[doc_idx];

    ref_off = static_cast<ukv_length_t>(output.size());

    switch (value.type()) {
    case json_t::value_t::null:
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid &= ~mask_bitmap;
        ref_off = ref_len = ukv_length_missing_k;
        break;
    case json_t::value_t::discarded:
    case json_t::value_t::object:
    case json_t::value_t::array:
        ref_convert &= ~mask_bitmap;
        ref_collide |= mask_bitmap;
        ref_valid &= ~mask_bitmap;
        ref_off = ref_len = ukv_length_missing_k;
        break;

    case json_t::value_t::binary: {
        json_t::binary_t const& str = value.get_ref<json_t::binary_t const&>();
        ref_len = static_cast<ukv_length_t>(str.size());
        auto view = to_view((char*)str.data(), str.size());
        output.insert(output.end(), view.begin(), view.end());
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    }
    case json_t::value_t::string: {
        json_t::string_t const& str = value.get_ref<json_t::string_t const&>();
        ref_len = static_cast<ukv_length_t>(str.size());
        auto view = to_view((char*)str.data(), str.size() + 1);
        output.insert(output.end(), view.begin(), view.end());
        ref_convert &= ~mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    }
    case json_t::value_t::boolean: {
        if (value.get<json_t::boolean_t>()) {
            ref_len = 5;
            output.insert(output.end(),
                          reinterpret_cast<byte_t const*>(true_k),
                          reinterpret_cast<byte_t const*>(true_k) + 5);
        }
        else {
            ref_len = 6;
            output.insert(output.end(),
                          reinterpret_cast<byte_t const*>(false_k),
                          reinterpret_cast<byte_t const*>(false_k) + 6);
        }
        ref_convert |= mask_bitmap;
        ref_collide &= ~mask_bitmap;
        ref_valid |= mask_bitmap;
        break;
    }
    case json_t::value_t::number_integer:
        ref_len = print_scalar(value.get<json_t::number_integer_t>(), output);
        ref_convert |= mask_bitmap;
        ref_collide = ref_len != ukv_length_missing_k ? (ref_collide & ~mask_bitmap) : (ref_collide | mask_bitmap);
        ref_valid = ref_len == ukv_length_missing_k ? (ref_valid & ~mask_bitmap) : (ref_valid | mask_bitmap);
        break;

    case json_t::value_t::number_unsigned:
        ref_len = print_scalar(value.get<json_t::number_unsigned_t>(), output);
        ref_convert |= mask_bitmap;
        ref_collide = ref_len != ukv_length_missing_k ? (ref_collide & ~mask_bitmap) : (ref_collide | mask_bitmap);
        ref_valid = ref_len == ukv_length_missing_k ? (ref_valid & ~mask_bitmap) : (ref_valid | mask_bitmap);
        break;
    case json_t::value_t::number_float:
        ref_len = print_scalar(value.get<json_t::number_float_t>(), output);
        ref_convert |= mask_bitmap;
        ref_collide = ref_len != ukv_length_missing_k ? (ref_collide & ~mask_bitmap) : (ref_collide | mask_bitmap);
        ref_valid = ref_len == ukv_length_missing_k ? (ref_valid & ~mask_bitmap) : (ref_valid | mask_bitmap);
        break;
    }
}

void ukv_docs_gather( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_docs_count,
    ukv_size_t const c_fields_count,

    ukv_collection_t const* c_cols,
    ukv_size_t const c_cols_stride,

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
        c_cols,
        c_cols_stride,
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

    strided_iterator_gt<ukv_collection_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
    strided_iterator_gt<ukv_type_t const> types {c_types, c_types_stride};

    joined_bins_t found_binaries {found_binary_begin, found_binary_offs, c_docs_count};
    joined_bins_iterator_t found_binary_it = found_binaries.begin();

    // Parse all the field names
    heapy_fields_t heapy_fields(std::nullopt);
    parse_fields(fields, c_fields_count, heapy_fields, c_error);
    return_on_error(c_error);

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
    auto first_col_validities = reinterpret_cast<ukv_octet_t*>(tape_ptr + bytes_for_addresses);
    auto first_col_conversions = wants_conversions //
                                     ? first_col_validities + slots_per_bitmap * c_fields_count
                                     : first_col_validities;
    auto first_col_collisions = wants_collisions //
                                    ? first_col_conversions + slots_per_bitmap * c_fields_count
                                    : first_col_validities;
    auto first_col_scalars = reinterpret_cast<ukv_byte_t*>(tape_ptr + bytes_for_addresses + bytes_for_bitmaps);

    // 1, 2, 3. Export validity maps addresses
    std::size_t tape_progress = 0;
    {
        auto addresses = *c_result_bitmap_valid = reinterpret_cast<ukv_octet_t**>(tape_ptr + tape_progress);
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx)
            addresses[field_idx] = first_col_validities + field_idx * slots_per_bitmap;
        tape_progress += bytes_per_addresses_row;
    }
    if (wants_conversions) {
        auto addresses = *c_result_bitmap_converted = reinterpret_cast<ukv_octet_t**>(tape_ptr + tape_progress);
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx)
            addresses[field_idx] = first_col_conversions + field_idx * slots_per_bitmap;
        tape_progress += bytes_per_addresses_row;
    }
    if (wants_collisions) {
        auto addresses = *c_result_bitmap_collision = reinterpret_cast<ukv_octet_t**>(tape_ptr + tape_progress);
        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx)
            addresses[field_idx] = first_col_collisions + field_idx * slots_per_bitmap;
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

        auto scalars_tape = first_col_scalars;
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

    // Prepare constant values
    json_t const null_object;

    std::pmr::vector<byte_t> string_tape(&arena.resource);
    // Go though all the documents extracting and type-checking the relevant parts
    for (ukv_size_t doc_idx = 0; doc_idx != c_docs_count; ++doc_idx, ++found_binary_it) {
        value_view_t binary_doc = *found_binary_it;
        json_t parsed = parse_any(binary_doc, internal_format_k, c_error);
        return_on_error(c_error);

        for (ukv_size_t field_idx = 0; field_idx != c_fields_count; ++field_idx) {

            // Find this field within document
            ukv_type_t type = types[field_idx];
            heapy_field_t const& name_or_path = (*heapy_fields)[field_idx];
            json_t::iterator found_value_it = parsed.end();
            json_t const& found_value =
                name_or_path.index() == 2 //
                    ?
                    // This libraries doesn't implement `find` for JSON-Pointers:
                    (parsed.contains(std::get<2>(name_or_path)) //
                         ? parsed.at(std::get<2>(name_or_path))
                         : null_object)
                    // But with simple names we can query members with iterators:
                    : ((found_value_it = parsed.find(std::get<1>(name_or_path))) != parsed.end() //
                           ? found_value_it.value()
                           : null_object);

            column_begin_t column {
                .validities = (*c_result_bitmap_valid)[field_idx],
                .conversions = (*c_result_bitmap_converted)[field_idx],
                .collisions = (*c_result_bitmap_collision)[field_idx],
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

            case ukv_type_str_k: export_string_column(found_value, doc_idx, column, string_tape); break;
            case ukv_type_bin_k: export_string_column(found_value, doc_idx, column, string_tape); break;

            default: break;
            }
        }
    }

    *c_result_strs_contents = reinterpret_cast<ukv_byte_t*>(string_tape.data());
}
