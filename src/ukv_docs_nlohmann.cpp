/**
 * @file ukv_stl.cpp
 * @author Ashot Vardanian
 *
 * @brief Document Store implementation on top of "nlohmann/JSON".
 */

#include <vector>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "ukv_docs.h"
#include "helpers.hpp"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

using json_t = nlohmann::json;
using json_ptr_t = json_t::json_pointer;
using serializer_t = nlohmann::detail::serializer<json_t>;

/**
 * @brief Extracts a select subset of keys by from input document.
 *
 * Can be implemented through flattening, sampling and unflattening.
 * https://json.nlohmann.me/api/json_pointer/
 */
json_t sample_fields(json_t&& original,
                     std::vector<json_ptr_t> const& json_pointers,
                     std::vector<std::string> const& json_pointers_strs) {

    if (json_pointers.empty())
        return std::move(original);

    json_t empty {nullptr};
    json_t result = json_t::object();
    for (size_t ptr_idx = 0; ptr_idx != json_pointers.size(); ++ptr_idx) {

        auto const& ptr = json_pointers[ptr_idx];
        auto const& ptr_str = json_pointers_strs[ptr_idx];

        // An exception-safe approach to searching for JSON-pointers:
        // https://json.nlohmann.me/api/basic_json/at/#exceptions
        // https://json.nlohmann.me/api/basic_json/operator%5B%5D/#exceptions
        // https://json.nlohmann.me/api/basic_json/value/#exception-safety
        auto found = original.value(ptr, empty);
        if (found != empty)
            result[ptr_str] = std::move(found);
    }

    // https://json.nlohmann.me/features/json_pointer/
    // https://json.nlohmann.me/api/basic_json/flatten/
    // https://json.nlohmann.me/api/basic_json/unflatten/
    return result.unflatten();
}

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

json_t parse_any(ukv_tape_ptr_t bytes, ukv_val_len_t len, ukv_format_t const c_format, ukv_error_t* c_error) {
    auto str = reinterpret_cast<char const*>(bytes);
    switch (c_format) {
    case ukv_format_json_k:
    case ukv_format_json_patch_k: return json_t::parse(str, str + len, nullptr, true, false);
    case ukv_format_msgpack_k: return json_t::from_msgpack(str, str + len, true, false);
    case ukv_format_bson_k: return json_t::from_bson(str, str + len, true, false);
    case ukv_format_cbor_k: return json_t::from_cbor(str, str + len, true, false);
    case ukv_format_ubjson_k: return json_t::from_ubjson(str, str + len, true, false);
    default: *c_error = "Unsupported unput format"; return {};
    }
}

buffer_t dump_any(json_t const& json, ukv_format_t const c_format, ukv_error_t* c_error) {
    buffer_t result;
    // Yes, it's a dirty hack, but it works :)
    // nlohmann::detail::output_vector_adapter<byte_t> output(result);
    auto& result_chars = reinterpret_cast<std::vector<char>&>(result);
    switch (c_format) {
    case ukv_format_json_k: {
        auto adapt = std::make_shared<nlohmann::detail::output_vector_adapter<char>>(result_chars);
        serializer_t(adapt, ' ').dump(json, false, false, 0, 0);
        break;
    }
    case ukv_format_msgpack_k: json_t::to_msgpack(json, result_chars); break;
    case ukv_format_bson_k: json_t::to_bson(json, result_chars); break;
    case ukv_format_cbor_k: json_t::to_cbor(json, result_chars); break;
    case ukv_format_ubjson_k: json_t::to_ubjson(json, result_chars); break;
    default: *c_error = "Unsupported unput format"; break;
    }

    return result;
}

void ukv_docs_write( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_str_view_t const* c_fields,
    ukv_size_t const c_fields_count,
    ukv_size_t const c_fields_stride,

    ukv_options_t const c_options,
    ukv_format_t const c_format,

    ukv_tape_ptr_t const* c_values,
    ukv_size_t const c_values_stride,

    ukv_val_len_t const* c_lengths,
    ukv_size_t const c_lengths_stride,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {

    std::vector<json_t> jsons(c_keys_count);

    if (c_fields_count) {
        strided_ptr_gt<ukv_str_view_t const> fields {c_fields, c_fields_stride};
        std::vector<std::string> fields_strs;
        std::vector<json_ptr_t> fields_ptrs;
    }

    std::vector<buffer_t> msgpacks(c_keys_count);

    if (jsons[0].is_discarded()) {
        *c_error = "Couldn't parse inputs";
        return;
    }
}

void ukv_docs_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_count,
    ukv_size_t const c_keys_stride,

    ukv_str_view_t const* c_fields,
    ukv_size_t const c_fields_count,
    ukv_size_t const c_fields_stride,

    ukv_options_t const c_options,
    ukv_format_t const c_format,

    ukv_tape_ptr_t* c_tape,
    ukv_size_t* c_capacity,
    ukv_error_t* c_error) {
}