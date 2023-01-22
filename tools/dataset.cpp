
#include <fcntl.h>     // `open` files
#include <sys/stat.h>  // `stat` to obtain file metadata
#include <sys/mman.h>  // `mmap` to read datasets faster
#include <uuid/uuid.h> // `uuid` to make file name
#include <unistd.h>    // `close` files

#include <vector>
#include <cstring>
#include <numeric>
#include <algorithm>
#include <filesystem>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <arrow/csv/writer.h>
#include <arrow/memory_pool.h>
#include <parquet/arrow/reader.h>
#include <parquet/stream_writer.h>
#include <arrow/compute/api_aggregate.h>

#include <simdjson.h>
#include <fmt/format.h>

#include <ukv/ukv.hpp>

#include "dataset.h"
#include "../benchmarks/mixed.hpp"
#include "helpers/linked_memory.hpp" // `linked_memory_lock_t`

#include <ukv/cpp/ranges.hpp>
#include <ukv/cpp/blobs_range.hpp> // `keys_stream_t`

using namespace unum::ukv::bench;
using namespace unum::ukv;

// 2 vertices and 1 edge
constexpr std::size_t vertices_edge_k = 3;
// Count of symbols to make json ('"', '"', ':', ',')
constexpr std::size_t symbols_count_k = 4;
// Length of generated uuid
constexpr std::size_t uuid_length_k = 36;
// Json object open brackets for json and parquet
constexpr ukv_str_view_t prefix_k = "{";
// Json object open brackets for csv
constexpr ukv_str_view_t csv_prefix_k = "\"{";

using tape_t = ptr_range_gt<char>;
using fields_t = strided_iterator_gt<ukv_str_view_t const>;
using keys_length_t = std::pair<ukv_key_t*, ukv_size_t>;
using val_t = std::pair<ukv_bytes_ptr_t, ukv_size_t>;
using chunked_array_t = std::shared_ptr<arrow::ChunkedArray>;
using array_t = std::shared_ptr<arrow::Array>;

#pragma region - Helpers

template <typename at>
class arena_allocator_gt {
  public:
    using value_type = at;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using reference = value_type&;
    using const_reference = value_type const&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename other_at>
    struct rebind {
        using other = arena_allocator_gt<other_at>;
    };

    inline explicit arena_allocator_gt()
        : arena_(linked_memory(nullptr, ukv_options_default_k, nullptr)), error_(nullptr) {}
    inline ~arena_allocator_gt() {}
    inline explicit arena_allocator_gt(arena_allocator_gt const& other) : arena_(other.arena_), error_(other.error_) {}
    template <typename other_at>
    inline explicit arena_allocator_gt(arena_allocator_gt<other_at> const& other)
        : arena_(other.arena_), error_(other.error_) {}
    inline explicit arena_allocator_gt(linked_memory_lock_t const& arena, ukv_error_t* error)
        : arena_(arena), error_(error) {}

    inline const_pointer address(const_reference r) { return &r; }
    inline pointer address(reference r) { return &r; }

    inline pointer allocate(size_type sz, typename std::allocator<void>::const_pointer = nullptr) {
        return arena_.alloc<value_type>(sz, error_).begin();
    }
    inline void deallocate(pointer p, size_type) { p->~value_type(); }
    inline size_type max_size() const { return std::numeric_limits<size_type>::max() / sizeof(value_type); }
    inline void construct(pointer p, value_type const& t) { new (p) value_type(t); }
    inline void destroy(pointer p) { p->~value_type(); }
    inline bool operator==(arena_allocator_gt const&) { return true; }
    inline bool operator!=(arena_allocator_gt const& a) { return !operator==(a); }

  private:
    linked_memory_lock_t arena_;
    ukv_error_t* error_;
};

template <typename at>
using alloc_t = arena_allocator_gt<at>;

template <typename at>
using stl_vector_t = std::vector<at, alloc_t<at>>;
using edges_t = stl_vector_t<edge_t>;
using docs_t = stl_vector_t<value_view_t>;
using keys_t = stl_vector_t<keys_length_t>;
using vals_t = stl_vector_t<val_t>;

class arrow_visitor_t {
  public:
    arrow_visitor_t(std::string& json) : json(json) {}

    arrow::Status Visit(arrow::NullArray const& arr) {
        fmt::format_to(std::back_inserter(json), "\"\",");
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::BooleanArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Int8Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Int16Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Int32Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Int64Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::UInt8Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::UInt16Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::UInt32Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::UInt64Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::HalfFloatArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::FloatArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::DoubleArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::StringArray const& arr) { return format_bin_str(arr, idx); }
    arrow::Status Visit(arrow::BinaryArray const& arr) { return format_bin_str(arr, idx); }
    arrow::Status Visit(arrow::LargeStringArray const& arr) { return format_bin_str(arr, idx); }
    arrow::Status Visit(arrow::LargeBinaryArray const& arr) { return format_bin_str(arr, idx); }
    arrow::Status Visit(arrow::FixedSizeBinaryArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Date32Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Date64Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Time32Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Time64Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::TimestampArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::DayTimeIntervalArray const& arr) {
        auto ds = arr.Value(idx);
        fmt::format_to(std::back_inserter(json), "{{\"days\":{},\"ms-s\":{}}},", ds.days, ds.milliseconds);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::MonthDayNanoIntervalArray const& arr) {
        auto mdn = arr.Value(idx);
        fmt::format_to(std::back_inserter(json),
                       "{{\"months\":{},\"days\":{},\"us-s\":{}}},",
                       mdn.months,
                       mdn.days,
                       mdn.nanoseconds);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::MonthIntervalArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::DurationArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Decimal128Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Decimal256Array const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::ListArray const& arr) {
        arrow::VisitArrayInline(*arr.values().get(), this);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::LargeListArray const& arr) {
        arrow::VisitArrayInline(*arr.values().get(), this);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::MapArray const& arr) {
        arrow::VisitArrayInline(*arr.values().get(), this);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::FixedSizeListArray const& arr) {
        arrow::VisitArrayInline(*arr.values().get(), this);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::DictionaryArray const& arr) {
        fmt::format_to(std::back_inserter(json), "{},", arr.GetValueIndex(idx));
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::ExtensionArray const& arr) {
        arrow::VisitArrayInline(*arr.storage().get(), this);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::StructArray const& arr) {
        return arrow::Status(arrow::StatusCode::TypeError, "Not supported type");
    }
    arrow::Status Visit(arrow::SparseUnionArray const& arr) {
        return arrow::Status(arrow::StatusCode::TypeError, "Not supported type");
    }
    arrow::Status Visit(arrow::DenseUnionArray const& arr) {
        return arrow::Status(arrow::StatusCode::TypeError, "Not supported type");
    }

    std::string& json;
    size_t idx = 0;

  private:
    inline static constexpr char int_to_hex_k[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    inline void char_to_hex(uint8_t const c, uint8_t* hex) noexcept {
        hex[0] = int_to_hex_k[c >> 4];
        hex[1] = int_to_hex_k[c & 0x0F];
    }

    template <typename at>
    arrow::Status format(at const& cont, size_t idx) {
        fmt::format_to(std::back_inserter(json), "{},", cont.Value(idx));
        return arrow::Status::OK();
    }

    template <typename at>
    arrow::Status format_bin_str(at const& cont, size_t idx) {
        auto str = cont.Value(idx);
        std::string output;
        output.reserve(str.size());
        for (std::size_t i = 0; i != str.size(); ++i) {
            uint8_t c = str[i];
            switch (c) {
            case 34: output += "\\\""; break;
            case 92: output += "\\\\"; break;
            case 8: output += "\\b"; break;
            case 9: output += "\\t"; break;
            case 10: output += "\\n"; break;
            case 12: output += "\\f"; break;
            case 13: output += "\\r"; break;
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 11:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 22:
            case 23:
            case 24:
            case 25:
            case 26:
            case 27:
            case 28:
            case 29:
            case 30:
            case 31: {
                output += "\\u0000";
                auto target_ptr = reinterpret_cast<uint8_t*>(output.data() + output.size() - 2);
                char_to_hex(c, target_ptr);
                break;
            }
            default: output += c;
            }
        }
        if (output.back() == '\n')
            output.pop_back();
        fmt::format_to(std::back_inserter(json), "\"{}\",", output.data());
        return arrow::Status::OK();
    }
};

bool strcmp_(char const* lhs, char const* rhs) {
    return std::strcmp(lhs, rhs) == 0;
}

bool strncmp_(char const* lhs, char const* rhs, size_t sz) {
    return std::strncmp(lhs, rhs, sz) == 0;
}

bool chrcmp_(const char lhs, const char rhs) {
    return lhs == rhs;
}

bool is_json_ptr(ukv_str_view_t field) {
    return chrcmp_(field[0], '/');
}

void generate_uuid(char* out, size_t sz) {
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse(uuid, out);
    out[sz - 1] = '\0'; // end of string
}

template <typename imp_exp_at>
bool validate_graph_fields(imp_exp_at& imp_exp, bool is_exp = false) {
    if (!imp_exp.source_id_field) {
        *imp_exp.error = "Invalid source id field";
        return false;
    }
    if (!imp_exp.target_id_field) {
        *imp_exp.error = "Invalid target id field";
        return false;
    }

    if (is_exp) {
        auto check_first_char = [](char ch) {
            return (ch >= 65 && ch <= 90) || (ch >= 97 && ch <= 122) || ch == 95;
        };

        auto check_char = [](char ch) {
            return (ch >= 48 && ch <= 57) || (ch >= 65 && ch <= 90) || (ch >= 97 && ch <= 122) || //
                   ch == 32 || ch == 45 || ch == 95;
        };

        auto validate_field = [=](ukv_str_view_t field) {
            for (size_t idx = 1; idx < strlen(field); ++idx) {
                if (check_char(field[idx]))
                    continue;

                return false;
            }
            return true;
        };

        if (!check_first_char(imp_exp.source_id_field[0])) {
            *imp_exp.error = "(source) field must start with A-Z, a-z, '_'";
            return false;
        }

        if (!check_first_char(imp_exp.target_id_field[0])) {
            *imp_exp.error = "(target) field must start with A-Z, a-z, '_'";
            return false;
        }

        if (imp_exp.edge_id_field) {
            if (!check_first_char(imp_exp.edge_id_field[0])) {
                *imp_exp.error = "(edge) field must start with A-Z, a-z, '_'";
                return false;
            }
        }

        if (!validate_field(imp_exp.source_id_field)) {
            *imp_exp.error = "(source) field can contain A-Z, a-z, 0-9, ' ', '-', '_'";
            return false;
        }

        if (!validate_field(imp_exp.target_id_field)) {
            *imp_exp.error = "(target) field can contain A-Z, a-z, 0-9, ' ', '-', '_'";
            return false;
        }

        if (imp_exp.edge_id_field) {
            if (!validate_field(imp_exp.edge_id_field)) {
                *imp_exp.error = "(edge) field can contain A-Z, a-z, 0-9, ' ', '-', '_'";
                return false;
            }
        }
    }

    return true;
}

template <typename imp_exp_at>
void validate_docs_fields(imp_exp_at& imp_exp) {
    if (imp_exp.fields_count == 0 && imp_exp.fields == nullptr)
        return;
    return_error_if_m(!(imp_exp.fields_count == 0 && imp_exp.fields != nullptr),
                      imp_exp.error,
                      uninitialized_state_k,
                      "Fields count must be initialized");
    return_error_if_m(!(imp_exp.fields_count != 0 && imp_exp.fields == nullptr),
                      imp_exp.error,
                      uninitialized_state_k,
                      "Fields must be initialized");
    return_error_if_m(!(imp_exp.fields_count != 0 && imp_exp.fields != nullptr && imp_exp.fields_stride == 0),
                      imp_exp.error,
                      uninitialized_state_k,
                      "Fields stride must be initialized");

    fields_t fields {imp_exp.fields, imp_exp.fields_stride};
    for (size_t idx = 0; idx < imp_exp.fields_count; ++idx) {
        return_error_if_m(fields[idx] != nullptr, imp_exp.error, 0, "Invalid field!");
    }
}

void check_for_id_field(ukv_docs_import_t& imp) {
    if (imp.fields == nullptr)
        return;

    bool state = false;
    fields_t fields {imp.fields, imp.fields_stride};
    for (size_t idx = 0; idx < imp.fields_count; ++idx) {
        if (strcmp_(fields[idx], imp.id_field))
            state = true;
    }
    return_error_if_m(state, imp.error, 0, "Fields must contain id_field");
}

template <typename ukv_docs_imp_exp_t>
fields_t prepare_fields(ukv_docs_imp_exp_t& c, linked_memory_lock_t& arena) {

    if (c.fields_count == 1)
        return {c.fields, c.fields_stride};
    fields_t fields {c.fields, c.fields_stride};
    stl_vector_t<ukv_str_view_t> prepared_fields(alloc_t<ukv_str_view_t>(arena, c.error));
    prepared_fields.reserve(c.fields_count);
    size_t next_idx = 0;
    size_t count = 0;

    for (size_t idx = 0; idx < c.fields_count - 1; ++count) {
        next_idx = idx + 1;
        size_t len = strlen(fields[idx]);
        auto field = arena.alloc<char>(len + 1, c.error);
        std::memcpy(field.begin(), fields[idx], len + 1);
        prepared_fields.push_back(field.begin());
        while (next_idx < c.fields_count && chrcmp_(fields[idx][0], '/') &&
               strncmp_(fields[idx], fields[next_idx], strlen(fields[idx])))
            ++next_idx;
        if (next_idx == idx + 1 && next_idx == c.fields_count - 1) {
            size_t len = strlen(fields[next_idx]);
            auto field = arena.alloc<char>(len + 1, c.error);
            std::memcpy(field.begin(), fields[next_idx], len + 1);
            prepared_fields.push_back(field.begin());
        }
        idx = next_idx;
    }
    return {prepared_fields.data(), sizeof(ukv_str_view_t)};
}

simdjson::ondemand::object& rewinded(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

void get_value(simdjson::ondemand::object& data, ukv_str_view_t json_field, ukv_str_view_t field, std::string& json) {

    bool state = is_json_ptr(field);

    auto get_result = [&]() {
        return state ? rewinded(data).at_pointer(field) : rewinded(data)[field];
    };

    switch (get_result().type()) {
    case simdjson::ondemand::json_type::array:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            get_result().get_array().value().raw_json().value());
        break;
    case simdjson::ondemand::json_type::object:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            get_result().get_object().value().raw_json().value());
        break;
    case simdjson::ondemand::json_type::number:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            std::string_view(get_result().raw_json_token().value()));
        break;
    case simdjson::ondemand::json_type::string:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            std::string_view(get_result().raw_json_token().value()));
        break;
    case simdjson::ondemand::json_type::boolean:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            bool(get_result().value()) ? "true" : "false");
        break;
    case simdjson::ondemand::json_type::null:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            "null");
        break;
    default: break;
    }
}

void simdjson_object_parser( //
    simdjson::ondemand::object& object,
    stl_vector_t<size_t> const& counts,
    fields_t const& fields,
    size_t fields_count,
    tape_t const& tape,
    std::string& json) {

    strings_tape_iterator_t iter(fields_count * fields_count, tape.begin());
    auto counts_iter = counts.begin();

    auto try_close = [&]() {
        if (!strcmp_(*iter, "}"))
            return;

        do {
            json.insert(json.size() - 1, *iter);
            ++iter;
        } while (strcmp_(*iter, "}"));
    };

    for (size_t idx = 0; idx < fields_count;) {
        try_close();
        if (is_json_ptr(fields[idx])) {
            if (strchr(fields[idx] + 1, '/') != nullptr) {
                while ((*iter)[strlen(*iter) - 1] == '{') {
                    fmt::format_to(std::back_inserter(json), "{}", *iter);
                    ++iter;
                }
                for (size_t pos = idx; idx < pos + (*counts_iter); ++idx, ++iter)
                    get_value(object, *iter, fields[idx], json);
                ++counts_iter;
                continue;
            }
        }
        get_value(object, *iter, fields[idx], json);
        ++iter;
        ++idx;
    }
    try_close();
    json.back() = '}';
}

void fields_parser( //
    ukv_error_t* error,
    linked_memory_lock_t& arena,
    stl_vector_t<size_t>& counts,
    fields_t const& fields,
    size_t fields_count,
    tape_t& tape) {

    stl_vector_t<char*> prefixes(alloc_t<char*>(arena, error));

    size_t pre_idx = 0;
    size_t offset = 0;
    size_t pos = 0;

    auto close_bracket = [&]() {
        prefixes.pop_back();
        tape[offset] = '}';
        tape[offset + 1] = '\0';
        offset += 2;
    };

    auto fill_prefixes = [&](ukv_str_view_t field) {
        while (pos <= strlen(field)) {
            prefixes.push_back(arena.alloc<char>(pos + 2, error).begin());
            std::memcpy(prefixes.back(), field, pos + 1);
            prefixes.back()[pos + 1] = '\0';
            auto substr = arena.alloc<char>(pos - pre_idx + 2, error);
            std::memcpy(substr.begin(), field + pre_idx, pos - pre_idx);
            substr[substr.size() - 2] = '\0';
            auto str = fmt::format("\"{}\":{{{}", substr.begin(), '\0');
            strncpy(tape.begin() + offset, str.data(), str.size());
            offset += str.size();
            pre_idx = pos + 1;
            pos = strchr(field + pre_idx, '/') - field;
        }
    };

    for (size_t idx = 0; idx < fields_count;) {
        if (is_json_ptr(fields[idx])) {
            pre_idx = 1;

            pos = strchr(fields[idx] + pre_idx, '/') - fields[idx];
            if (pos <= strlen(fields[idx])) {
                fill_prefixes(fields[idx]);
                while (prefixes.size()) {
                    counts.push_back(0);
                    while (strncmp_(prefixes.back(), fields[idx], strlen(prefixes.back()))) {
                        pre_idx = strlen(prefixes.back()) + 1;
                        size_t length = strlen(fields[idx]) - pre_idx + 2;
                        auto substr = arena.alloc<char>(length, error);
                        std::memcpy(substr.begin(), fields[idx] + pre_idx - 1, length);
                        substr[substr.size() - 1] = '\0';
                        auto str = fmt::format("\"{}\":{}", substr.begin(), '\0');
                        strncpy(tape.begin() + offset, str.data(), str.size());
                        offset += str.size();
                        ++counts.back();
                        ++idx;
                        if (idx == fields_count) {
                            while (prefixes.size())
                                close_bracket();
                            return;
                        }
                        size_t sz = prefixes.size();
                        while (prefixes.size() && !strncmp_( //
                                                      prefixes.back(),
                                                      fields[idx],
                                                      strlen(prefixes.back()))) {
                            close_bracket();
                        }

                        if (prefixes.size() == 0)
                            break;
                        else if (sz > prefixes.size()) {
                            pre_idx = strlen(prefixes.back()) + 1;
                            counts.push_back(0);
                        }

                        pos = strchr(fields[idx] + pre_idx, '/') - fields[idx];
                        if (pos <= strlen(fields[idx])) {
                            --pre_idx;
                            fill_prefixes(fields[idx]);
                            counts.push_back(0);
                        }
                    }
                }
            }
            else {
                auto str = fmt::format("\"{}\":{}", fields[idx] + 1, '\0');
                strncpy(tape.begin() + offset, str.data(), str.size());
                offset += str.size();
                break;
                ++idx;
            }
        }
        else {
            auto str = fmt::format("\"{}\":{}", fields[idx], '\0');
            strncpy(tape.begin() + offset, str.data(), str.size());
            offset += str.size();
            ++idx;
        }
    }
}

#pragma region - Upserting

void upsert_graph(ukv_graph_import_t& c, edges_t const& edges_src) {

    auto strided = edges(edges_src);
    ukv_graph_upsert_edges_t graph_upsert_edges {
        .db = c.db,
        .error = c.error,
        .arena = c.arena,
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = edges_src.size(),
        .collections = &c.collection,
        .edges_ids = strided.edge_ids.begin().get(),
        .edges_stride = strided.edge_ids.stride(),
        .sources_ids = strided.source_ids.begin().get(),
        .sources_stride = strided.source_ids.stride(),
        .targets_ids = strided.target_ids.begin().get(),
        .targets_stride = strided.target_ids.stride(),
    };

    ukv_graph_upsert_edges(&graph_upsert_edges);
}

void upsert_docs(ukv_docs_import_t& c, docs_t& docs) {

    ukv_docs_write_t docs_write {
        .db = c.db,
        .error = c.error,
        .arena = c.arena,
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = docs.size(),
        .type = ukv_doc_field_json_k,
        .modification = ukv_doc_modify_upsert_k,
        .collections = &c.collection,
        .lengths = docs.front().member_length(),
        .lengths_stride = sizeof(value_view_t),
        .values = docs.front().member_ptr(),
        .values_stride = sizeof(value_view_t),
        .id_field = c.id_field,
    };

    ukv_docs_write(&docs_write);
}

#pragma region - Graph Begin
#pragma region - Parsing with Apache Arrow

void parse_arrow_table(ukv_graph_import_t& c, ukv_size_t task_count, std::shared_ptr<arrow::Table> const& table) {

    auto arena = linked_memory(c.arena, c.options, c.error);
    edges_t vertices_edges(alloc_t<edge_t>(arena, c.error));

    auto sources = table->GetColumnByName(c.source_id_field);
    return_error_if_m(sources, c.error, 0, "The source field does not exist");
    auto targets = table->GetColumnByName(c.target_id_field);
    return_error_if_m(targets, c.error, 0, "The target field does not exist");
    auto edges = c.edge_id_field ? table->GetColumnByName(c.edge_id_field) : nullptr;
    return_error_if_m(edges || (c.edge_id_field == nullptr), c.error, 0, "The edge field does not exist");
    size_t count = sources->num_chunks();
    return_error_if_m(count > 0, c.error, 0, "Empty Input");
    vertices_edges.reserve(std::min(ukv_size_t(sources->chunk(0)->length()), task_count));

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto source_chunk = sources->chunk(chunk_idx);
        auto target_chunk = targets->chunk(chunk_idx);
        auto edge_chunk = edges ? edges->chunk(chunk_idx) : array_t {};
        auto source_array = std::static_pointer_cast<arrow::Int64Array>(source_chunk);
        auto target_array = std::static_pointer_cast<arrow::Int64Array>(target_chunk);
        auto edge_array = std::static_pointer_cast<arrow::Int64Array>(edge_chunk);
        for (size_t value_idx = 0; value_idx != source_array->length(); ++value_idx) {
            edge_t edge {
                .source_id = source_array->Value(value_idx),
                .target_id = target_array->Value(value_idx),
                .id = edges ? edge_array->Value(value_idx) : ukv_default_edge_id_k,
            };
            vertices_edges.push_back(edge);
            if (vertices_edges.size() == task_count) {
                upsert_graph(c, vertices_edges);
                vertices_edges.clear();
            }
        }
    }
    if (vertices_edges.size() != 0)
        upsert_graph(c, vertices_edges);
}

template <typename import_t>
void import_parquet(import_t& c, std::shared_ptr<arrow::Table>& table) {

    arrow::Status status;
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Open File
    auto maybe_input = arrow::io::ReadableFile::Open(c.paths_pattern);
    return_error_if_m(maybe_input.ok(), c.error, 0, status.ToString().c_str());
    auto input = *maybe_input;

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    status = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    return_error_if_m(status.ok(), c.error, 0, status.ToString().c_str());

    // Read File into table
    status = arrow_reader->ReadTable(&table);
    return_error_if_m(status.ok(), c.error, 0, status.ToString().c_str());
}

void export_parquet_graph(ukv_graph_export_t& c, keys_t const& ids, ukv_length_t length, linked_memory_lock_t& arena) {

    parquet::schema::NodeVector fields;
    fields.push_back(parquet::schema::PrimitiveNode::Make( //
        c.source_id_field,
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::INT_64));

    fields.push_back(parquet::schema::PrimitiveNode::Make( //
        c.target_id_field,
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::INT_64));

    if (c.edge_id_field)
        fields.push_back(parquet::schema::PrimitiveNode::Make( //
            c.edge_id_field,
            parquet::Repetition::REQUIRED,
            parquet::Type::INT64,
            parquet::ConvertedType::INT_64));

    std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    char file_name[uuid_length_k];
    generate_uuid(file_name, uuid_length_k);

    auto maybe_outfile = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_error_if_m(maybe_outfile.ok(), c.error, 0, "Can't open file");
    auto outfile = *maybe_outfile;

    parquet::WriterProperties::Builder builder;
    builder.memory_pool(arrow::default_memory_pool());
    builder.write_batch_size(length);

    parquet::StreamWriter os {parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

    ukv_key_t* data = nullptr;

    for (auto id : ids) {
        data = id.first;
        for (size_t idx = 0; idx < id.second; idx += 3) {
            os << data[idx] << data[idx + 1];
            if (c.edge_id_field)
                os << data[idx + 2];
            os << parquet::EndRow;
        }
    }
}

template <typename import_t>
void import_csv(import_t& c, std::shared_ptr<arrow::Table>& table) {

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto maybe_input = arrow::io::ReadableFile::Open(c.paths_pattern);
    return_error_if_m(maybe_input.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::InputStream> input = *maybe_input;

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto maybe_reader = arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
    return_error_if_m(maybe_reader.ok(), c.error, 0, "Can't instantiate reader");
    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

    // Read table from CSV file
    auto maybe_table = reader->Read();
    return_error_if_m(maybe_table.ok(), c.error, 0, "Can't read file");
    table = *maybe_table;
}

void export_csv_graph(ukv_graph_export_t& c, keys_t const& ids, ukv_length_t length, linked_memory_lock_t& arena) {

    arrow::Status status;
    arrow::NumericBuilder<arrow::Int64Type> builder;
    status = builder.Resize(length / 3);
    return_error_if_m(status.ok(), c.error, 0, "Can't instantiate builder");

    array_t sources_array;
    array_t targets_array;
    array_t edges_array;

    auto values = arena.alloc<ukv_key_t>(length / 3, c.error);
    return_if_error_m(c.error);

    auto fill_values = [&](size_t offset) {
        ukv_key_t* data = nullptr;
        for (auto id : ids) {
            data = id.first;
            for (size_t idx_in_data = offset, idx = 0; idx_in_data < id.second; idx_in_data += 3, ++idx)
                values[idx] = data[idx_in_data];
        }
    };

    fill_values(0);
    status = builder.AppendValues(values.begin(), values.size());
    return_error_if_m(status.ok(), c.error, 0, "Can't append values(sources)");
    status = builder.Finish(&sources_array);
    return_error_if_m(status.ok(), c.error, 0, "Can't finish array(sources)");

    fill_values(1);
    status = builder.AppendValues(values.begin(), values.size());
    return_error_if_m(status.ok(), c.error, 0, "Can't append values(targets)");
    status = builder.Finish(&targets_array);
    return_error_if_m(status.ok(), c.error, 0, "Can't finish array(targets)");

    if (c.edge_id_field) {
        fill_values(2);
        status = builder.AppendValues(values.begin(), values.size());
        return_error_if_m(status.ok(), c.error, 0, "Can't append values(edges)");
        status = builder.Finish(&edges_array);
        return_error_if_m(status.ok(), c.error, 0, "Can't finish array(edges)");
    }

    arrow::FieldVector fields;

    fields.push_back(arrow::field(c.source_id_field, arrow::int64()));
    fields.push_back(arrow::field(c.target_id_field, arrow::int64()));
    if (c.edge_id_field)
        fields.push_back(arrow::field(c.edge_id_field, arrow::int64()));

    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    std::shared_ptr<arrow::Table> table = nullptr;

    if (c.edge_id_field)
        table = arrow::Table::Make(schema, {sources_array, targets_array, edges_array});
    else
        table = arrow::Table::Make(schema, {sources_array, targets_array});
    return_error_if_m(table, c.error, 0, "Can't make schema");

    char file_name[uuid_length_k];
    generate_uuid(file_name, uuid_length_k);

    auto maybe_outstream = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_error_if_m(maybe_outstream.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *maybe_outstream;

    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
    return_error_if_m(status.ok(), c.error, 0, "Can't write in file");
}

#pragma region - Parsing with SIMDJSON

void import_ndjson_graph(ukv_graph_import_t& c, ukv_size_t task_count) noexcept {

    auto arena = linked_memory(c.arena, c.options, c.error);
    edges_t edges(alloc_t<edge_t>(arena, c.error));
    edges.reserve(task_count);

    auto handle = open(c.paths_pattern, O_RDONLY);
    return_error_if_m(handle != -1, c.error, 0, "Can't open file");

    size_t file_size = std::filesystem::file_size(std::filesystem::path(c.paths_pattern));
    auto begin = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), file_size);
    madvise(begin, file_size, MADV_SEQUENTIAL);

    auto get_data = [&](simdjson::ondemand::object& data, ukv_str_view_t field) {
        return chrcmp_(field[0], '/') ? rewinded(data).at_pointer(field) : rewinded(data)[field];
    };

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    ukv_key_t edge = ukv_default_edge_id_k;
    for (auto doc : docs) {
        simdjson::ondemand::object data = doc.get_object().value();
        try {
            if (c.edge_id_field)
                edge = get_data(data, c.edge_id_field);
            edges.push_back(edge_t {get_data(data, c.source_id_field), get_data(data, c.target_id_field), edge});
        }
        catch (simdjson::simdjson_error const& ex) {
            *c.error = ex.what();
            return_if_error_m(c.error);
        }

        if (edges.size() == task_count) {
            upsert_graph(c, edges);
            edges.clear();
        }
    }
    if (edges.size() != 0)
        upsert_graph(c, edges);

    munmap((void*)mapped_content.data(), mapped_content.size());
    close(handle);
}

void export_ndjson_graph(ukv_graph_export_t& c, keys_t const& ids, ukv_length_t length, linked_memory_lock_t& arena) {

    ukv_key_t* data = nullptr;
    char file_name[uuid_length_k];
    generate_uuid(file_name, uuid_length_k);
    auto handle = open(fmt::format("{}{}", file_name, c.paths_extension).data(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

    if (!c.edge_id_field) {
        for (auto id : ids) {
            data = id.first;
            for (size_t idx = 0; idx < id.second; idx += 3) {
                auto str = fmt::format( //
                    "{{\"{}\":{},\"{}\":{}}}\n",
                    c.source_id_field,
                    data[idx],
                    c.target_id_field,
                    data[idx + 1]);
                write(handle, str.data(), str.size());
            }
        }
    }
    else {
        for (auto id : ids) {
            data = id.first;
            for (size_t idx = 0; idx < id.second; idx += 3) {
                auto str = fmt::format( //
                    "{{\"{}\":{},\"{}\":{},\"{}\":{}}}\n",
                    c.source_id_field,
                    data[idx],
                    c.target_id_field,
                    data[idx + 1],
                    c.edge_id_field,
                    data[idx + 2]);
                write(handle, str.data(), str.size());
            }
        }
    }
    close(handle);
}

#pragma region - Main Functions(Graph)

void ukv_graph_import(ukv_graph_import_t* c_ptr) {

    ukv_graph_import_t& c = *c_ptr;

    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    return_error_if_m(c.paths_pattern, c.error, uninitialized_state_k, "Paths pattern is uninitialized");
    return_error_if_m(c.arena, c.error, uninitialized_state_k, "Arena is uninitialized");
    return_error_if_m(c.max_batch_size, c.error, uninitialized_state_k, "Max batch size is 0");
    if (!validate_graph_fields(c))
        return_if_error_m(c.error);

    auto ext = std::filesystem::path(c.paths_pattern).extension();
    ukv_size_t task_count = c.max_batch_size / sizeof(edge_t);
    if (ext == ".ndjson")
        import_ndjson_graph(c, task_count);
    else {
        std::shared_ptr<arrow::Table> table;
        if (ext == ".parquet")
            import_parquet(c, table);
        else if (ext == ".csv")
            import_csv(c, table);
        return_if_error_m(c.error);
        parse_arrow_table(c, task_count, table);
    }
}

void ukv_graph_export(ukv_graph_export_t* c_ptr) {

    ukv_graph_export_t& c = *c_ptr;

    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    if (!validate_graph_fields(c, true))
        return_if_error_m(c.error);
    return_error_if_m(c.paths_extension, c.error, uninitialized_state_k, "Paths extension is uninitialized");
    return_error_if_m(c.arena, c.error, uninitialized_state_k, "Arena is uninitialized");
    return_error_if_m(c.max_batch_size, c.error, uninitialized_state_k, "Max batch size is 0");

    auto ext = c.paths_extension;
    auto export_method = strcmp_(ext, ".parquet")  ? &export_parquet_graph
                         : strcmp_(ext, ".ndjson") ? &export_ndjson_graph
                         : strcmp_(ext, ".csv")    ? &export_csv_graph
                                                   : nullptr;

    return_error_if_m(export_method, c.error, 0, "Not supported format");

    std::plus plus;

    auto arena = linked_memory(c.arena, c.options, c.error);
    keys_t ids_in_edges(alloc_t<keys_length_t>(arena, c.error));
    ukv_vertex_degree_t* degrees = nullptr;
    ukv_vertex_role_t const role = ukv_vertex_role_any_k;

    ukv_size_t count = 0;
    ukv_size_t batch_ids = 0;
    ukv_size_t total_ids = 0;
    ukv_size_t task_count = c.max_batch_size / sizeof(edge_t);

    keys_stream_t stream(c.db, c.collection, task_count, nullptr);
    auto status = stream.seek_to_first();
    return_error_if_m(status, c.error, 0, "No batches in stream");

    while (!stream.is_end()) {
        ids_in_edges.push_back({nullptr, 0});
        count = stream.keys_batch().size();

        ukv_graph_find_edges_t graph_find {
            .db = c.db,
            .error = c.error,
            .arena = c.arena,
            .options = ukv_option_dont_discard_memory_k,
            .tasks_count = count,
            .collections = &c.collection,
            .vertices = stream.keys_batch().begin(),
            .vertices_stride = sizeof(ukv_key_t),
            .roles = &role,
            .degrees_per_vertex = &degrees,
            .edges_per_vertex = &ids_in_edges.back().first,
        };
        ukv_graph_find_edges(&graph_find);

        batch_ids = std::transform_reduce(degrees, degrees + count, 0ul, plus, [](ukv_vertex_degree_t d) {
            return d != ukv_vertex_degree_missing_k ? d : 0;
        });
        batch_ids *= vertices_edge_k;
        total_ids += batch_ids;
        ids_in_edges.back().second = batch_ids;

        status = stream.seek_to_next_batch();
        return_error_if_m(status, c.error, 0, "Invalid batch");
    }
    export_method(c, ids_in_edges, total_ids, arena);
}

#pragma region - Graph End

#pragma region - Docs Begin
#pragma region - Parsing with Apache Arrow

void parse_arrow_table(ukv_docs_import_t& c, std::shared_ptr<arrow::Table> const& table) {

    fields_t fields;
    size_t used_mem = 0;
    char* field = nullptr;
    std::string json = "{";
    char* json_cstr = nullptr;
    arrow_visitor_t visitor(json);
    auto arena = linked_memory(c.arena, c.options, c.error);

    if (!c.fields) {
        auto clmn_names = table->ColumnNames();
        c.fields_count = clmn_names.size();
        auto names = arena.alloc<ukv_str_view_t>(c.fields_count, c.error);

        for (size_t idx = 0; idx < c.fields_count; ++idx) {
            field = arena.alloc<char>(clmn_names[idx].size() + 1, c.error).begin();
            std::memcpy(field, clmn_names[idx].data(), clmn_names[idx].size() + 1);
            names[idx] = field;
        }

        c.fields_stride = sizeof(ukv_str_view_t);
        fields = fields_t {names.begin(), c.fields_stride};
    }
    else {
        fields = fields_t {c.fields, c.fields_stride};
        c.fields_count = c.fields_count;
    }

    docs_t values(alloc_t<value_view_t>(arena, c.error));
    stl_vector_t<chunked_array_t> columns(c.fields_count, alloc_t<chunked_array_t>(arena, c.error));
    stl_vector_t<array_t> chunks(c.fields_count, alloc_t<array_t>(arena, c.error));

    for (size_t idx = 0; idx < c.fields_count; ++idx) {
        chunked_array_t column = table->GetColumnByName(fields[idx]);
        return_error_if_m(column, c.error, 0, fmt::format("{} is not exist", fields[idx]).c_str());
        columns[idx] = column;
    }

    size_t count = columns[0]->num_chunks();
    values.reserve(ukv_size_t(columns[0]->chunk(0)->length()));

    for (size_t chunk_idx = 0, g_idx = 0; chunk_idx != count; ++chunk_idx, g_idx = 0) {

        for (auto column : columns) {
            chunks[g_idx] = column->chunk(chunk_idx);
            ++g_idx;
        }

        for (size_t value_idx = 0; value_idx < columns[0]->chunk(chunk_idx)->length(); ++value_idx) {

            g_idx = 0;
            for (auto it = fields; g_idx < c.fields_count; ++g_idx, ++it) {
                fmt::format_to(std::back_inserter(json), "\"{}\":", *it);
                visitor.idx = value_idx;
                arrow::VisitArrayInline(*chunks[g_idx].get(), &visitor);
            }

            json[json.size() - 1] = '}';
            json.push_back('\n');
            json_cstr = arena.alloc<char>(json.size() + 1, c.error).begin();
            std::memcpy(json_cstr, json.data(), json.size() + 1);

            values.push_back(json_cstr);
            used_mem += json.size();
            json = "{";

            if (used_mem >= c.max_batch_size) {
                upsert_docs(c, values);
                values.clear();
                used_mem = 0;
            }
        }
    }
    if (values.size() != 0) {
        upsert_docs(c, values);
        values.clear();
    }
}

#pragma region - Parsing with SIMDJSON

void import_whole_ndjson(ukv_docs_import_t& c, simdjson::ondemand::document_stream& docs) {

    auto arena = linked_memory(c.arena, c.options, c.error);
    docs_t values(alloc_t<value_view_t>(arena, c.error));

    size_t used_mem = 0;

    for (auto doc : docs) {
        simdjson::ondemand::object object = doc.get_object().value();
        values.push_back(rewinded(object).raw_json().value());
        used_mem += values.back().size();
        if (used_mem >= c.max_batch_size) {
            upsert_docs(c, values);
            values.clear();
        }
    }
    if (values.size() != 0)
        upsert_docs(c, values);
}

void import_sub_ndjson(ukv_docs_import_t& c, simdjson::ondemand::document_stream& docs) noexcept {

    auto arena = linked_memory(c.arena, c.options, c.error);
    auto fields = prepare_fields(c, arena);
    size_t max_size = c.fields_count * symbols_count_k;
    for (size_t idx = 0; idx < c.fields_count; ++idx)
        max_size += strlen(fields[idx]);

    docs_t values(alloc_t<value_view_t>(arena, c.error));

    size_t used_mem = 0;
    std::string json = "{";
    char* json_cstr = nullptr;

    stl_vector_t<size_t> counts(alloc_t<size_t>(arena, c.error));
    counts.reserve(c.fields_count);

    auto tape = arena.alloc<char>(max_size, c.error);
    fields_parser(c.error, arena, counts, fields, c.fields_count, tape);

    for (auto doc : docs) {
        simdjson::ondemand::object object = doc.get_object().value();

        try {
            simdjson_object_parser(object, counts, fields, c.fields_count, tape, json);
        }
        catch (simdjson::simdjson_error const& ex) {
            *c.error = ex.what();
            return_if_error_m(c.error);
        }

        json.push_back('\n');
        json_cstr = arena.alloc<char>(json.size() + 1, c.error).begin();
        std::memcpy(json_cstr, json.data(), json.size() + 1);

        values.push_back(json_cstr);
        used_mem += json.size();
        json = "{";

        if (used_mem >= c.max_batch_size) {
            upsert_docs(c, values);
            values.clear();
        }
    }
    if (values.size() != 0)
        upsert_docs(c, values);
}

void import_ndjson_docs(ukv_docs_import_t& c) {

    auto handle = open(c.paths_pattern, O_RDONLY);
    return_error_if_m(handle != -1, c.error, 0, "Can't open file");

    size_t file_size = std::filesystem::file_size(std::filesystem::path(c.paths_pattern));
    auto begin = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), file_size);
    madvise(begin, file_size, MADV_SEQUENTIAL);

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    if (!c.fields)
        import_whole_ndjson(c, docs);
    else
        import_sub_ndjson(c, docs);

    munmap((void*)mapped_content.data(), mapped_content.size());
    close(handle);
}

void export_whole_docs( //
    ukv_error_t* error,
    linked_memory_lock_t arena,
    ptr_range_gt<ukv_key_t const>& keys,
    ptr_range_gt<ukv_key_t>* keys_ptr,
    ptr_range_gt<char*>* docs_ptr,
    parquet::StreamWriter* os_ptr,
    val_t const& values,
    int handle,
    int flag) {

    ptr_range_gt<char*>& docs_vec = *docs_ptr;
    ptr_range_gt<ukv_key_t>& keys_vec = *keys_ptr;
    parquet::StreamWriter& os = *os_ptr;

    size_t idx = 0;
    auto iter = keys.begin();

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        values.first,
        values.second,
        1000000ul);

    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();
        auto value = rewinded(obj).raw_json().value();
        auto json = std::string(value.data(), value.size());
        json.pop_back();

        if (flag == 0) {
            auto val = *iter;
            os << *iter << json.data();
            os << parquet::EndRow;
        }
        else if (flag == 1) {
            keys_vec[idx] = *iter;
            docs_vec[idx] = arena.alloc<char>(json.size() + 1, error).begin();
            std::memcpy(docs_vec[idx], json.data(), json.size() + 1);
            ++idx;
        }
        else {
            auto str = fmt::format("{{\"_id\":{},\"doc\":{}}}\n", *iter, json.data());
            write(handle, str.data(), str.size());
        }
        ++iter;
    }
}

void export_sub_docs( //
    ukv_docs_export_t& c,
    linked_memory_lock_t arena,
    parquet::StreamWriter* os_ptr,
    ptr_range_gt<char*>* docs_ptr,
    ptr_range_gt<ukv_key_t>* keys_ptr,
    ptr_range_gt<ukv_key_t const>& keys,
    stl_vector_t<size_t> const& counts,
    ptr_range_gt<char> const& tape,
    fields_t const& fields,
    val_t const& values,
    int handle,
    int flag) {

    ptr_range_gt<char*>& docs_vec = *docs_ptr;
    ptr_range_gt<ukv_key_t>& keys_vec = *keys_ptr;
    parquet::StreamWriter& os = *os_ptr;

    size_t idx = 0;
    auto iter = keys.begin();
    std::string json = prefix_k;

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        values.first,
        values.second,
        1000000ul);

    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();
        try {
            simdjson_object_parser(obj, counts, fields, c.fields_count, tape, json);
        }
        catch (simdjson::simdjson_error const& ex) {
            *c.error = ex.what();
            return_if_error_m(c.error);
        }
        if (flag == 0) {
            os << *iter << json.data();
            os << parquet::EndRow;
        }
        else if (flag == 1) {
            keys_vec[idx] = *iter;
            docs_vec[idx] = arena.alloc<char>(json.size() + 1, c.error).begin();
            std::memcpy(docs_vec[idx], json.data(), json.size() + 1);
            ++idx;
        }
        else {
            auto dat = *iter;
            auto str = fmt::format("{{\"_id\":{},\"doc\":{}}}\n", *iter, json.data());
            write(handle, str.data(), str.size());
        }
        json = prefix_k;
        ++iter;
    }
}

void make_parquet(ukv_docs_export_t& c, parquet::StreamWriter& os) {

    parquet::schema::NodeVector nodes;
    nodes.push_back(parquet::schema::PrimitiveNode::Make( //
        "_id",
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::INT_64));

    nodes.push_back(parquet::schema::PrimitiveNode::Make( //
        "doc",
        parquet::Repetition::REQUIRED,
        parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, nodes));

    char file_name[uuid_length_k];
    generate_uuid(file_name, uuid_length_k);

    auto maybe_outfile = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_error_if_m(maybe_outfile.ok(), c.error, 0, "Can't open file");
    auto outfile = *maybe_outfile;

    parquet::WriterProperties::Builder builder;
    builder.memory_pool(arrow::default_memory_pool());
    builder.write_batch_size(c.max_batch_size);

    os = parquet::StreamWriter {parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};
}

void write_in_parquet( //
    ukv_docs_export_t& c,
    linked_memory_lock_t& arena,
    parquet::StreamWriter& ostream,
    ptr_range_gt<ukv_key_t const>& keys,
    stl_vector_t<size_t> const& counts,
    ptr_range_gt<char> const& tape,
    fields_t const& fields,
    val_t const& values) {

    if (c.fields)
        export_sub_docs(c, arena, &ostream, nullptr, nullptr, keys, counts, tape, fields, values, 0, 0);
    else
        export_whole_docs(c.error, arena, keys, nullptr, nullptr, &ostream, values, 0, 0);
}

void write_in_csv( //
    ukv_docs_export_t& c,
    linked_memory_lock_t& arena,
    ptr_range_gt<char*>& docs_vec,
    ptr_range_gt<ukv_key_t>& keys_vec,
    ptr_range_gt<ukv_key_t const>& keys,
    arrow::NumericBuilder<arrow::Int64Type>& int_builder,
    arrow::StringBuilder& string_builder,
    stl_vector_t<size_t> const& counts,
    ptr_range_gt<char> const& tape,
    fields_t const& fields,
    val_t const& values) {

    arrow::Status status;
    size_t size = keys.size();
    status = int_builder.Resize(int_builder.capacity() + size);
    return_error_if_m(status.ok(), c.error, 0, "Can't resize builder");
    status = string_builder.Resize(string_builder.capacity() + size);
    return_error_if_m(status.ok(), c.error, 0, "Can't resize builder");

    keys_vec.end_ = keys_vec.begin() + size;
    docs_vec.end_ = docs_vec.begin() + size;

    if (c.fields)
        export_sub_docs(c, arena, nullptr, &docs_vec, &keys_vec, keys, counts, tape, fields, values, 0, 1);
    else
        export_whole_docs(c.error, arena, keys, &keys_vec, &docs_vec, nullptr, values, 0, 1);

    status = int_builder.AppendValues(keys_vec.begin(), keys_vec.size());
    return_error_if_m(status.ok(), c.error, 0, "Can't append keys");
    status = string_builder.AppendValues((char const**)docs_vec.begin(), docs_vec.size());
    return_error_if_m(status.ok(), c.error, 0, "Can't append docs");
}

void end_csv( //
    ukv_docs_export_t& c,
    arrow::StringBuilder& string_builder,
    arrow::NumericBuilder<arrow::Int64Type>& int_builder) {

    array_t keys_array;
    array_t docs_array;
    arrow::Status status;

    status = int_builder.Finish(&keys_array);
    return_error_if_m(status.ok(), c.error, 0, "Can't finish array(keys)");
    status = string_builder.Finish(&docs_array);
    return_error_if_m(status.ok(), c.error, 0, "Can't finish array(docs)");

    arrow::FieldVector fields;
    fields.push_back(arrow::field("_id", arrow::int64()));
    fields.push_back(arrow::field("doc", arrow::large_binary()));
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);

    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, {keys_array, docs_array});

    char file_name[uuid_length_k];
    generate_uuid(file_name, uuid_length_k);

    auto maybe_outstream = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_error_if_m(maybe_outstream.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *maybe_outstream;

    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
    return_error_if_m(status.ok(), c.error, 0, "Can't write in file");
}

int make_ndjson(ukv_docs_export_t& c) {
    char file_name[uuid_length_k];
    generate_uuid(file_name, uuid_length_k);
    return open(fmt::format("{}{}", file_name, c.paths_extension).data(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
}

void write_in_ndjson( //
    ukv_docs_export_t& c,
    linked_memory_lock_t& arena,
    ptr_range_gt<ukv_key_t const>& keys,
    stl_vector_t<size_t> const& counts,
    ptr_range_gt<char> const& tape,
    fields_t const& fields,
    val_t const& values,
    int handle) {

    if (c.fields)
        export_sub_docs(c, arena, nullptr, nullptr, nullptr, keys, counts, tape, fields, values, handle, 2);
    else
        export_whole_docs(c.error, arena, keys, nullptr, nullptr, nullptr, values, handle, 2);
}

void end_ndjson(int fd) {
    close(fd);
}

#pragma region - Main Functions(Docs)

void ukv_docs_import(ukv_docs_import_t* c_ptr) {

    ukv_docs_import_t& c = *c_ptr;

    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    validate_docs_fields(c);
    return_if_error_m(c.error);
    return_error_if_m(c.id_field, c.error, uninitialized_state_k, "id_field must be initialized");
    check_for_id_field(c);
    return_if_error_m(c.error);
    return_error_if_m(c.arena, c.error, uninitialized_state_k, "Arena is uninitialized");
    return_error_if_m(c.max_batch_size, c.error, uninitialized_state_k, "Max batch size is 0");
    return_error_if_m(c.paths_pattern, c.error, uninitialized_state_k, "Paths pattern is uninitialized");

    auto ext = std::filesystem::path(c.paths_pattern).extension();
    if (ext == ".ndjson")
        import_ndjson_docs(c);
    else {
        std::shared_ptr<arrow::Table> table;
        if (ext == ".parquet")
            import_parquet(c, table);
        else if (ext == ".csv")
            import_csv(c, table);
        return_if_error_m(c.error);
        parse_arrow_table(c, table);
    }
}

void ukv_docs_export(ukv_docs_export_t* c_ptr) {

    ukv_docs_export_t& c = *c_ptr;

    return_error_if_m(c.db, c.error, uninitialized_state_k, "DataBase is uninitialized");
    validate_docs_fields(c);
    return_if_error_m(c.error);
    return_error_if_m(c.paths_extension, c.error, uninitialized_state_k, "Paths extension is uninitialized");
    return_error_if_m(c.arena, c.error, uninitialized_state_k, "Arena is uninitialized");
    return_error_if_m(c.max_batch_size, c.error, uninitialized_state_k, "Max batch size is 0");

    auto ext = c.paths_extension;
    int pcn = strcmp_(ext, ".parquet") ? 0 : strcmp_(ext, ".csv") ? 1 : strcmp_(ext, ".ndjson") ? 2 : -1;
    return_error_if_m(!(pcn == -1), c.error, 0, "Not supported format");

    int handle = 0;
    parquet::StreamWriter os;
    ptr_range_gt<char*> docs_vec;
    ptr_range_gt<ukv_key_t> keys_vec;
    arrow::StringBuilder string_builder;
    arrow::NumericBuilder<arrow::Int64Type> int_builder;

    ukv_size_t task_count = 4096;
    keys_stream_t stream(c.db, c.collection, task_count);
    auto arena = linked_memory(c.arena, c.options, c.error);

    fields_t fields;
    ptr_range_gt<char> tape;
    stl_vector_t<size_t> counts(alloc_t<size_t>(arena, c.error));
    counts.reserve(c.fields_count);

    if (pcn == 0)
        make_parquet(c, os);
    else if (pcn == 1) {
        keys_vec = arena.alloc<ukv_key_t>(task_count, c.error);
        docs_vec = arena.alloc<char*>(task_count, c.error);
    }
    else
        handle = make_ndjson(c);

    if (c.fields) {
        fields = prepare_fields(c, arena);
        size_t max_size = c.fields_count * symbols_count_k;
        for (size_t idx = 0; idx < c.fields_count; ++idx)
            max_size += strlen(fields[idx]);

        tape = arena.alloc<char>(max_size, c.error);
        fields_parser(c.error, arena, counts, fields, c.fields_count, tape);
    }

    auto status = stream.seek_to_first();
    return_error_if_m(status, c.error, 0, "No batches in stream");

    while (!stream.is_end()) {
        val_t values {nullptr, 0};
        ukv_length_t* offsets = nullptr;
        ukv_length_t* lengths = nullptr;
        auto keys = stream.keys_batch();

        ukv_docs_read_t docs_read {
            .db = c.db,
            .error = c.error,
            .arena = c.arena,
            .options = ukv_option_dont_discard_memory_k,
            .tasks_count = keys.size(),
            .collections = &c.collection,
            .keys = keys.begin(),
            .keys_stride = sizeof(ukv_key_t),
            .offsets = &offsets,
            .lengths = &lengths,
            .values = &values.first,
        };
        ukv_docs_read(&docs_read);
        values.second = offsets[keys.size() - 1] + lengths[keys.size() - 1];

        if (pcn == 0)
            write_in_parquet(c, arena, os, keys, counts, tape, fields, values);
        else if (pcn == 1)
            write_in_csv(c, arena, docs_vec, keys_vec, keys, int_builder, string_builder, counts, tape, fields, values);
        else
            write_in_ndjson(c, arena, keys, counts, tape, fields, values, handle);

        status = stream.seek_to_next_batch();
        return_error_if_m(status, c.error, 0, "Invalid batch");
    }

    if (pcn == 1)
        end_csv(c, string_builder, int_builder);
    else if (pcn == 2)
        end_ndjson(handle);
}

#pragma region - Docs End