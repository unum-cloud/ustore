
#include <fcntl.h>     // `open` files
#include <sys/stat.h>  // `stat` to obtain file metadata
#include <sys/mman.h>  // `mmap` to read datasets faster
#include <uuid/uuid.h> // `uuid` to make file name
#include <unistd.h>

#include <vector>
#include <fstream>
#include <cstring>
#include <numeric>
#include <algorithm>
#include <filesystem>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
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
#pragma GCC diagnostic pop

#include <simdjson.h>
#include <fmt/format.h>

#include <ukv/ukv.hpp>

#include "dataset.h"
#include "../benchmarks/mixed.hpp"
#include "../src/helpers/linked_memory.hpp" // `linked_memory_lock_t`

#include <ukv/cpp/ranges.hpp>      // `sort_and_deduplicate`
#include <ukv/cpp/blobs_range.hpp> // `keys_stream_t`

using namespace unum::ukv::bench;
using namespace unum::ukv;

constexpr std::size_t vertices_edge_k = 3;
constexpr std::size_t symbols_count_k = 4;
constexpr std::size_t uuid_length_k = 36;
constexpr ukv_str_view_t prefix_k = "{";
constexpr ukv_str_view_t csv_prefix_k = "\"{";

using tape_t = ptr_range_gt<char>;
using edges_t = std::vector<edge_t>;
using docs_t = std::vector<value_view_t>;
using fields_t = strided_iterator_gt<ukv_str_view_t const>;

using ids_t = std::vector<std::pair<ukv_key_t*, ukv_size_t>>;
using vals_t = std::vector<std::pair<ukv_bytes_ptr_t, ukv_size_t>>;

#pragma region - Helpers

class arrow_visitor_t {
  public:
    arrow_visitor_t(std::string& json) : json(json) {}
    arrow::Status Visit(arrow::NullArray const& arr) {
        return arrow::Status(arrow::StatusCode::TypeError, "Not supported type");
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
        fmt::format_to(json.data(), "{}{{\"days\":{},\"ms-s\":{}}},", json, ds.days, ds.milliseconds);
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::MonthDayNanoIntervalArray const& arr) {
        auto mdn = arr.Value(idx);
        fmt::format_to(json.data(),
                       "{}{{\"months\":{},\"days\":{},\"us-s\":{}}},",
                       json,
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
        json = fmt::format("{}{},", json, arr.GetValueIndex(idx));
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
    template <typename cont_at>
    arrow::Status format(cont_at const& cont, size_t idx) {
        fmt::format_to(std::back_inserter(json), "{},", cont.Value(idx));
        return arrow::Status::OK();
    }

    template <typename cont_at>
    arrow::Status format_bin_str(cont_at const& cont, size_t idx) {
        auto str = std::string(cont.Value(idx).data(), cont.Value(idx).size());
        if (str.back() == '\n')
            str.pop_back();
        fmt::format_to(std::back_inserter(json), "{},", str.data());
        return arrow::Status::OK();
    }
};

class string_iterator_t {
  public:
    string_iterator_t(char* ptr) : ptr_(ptr) { length_ = strlen(ptr_); }

    size_t length() { return length_; }
    size_t size() { return length(); }

    string_iterator_t& operator++() {
        ptr_ += length_ + 1;
        length_ = strlen(ptr_);
    }
    char* operator*() const { return ptr_; }
    char operator[](size_t position) { return position < length_ ? ptr_[position] : '\0'; }

  private:
    char* ptr_;
    size_t length_;
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

bool is_ptr(ukv_str_view_t field) {
    return chrcmp_(field[0], '/');
}

void make_uuid(char* out) {
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse(uuid, out);
    out[uuid_length_k - 1] = '\0'; // end of string
}

void prepare_for_csv(std::string& str, size_t pos = 0) {
    pos = str.find('\"', pos);
    while (pos != std::string::npos) {
        str.insert(pos, 1, '\"');
        pos += 2;
        pos = str.find('\"', pos);
    }
}

simdjson::ondemand::object& rewinded(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

void get_value( //
    simdjson::ondemand::object& data,
    ukv_str_view_t json_field,
    ukv_str_view_t field,
    std::string& json) {

    bool state = is_ptr(field);

    switch (state ? rewinded(data).at_pointer(field).type() : rewinded(data)[field].type()) {
    case simdjson::ondemand::json_type::array:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            (state ? rewinded(data).at_pointer(field) : rewinded(data)[field]).get_array().value().raw_json().value());
        break;
    case simdjson::ondemand::json_type::object:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            (state ? rewinded(data).at_pointer(field) : rewinded(data)[field]).get_object().value().raw_json().value());
        break;
    case simdjson::ondemand::json_type::number:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            std::string_view(
                (state ? rewinded(data).at_pointer(field) : rewinded(data)[field]).raw_json_token().value()));
        break;
    case simdjson::ondemand::json_type::string:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            std::string_view(
                (state ? rewinded(data).at_pointer(field) : rewinded(data)[field]).raw_json_token().value()));
        break;
    case simdjson::ondemand::json_type::boolean:
        fmt::format_to( //
            std::back_inserter(json),
            "{}{},",
            json_field,
            std::string_view((state ? rewinded(data).at_pointer(field) : rewinded(data)[field]).value()));
        break;
    default: break;
    }
}

void simdjson_object_parser( //
    simdjson::ondemand::object& object,
    std::vector<size_t> const& counts,
    fields_t const& fields,
    size_t fields_count,
    tape_t const& tape,
    std::string& json) {

    string_iterator_t iter(tape.begin());
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
        if (is_ptr(fields[idx])) {
            if (strchr(fields[idx] + 1, '/') != NULL) {
                while (iter[iter.size() - 1] == '{') {
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
    std::vector<size_t>& counts,
    fields_t const& strided_fields,
    size_t fields_count,
    tape_t& tape) {

    std::vector<std::string> fields(fields_count);
    std::vector<std::string> prefixes;

    size_t pre_idx = 0;
    size_t offset = 0;
    size_t pos = 0;

    for (size_t idx = 0; idx < fields_count; ++idx)
        fields[idx] = strided_fields[idx];

    auto close_bracket = [&]() {
        prefixes.pop_back();
        tape[offset] = '}';
        tape[offset + 1] = '\0';
        offset += 2;
    };

    auto fill_prefixes = [&](size_t idx) {
        while (pos != std::string::npos) {
            prefixes.push_back(fields[idx].substr(0, pos + 1));
            auto str = fmt::format("\"{}\":{{{}", fields[idx].substr(pre_idx, pos - pre_idx), '\0');
            strncpy(tape.begin() + offset, str.data(), str.size());
            offset += str.size();
            pre_idx = pos + 1;
            pos = fields[idx].find('/', pre_idx);
        }
    };

    for (size_t idx = 0; idx < fields_count;) {
        fields[idx] = strided_fields[idx];
        if (is_ptr(fields[idx].data())) {
            pre_idx = 1;

            pos = fields[idx].find('/', pre_idx);
            if (pos != std::string::npos) {
                fill_prefixes(idx);
                while (prefixes.size()) {
                    counts.push_back(0);
                    pre_idx = prefixes.back().size() + 1;
                    while (strncmp_(prefixes.back().data(), fields[idx].data(), prefixes.back().size())) {
                        auto str = fmt::format("\"{}\":{}", fields[idx].substr(pre_idx - 1), '\0');
                        strncpy(tape.begin() + offset, str.data(), str.size());
                        offset += str.size();
                        ++counts.back();
                        ++idx;
                        if (idx == fields.size()) {
                            close_bracket();
                            return;
                        }
                        while (prefixes.size() && !strncmp_( //
                                                      prefixes.back().data(),
                                                      fields[idx].data(),
                                                      prefixes.back().size()))
                            close_bracket();

                        if (prefixes.size() == 0)
                            break;

                        pos = fields[idx].find('/', pre_idx);
                        if (pos != std::string::npos) {
                            fill_prefixes(idx);
                            counts.push_back(0);
                        }
                    }
                }
            }
            else {
                auto str = fmt::format("\"{}\":{}", fields[idx].substr(1), '\0');
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
        .options = c.options,
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

void upsert_docs(ukv_docs_import_t& c, docs_t const& docs) {

    ukv_docs_write_t docs_write {
        .db = c.db,
        .error = c.error,
        .arena = c.arena,
        .options = c.options,
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

    edges_t vertices_edges;

    auto sources = table->GetColumnByName(c.source_id_field);
    return_if_error(sources, c.error, 0, fmt::format("{} is not exist", c.source_id_field).c_str());
    auto targets = table->GetColumnByName(c.target_id_field);
    return_if_error(targets, c.error, 0, fmt::format("{} is not exist", c.target_id_field).c_str());
    auto edges = table->GetColumnByName(c.edge_id_field);
    size_t count = sources->num_chunks();
    vertices_edges.reserve(std::min(ukv_size_t(sources->chunk(0)->length()), task_count));

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto source_chunk = sources->chunk(chunk_idx);
        auto target_chunk = targets->chunk(chunk_idx);
        auto edge_chunk = edges ? edges->chunk(chunk_idx) : std::shared_ptr<arrow::Array> {};
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
    return_if_error(maybe_input.ok(), c.error, 0, "Can't open file");
    auto input = *maybe_input;

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    status = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    return_if_error(status.ok(), c.error, 0, "Can't instatinate reader");

    // Read File into table
    status = arrow_reader->ReadTable(&table);
    return_if_error(status.ok(), c.error, 0, "Can't read file");
}

void export_parquet_graph(ukv_graph_export_t& c, ids_t const& ids, ukv_length_t length) {

    bool edge_state = strcmp_(c.edge_id_field, "edge");

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

    if (!edge_state)
        fields.push_back(parquet::schema::PrimitiveNode::Make( //
            c.edge_id_field,
            parquet::Repetition::REQUIRED,
            parquet::Type::INT64,
            parquet::ConvertedType::INT_64));

    std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    char file_name[uuid_length_k];
    make_uuid(file_name);

    auto maybe_outfile = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_if_error(maybe_outfile.ok(), c.error, 0, "Can't open file");
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
            if (!edge_state)
                os << data[idx + 2];
            os << parquet::EndRow;
        }
    }
}

template <typename import_t>
void import_csv(import_t& c, std::shared_ptr<arrow::Table>& table) {

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto maybe_input = arrow::io::ReadableFile::Open(c.paths_pattern);
    return_if_error(maybe_input.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::InputStream> input = *maybe_input;

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto maybe_reader = arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
    return_if_error(maybe_reader.ok(), c.error, 0, "Can't instatinate reader");
    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

    // Read table from CSV file
    auto maybe_table = reader->Read();
    return_if_error(maybe_table.ok(), c.error, 0, "Can't read file");
    table = *maybe_table;
}

void export_csv_graph(ukv_graph_export_t& c, ids_t const& ids, ukv_length_t length) {

    bool edge_state = strcmp_(c.edge_id_field, "edge");
    arrow::Status status;

    arrow::NumericBuilder<arrow::Int64Type> builder;
    status = builder.Resize(length / 3);
    return_if_error(status.ok(), c.error, 0, "Can't instatinate builder");

    std::shared_ptr<arrow::Array> sources_array;
    std::shared_ptr<arrow::Array> targets_array;
    std::shared_ptr<arrow::Array> edges_array;
    std::vector<ukv_key_t> values(length / 3);

    auto fill_values = [&](size_t offset) {
        ukv_key_t* data = nullptr;
        for (auto id : ids) {
            data = id.first;
            for (size_t idx_in_data = offset, idx = 0; idx_in_data < id.second; idx_in_data += 3, ++idx)
                values[idx] = data[idx_in_data];
        }
    };

    fill_values(0);
    status = builder.AppendValues(values);
    return_if_error(status.ok(), c.error, 0, "Can't append values(sources)");
    status = builder.Finish(&sources_array);
    return_if_error(status.ok(), c.error, 0, "Can't finish array(sources)");

    fill_values(1);
    status = builder.AppendValues(values);
    return_if_error(status.ok(), c.error, 0, "Can't append values(targets)");
    status = builder.Finish(&targets_array);
    return_if_error(status.ok(), c.error, 0, "Can't finish array(targets)");

    if (!edge_state) {
        fill_values(2);
        status = builder.AppendValues(values);
        return_if_error(status.ok(), c.error, 0, "Can't append values(edges)");
        status = builder.Finish(&edges_array);
        return_if_error(status.ok(), c.error, 0, "Can't finish array(edges)");
    }

    arrow::FieldVector fields;

    fields.push_back(arrow::field(c.source_id_field, arrow::int64()));
    fields.push_back(arrow::field(c.target_id_field, arrow::int64()));
    if (!edge_state)
        fields.push_back(arrow::field(c.edge_id_field, arrow::int64()));

    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    std::shared_ptr<arrow::Table> table;

    if (!edge_state)
        table = arrow::Table::Make(schema, {sources_array, targets_array, edges_array});
    else
        table = arrow::Table::Make(schema, {sources_array, targets_array});

    char file_name[uuid_length_k];
    make_uuid(file_name);

    auto maybe_outstream = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_if_error(maybe_outstream.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *maybe_outstream;

    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
    return_if_error(status.ok(), c.error, 0, "Can't write in file");
}

#pragma region - Parsing with SIMDJSON

void import_ndjson_graph(ukv_graph_import_t& c, ukv_size_t task_count) {

    edges_t edges;
    edges.reserve(task_count);
    bool edge_state = !strcmp_(c.edge_id_field, "edge");

    auto handle = open(c.paths_pattern, O_RDONLY);
    return_if_error(handle != -1, c.error, 0, "Can't open file");

    auto begin = mmap(NULL, c.file_size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), c.file_size);
    madvise(begin, c.file_size, MADV_SEQUENTIAL);

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
        if (edge_state)
            edge = get_data(data, c.edge_id_field);
        edges.push_back(edge_t {.source_id = get_data(data, c.source_id_field),
                                .target_id = get_data(data, c.target_id_field),
                                .id = edge});
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

void export_ndjson_graph(ukv_graph_export_t& c, ids_t const& ids, ukv_length_t length) {

    ukv_key_t* data = nullptr;
    char file_name[uuid_length_k];
    make_uuid(file_name);
    auto handle = open(fmt::format("{}{}", file_name, c.paths_extension).data(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

    if (strcmp_(c.edge_id_field, "edge")) {
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

    ukv_size_t task_count = c.max_batch_size / sizeof(edge_t);
    auto ext = std::filesystem::path(c.paths_pattern).extension();

    if (ext == ".ndjson")
        import_ndjson_graph(c, task_count);
    else {
        std::shared_ptr<arrow::Table> table;
        if (ext == ".parquet")
            import_parquet(c, table);
        else if (ext == ".csv")
            import_csv(c, table);
        parse_arrow_table(c, task_count, table);
    }
}

void ukv_graph_export(ukv_graph_export_t* c_ptr) {

    ukv_graph_export_t& c = *c_ptr;

    ///////// Choosing a method /////////

    auto ext = c.paths_extension;
    auto export_method = strcmp_(ext, ".parquet") //
                             ? &export_parquet_graph
                             : strcmp_(ext, ".ndjson") //
                                   ? &export_ndjson_graph
                                   : strcmp_(ext, ".csv") //
                                         ? &export_csv_graph
                                         : nullptr;

    return_if_error(export_method, c.error, 0, "Not supported format");

    std::plus plus;

    ids_t ids_in_edges;
    ukv_vertex_degree_t* degrees = nullptr;
    ukv_vertex_role_t const role = ukv_vertex_role_any_k;

    ukv_size_t count = 0;
    ukv_size_t batch_ids = 0;
    ukv_size_t total_ids = 0;
    ukv_size_t task_count = c.max_batch_size / sizeof(edge_t);

    keys_stream_t stream(c.db, c.collection, task_count, nullptr);
    auto status = stream.seek_to_first();
    return_if_error(status, c.error, 0, "No batches in stream");

    while (!stream.is_end()) {
        ids_in_edges.push_back({nullptr, 0});
        count = stream.keys_batch().size();

        ukv_graph_find_edges_t graph_find {
            .db = c.db,
            .error = c.error,
            .arena = c.arena,
            .options = c.options,
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
        return_if_error(status, c.error, 0, "Invalid batch");
    }
    export_method(c, ids_in_edges, total_ids);
}

#pragma region - Graph End

#pragma region - Docs Begin
#pragma region - Parsing with Apache Arrow

void parse_arrow_table(ukv_docs_import_t& c, std::shared_ptr<arrow::Table> const& table) {

    fields_t fields;
    char* field = nullptr;
    std::vector<ukv_str_view_t> names;

    if (!c.fields) {
        auto clmn_names = table->ColumnNames();
        c.fields_count = clmn_names.size();
        names.resize(c.fields_count);

        for (size_t idx = 0; idx < c.fields_count; ++idx) {
            field = (char*)malloc(clmn_names[idx].size() + 1);
            std::memcpy(field, clmn_names[idx].data(), clmn_names[idx].size() + 1);
            names[idx] = field;
        }

        c.fields_stride = sizeof(ukv_str_view_t);
        fields = fields_t {names.data(), c.fields_stride};
    }
    else {
        fields = fields_t {c.fields, c.fields_stride};
        c.fields_count = c.fields_count;
    }

    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(c.fields_count);
    std::vector<std::shared_ptr<arrow::Array>> chunks(c.fields_count);
    docs_t values;
    char* u_json = nullptr;
    std::string json = "{";
    arrow_visitor_t visitor(json);
    size_t used_mem = 0;
    for (size_t idx = 0; idx < c.fields_count; ++idx) {
        std::shared_ptr<arrow::ChunkedArray> column = table->GetColumnByName(fields[idx]);
        return_if_error(column, c.error, 0, fmt::format("{} is not exist", fields[idx]).c_str());
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
                json = fmt::format("{}\"{}\":", json, *it);
                visitor.idx = value_idx;
                arrow::VisitArrayInline(*chunks[g_idx].get(), &visitor);
            }

            json[json.size() - 1] = '}';
            json.push_back('\n');
            u_json = (char*)malloc(json.size() + 1);
            std::memcpy(u_json, json.data(), json.size() + 1);

            values.push_back(u_json);
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

    docs_t values;
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

void import_sub_ndjson(ukv_docs_import_t& c, simdjson::ondemand::document_stream& docs) {

    fields_t fields {c.fields, c.fields_stride};
    size_t max_size = c.fields_count * symbols_count_k;
    for (size_t idx = 0; idx < c.fields_count; ++idx)
        max_size += strlen(fields[idx]);

    std::vector<size_t> counts;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    auto tape = arena.alloc<char>(max_size, c.error);
    fields_parser(counts, fields, c.fields_count, tape);

    std::string json = "{";
    char* u_json = nullptr;
    size_t used_mem = 0;
    docs_t values;

    for (auto doc : docs) {
        simdjson::ondemand::object object = doc.get_object().value();

        simdjson_object_parser(object, counts, fields, c.fields_count, tape, json);
        json.push_back('\n');
        u_json = (char*)malloc(json.size() + 1);
        std::memcpy(u_json, json.data(), json.size() + 1);

        values.push_back(u_json);
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
    return_if_error(handle != -1, c.error, 0, "Can't open file");

    auto begin = mmap(NULL, c.file_size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), c.file_size);
    madvise(begin, c.file_size, MADV_SEQUENTIAL);

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
    vals_t const& values,
    std::vector<ptr_range_gt<ukv_key_t const>> const& keys,
    std::vector<std::string>* docs_ptr,
    std::vector<ukv_key_t>* keys_ptr,
    parquet::StreamWriter* os_ptr,
    int handle,
    int flag) {

    auto iter = pass_through_iterator(keys);

    std::vector<std::string>& docs_vec = *docs_ptr;
    std::vector<ukv_key_t>& keys_vec = *keys_ptr;
    parquet::StreamWriter& os = *os_ptr;

    for (auto value : values) {
        simdjson::ondemand::parser parser;
        simdjson::ondemand::document_stream docs = parser.iterate_many( //
            value.first,
            value.second,
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
                prepare_for_csv(json);
                json.insert(0, 1, '\"');
                json.push_back('\"');
                keys_vec.push_back(*iter);
                docs_vec.push_back(json);
            }
            else {
                auto str = fmt::format("{{\"_id\":{},\"doc\":{}}}\n", *iter, json.data());
                write(handle, str.data(), str.size());
            }
            ++iter;
        }
    }
}

void export_sub_docs( //
    ukv_docs_export_t& c,
    vals_t const& values,
    std::vector<ptr_range_gt<ukv_key_t const>> const& keys,
    std::vector<std::string>* docs_ptr,
    std::vector<ukv_key_t>* keys_ptr,
    parquet::StreamWriter* os_ptr,
    int handle,
    int flag) {

    auto iter = pass_through_iterator(keys);
    fields_t fields {c.fields, c.fields_stride};
    std::string json = flag == 1 ? csv_prefix_k : prefix_k;

    std::vector<std::string>& docs_vec = *docs_ptr;
    std::vector<ukv_key_t>& keys_vec = *keys_ptr;
    parquet::StreamWriter& os = *os_ptr;

    size_t max_size = c.fields_count * symbols_count_k;
    for (size_t idx = 0; idx < c.fields_count; ++idx)
        max_size += strlen(fields[idx]);

    std::vector<size_t> counts;
    linked_memory_lock_t arena = linked_memory(c.arena, c.options, c.error);
    auto tape = arena.alloc<char>(max_size, c.error);
    fields_parser(counts, fields, c.fields_count, tape);

    for (auto value : values) {
        simdjson::ondemand::parser parser;
        simdjson::ondemand::document_stream docs = parser.iterate_many( //
            value.first,
            value.second,
            1000000ul);

        for (auto doc : docs) {
            simdjson::ondemand::object obj = doc.get_object().value();
            simdjson_object_parser(obj, counts, fields, c.fields_count, tape, json);
            if (flag == 0) {
                os << *iter << json.data();
                os << parquet::EndRow;
            }
            else if (flag == 1) {
                prepare_for_csv(json, 1);
                json.push_back('\"');
                keys_vec.push_back(*iter);
                docs_vec.push_back(json);
            }
            else {
                auto str = fmt::format("{{\"_id\":{},\"doc\":{}}}\n", *iter, json.data());
                write(handle, str.data(), str.size());
            }
            json = flag == 1 ? csv_prefix_k : prefix_k;
            ++iter;
        }
    }
}

void export_parquet_docs( //
    ukv_docs_export_t& c,
    std::vector<ptr_range_gt<ukv_key_t const>> const& keys,
    ukv_size_t size_in_bytes,
    vals_t const& values) {

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
    make_uuid(file_name);

    auto maybe_outfile = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_if_error(maybe_outfile.ok(), c.error, 0, "Can't open file");
    auto outfile = *maybe_outfile;

    parquet::WriterProperties::Builder builder;
    builder.memory_pool(arrow::default_memory_pool());
    builder.write_batch_size(std::min(size_in_bytes, c.max_batch_size));

    parquet::StreamWriter os {parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

    if (c.fields)
        export_sub_docs(c, values, keys, nullptr, nullptr, &os, 0, 0);
    else
        export_whole_docs(values, keys, nullptr, nullptr, &os, 0, 0);
}

void export_csv_docs( //
    ukv_docs_export_t& c,
    std::vector<ptr_range_gt<ukv_key_t const>> const& keys,
    ukv_size_t size_in_bytes,
    vals_t const& values) {

    ukv_size_t size = 0;
    arrow::Status status;

    for (auto _ : keys)
        size += _.size();

    arrow::NumericBuilder<arrow::Int64Type> int_builder;
    arrow::StringBuilder str_builder;

    status = int_builder.Resize(size);
    return_if_error(status.ok(), c.error, 0, "Can't instatinate builder");
    status = str_builder.Resize(size);
    return_if_error(status.ok(), c.error, 0, "Can't instatinate builder");

    std::shared_ptr<arrow::Array> keys_array;
    std::shared_ptr<arrow::Array> docs_array;
    std::vector<ukv_key_t> keys_vec;
    std::vector<std::string> docs_vec;

    keys_vec.reserve(size);
    docs_vec.reserve(size);

    if (c.fields)
        export_sub_docs(c, values, keys, &docs_vec, &keys_vec, nullptr, 0, 1);
    else
        export_whole_docs(values, keys, &docs_vec, &keys_vec, nullptr, 0, 1);

    status = int_builder.AppendValues(keys_vec);
    return_if_error(status.ok(), c.error, 0, "Can't append keys");
    status = int_builder.Finish(&keys_array);
    return_if_error(status.ok(), c.error, 0, "Can't finish array(keys)");

    status = str_builder.AppendValues(docs_vec);
    return_if_error(status.ok(), c.error, 0, "Can't append docs");
    status = str_builder.Finish(&docs_array);
    return_if_error(status.ok(), c.error, 0, "Can't finish array(docs)");

    arrow::FieldVector fields;
    fields.push_back(arrow::field("_id", arrow::int64()));
    fields.push_back(arrow::field("doc", arrow::int64()));
    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);

    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, {keys_array, docs_array});

    char file_name[uuid_length_k];
    make_uuid(file_name);

    auto maybe_outstream = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_if_error(maybe_outstream.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *maybe_outstream;

    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
    return_if_error(status.ok(), c.error, 0, "Can't write in file");
}

void export_ndjson_docs( //
    ukv_docs_export_t& c,
    std::vector<ptr_range_gt<ukv_key_t const>> const& keys,
    ukv_size_t size_in_bytes,
    vals_t const& values) {

    char file_name[uuid_length_k];
    make_uuid(file_name);
    auto handle = open(fmt::format("{}{}", file_name, c.paths_extension).data(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

    if (c.fields)
        export_sub_docs(c, values, keys, nullptr, nullptr, nullptr, handle, 2);
    else
        export_whole_docs(values, keys, nullptr, nullptr, nullptr, handle, 2);

    close(handle);
}

#pragma region - Main Functions(Docs)

void ukv_docs_import(ukv_docs_import_t* c_ptr) {

    ukv_docs_import_t& c = *c_ptr;
    auto ext = std::filesystem::path(c.paths_pattern).extension();

    if (ext == ".ndjson")
        import_ndjson_docs(c);
    else {
        std::shared_ptr<arrow::Table> table;
        if (ext == ".parquet")
            import_parquet(c, table);
        else if (ext == ".csv")
            import_csv(c, table);
        parse_arrow_table(c, table);
    }
}

void ukv_docs_export(ukv_docs_export_t* c_ptr) {

    ukv_docs_export_t& c = *c_ptr;

    ///////// Choosing a method /////////

    auto ext = c.paths_extension;
    auto export_method = strcmp_(ext, ".parquet") //
                             ? &export_parquet_docs
                             : strcmp_(ext, ".ndjson") //
                                   ? &export_ndjson_docs
                                   : strcmp_(ext, ".csv") //
                                         ? &export_csv_docs
                                         : nullptr;

    return_if_error(export_method, c.error, 0, "Not supported format");

    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;

    ukv_size_t size_in_bytes = 0;
    ukv_size_t task_count = 1024;

    keys_stream_t stream(c.db, c.collection, task_count);
    std::vector<ptr_range_gt<ukv_key_t const>> keys;
    vals_t values;

    auto status = stream.seek_to_first();
    return_if_error(status, c.error, 0, "No batches in stream");

    while (!stream.is_end()) {
        keys.push_back(stream.keys_batch());
        values.push_back({nullptr, 0});

        ukv_docs_read_t docs_read {
            .db = c.db,
            .error = c.error,
            .arena = c.arena,
            .options = c.options,
            .tasks_count = stream.keys_batch().size(),
            .collections = &c.collection,
            .keys = stream.keys_batch().begin(),
            .keys_stride = sizeof(ukv_key_t),
            .offsets = &offsets,
            .lengths = &lengths,
            .values = &values.back().first,
        };
        ukv_docs_read(&docs_read);

        size_t count = stream.keys_batch().size();
        size_in_bytes += offsets[count - 1] + lengths[count - 1];
        values.back().second = offsets[count - 1] + lengths[count - 1];

        status = stream.seek_to_next_batch();
        return_if_error(status, c.error, 0, "Invalid batch");
    }

    export_method( //
        c,
        keys,
        size_in_bytes,
        values);
}

#pragma region - Docs End