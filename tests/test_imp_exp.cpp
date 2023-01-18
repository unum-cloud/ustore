#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster
#include <unistd.h>

#include <string>
#include <cstring>
#include <filesystem>
#include <unordered_map>

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
#include <gtest/gtest.h>

#include <ukv/ukv.hpp>
#include "../tools/dataset.h"

using namespace unum::ukv;
using graph_t = std::vector<edge_t>;
using docs_t = std::unordered_map<ukv_key_t, std::string>;

namespace fs = std::filesystem;

constexpr ukv_str_view_t dataset_path_k = "/home/vscode/Datasets/tweets32K.ndjson";
constexpr ukv_str_view_t parquet_path_k = "/home/vscode/Datasets/tweets32K-clean.parquet";
constexpr ukv_str_view_t csv_path_k = "/home/vscode/Datasets/tweets32K-clean.csv";
constexpr ukv_str_view_t ndjson_path_k = "sample_docs.ndjson";
constexpr ukv_str_view_t path_k = "./";
constexpr size_t rows_count_k = 1000;

static constexpr ukv_str_view_t parquet_k = "sample.parquet";
static constexpr ukv_str_view_t ndjson_k = "sample.ndjson";
static constexpr ukv_str_view_t csv_k = "sample.csv";
static constexpr ukv_str_view_t ext_parquet_k = ".parquet";
static constexpr ukv_str_view_t ext_ndjson_k = ".ndjson";
static constexpr ukv_str_view_t ext_csv_k = ".csv";

constexpr size_t prefixes_count_k = 4;
constexpr ukv_str_view_t prefixes_ak[prefixes_count_k] = {
    "id",
    "id_str",
    "user",
    "quoted_status",
};

constexpr size_t fields_paths_count_k = 13;
static constexpr ukv_str_view_t fields_paths_ak[fields_paths_count_k] = {
    "id",
    "id_str",
    "/user/id",
    "/user/followers_count",
    "/quoted_status/id",
    "/quoted_status/user",
    "/quoted_status/entities/hashtags",
    "/quoted_status/entities/media/0/id",
    "/quoted_status/entities/media/0/sizes/small",
    "/quoted_status/entities/media/0/sizes/large",
    "/quoted_status/extended_entities/media/0/video_info/variants/0",
    "/quoted_status/extended_entities/media/0/video_info/variants/1",
    "/quoted_status/extended_entities/media/0/sizes",
};

constexpr size_t fields_columns_count_k = 7;
static constexpr ukv_str_view_t fields_columns_ak[fields_columns_count_k] = {
    "id",
    "id_str",
    "user_id",
    "user_followers_count",
    "geo_type",
    "favorited",
    "retweeted",
};

static constexpr ukv_str_view_t source_path_k = "id";
static constexpr ukv_str_view_t target_path_k = "/user/id";
static constexpr ukv_str_view_t edge_path_k = "/user/followers_count";
static constexpr ukv_str_view_t source_field_k = "id";
static constexpr ukv_str_view_t target_field_k = "user_id";
static constexpr ukv_str_view_t edge_field_k = "user_followers_count";
static constexpr ukv_str_view_t doc_k = "doc";
static constexpr ukv_str_view_t id_k = "_id";

std::vector<std::string> paths;
graph_t expected_edges;
docs_t docs_w_keys;

static database_t db;

simdjson::ondemand::object& rewind(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

class arrow_visitor_at {
  public:
    arrow_visitor_at(std::string& json) : json(json) {}

    arrow::Status Visit(arrow::NullArray const& arr) {
        fmt::format_to(std::back_inserter(json), "\"\",");
        return arrow::Status::OK();
    }
    arrow::Status Visit(arrow::BooleanArray const& arr) { return format(arr, idx); }
    arrow::Status Visit(arrow::Int8Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::Int16Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::Int32Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::Int64Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::UInt8Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::UInt16Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::UInt32Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::UInt64Array const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::HalfFloatArray const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::FloatArray const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
    arrow::Status Visit(arrow::DoubleArray const& arr) {
        if (id_field)
            key = arr.Value(idx), id_field = false;
        return format(arr, idx);
    }
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
    bool id_field = false;
    ukv_key_t key;
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
        for (std::size_t ch_idx = 0; ch_idx != str.size(); ++ch_idx) {
            uint8_t c = str[ch_idx];
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

void make_ndjson_docs() {
    std::filesystem::path pt {dataset_path_k};
    size_t size = std::filesystem::file_size(pt);

    int fd = open(ndjson_path_k, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    auto handle = open(dataset_path_k, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);

    auto get_value = [](simdjson::ondemand::object& obj, ukv_str_view_t field) {
        return (field[0] == '/') ? rewind(obj).at_pointer(field) : rewind(obj)[field];
    };

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    std::string json;
    size_t row = 0;
    bool state = false;
    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();
        state = false;
        json = "{";
        for (size_t idx = 0; idx < fields_paths_count_k; ++idx) {
            auto result = get_value(obj, fields_paths_ak[idx]);
            if (result.error() != simdjson::SUCCESS) {
                state = true;
                break;
            }
        }
        if (state)
            continue;
        for (size_t idx = 0; idx < prefixes_count_k; ++idx) {
            switch (obj[prefixes_ak[idx]].type()) {
            case simdjson::ondemand::json_type::array:
                fmt::format_to(std::back_inserter(json),
                               "\"{}\":{},",
                               prefixes_ak[idx],
                               obj[prefixes_ak[idx]].get_array().value().raw_json().value());
                break;
            case simdjson::ondemand::json_type::object:
                fmt::format_to(std::back_inserter(json),
                               "\"{}\":{},",
                               prefixes_ak[idx],
                               obj[prefixes_ak[idx]].get_object().value().raw_json().value());
                break;
            case simdjson::ondemand::json_type::number:
                fmt::format_to(std::back_inserter(json),
                               "\"{}\":{},",
                               prefixes_ak[idx],
                               std::string_view(obj[prefixes_ak[idx]].raw_json_token().value()));
                break;
            case simdjson::ondemand::json_type::string:
                fmt::format_to(std::back_inserter(json),
                               "\"{}\":{},",
                               prefixes_ak[idx],
                               std::string_view(obj[prefixes_ak[idx]].raw_json_token().value()));
                break;
            case simdjson::ondemand::json_type::boolean:
                fmt::format_to(std::back_inserter(json),
                               "\"{}\":{},",
                               prefixes_ak[idx],
                               std::string_view(obj[prefixes_ak[idx]].value()));
                break;
            default: break;
            }
        }
        json.back() = '}';
        json.push_back('\n');

        write(fd, json.data(), json.size());
        ++row;
        if (row == rows_count_k)
            break;
    }
    close(fd);
    close(handle);
}

void make_parquet_graph() {
    parquet::schema::NodeVector fields;
    fields.push_back(parquet::schema::PrimitiveNode::Make( //
        source_field_k,
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::INT_64));

    fields.push_back(parquet::schema::PrimitiveNode::Make( //
        target_field_k,
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::INT_64));

    fields.push_back(parquet::schema::PrimitiveNode::Make( //
        edge_field_k,
        parquet::Repetition::REQUIRED,
        parquet::Type::INT64,
        parquet::ConvertedType::INT_64));

    std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    auto outfile = *arrow::io::FileOutputStream::Open(parquet_k);

    parquet::WriterProperties::Builder builder;
    builder.memory_pool(arrow::default_memory_pool());
    builder.write_batch_size(rows_count_k);

    parquet::StreamWriter os {parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

    for (auto edge : expected_edges) {
        os << edge.source_id << edge.target_id << edge.id << parquet::EndRow;
    }
}

void make_csv_graph() {
    arrow::Status status;
    arrow::NumericBuilder<arrow::Int64Type> builder;
    status = builder.Resize(expected_edges.size());
    std::shared_ptr<arrow::Array> sources_array;
    std::shared_ptr<arrow::Array> targets_array;
    std::shared_ptr<arrow::Array> edges_array;
    std::vector<ukv_key_t> values(expected_edges.size());

    size_t idx = 0;
    for (auto edge : expected_edges)
        values[idx] = edge.source_id, ++idx;
    status = builder.AppendValues(values);
    status = builder.Finish(&sources_array);
    idx = 0;
    for (auto edge : expected_edges)
        values[idx] = edge.target_id, ++idx;
    status = builder.AppendValues(values);
    status = builder.Finish(&targets_array);
    idx = 0;
    for (auto edge : expected_edges)
        values[idx] = edge.id, ++idx;
    status = builder.AppendValues(values);
    status = builder.Finish(&edges_array);

    arrow::FieldVector fields;
    fields.push_back(arrow::field(source_field_k, arrow::int64()));
    fields.push_back(arrow::field(target_field_k, arrow::int64()));
    fields.push_back(arrow::field(edge_field_k, arrow::int64()));

    std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
    std::shared_ptr<arrow::Table> table = nullptr;
    table = arrow::Table::Make(schema, {sources_array, targets_array, edges_array});
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *arrow::io::FileOutputStream::Open(csv_k);
    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
}

void make_ndjson_graph() {
    auto handle = open(ndjson_k, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

    for (auto edge : expected_edges) {
        auto str = fmt::format( //
            "{{\"{}\":{},\"{}\":{},\"{}\":{}}}\n",
            source_field_k,
            edge.source_id,
            target_field_k,
            edge.target_id,
            edge_field_k,
            edge.id);
        write(handle, str.data(), str.size());
    }
    close(handle);
}

void make_test_files_graph() {
    std::filesystem::path pt {dataset_path_k};
    size_t size = std::filesystem::file_size(pt);

    auto handle = open(dataset_path_k, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);

    auto get_value = [](simdjson::ondemand::object& obj, ukv_str_view_t field) {
        return (field[0] == '/') ? rewind(obj).at_pointer(field) : rewind(obj)[field];
    };

    expected_edges.reserve(rows_count_k);
    edge_t edge;

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();
        try {
            edge = edge_t {
                get_value(obj, source_path_k),
                get_value(obj, target_field_k),
                get_value(obj, edge_field_k),
            };
        }
        catch (simdjson::simdjson_error const& ex) {
            continue;
        }
        expected_edges.push_back(edge);
        if (expected_edges.size() == rows_count_k)
            break;
    }
    make_parquet_graph();
    make_csv_graph();
    make_ndjson_graph();
    for (size_t size = expected_edges.size(), idx = 0; idx < size; ++idx) {
        expected_edges.push_back(edge_t {
            expected_edges[idx].target_id,
            expected_edges[idx].source_id,
            expected_edges[idx].id,
        });
    }
    std::sort(expected_edges.begin(), expected_edges.end(), [](auto const& lhs, auto const& rhs) {
        return lhs.source_id < rhs.source_id;
    });
    munmap((void*)mapped_content.data(), mapped_content.size());
    close(handle);
}

void delete_test_files() {
    std::remove(parquet_k);
    std::remove(csv_k);
    std::remove(ndjson_k);
    std::remove(ndjson_path_k);
}

void fill_array_from_ndjson(graph_t& array, std::string_view const& mapped_content) {

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    auto get_value = [](simdjson::ondemand::object& data, ukv_str_view_t field) {
        return (field[0] == '/') ? rewind(data).at_pointer(field) : rewind(data)[field];
    };

    for (auto doc : docs) {
        simdjson::ondemand::object data = doc.get_object().value();
        array.push_back(edge_t {
            get_value(data, source_field_k),
            get_value(data, target_field_k),
            get_value(data, edge_field_k),
        });
    }
}

void fill_array_from_table(graph_t& array, std::shared_ptr<arrow::Table>& table) {

    auto sources = table->GetColumnByName(source_field_k);
    auto targets = table->GetColumnByName(target_field_k);
    auto edges = table->GetColumnByName(edge_field_k);
    size_t count = sources->num_chunks();
    array.reserve(ukv_size_t(sources->chunk(0)->length()));

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto source_chunk = sources->chunk(chunk_idx);
        auto target_chunk = targets->chunk(chunk_idx);
        auto edge_chunk = edges->chunk(chunk_idx);
        auto source_array = std::static_pointer_cast<arrow::Int64Array>(source_chunk);
        auto target_array = std::static_pointer_cast<arrow::Int64Array>(target_chunk);
        auto edge_array = std::static_pointer_cast<arrow::Int64Array>(edge_chunk);
        for (size_t value_idx = 0; value_idx != source_array->length(); ++value_idx) {
            array.push_back(edge_t {
                source_array->Value(value_idx),
                target_array->Value(value_idx),
                edge_array->Value(value_idx),
            });
        }
    }
}

void fill_array(ukv_str_view_t file_name, graph_t& array) {

    auto ext = std::filesystem::path(file_name).extension();

    if (ext == ".ndjson") {
        std::filesystem::path pt {file_name};
        size_t size = std::filesystem::file_size(pt);

        auto handle = open(file_name, O_RDONLY);
        auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
        std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
        madvise(begin, size, MADV_SEQUENTIAL);
        fill_array_from_ndjson(array, mapped_content);
        munmap((void*)mapped_content.data(), mapped_content.size());
    }
    else {
        std::shared_ptr<arrow::Table> table;
        if (ext == ".parquet") {
            arrow::MemoryPool* pool = arrow::default_memory_pool();
            auto input = *arrow::io::ReadableFile::Open(file_name);
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            parquet::arrow::OpenFile(input, pool, &arrow_reader);
            arrow_reader->ReadTable(&table);
        }
        else if (ext == ".csv") {
            std::shared_ptr<arrow::io::InputStream> input = *arrow::io::ReadableFile::Open(file_name);
            auto read_options = arrow::csv::ReadOptions::Defaults();
            auto parse_options = arrow::csv::ParseOptions::Defaults();
            auto convert_options = arrow::csv::ConvertOptions::Defaults();
            arrow::io::IOContext io_context = arrow::io::default_io_context();

            std::shared_ptr<arrow::csv::TableReader> reader =
                *arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
            table = *reader->Read();
        }
        fill_array_from_table(array, table);
    }
}

bool cmp_graph(ukv_str_view_t src) {

    graph_t edges;

    fill_array(src, edges);
    EXPECT_EQ(edges.size(), expected_edges.size());

    for (size_t idx = 0; idx < expected_edges.size(); ++idx) {
        size_t idx_ = idx;
        while (edges[idx].target_id != expected_edges[idx_].target_id)
            ++idx_;
        if (idx != idx_)
            std::swap(expected_edges[idx], expected_edges[idx_]);
        EXPECT_EQ(expected_edges[idx].source_id, edges[idx].source_id);
        EXPECT_EQ(expected_edges[idx].target_id, edges[idx].target_id);
        EXPECT_EQ(expected_edges[idx].id, edges[idx].id);
    }
    return true;
}

bool test_graph(ukv_str_view_t file, ukv_str_view_t ext) {

    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    std::vector<std::string> updated_paths;
    std::string new_file;

    ukv_graph_import_t imp {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_import(&imp);

    EXPECT_TRUE(status);

    ukv_graph_export_t exp {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_export(&exp);

    EXPECT_TRUE(status);

    for (const auto& entry : fs::directory_iterator(path_k))
        updated_paths.push_back(entry.path());

    EXPECT_GT(updated_paths.size(), paths.size());

    for (size_t idx = 0; idx < paths.size(); ++idx) {
        if (paths[idx] != updated_paths[idx]) {
            new_file = updated_paths[idx];
            break;
        }
    }
    if (new_file.size() == 0)
        new_file = updated_paths.back();

    new_file.erase(0, 2);
    EXPECT_TRUE(cmp_graph(new_file.data()));

    std::remove(new_file.data());
    db.clear().throw_unhandled();
    return true;
}

void fill_from_table(std::shared_ptr<arrow::Table>& table) {
    auto id = table->GetColumnByName(id_k);
    auto doc = table->GetColumnByName(doc_k);
    size_t count = id->num_chunks();

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto id_chunk = id->chunk(chunk_idx);
        auto doc_chunk = doc->chunk(chunk_idx);
        auto id_array = std::static_pointer_cast<arrow::Int64Array>(id_chunk);
        auto doc_array = std::static_pointer_cast<arrow::BinaryArray>(doc_chunk);
        for (size_t value_idx = 0; value_idx != id_array->length(); ++value_idx)
            docs_w_keys[id_array->Value(value_idx)] = doc_array->Value(value_idx);
    }
}

void fill_from_ndjson(ukv_str_view_t file_name) {

    std::filesystem::path pt {file_name};
    size_t size = std::filesystem::file_size(pt);

    auto handle = open(file_name, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();
        auto data = rewind(obj)[doc_k].get_object().value().raw_json().value();
        auto str = std::string(data.data(), data.size());
        ukv_key_t key = rewind(obj)[id_k];
        docs_w_keys[key] = str;
    }
    close(handle);
}

void fill_docs_w_keys(ukv_str_view_t file_name) {

    docs_w_keys.clear();
    auto ext = std::filesystem::path(file_name).extension();

    if (ext == ".ndjson")
        fill_from_ndjson(file_name);
    else {
        std::shared_ptr<arrow::Table> table;
        if (ext == ".parquet") {
            arrow::MemoryPool* pool = arrow::default_memory_pool();
            auto input = *arrow::io::ReadableFile::Open(file_name);
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            parquet::arrow::OpenFile(input, pool, &arrow_reader);
            arrow_reader->ReadTable(&table);
        }
        else if (ext == ".csv") {
            std::shared_ptr<arrow::io::InputStream> input = *arrow::io::ReadableFile::Open(file_name);
            auto read_options = arrow::csv::ReadOptions::Defaults();
            auto parse_options = arrow::csv::ParseOptions::Defaults();
            auto convert_options = arrow::csv::ConvertOptions::Defaults();
            arrow::io::IOContext io_context = arrow::io::default_io_context();

            std::shared_ptr<arrow::csv::TableReader> reader =
                *arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
            table = *reader->Read();
        }
        fill_from_table(table);
    }
}

bool cmp_ndjson_docs_sub(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    std::filesystem::path pt {lhs};
    size_t size = std::filesystem::file_size(pt);

    auto handle = open(lhs, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);
    fill_docs_w_keys(rhs);

    auto get_value = [](simdjson::ondemand::object& obj, ukv_str_view_t field) {
        return (field[0] == '/') ? rewind(obj).at_pointer(field) : rewind(obj)[field];
    };

    simdjson::ondemand::parser parser_l;
    simdjson::ondemand::parser parser_r;
    simdjson::ondemand::document_stream docs = parser_l.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    for (auto doc_l : docs) {
        simdjson::ondemand::object obj_l = doc_l.get_object().value();
        simdjson::ondemand::document doc_r = parser_r.iterate( //
            docs_w_keys[get_value(obj_l, fields_paths_ak[0])],
            1000000);
        simdjson::ondemand::object obj_r = doc_r.get_object().value();

        for (size_t idx = 0; idx < fields_paths_count_k; ++idx) {
            auto data_l = get_value(obj_l, fields_paths_ak[idx]);
            auto data_r = get_value(obj_r, fields_paths_ak[idx]);
            switch (data_l.type()) {
            case simdjson::ondemand::json_type::object:
                EXPECT_EQ(data_l.get_object().value().raw_json().value(),
                          data_r.get_object().value().raw_json().value());
                break;
            case simdjson::ondemand::json_type::array:
                EXPECT_EQ(data_l.get_array().value().raw_json().value(), data_r.get_array().value().raw_json().value());
                break;
            case simdjson::ondemand::json_type::string:
                EXPECT_EQ(data_l.raw_json_token().value(), data_r.raw_json_token().value());
                break;
            case simdjson::ondemand::json_type::number: {
                if (data_l.is_integer())
                    EXPECT_EQ(ukv_key_t(data_l), ukv_key_t(data_r));
                else
                    EXPECT_EQ(double(data_l), double(data_r));
            } break;
            case simdjson::ondemand::json_type::boolean: EXPECT_EQ(bool(data_l), bool(data_r)); break;
            default: break;
            }
        }
    }

    munmap((void*)mapped_content.data(), mapped_content.size());
    close(handle);
    return true;
}

bool cmp_ndjson_docs_whole(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    std::filesystem::path pt {lhs};
    size_t size = std::filesystem::file_size(pt);

    auto handle = open(lhs, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);
    fill_docs_w_keys(rhs);

    auto get_value = [](simdjson::ondemand::object& obj, ukv_str_view_t field) {
        return (field[0] == '/') ? rewind(obj).at_pointer(field) : rewind(obj)[field];
    };

    simdjson::ondemand::parser parser_l;
    simdjson::ondemand::parser parser_r;
    simdjson::ondemand::document_stream docs = parser_l.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    for (auto doc_l : docs) {
        simdjson::ondemand::object obj_l = doc_l.get_object().value();
        simdjson::ondemand::document doc_r = parser_r.iterate( //
            docs_w_keys[get_value(obj_l, fields_paths_ak[0])],
            1000000);
        simdjson::ondemand::object obj_r = doc_r.get_object().value();

        auto data_l = rewind(obj_l).raw_json().value();
        auto data_r = rewind(obj_r).raw_json().value();
        std::string str_l(data_l.data(), data_l.size() - 1);
        std::string str_r(data_r.data(), data_r.size());
        EXPECT_EQ(str_l, str_r);
    }

    munmap((void*)mapped_content.data(), mapped_content.size());
    close(handle);
    return true;
}

bool cmp_table_docs_whole(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    docs_t docs_w_keys_;
    std::string json = "{";
    std::vector<ukv_key_t> keys;
    arrow_visitor_at visitor(json);
    std::shared_ptr<arrow::Table> table;
    auto ext = std::filesystem::path(lhs).extension();
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;

    if (ext == ".csv") {
        std::shared_ptr<arrow::io::InputStream> input = *arrow::io::ReadableFile::Open(lhs);
        auto read_options = arrow::csv::ReadOptions::Defaults();
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        arrow::io::IOContext io_context = arrow::io::default_io_context();
        std::shared_ptr<arrow::csv::TableReader> reader =
            *arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
        table = *reader->Read();
    }
    else if (ext == ".parquet") {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto input = *arrow::io::ReadableFile::Open(lhs);
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        parquet::arrow::OpenFile(input, pool, &arrow_reader);
        arrow_reader->ReadTable(&table);
    }
    fill_docs_w_keys(rhs);

    auto fields = table->ColumnNames();
    chunks.resize(fields.size());
    columns.resize(fields.size());
    for (size_t idx = 0; idx < fields.size(); ++idx)
        columns[idx] = table->GetColumnByName(fields[idx]);

    size_t count = columns[0]->num_chunks();
    for (size_t chunk_idx = 0, g_idx = 0; chunk_idx != count; ++chunk_idx, g_idx = 0) {
        for (auto column : columns) {
            chunks[g_idx] = column->chunk(chunk_idx);
            ++g_idx;
        }
        for (size_t value_idx = 0; value_idx < columns[0]->chunk(chunk_idx)->length(); ++value_idx) {
            g_idx = 0;
            visitor.id_field = true;
            for (auto field : fields) {
                fmt::format_to(std::back_inserter(json), "\"{}\":", field);
                visitor.idx = value_idx;
                arrow::VisitArrayInline(*chunks[g_idx].get(), &visitor);
                ++g_idx;
            }
            json[json.size() - 1] = '}';
            docs_w_keys_[visitor.key] = json;
            keys.push_back(visitor.key);
            json = "{";
        }
    }

    for (auto key : keys)
        EXPECT_EQ(docs_w_keys[key], docs_w_keys_[key]);

    return true;
}

bool cmp_table_docs_sub(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    docs_t docs_w_keys_;
    std::string json = "{";
    std::vector<ukv_key_t> keys;
    arrow_visitor_at visitor(json);
    std::shared_ptr<arrow::Table> table;
    auto ext = std::filesystem::path(lhs).extension();
    std::vector<std::shared_ptr<arrow::Array>> chunks(fields_columns_count_k);
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(fields_columns_count_k);

    if (ext == ".csv") {
        std::shared_ptr<arrow::io::InputStream> input = *arrow::io::ReadableFile::Open(lhs);
        auto read_options = arrow::csv::ReadOptions::Defaults();
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        arrow::io::IOContext io_context = arrow::io::default_io_context();
        std::shared_ptr<arrow::csv::TableReader> reader =
            *arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
        table = *reader->Read();
    }
    else if (ext == ".parquet") {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto input = *arrow::io::ReadableFile::Open(lhs);
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        parquet::arrow::OpenFile(input, pool, &arrow_reader);
        arrow_reader->ReadTable(&table);
    }
    fill_docs_w_keys(rhs);

    for (size_t idx = 0; idx < fields_columns_count_k; ++idx)
        columns[idx] = table->GetColumnByName(fields_columns_ak[idx]);

    size_t count = columns[0]->num_chunks();
    for (size_t chunk_idx = 0, g_idx = 0; chunk_idx != count; ++chunk_idx, g_idx = 0) {
        for (auto column : columns) {
            chunks[g_idx] = column->chunk(chunk_idx);
            ++g_idx;
        }
        for (size_t value_idx = 0; value_idx < columns[0]->chunk(chunk_idx)->length(); ++value_idx) {
            g_idx = 0;
            visitor.id_field = true;
            for (auto it = fields_columns_ak; g_idx < fields_columns_count_k; ++g_idx, ++it) {
                fmt::format_to(std::back_inserter(json), "\"{}\":", *it);
                visitor.idx = value_idx;
                arrow::VisitArrayInline(*chunks[g_idx].get(), &visitor);
            }
            json[json.size() - 1] = '}';
            docs_w_keys_[visitor.key] = json;
            keys.push_back(visitor.key);
            json = "{";
        }
    }

    for (auto key : keys)
        EXPECT_EQ(docs_w_keys[key], docs_w_keys_[key]);

    return true;
}

template <typename comparator>
bool test_sub_docs(ukv_str_view_t file, ukv_str_view_t ext, comparator cmp, bool state = false) {

    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    std::vector<std::string> updated_paths;
    std::string new_file;

    ukv_docs_import_t docs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .fields_count = state ? fields_columns_count_k : fields_paths_count_k,
        .fields = state ? fields_columns_ak : fields_paths_ak,
        .fields_stride = sizeof(ukv_str_view_t),
        .id_field = fields_columns_ak[0],
    };
    ukv_docs_import(&docs);

    EXPECT_TRUE(status);

    ukv_docs_export_t exdocs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .fields_count = state ? fields_columns_count_k : fields_paths_count_k,
        .fields = state ? fields_columns_ak : fields_paths_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_export(&exdocs);

    EXPECT_TRUE(status);

    for (const auto& entry : fs::directory_iterator(path_k))
        updated_paths.push_back(entry.path());

    EXPECT_GT(updated_paths.size(), paths.size());

    for (size_t idx = 0; idx < paths.size(); ++idx) {
        if (paths[idx] != updated_paths[idx]) {
            new_file = updated_paths[idx];
            break;
        }
    }
    if (new_file.size() == 0)
        new_file = updated_paths.back();

    new_file.erase(0, 2);
    EXPECT_TRUE(cmp(file, new_file.data()));

    std::remove(new_file.data());
    db.clear().throw_unhandled();
    return true;
}

template <typename comparator>
bool test_whole_docs(ukv_str_view_t file, ukv_str_view_t ext, comparator cmp, bool state = false) {

    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    std::vector<std::string> updated_paths;
    std::string new_file;

    ukv_docs_import_t docs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .id_field = fields_paths_ak[0],
    };
    ukv_docs_import(&docs);

    EXPECT_TRUE(status);

    ukv_docs_export_t exdocs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
    };
    ukv_docs_export(&exdocs);

    EXPECT_TRUE(status);

    for (const auto& entry : fs::directory_iterator(path_k))
        updated_paths.push_back(entry.path());

    EXPECT_GT(updated_paths.size(), paths.size());

    for (size_t idx = 0; idx < paths.size(); ++idx) {
        if (paths[idx] != updated_paths[idx]) {
            new_file = updated_paths[idx];
            break;
        }
    }
    if (new_file.size() == 0)
        new_file = updated_paths.back();

    new_file.erase(0, 2);
    EXPECT_TRUE(cmp(file, new_file.data()));

    std::remove(new_file.data());
    db.clear().throw_unhandled();
    return true;
}

bool test_crash_cases_graph_import(ukv_str_view_t file) {
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t size = 0;

    if (std::strcmp(ndjson_k, file) == 0) {
        std::filesystem::path pt {file};
        size = std::filesystem::file_size(pt);
    }

    ukv_graph_import_t imp_path_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = nullptr,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_import(&imp_path_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_graph_import_t imp_source_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .source_id_field = nullptr,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_import(&imp_source_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_graph_import_t imp_target_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .source_id_field = source_field_k,
        .target_id_field = nullptr,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_import(&imp_target_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_graph_import_t imp_edge_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = nullptr,
    };
    ukv_graph_import(&imp_edge_null);
    EXPECT_TRUE(status);
    status.release_error();

    ukv_graph_import_t imp_db_null {
        .db = nullptr,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_import(&imp_db_null);
    EXPECT_FALSE(status);
    db.clear().throw_unhandled();
    return true;
}

bool test_crash_cases_graph_export(ukv_str_view_t ext) {
    auto collection = db.main();
    arena_t arena(db);
    status_t status;
    size_t size = 0;

    ukv_graph_export_t exp_path_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = nullptr,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_export(&exp_path_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_graph_export_t exp_source_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .source_id_field = nullptr,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_export(&exp_source_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_graph_export_t exp_target_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .source_id_field = source_field_k,
        .target_id_field = nullptr,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_export(&exp_target_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_graph_export_t exp_edge_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = nullptr,
    };
    ukv_graph_export(&exp_edge_null);
    EXPECT_TRUE(status);
    status.release_error();

    ukv_graph_export_t exp_db_null {
        .db = nullptr,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .source_id_field = source_field_k,
        .target_id_field = target_field_k,
        .edge_id_field = edge_field_k,
    };
    ukv_graph_export(&exp_db_null);
    EXPECT_FALSE(status);

    for (const auto& entry : fs::directory_iterator(path_k)) {
        std::string path = entry.path();
        if (std::strcmp(path.data() + (path.size() - strlen(ext)), ext) == 0)
            std::remove(path.data());
    }
    db.clear().throw_unhandled();
    return true;
}

bool test_crash_cases_docs_import(ukv_str_view_t file) {
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t size = 0;

    if (std::strcmp(ndjson_k, file) == 0) {
        std::filesystem::path pt {file};
        size = std::filesystem::file_size(pt);
    }

    ukv_docs_import_t imp_path_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = nullptr,
        .fields_count = prefixes_count_k,
        .fields = prefixes_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_import(&imp_path_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_import_t imp_count_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .fields_count = 0,
        .fields = prefixes_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_import(&imp_count_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_import_t imp_fields_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .fields_count = prefixes_count_k,
        .fields = nullptr,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_import(&imp_fields_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_import_t imp_stride_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .fields_count = prefixes_count_k,
        .fields = prefixes_ak,
        .fields_stride = 0,
    };
    ukv_docs_import(&imp_stride_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_import_t imp_db_null {
        .db = nullptr,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_pattern = file,
        .fields_count = prefixes_count_k,
        .fields = prefixes_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_import(&imp_db_null);
    EXPECT_FALSE(status);
    db.clear().throw_unhandled();
    return true;
}

bool test_crash_cases_docs_export(ukv_str_view_t ext) {
    auto collection = db.main();
    arena_t arena(db);
    status_t status;
    size_t size = 0;

    ukv_docs_export_t imp_path_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = nullptr,
        .fields_count = prefixes_count_k,
        .fields = prefixes_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_export(&imp_path_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_export_t imp_count_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .fields_count = 0,
        .fields = prefixes_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_export(&imp_count_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_export_t imp_fields_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .fields_count = prefixes_count_k,
        .fields = nullptr,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_export(&imp_fields_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_export_t imp_stride_null {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .fields_count = prefixes_count_k,
        .fields = prefixes_ak,
        .fields_stride = 0,
    };
    ukv_docs_export(&imp_stride_null);
    EXPECT_FALSE(status);
    status.release_error();

    ukv_docs_export_t imp_db_null {
        .db = nullptr,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection,
        .paths_extension = ext,
        .fields_count = prefixes_count_k,
        .fields = prefixes_ak,
        .fields_stride = sizeof(ukv_str_view_t),
    };
    ukv_docs_export(&imp_db_null);
    EXPECT_FALSE(status);
    db.clear().throw_unhandled();
    return true;
}

TEST(import_export_graph, ndjosn_ndjson) {
    test_graph(ndjson_k, ext_ndjson_k);
}
TEST(import_export_graph, ndjosn_parquet) {
    test_graph(ndjson_k, ext_parquet_k);
}
TEST(import_export_graph, ndjosn_csv) {
    test_graph(ndjson_k, ext_csv_k);
}

TEST(import_export_graph, parquet_ndjson) {
    test_graph(parquet_k, ext_ndjson_k);
}
TEST(import_export_graph, parquet_parquet) {
    test_graph(parquet_k, ext_parquet_k);
}
TEST(import_export_graph, parquet_csv) {
    test_graph(parquet_k, ext_csv_k);
}

TEST(import_export_graph, csv_ndjson) {
    test_graph(csv_k, ext_ndjson_k);
}
TEST(import_export_graph, csv_parquet) {
    test_graph(csv_k, ext_parquet_k);
}
TEST(import_export_graph, csv_csv) {
    test_graph(csv_k, ext_csv_k);
}

TEST(import_export_docs_whole, ndjosn_ndjson) {
    test_whole_docs(ndjson_path_k, ext_ndjson_k, cmp_ndjson_docs_whole);
}
TEST(import_export_docs_whole, ndjosn_parquet) {
    test_whole_docs(ndjson_path_k, ext_parquet_k, cmp_ndjson_docs_whole);
}
TEST(import_export_docs_whole, ndjosn_csv) {
    test_whole_docs(ndjson_path_k, ext_csv_k, cmp_ndjson_docs_whole);
}

TEST(import_export_docs_whole, parquet_ndjson) {
    test_whole_docs(parquet_path_k, ext_ndjson_k, cmp_table_docs_whole, true);
}
TEST(import_export_docs_whole, parquet_parquet) {
    test_whole_docs(parquet_path_k, ext_parquet_k, cmp_table_docs_whole, true);
}
TEST(import_export_docs_whole, parquet_csv) {
    test_whole_docs(parquet_path_k, ext_csv_k, cmp_table_docs_whole, true);
}

TEST(import_export_docs_whole, csv_ndjson) {
    test_whole_docs(csv_path_k, ext_ndjson_k, cmp_table_docs_whole, true);
}
TEST(import_export_docs_whole, csv_parquet) {
    test_whole_docs(csv_path_k, ext_parquet_k, cmp_table_docs_whole, true);
}
TEST(import_export_docs_whole, csv_csv) {
    test_whole_docs(csv_path_k, ext_csv_k, cmp_table_docs_whole, true);
}

TEST(import_export_docs_sub, ndjosn_ndjson) {
    test_sub_docs(ndjson_path_k, ext_ndjson_k, cmp_ndjson_docs_sub);
}
TEST(import_export_docs_sub, ndjosn_parquet) {
    test_sub_docs(ndjson_path_k, ext_parquet_k, cmp_ndjson_docs_sub);
}
TEST(import_export_docs_sub, ndjosn_csv) {
    test_sub_docs(ndjson_path_k, ext_csv_k, cmp_ndjson_docs_sub);
}

TEST(import_export_docs_sub, parquet_ndjson) {
    test_sub_docs(parquet_path_k, ext_ndjson_k, cmp_table_docs_sub, true);
}
TEST(import_export_docs_sub, parquet_parquet) {
    test_sub_docs(parquet_path_k, ext_parquet_k, cmp_table_docs_sub, true);
}
TEST(import_export_docs_sub, parquet_csv) {
    test_sub_docs(parquet_path_k, ext_csv_k, cmp_table_docs_sub, true);
}

TEST(import_export_docs_sub, csv_ndjson) {
    test_sub_docs(csv_path_k, ext_ndjson_k, cmp_table_docs_sub, true);
}
TEST(import_export_docs_sub, csv_parquet) {
    test_sub_docs(csv_path_k, ext_parquet_k, cmp_table_docs_sub, true);
}
TEST(import_export_docs_sub, csv_csv) {
    test_sub_docs(csv_path_k, ext_csv_k, cmp_table_docs_sub, true);
}

TEST(crash_cases, graph_import) {
    test_crash_cases_graph_import(ndjson_k);
    test_crash_cases_graph_import(ndjson_k);
    test_crash_cases_graph_import(ndjson_k);
    test_crash_cases_graph_import(parquet_k);
    test_crash_cases_graph_import(parquet_k);
    test_crash_cases_graph_import(parquet_k);
    test_crash_cases_graph_import(csv_k);
    test_crash_cases_graph_import(csv_k);
    test_crash_cases_graph_import(csv_k);
}

TEST(crash_cases, graph_export) {
    test_crash_cases_graph_export(ext_ndjson_k);
    test_crash_cases_graph_export(ext_parquet_k);
    test_crash_cases_graph_export(ext_csv_k);
    test_crash_cases_graph_export(ext_ndjson_k);
    test_crash_cases_graph_export(ext_parquet_k);
    test_crash_cases_graph_export(ext_csv_k);
    test_crash_cases_graph_export(ext_ndjson_k);
    test_crash_cases_graph_export(ext_parquet_k);
    test_crash_cases_graph_export(ext_csv_k);
}

TEST(crash_cases, docs_import) {
    test_crash_cases_docs_import(ndjson_path_k);
    test_crash_cases_docs_import(ndjson_path_k);
    test_crash_cases_docs_import(ndjson_path_k);
    test_crash_cases_docs_import(parquet_path_k);
    test_crash_cases_docs_import(parquet_path_k);
    test_crash_cases_docs_import(parquet_path_k);
    test_crash_cases_docs_import(csv_path_k);
    test_crash_cases_docs_import(csv_path_k);
    test_crash_cases_docs_import(csv_path_k);
}

TEST(crash_cases, docs_export) {
    test_crash_cases_docs_export(ext_ndjson_k);
    test_crash_cases_docs_export(ext_parquet_k);
    test_crash_cases_docs_export(ext_csv_k);
    test_crash_cases_docs_export(ext_ndjson_k);
    test_crash_cases_docs_export(ext_parquet_k);
    test_crash_cases_docs_export(ext_csv_k);
    test_crash_cases_docs_export(ext_ndjson_k);
    test_crash_cases_docs_export(ext_parquet_k);
    test_crash_cases_docs_export(ext_csv_k);
}

int main(int argc, char** argv) {
    make_test_files_graph();
    make_ndjson_docs();
    for (const auto& entry : fs::directory_iterator(path_k))
        paths.push_back(ukv_str_span_t(entry.path().c_str()));

    db.open().throw_unhandled();
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    delete_test_files();
    return 0;
}