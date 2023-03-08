/**
 * @file tabular_graph.cpp
 * @brief Imports a big Parquet/CSV/ORCA dataset as a labeled graph.
 * @version 0.1
 * @date 2022-10-02
 *
 * Every row is treated as a separate edge.
 * All of its columns are treated as different document fields, except for
 * - Integer column for source node ID.
 * - Integer column for target node ID.
 * - Optional integer column for document/edge ID.
 * If the last one isn't provided, the row number is used as the document ID.
 *
 * https://arrow.apache.org/docs/cpp/dataset.html#dataset-discovery
 * https://arrow.apache.org/docs/cpp/parquet.html
 * https://arrow.apache.org/docs/cpp/csv.html
 */

#include <cstring>     // `std::memchr`
#include <algorithm>   // `std::search`
#include <filesystem>  // Listing directories is too much pain in C
#include <string_view> //
#include <vector>      //
#include <thread>      //
#include <mutex>       //
#include <random>      // `std::random_device` for each thread

#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster
#include <unistd.h>

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

#include <benchmark/benchmark.h>
#include <simdjson.h>
#include <fmt/format.h>

#include <ukv/ukv.hpp>
#include <ukv/cpp/ranges.hpp> // `sort_and_deduplicate`
#include "dataset.h"

namespace bm = benchmark;
using namespace unum::ukv;

constexpr std::size_t max_batch_size_k = 1024 * 1024 * 1024;
constexpr std::size_t id_str_max_length_k = 24;
constexpr std::size_t max_input_files = 30;

static database_t db;
static std::string dataset_directory = "~/sftp/Datasets/Twitter/";
std::filesystem::directory_options opts = std::filesystem::directory_options::follow_directory_symlink;

constexpr ukv_str_view_t dataset_clean_path_k = "/home/vscode/Datasets/tweets32K-clean.ndjson";
constexpr ukv_str_view_t dataset_path_k = "/home/vscode/Datasets/tweets32K.ndjson";
constexpr ukv_str_view_t parquet_path_k = "/home/vscode/Datasets/tweets32K-clean.parquet";
constexpr ukv_str_view_t csv_path_k = "/home/vscode/Datasets/tweets32K-clean.csv";
constexpr ukv_str_view_t ndjson_path_k = "sample_docs.ndjson";

constexpr ukv_str_view_t path_parquet_k = "sample.parquet";
constexpr ukv_str_view_t path_ndjson_k = "sample.ndjson";
constexpr ukv_str_view_t path_csv_k = "sample.csv";
constexpr ukv_str_view_t parquet_ext_k = ".parquet";
constexpr ukv_str_view_t ndjson_ext_k = ".ndjson";
constexpr ukv_str_view_t csv_ext_k = ".csv";
constexpr ukv_str_view_t path_k = "./";

constexpr ukv_str_view_t source_field_k = "id";
constexpr ukv_str_view_t target_field_k = "user_id";
constexpr ukv_str_view_t edge_field_k = "user_followers_count";
constexpr ukv_str_view_t source_path_k = "id";
constexpr ukv_str_view_t target_path_k = "/user/id";
constexpr ukv_str_view_t edge_path_k = "/user/followers_count";

constexpr size_t rows_count_k = 1000;
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

enum ext_t {
    parquet_k = 0,
    csv_k,
    ndjson_k,
};

std::mutex mtx;

simdjson::ondemand::object& rewind(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

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

void make_parquet_graph(std::vector<edge_t> const& edges) {
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

    auto outfile = *arrow::io::FileOutputStream::Open(path_parquet_k);

    parquet::WriterProperties::Builder builder;
    builder.memory_pool(arrow::default_memory_pool());
    builder.write_batch_size(rows_count_k);

    parquet::StreamWriter os {parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

    for (auto edge : edges) {
        os << edge.source_id << edge.target_id << edge.id << parquet::EndRow;
    }
}

void make_csv_graph(std::vector<edge_t> const& edges) {
    arrow::Status status;
    arrow::NumericBuilder<arrow::Int64Type> builder;
    status = builder.Resize(edges.size());
    std::shared_ptr<arrow::Array> sources_array;
    std::shared_ptr<arrow::Array> targets_array;
    std::shared_ptr<arrow::Array> edges_array;
    std::vector<ukv_key_t> values(edges.size());

    size_t idx = 0;
    for (auto edge : edges)
        values[idx] = edge.source_id, ++idx;
    status = builder.AppendValues(values);
    status = builder.Finish(&sources_array);
    idx = 0;
    for (auto edge : edges)
        values[idx] = edge.target_id, ++idx;
    status = builder.AppendValues(values);
    status = builder.Finish(&targets_array);
    idx = 0;
    for (auto edge : edges)
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
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *arrow::io::FileOutputStream::Open(path_csv_k);
    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
}

void make_ndjson_graph(std::vector<edge_t> const& edges) {
    auto handle = open(path_ndjson_k, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

    for (auto edge : edges) {
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
    std::filesystem::path pt {dataset_clean_path_k};
    size_t size = std::filesystem::file_size(pt);

    auto handle = open(dataset_clean_path_k, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);

    auto get_value = [](simdjson::ondemand::object& obj, ukv_str_view_t field) {
        return (field[0] == '/') ? rewind(obj).at_pointer(field) : rewind(obj)[field];
    };

    std::vector<edge_t> edges;
    edges.reserve(rows_count_k);
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
        edges.push_back(edge);
        if (edges.size() == rows_count_k)
            break;
    }
    make_parquet_graph(edges);
    make_csv_graph(edges);
    make_ndjson_graph(edges);
    munmap((void*)mapped_content.data(), mapped_content.size());
    close(handle);
}

void delete_test_files() {
    std::remove(path_parquet_k);
    std::remove(path_csv_k);
    std::remove(path_ndjson_k);
    std::remove(ndjson_path_k);
}

static void bench_docs_import(bm::State& state, ukv_str_view_t path) {
    if (!path)
        return;
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t idx = 0;
    size_t size = 0;
    size_t successes = 0;

    std::filesystem::path pt {path};
    size_t file_size = std::filesystem::file_size(pt);

    for (auto _ : state) {
        ukv_docs_import_t docs {};
        docs.db = db;
        docs.error = status.member_ptr();
        docs.arena = arena.member_ptr();
        docs.collection = collection;
        docs.paths_pattern = path;
        docs.max_batch_size = max_batch_size_k;
        docs.id_field = "id";
        ukv_docs_import(&docs);

        state.PauseTiming();
        successes += status;
        if (status)
            size += file_size;
        status.release_error();
        ++idx;
        state.ResumeTiming();
    }
    state.counters["bytes/s"] = bm::Counter(size, bm::Counter::kIsRate);
    state.counters["fails,%"] = bm::Counter((state.iterations() - successes) * 100.0, bm::Counter::kAvgThreads);
}

static void bench_graph_import(bm::State& state, ukv_str_view_t path) {
    if (!path)
        return;
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t idx = 0;
    size_t size = 0;
    size_t successes = 0;

    std::filesystem::path pt {path};
    size_t file_size = std::filesystem::file_size(pt);

    for (auto _ : state) {
        ukv_graph_import_t graph {};
        graph.db = db;
        graph.error = status.member_ptr();
        graph.arena = arena.member_ptr();
        graph.collection = collection;
        graph.paths_pattern = path;
        graph.max_batch_size = max_batch_size_k;
        graph.source_id_field = source_field_k;
        graph.target_id_field = target_field_k;
        graph.edge_id_field = edge_field_k;
        ukv_graph_import(&graph);

        state.PauseTiming();
        successes += status;
        if (status)
            size += file_size;
        status.release_error();
        ++idx;
        state.ResumeTiming();
    }
    state.counters["bytes/s"] = bm::Counter(size, bm::Counter::kIsRate);
    state.counters["fails,%"] = bm::Counter((state.iterations() - successes) * 100.0, bm::Counter::kAvgThreads);
}

size_t find_and_delete() {
    mtx.lock();
    time_t now = time(0);
    std::string exp = ctime(&now);
    exp = exp.substr(0, 10);
    for (auto& ch : exp) {
        if ((ch == ' ') | (ch == ':'))
            ch = '_';
    }
    for (const auto& entry : std::filesystem::directory_iterator("./")) {
        if (entry.path().string().substr(2, 10) == exp) {
            size_t size = entry.file_size();
            std::remove(entry.path().c_str());
            mtx.unlock();
            return size;
        }
    }
    mtx.unlock();
    return 0;
}

static void bench_docs_export(bm::State& state, ukv_str_view_t ext) {
    if (!ext)
        return;
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t idx = 0;
    size_t size = 0;
    size_t successes = 0;

    for (auto _ : state) {
        ukv_docs_export_t docs {};
        docs.db = db;
        docs.error = status.member_ptr();
        docs.arena = arena.member_ptr();
        docs.collection = collection;
        docs.paths_extension = ext;
        docs.max_batch_size = max_batch_size_k;
        ukv_docs_export(&docs);

        state.PauseTiming();
        successes += status;
        if (status)
            size += find_and_delete();
        status.release_error();
        ++idx;
        state.ResumeTiming();
    }
    state.counters["bytes/s"] = bm::Counter(size, bm::Counter::kIsRate);
    state.counters["fails,%"] = bm::Counter((state.iterations() - successes) * 100.0, bm::Counter::kAvgThreads);
}

static void bench_graph_export(bm::State& state, ukv_str_view_t ext) {
    if (!ext)
        return;
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t idx = 0;
    size_t size = 0;
    size_t successes = 0;

    for (auto _ : state) {
        ukv_graph_export_t graph {};
        graph.db = db;
        graph.error = status.member_ptr();
        graph.arena = arena.member_ptr();
        graph.collection = collection;
        graph.paths_extension = ext;
        graph.max_batch_size = max_batch_size_k;
        graph.source_id_field = source_field_k;
        graph.target_id_field = target_field_k;
        graph.edge_id_field = edge_field_k;
        ukv_graph_export(&graph);

        state.PauseTiming();
        successes += status;
        if (status)
            size += find_and_delete();
        status.release_error();
        ++idx;
        state.ResumeTiming();
    }
    state.counters["bytes/s"] = bm::Counter(size, bm::Counter::kIsRate);
    state.counters["fails,%"] = bm::Counter((state.iterations() - successes) * 100.0, bm::Counter::kAvgThreads);
}

void bench_docs(ext_t pcn, size_t threads = 1) {
    bm::RegisterBenchmark(fmt::format("docs_import_{}",
                                      pcn == parquet_k  ? "parquet"
                                      : pcn == csv_k    ? "csv"
                                      : pcn == ndjson_k ? "ndjson"
                                                        : nullptr)
                              .c_str(),
                          [&](bm::State& s) {
                              bench_docs_import(s,
                                                pcn == parquet_k  ? parquet_path_k
                                                : pcn == csv_k    ? csv_path_k
                                                : pcn == ndjson_k ? ndjson_path_k
                                                                  : nullptr);
                          });
        // ->Threads(threads);
    bm::RegisterBenchmark(fmt::format("docs_export_{}",
                                      pcn == parquet_k  ? "parquet"
                                      : pcn == csv_k    ? "csv"
                                      : pcn == ndjson_k ? "ndjson"
                                                        : nullptr)
                              .c_str(),
                          [&](bm::State& s) {
                              bench_docs_export(s,
                                                pcn == parquet_k  ? parquet_ext_k
                                                : pcn == csv_k    ? csv_ext_k
                                                : pcn == ndjson_k ? ndjson_ext_k
                                                                  : nullptr);
                          });
        // ->Threads(threads);
    db.clear().throw_unhandled();
}

void bench_graph(ext_t pcn, size_t threads = 1) {
    bm::RegisterBenchmark(fmt::format("graph_import_{}",
                                      pcn == parquet_k  ? "parquet"
                                      : pcn == csv_k    ? "csv"
                                      : pcn == ndjson_k ? "ndjson"
                                                        : nullptr)
                              .c_str(),
                          [&](bm::State& s) {
                              bench_graph_import(s,
                                                 pcn == parquet_k  ? path_parquet_k
                                                 : pcn == csv_k    ? path_csv_k
                                                 : pcn == ndjson_k ? path_ndjson_k
                                                                   : nullptr);
                          });
        // ->Threads(threads);
    bm::RegisterBenchmark(fmt::format("graph_export_{}",
                                      pcn == parquet_k  ? "parquet"
                                      : pcn == csv_k    ? "csv"
                                      : pcn == ndjson_k ? "ndjson"
                                                        : nullptr)
                              .c_str(),
                          [&](bm::State& s) {
                              bench_graph_export(s,
                                                 pcn == parquet_k  ? parquet_ext_k
                                                 : pcn == csv_k    ? csv_ext_k
                                                 : pcn == ndjson_k ? ndjson_ext_k
                                                                   : nullptr);
                          });
        // ->Threads(threads);
    db.clear().throw_unhandled();
}

int main(int argc, char** argv) {
    bm::Initialize(&argc, argv);

    make_test_files_graph();
    make_ndjson_docs();
    db.open().throw_unhandled();

    bench_docs(parquet_k);
    bench_docs(csv_k);
    bench_docs(ndjson_k);
    bench_graph(parquet_k);
    bench_graph(csv_k);
    bench_graph(ndjson_k);
    // bench_docs(parquet_k, 2);
    // bench_docs(csv_k, 2);
    // bench_docs(ndjson_k, 2);
    // bench_docs(parquet_k, 4);
    // bench_docs(csv_k, 4);
    // bench_docs(ndjson_k, 4);
    // bench_docs(parquet_k, 16);
    // bench_docs(csv_k, 16);
    // bench_docs(ndjson_k, 16);
    // bench_docs(parquet_k, 32);
    // bench_docs(csv_k, 32);
    // bench_docs(ndjson_k, 32);

    bm::RunSpecifiedBenchmarks();
    bm::Shutdown();
    delete_test_files();
    db.close();
    return 0;
}