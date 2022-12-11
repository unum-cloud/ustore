#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster

#include <string>
#include <cstring>
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
#include <gtest/gtest.h>

#include <ukv/ukv.hpp>
#include "../tools/dataset.h"

using namespace unum::ukv;
using graph_t = std::vector<edge_t>;

namespace fs = std::filesystem;

constexpr ukv_str_view_t parquet_k = "assets/sample.parquet";
constexpr ukv_str_view_t ndjson_k = "assets/sample.ndjson";
constexpr ukv_str_view_t csv_k = "assets/sample.csv";

static constexpr ukv_str_view_t ext_parquet_k = ".parquet";
static constexpr ukv_str_view_t ext_ndjson_k = ".ndjson";
static constexpr ukv_str_view_t ext_csv_k = ".csv";

static constexpr ukv_str_view_t source_k = "number";
static constexpr ukv_str_view_t target_k = "difficulty";
static constexpr ukv_str_view_t edge_k = "size";
static constexpr ukv_str_view_t doc_k = "doc";
static constexpr ukv_str_view_t id_k = "_id";

static constexpr ukv_str_view_t path = "./";

std::vector<std::string> paths;
std::vector<std::pair<ukv_key_t, std::string>> docs_w_keys;

static database_t db;
static ukv_collection_t collection_docs_k = ukv_collection_main_k;
static ukv_collection_t collection_graph_k = ukv_collection_main_k;

simdjson::ondemand::document& rewind(simdjson::ondemand::document& doc) noexcept {
    doc.rewind();
    return doc;
}

simdjson::ondemand::object& rewind(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

int find_key(ukv_key_t key) {
    int idx = 0;
    for (auto _ : docs_w_keys) {
        if (_.first == key)
            return idx;
        ++idx;
    }
    return -1;
}

void fill_array_from_ndjson(graph_t& array, std::string_view const& mapped_content) {
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    for (auto doc : docs) {
        simdjson::ondemand::object data = doc.get_object().value();
        array.push_back(edge_t {.source_id = rewind(data)[source_k],
                                .target_id = rewind(data)[target_k],
                                .id = rewind(data)[edge_k]});
    }
}

void fill_array_from_table(graph_t& array, std::shared_ptr<arrow::Table>& table) {

    auto sources = table->GetColumnByName(source_k);
    auto targets = table->GetColumnByName(target_k);
    auto edges = table->GetColumnByName(edge_k);
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
            edge_t edge {
                .source_id = source_array->Value(value_idx),
                .target_id = target_array->Value(value_idx),
                .id = edge_array->Value(value_idx),
            };
            array.push_back(edge);
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

bool cmp_graph(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    graph_t array_l;
    graph_t array_r;

    fill_array(lhs, array_l);
    fill_array(rhs, array_r);

    EXPECT_EQ(array_l.size(), array_r.size());

    for (size_t idx = 0; idx < array_l.size(); ++idx) {
        EXPECT_EQ(array_l[idx].source_id, array_r[idx].source_id);
        EXPECT_EQ(array_l[idx].target_id, array_r[idx].target_id);
        EXPECT_EQ(array_l[idx].id, array_r[idx].id);
    }
    return true;
}

bool test_graph(ukv_str_view_t file, ukv_str_view_t ext) {

    arena_t arena(db);
    status_t status;

    std::vector<std::string> updated_paths;
    std::string new_file;
    size_t size = 0;

    if (std::strcmp(ndjson_k, file) == 0) {
        std::filesystem::path pt {file};
        size = std::filesystem::file_size(pt);
    }

    ukv_graph_import_t imp {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection_graph_k,
        .paths_pattern = file,
        .file_size = size,
        .source_id_field = source_k,
        .target_id_field = target_k,
        .edge_id_field = edge_k,
    };
    ukv_graph_import(&imp);

    ukv_graph_export_t exp {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection_graph_k,
        .paths_extension = ext,
        .source_id_field = source_k,
        .target_id_field = target_k,
        .edge_id_field = edge_k,
    };
    ukv_graph_export(&exp);

    for (const auto& entry : fs::directory_iterator(path))
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
    EXPECT_TRUE(cmp_graph(file, new_file.data()));

    std::remove(new_file.data());
    return true;
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

    int state = 0;
    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();
        auto data = rewind(obj).raw_json().value();
        auto str = std::string(data.data(), data.size());
        str.pop_back();
        state = find_key(rewind(obj)[edge_k]);
        if (state == -1) {
            docs_w_keys.push_back({rewind(obj)[edge_k], str});
            continue;
        }
        docs_w_keys[state].second = str;
    }
}

void fill_from_table(std::shared_ptr<arrow::Table>& table) {

    auto sources = table->GetColumnByName(source_k);
    auto targets = table->GetColumnByName(target_k);
    auto edges = table->GetColumnByName(edge_k);
    size_t count = sources->num_chunks();

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto source_chunk = sources->chunk(chunk_idx);
        auto target_chunk = targets->chunk(chunk_idx);
        auto edge_chunk = edges->chunk(chunk_idx);
        auto source_array = std::static_pointer_cast<arrow::Int64Array>(source_chunk);
        auto target_array = std::static_pointer_cast<arrow::Int64Array>(target_chunk);
        auto edge_array = std::static_pointer_cast<arrow::Int64Array>(edge_chunk);
        for (size_t value_idx = 0; value_idx != source_array->length(); ++value_idx) {
            auto str = fmt::format("{{\"{}\":{},\"{}\":{},\"{}\":{}}}",
                                   source_k,
                                   source_array->Value(value_idx),
                                   target_k,
                                   target_array->Value(value_idx),
                                   edge_k,
                                   edge_array->Value(value_idx));
            int state = find_key(edge_array->Value(value_idx));
            if (state == -1) {
                docs_w_keys.push_back({edge_array->Value(value_idx), str});
                continue;
            }
            docs_w_keys[state].second = str;
        }
    }
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

bool cmp_ndjson_docs(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    std::filesystem::path pt {rhs};
    size_t size = std::filesystem::file_size(pt);

    auto handle = open(rhs, O_RDONLY);
    auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), size);
    madvise(begin, size, MADV_SEQUENTIAL);
    fill_docs_w_keys(lhs);

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    int state = 0;
    for (auto doc : docs) {
        simdjson::ondemand::object obj = doc.get_object().value();

        auto data = rewind(obj)[doc_k].get_object().value().raw_json().value();
        auto str = std::string(data.data(), data.size());
        ukv_key_t key = rewind(obj)[id_k];
        int pos = find_key(key);

        EXPECT_EQ(docs_w_keys[pos].first, key);
        EXPECT_EQ(docs_w_keys[pos].second, str);
    }

    munmap((void*)mapped_content.data(), mapped_content.size());
    return true;
}

bool cmp_table_docs(ukv_str_view_t lhs, ukv_str_view_t rhs) {

    std::shared_ptr<arrow::Table> table;

    auto ext = std::filesystem::path(rhs).extension();

    if (ext == ".csv") {
        std::shared_ptr<arrow::io::InputStream> input = *arrow::io::ReadableFile::Open(rhs);
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
        auto input = *arrow::io::ReadableFile::Open(rhs);
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        parquet::arrow::OpenFile(input, pool, &arrow_reader);
        arrow_reader->ReadTable(&table);
    }
    fill_docs_w_keys(lhs);

    auto docs = table->GetColumnByName(doc_k);
    auto ids = table->GetColumnByName(id_k);
    size_t count = docs->num_chunks();

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto doc_chunk = docs->chunk(chunk_idx);
        auto id_chunk = ids->chunk(chunk_idx);
        auto doc_array = std::static_pointer_cast<arrow::StringArray>(doc_chunk);
        auto id_array = std::static_pointer_cast<arrow::Int64Array>(id_chunk);
        for (size_t value_idx = 0; value_idx != doc_array->length(); ++value_idx) {
            auto data = doc_array->Value(value_idx);
            auto str = std::string(data.data(), data.size());
            int pos = find_key(id_array->Value(value_idx));

            EXPECT_EQ(docs_w_keys[pos].first, id_array->Value(value_idx));
            EXPECT_EQ(docs_w_keys[pos].second, str);
        }
    }
    return true;
}

template <typename comparator>
bool test_docs(ukv_str_view_t file, ukv_str_view_t ext, comparator cmp) {

    arena_t arena(db);
    status_t status;

    std::vector<std::string> updated_paths;
    std::string new_file;
    size_t size = 0;

    if (std::strcmp(ndjson_k, file) == 0) {
        std::filesystem::path pt {file};
        size = std::filesystem::file_size(pt);
    }

    ukv_docs_import_t docs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection_docs_k,
        .paths_pattern = file,
        .file_size = size,
        .id_field = edge_k,
    };
    ukv_docs_import(&docs);

    ukv_docs_export_t exdocs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .collection = collection_docs_k,
        .paths_extension = ext,
    };
    ukv_docs_export(&exdocs);

    for (const auto& entry : fs::directory_iterator(path))
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
    return true;
}

TEST(import_export, graph) {
    test_graph(ndjson_k, ext_ndjson_k);
    test_graph(ndjson_k, ext_parquet_k);
    test_graph(ndjson_k, ext_csv_k);
    test_graph(parquet_k, ext_ndjson_k);
    test_graph(parquet_k, ext_parquet_k);
    test_graph(parquet_k, ext_csv_k);
    test_graph(csv_k, ext_ndjson_k);
    test_graph(csv_k, ext_parquet_k);
    test_graph(csv_k, ext_csv_k);
}

TEST(import_export, docs) {
    test_docs(ndjson_k, ext_ndjson_k, cmp_ndjson_docs);
    test_docs(ndjson_k, ext_parquet_k, cmp_table_docs);
    test_docs(ndjson_k, ext_csv_k, cmp_table_docs);
    test_docs(parquet_k, ext_ndjson_k, cmp_ndjson_docs);
    test_docs(parquet_k, ext_parquet_k, cmp_table_docs);
    test_docs(parquet_k, ext_csv_k, cmp_table_docs);
    test_docs(csv_k, ext_ndjson_k, cmp_ndjson_docs);
    test_docs(csv_k, ext_parquet_k, cmp_table_docs);
    test_docs(csv_k, ext_csv_k, cmp_table_docs);
}

int main(int argc, char** argv) {
    for (const auto& entry : fs::directory_iterator(path))
        paths.push_back(ukv_str_span_t(entry.path().c_str()));

    db.open().throw_unhandled();
    if (ukv_supports_named_collections_k) {
        status_t status;
        ukv_collection_create_t collection_init {
            .db = db,
            .error = status.member_ptr(),
            .name = "tabular.graph",
            .config = "",
            .id = &collection_graph_k,
        };

        ukv_collection_create(&collection_init);
        status.throw_unhandled();

        collection_init.name = "tabular.docs";
        collection_init.id = &collection_docs_k;

        ukv_collection_create(&collection_init);
        status.throw_unhandled();
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}