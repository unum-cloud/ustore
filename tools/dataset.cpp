#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster

#include <vector>
#include <filesystem>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/status.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <parquet/arrow/reader.h>
#pragma GCC diagnostic pop

#include <simdjson.h>
#include <fmt/format.h>

#include <ukv/ukv.hpp>

#include "dataset.h"

using namespace unum::ukv;

// Parsing with Apache Arrow

void fill_array( //
    std::vector<edge_t>& array,
    std::shared_ptr<arrow::Table> const& table,
    ukv_str_view_t source_id,
    ukv_str_view_t target_id,
    ukv_str_view_t edge_id,
    ukv_error_t* error) {

    auto sources = table->GetColumnByName(source_id);
    return_if_error(sources, error, 0, fmt::format("{} is not exist", source_id).c_str());
    auto targets = table->GetColumnByName(target_id);
    return_if_error(targets, error, 0, fmt::format("{} is not exist", target_id).c_str());
    auto edges = table->GetColumnByName(edge_id);
    int count = sources->num_chunks();

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
            };
            if (edges)
                edge.id = edge_array->Value(value_idx);
            array.push_back(edge);
        }
    }
}

void import_parquet( //
    ukv_str_view_t path,
    std::vector<edge_t>& array,
    ukv_str_view_t source_id,
    ukv_str_view_t target_id,
    ukv_str_view_t edge_id,
    ukv_error_t* error) {

    arrow::Status status;
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Open File
    auto maybe_input = arrow::io::ReadableFile::Open(path);
    return_if_error(maybe_input.ok(), error, 0, "Can't open file");
    auto input = *maybe_input;

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    status = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    return_if_error(status.ok(), error, 0, "Can't open file");

    // Read File into table
    std::shared_ptr<arrow::Table> table;
    status = arrow_reader->ReadTable(&table);
    return_if_error(status.ok(), error, 0, "Can't read file");

    fill_array(array, table, source_id, target_id, edge_id, error);
}

// Parsing with SIMDJSON

simdjson::ondemand::document& rewinded(simdjson::ondemand::document& doc) noexcept {
    doc.rewind();
    return doc;
}

simdjson::ondemand::object& rewinded(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

void import_json( //
    ukv_str_view_t path,
    ukv_size_t path_size,
    std::vector<edge_t>& array,
    ukv_str_view_t source_id,
    ukv_str_view_t target_id,
    ukv_str_view_t edge_id,
    ukv_error_t* error) {

    auto handle = open(path, O_RDONLY);
    return_if_error(handle != -1, error, 0, "Can't open file");

    auto begin = mmap(NULL, path_size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), path_size);
    madvise(begin, path_size, MADV_SEQUENTIAL);

    // https://github.com/simdjson/simdjson/blob/master/doc/basics.md#newline-delimited-json-ndjson-and-json-lines
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream docs = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    ukv_key_t source = 0;
    ukv_key_t target = 0;
    ukv_key_t edge = 0;

    for (auto doc : docs) {
        simdjson::ondemand::object data = doc.get_object().value();
        source = rewinded(data)[source_id];
        target = rewinded(data)[target_id];
        edge = rewinded(data)[edge_id];
        array.push_back(edge_t {.source_id = source, .target_id = target, .id = edge});
    }
    munmap((void*)mapped_content.data(), mapped_content.size());
}

void ukv_graph_import(ukv_graph_import_t* c_ptr) {

    ukv_graph_import_t& c = *c_ptr;
    std::vector<edge_t> edges_array;

    auto ext = std::filesystem::path(c.paths_pattern).extension();
    if (ext == ".ndjson") {
        import_json(c.paths_pattern,
                    c.paths_length,
                    edges_array,
                    c.source_id_field,
                    c.target_id_field,
                    c.edge_id_field,
                    c.error);
    }
    else {
        if (ext == ".parquet")
            import_parquet(c.paths_pattern,
                           edges_array,
                           c.source_id_field,
                           c.target_id_field,
                           c.edge_id_field,
                           c.error);
    }

    auto strided = edges(edges_array);
    ukv_graph_upsert_edges_t graph_upsert_edges {
        .db = c.db,
        .error = c.error,
        .arena = c.arena,
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
