#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#include <arrow/status.h>
#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/memory_pool.h>
#include "arrow/io/file.h"
#include "parquet/arrow/reader.h"
#pragma GCC diagnostic pop

#include <ukv/ukv.hpp>

#include "dataset.h"

using namespace unum::ukv;

void import_parquet(ukv_str_view_t path, std::shared_ptr<arrow::Table>& table, ukv_error_t* error) {
    arrow::Status status;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;

    // Open File
    PARQUET_ASSIGN_OR_THROW(input, arrow::io::ReadableFile::Open(path));
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    status = parquet::arrow::OpenFile(input, pool, &arrow_reader);
    return_if_error(status.ok(), error, 0, "Can't open file");

    // Read File into table
    status = arrow_reader->ReadTable(&table);
    return_if_error(status.ok(), error, 0, "Can't read file");
}

void fill( //
    std::vector<edge_t>& array,
    std::shared_ptr<arrow::Table> const& table,
    ukv_str_view_t source_id,
    ukv_str_view_t target_id) {

    auto sources = table->GetColumnByName(source_id);
    auto targets = table->GetColumnByName(target_id);
    int count = sources->num_chunks();

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto source_chunk = sources->chunk(chunk_idx);
        auto target_chunk = sources->chunk(chunk_idx);
        auto source_array = std::static_pointer_cast<arrow::Int64Array>(source_chunk);
        auto target_array = std::static_pointer_cast<arrow::Int64Array>(target_chunk);
        for (size_t value_idx = 0; value_idx != source_array->length(); ++value_idx) {
            array.push_back(edge_t {
                .source_id = source_array->Value(value_idx),
                .target_id = target_array->Value(value_idx),
            });
        }
    }
}

void fill_with_edges( //
    std::vector<edge_t>& array,
    std::shared_ptr<arrow::Table> const& table,
    ukv_str_view_t source_id,
    ukv_str_view_t target_id,
    ukv_str_view_t edge_id) {

    auto sources = table->GetColumnByName(source_id);
    auto targets = table->GetColumnByName(target_id);
    auto edges = table->GetColumnByName(edge_id);
    int count = sources->num_chunks();

    for (size_t chunk_idx = 0; chunk_idx != count; ++chunk_idx) {
        auto source_chunk = sources->chunk(chunk_idx);
        auto target_chunk = targets->chunk(chunk_idx);
        auto edge_chunk = edges->chunk(chunk_idx);
        auto source_array = std::static_pointer_cast<arrow::Int64Array>(source_chunk);
        auto target_array = std::static_pointer_cast<arrow::Int64Array>(target_chunk);
        auto edge_array = std::static_pointer_cast<arrow::Int64Array>(edge_chunk);
        for (size_t value_idx = 0; value_idx != source_array->length(); ++value_idx) {
            array.push_back(edge_t {
                .source_id = source_array->Value(value_idx),
                .target_id = target_array->Value(value_idx),
                .id = edge_array->Value(value_idx),
            });
        }
    }
}

void ukv_graph_import(ukv_graph_import_t* c_ptr) {

    ukv_graph_import_t& c = *c_ptr;
    std::vector<edge_t> edges_array;
    std::shared_ptr<arrow::Table> table;

    import_parquet(c.paths_pattern, table, c.error);

    if (c.edge_id_field != "edge")
        fill(edges_array, table, c.source_id_field, c.target_id_field);
    else
        fill_with_edges(edges_array, table, c.source_id_field, c.target_id_field, c.edge_id_field);

    auto strided = edges(edges_array);
    ukv_graph_upsert_edges_t graph_upsert_edges {
        .db = c.db,
        .error = c.error,
        .arena = c.arena,
        .collections = &c.collection,
        .edges_ids = (c.edge_id_field != "edge") ? strided.edge_ids.begin().get() : NULL,
        .edges_stride = (c.edge_id_field != "edge") ? strided.edge_ids.stride() : 0,
        .sources_ids = strided.source_ids.begin().get(),
        .sources_stride = strided.source_ids.stride(),
        .targets_ids = strided.target_ids.begin().get(),
        .targets_stride = strided.target_ids.stride(),
    };

    ukv_graph_upsert_edges(&graph_upsert_edges);
}
