#include <fcntl.h>     // `open` files
#include <sys/stat.h>  // `stat` to obtain file metadata
#include <sys/mman.h>  // `mmap` to read datasets faster
#include <uuid/uuid.h> // `uuid` to make file name

#include <vector>
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

#include <ukv/cpp/ranges.hpp>     // `sort_and_deduplicate`
#include <ukv/cpp/bins_range.hpp> // `keys_stream_t`

using namespace unum::ukv;

constexpr std::size_t uuid_length = 36;

bool strcmp_(const char* lhs, const char* rhs) {
    return std::strcmp(lhs, rhs) == 0;
}

void make_uuid(char* out) {
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse(uuid, out);
    out[uuid_length - 1] = '\0'; // end of string
}

void upsert_one_batch(ukv_graph_import_t& c, std::vector<edge_t> const& array) {

    auto strided = edges(array);
    ukv_graph_upsert_edges_t graph_upsert_edges {
        .db = c.db,
        .error = c.error,
        .arena = c.arena,
        .options = c.options,
        .tasks_count = array.size(),
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

////////// Parsing with Apache Arrow //////////

void fill_array(ukv_graph_import_t& c, ukv_size_t max_batch_size, std::shared_ptr<arrow::Table> const& table) {

    std::vector<edge_t> array;

    auto sources = table->GetColumnByName(c.source_id_field);
    return_if_error(sources, c.error, 0, fmt::format("{} is not exist", c.source_id_field).c_str());
    auto targets = table->GetColumnByName(c.target_id_field);
    return_if_error(targets, c.error, 0, fmt::format("{} is not exist", c.target_id_field).c_str());
    auto edges = table->GetColumnByName(c.edge_id_field);
    size_t count = sources->num_chunks();
    array.reserve(std::min(ukv_size_t(sources->chunk(0)->length()), max_batch_size));

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
                .id = edges ? edge_array->Value(value_idx) : ukv_key_t {},
            };
            array.push_back(edge);
            if (array.size() == max_batch_size) {
                upsert_one_batch(c, array);
                array.clear();
            }
        }
    }
    if (array.size() != 0)
        upsert_one_batch(c, array);
}

void import_parquet(ukv_graph_import_t& c, ukv_size_t max_batch_size) {

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
    std::shared_ptr<arrow::Table> table;
    status = arrow_reader->ReadTable(&table);
    return_if_error(status.ok(), c.error, 0, "Can't read file");

    fill_array(c, max_batch_size, table);
}

void export_parquet(ukv_graph_export_t& c, ukv_key_t const* data, ukv_size_t length) {

    bool edge_state = strcmp_(c.edge_id_field, "edge");

    parquet::schema::NodeVector fields;
    fields.push_back(                         //
        parquet::schema::PrimitiveNode::Make( //
            c.source_id_field,
            parquet::Repetition::REQUIRED,
            parquet::Type::INT64,
            parquet::ConvertedType::INT_64));

    fields.push_back(                         //
        parquet::schema::PrimitiveNode::Make( //
            c.target_id_field,
            parquet::Repetition::REQUIRED,
            parquet::Type::INT64,
            parquet::ConvertedType::INT_64));

    if (!edge_state)
        fields.push_back(                         //
            parquet::schema::PrimitiveNode::Make( //
                c.edge_id_field,
                parquet::Repetition::REQUIRED,
                parquet::Type::INT64,
                parquet::ConvertedType::INT_64));

    std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    char file_name[uuid_length];
    make_uuid(file_name);

    auto maybe_outfile = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_if_error(maybe_outfile.ok(), c.error, 0, "Can't open file");
    auto outfile = *maybe_outfile;

    parquet::WriterProperties::Builder builder;
    builder.memory_pool(arrow::default_memory_pool());
    builder.write_batch_size(length);

    parquet::StreamWriter os {parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};

    for (size_t idx = 0; idx < length; idx += 3) {
        os << *(data + idx) << *(data + idx + 1);
        if (!edge_state)
            os << *(data + idx + 2);

        auto dat = os.current_row();
        os << parquet::EndRow;
    }
}

void import_csv(ukv_graph_import_t& c, ukv_size_t max_batch_size) {

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
    std::shared_ptr<arrow::Table> table = *maybe_table;

    fill_array(c, max_batch_size, table);
}

void export_csv(ukv_graph_export_t& c, ukv_key_t const* data, ukv_size_t length) {

    bool edge_state = strcmp_(c.edge_id_field, "edge");
    arrow::Status status;

    arrow::NumericBuilder<arrow::Int64Type> builder;
    status = builder.Resize(length / 3);
    return_if_error(status.ok(), c.error, 0, "Can't instatinate builder");

    std::shared_ptr<arrow::Array> sources_array;
    std::shared_ptr<arrow::Array> targets_array;
    std::shared_ptr<arrow::Array> edges_array;
    std::vector<ukv_key_t> values(length / 3);

    auto func = [&](size_t offset) {
        for (size_t idx_in_data = offset, idx = 0; idx_in_data < length; idx_in_data += 3, ++idx) {
            values[idx] = *(data + idx_in_data);
        }
    };

    func(0);
    status = builder.AppendValues(values);
    return_if_error(status.ok(), c.error, 0, "Can't append values(sources)");
    status = builder.Finish(&sources_array);
    return_if_error(status.ok(), c.error, 0, "Can't finish array(sources)");

    func(1);
    status = builder.AppendValues(values);
    return_if_error(status.ok(), c.error, 0, "Can't append values(targets)");
    status = builder.Finish(&targets_array);
    return_if_error(status.ok(), c.error, 0, "Can't finish array(targets)");

    if (!edge_state) {
        func(2);
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

    char file_name[uuid_length];
    make_uuid(file_name);

    auto maybe_outstream = arrow::io::FileOutputStream::Open(fmt::format("{}{}", file_name, c.paths_extension));
    return_if_error(maybe_outstream.ok(), c.error, 0, "Can't open file");
    std::shared_ptr<arrow::io::FileOutputStream> outstream = *maybe_outstream;

    status = arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), outstream.get());
    return_if_error(status.ok(), c.error, 0, "Can't write in file");
}

////////// Parsing with SIMDJSON //////////

simdjson::ondemand::document& rewinded(simdjson::ondemand::document& doc) noexcept {
    doc.rewind();
    return doc;
}

simdjson::ondemand::object& rewinded(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

void import_json(ukv_graph_import_t& c, ukv_size_t max_batch_size) {

    std::vector<edge_t> array;
    bool edge_state = std::strcmp(c.edge_id_field, "edge");

    auto handle = open(c.paths_pattern, O_RDONLY);
    return_if_error(handle != -1, c.error, 0, "Can't open file");

    auto begin = mmap(NULL, c.paths_length, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<char const*>(begin), c.paths_length);
    madvise(begin, c.paths_length, MADV_SEQUENTIAL);

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
        source = rewinded(data)[c.source_id_field];
        target = rewinded(data)[c.target_id_field];
        edge = edge_state ? rewinded(data)[c.edge_id_field] : 0;
        array.push_back(edge_t {.source_id = source, .target_id = target, .id = edge});
        if (array.size() == max_batch_size) {
            upsert_one_batch(c, array);
            array.clear();
        }
    }
    if (array.size() != 0)
        upsert_one_batch(c, array);

    munmap((void*)mapped_content.data(), mapped_content.size());
}

void export_json(ukv_graph_export_t& c, ukv_key_t const* data, ukv_size_t length) {
}

void ukv_graph_import(ukv_graph_import_t* c_ptr) {

    ukv_graph_import_t& c = *c_ptr;

    ukv_size_t max_batch_size = c.max_batch_size / sizeof(edge_t);
    auto ext = std::filesystem::path(c.paths_pattern).extension();

    if (ext == ".ndjson")
        import_json(c, max_batch_size);
    else if (ext == ".parquet")
        import_parquet(c, max_batch_size);
    else if (ext == ".csv")
        import_csv(c, max_batch_size);
}

void ukv_graph_export(ukv_graph_export_t* c_ptr) {

    ukv_graph_export_t& c = *c_ptr;

    ////// Choosing a method  //////

    auto ext = c.paths_extension;
    auto export_method = strcmp_(ext, ".parquet") //
                             ? &export_parquet
                             : strcmp_(ext, ".json") //
                                   ? &export_json
                                   : strcmp_(ext, ".csv") //
                                         ? &export_csv
                                         : nullptr;

    return_if_error(export_method, c.error, 0, "Not supported format");

    std::plus plus;

    ukv_key_t* ids_in_edges = nullptr;
    ukv_vertex_degree_t* degrees = nullptr;
    ukv_vertex_role_t const role = ukv_vertex_role_any_k;

    ukv_size_t count = 0;
    ukv_size_t total_ids = 0;
    ukv_size_t max_batch_size = c.max_batch_size / sizeof(edge_t);

    keys_stream_t stream(c.db, c.collection, max_batch_size, nullptr);
    stream.seek_to_first().throw_unhandled();

    while (!stream.is_end()) {
        ukv_graph_find_edges_t graph_find {
            .db = c.db,
            .error = c.error,
            .arena = c.arena,
            .options = c.options,
            .tasks_count = max_batch_size,
            .collections = &c.collection,
            .vertices = stream.keys_batch().begin(),
            .vertices_stride = sizeof(ukv_key_t),
            .roles = &role,
            .degrees_per_vertex = &degrees,
            .edges_per_vertex = &ids_in_edges,
        };
        ukv_graph_find_edges(&graph_find);

        count = stream.keys_batch().size();
        total_ids = std::transform_reduce(degrees, degrees + count, 0ul, plus, [](ukv_vertex_degree_t d) {
            return d != ukv_vertex_degree_missing_k ? d : 0;
        });
        total_ids *= 3;

        export_method(c, ids_in_edges, total_ids);
        stream.seek_to_next_batch().throw_unhandled();
    }
}
