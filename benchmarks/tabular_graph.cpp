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
#include <random>      // `std::random_device` for each thread

#include <benchmark/benchmark.h>
#include <simdjson.h>

#include <ukv/ukv.hpp>
#include <ukv/cpp/ranges.hpp> // `sort_and_deduplicate`
#include "../tools/dataset.h"

namespace bm = benchmark;
using namespace unum::ukv;

constexpr std::size_t id_str_max_length_k = 24;
constexpr std::size_t max_input_files = 30;

static std::string dataset_directory = "~/Datasets/";
static std::vector<std::string> source_files;
static std::vector<std::size_t> source_sizes;

static database_t db;
static ukv_collection_t collection_graph_k = ukv_collection_main_k;
static ukv_collection_t collection_docs_k = ukv_collection_main_k;
std::filesystem::directory_options opts = std::filesystem::directory_options::follow_directory_symlink;

void parse_files(std::string dataset_path, ukv_str_view_t ext) {
    for (auto const& dir_entry : std::filesystem::directory_iterator(dataset_path, opts)) {
        if (dir_entry.is_directory())
            parse_files(dir_entry.path(), ext);
        if (dir_entry.path().extension() != ext)
            continue;

        source_files.push_back(dir_entry.path());
        source_sizes.push_back(dir_entry.file_size());
    }
}

static void bench_docs(bm::State& state) {
    arena_t arena(db);
    status_t status;

    size_t size = 0;

    auto dataset_path = dataset_directory;
    auto home_path = std::getenv("HOME");
    if (dataset_path.front() == '~')
        dataset_path = std::filesystem::path(home_path) / dataset_path.substr(2);

    parse_files(dataset_path, ".parquet");
    source_files.resize(std::min(max_input_files, source_files.size()));

    for(auto sz : source_sizes)
        size += sz;

    for (auto _ : state) {
        for (auto file : source_files) {
            ukv_docs_import_t docs {
                .db = db,
                .error = status.member_ptr(),
                .arena = arena.member_ptr(),
                .collection = collection_docs_k,
                .paths_pattern = file.c_str(),
                .id_field = "passenger_count",
            };
            ukv_docs_import(&docs);
        }
    }

    state.counters["bytes/s"] = benchmark::Counter(size, benchmark::Counter::kIsRate);
}

int main(int argc, char** argv) {
    bm::Initialize(&argc, argv);

    db.open().throw_unhandled();
    if (ukv_supports_named_collections_k) {
        status_t status;
        ukv_collection_create_t collection_init {
            .db = db,
            .error = status.member_ptr(),
            .name = "twitter.graph",
            .config = "",
            .id = &collection_graph_k,
        };

        ukv_collection_create(&collection_init);
        status.throw_unhandled();

        collection_init.name = "twitter.docs";
        collection_init.id = &collection_docs_k;

        ukv_collection_create(&collection_init);
        status.throw_unhandled();
    }

    bm::RegisterBenchmark("docs_import", &bench_docs)->Threads(16);

    bm::RunSpecifiedBenchmarks();
    bm::Shutdown();

    db.clear().throw_unhandled();
    return 0;
}