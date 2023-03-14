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

#include <benchmark/benchmark.h>
#include <fmt/format.h>
#include <argparse/argparse.hpp>

#include <ukv/ukv.hpp>
#include <ukv/cpp/ranges.hpp> // `sort_and_deduplicate`
#include <dataset.h>

namespace bm = benchmark;
using namespace unum::ukv;

constexpr std::size_t max_batch_size_k = 1024 * 1024 * 1024;
constexpr ukv_str_view_t path_k = "./";

static database_t db;

std::vector<std::string> paths;
std::mutex mtx;

struct args_t {
    std::string path;
    std::string extension;

    std::string source;
    std::string target;
    std::string edge;

    std::string id;

    size_t threads_count;
};

void parse_args(int argc, char* argv[], args_t& args) {
    argparse::ArgumentParser program(argv[0]);
    program.add_argument("-p", "--path").required().help("File path for importing");
    program.add_argument("-e", "--ext").required().help("File extension for exporting");
    program.add_argument("-s", "--source").required().help("Source field");
    program.add_argument("-t", "--target").required().help("Target field");
    program.add_argument("-ed", "--edge").required().help("Edge field");
    program.add_argument("-i", "--id").required().help("Id field");
    program.add_argument("-th", "--threads").default_value(std::string("1")).help("Threads count");

    program.parse_known_args(argc, argv);

    args.path = program.get("path");
    args.extension = program.get("ext");
    args.source = program.get("source");
    args.target = program.get("target");
    args.edge = program.get("edge");
    args.id = program.get("id");
    args.threads_count = std::stoi(program.get("threads"));

    if (args.threads_count == 0) {
        fmt::print("Zero threads count specified\n");
        exit(1);
    }
}

void make_bench_files(args_t const& args) {
    auto file_name = args.path.substr(args.path.find_last_of('/') + 1);
    paths.resize(args.threads_count);
    for (size_t idx = 0; idx < args.threads_count; ++idx) {
        if (idx == 0) {
            paths[idx] = args.path;
            continue;
        }

        fmt::format_to(std::back_inserter(paths[idx]), "./{}_{}", idx, file_name);
        std::filesystem::copy_file(args.path, paths[idx]);
    }
}

void delete_bench_files() {
    for (size_t idx = 0; idx < paths.size(); ++idx)
        std::remove(paths[idx].c_str());
}

size_t get_keys_count() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    ukv_key_t key = 0;
    ukv_length_t counts = -1;
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* keys = nullptr;

    ukv_scan_t scan {};
    scan.db = db;
    scan.error = status.member_ptr();
    scan.arena = arena.member_ptr();
    scan.tasks_count = 1;
    scan.collections = &collection;
    scan.start_keys = &key;
    scan.count_limits = &counts;
    scan.counts = &found_counts;
    scan.keys = &keys;
    ukv_scan(&scan);
    return (*found_counts * 3);
}

static void bench_docs_import(bm::State& state, args_t const& args) {
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t idx = 0;
    size_t size = 0;
    size_t successes = 0;

    std::filesystem::path pt {paths[state.thread_index()]};
    size_t file_size = std::filesystem::file_size(pt);

    for (auto _ : state) {
        ukv_docs_import_t docs {};
        docs.db = db;
        docs.error = status.member_ptr();
        docs.arena = arena.member_ptr();
        docs.collection = collection;
        docs.paths_pattern = paths[state.thread_index()].c_str();
        docs.max_batch_size = max_batch_size_k;
        docs.id_field = args.id.c_str();
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

static void bench_graph_import(bm::State& state, args_t const& args) {
    auto collection = db.main();
    arena_t arena(db);
    status_t status;

    size_t idx = 0;
    size_t size = 0;
    size_t keys_in_bytes = 0;
    size_t successes = 0;
    bool scan_state = true;
    for (auto _ : state) {
        ukv_graph_import_t graph {};
        graph.db = db;
        graph.error = status.member_ptr();
        graph.arena = arena.member_ptr();
        graph.collection = collection;
        graph.paths_pattern = paths[state.thread_index()].c_str();
        graph.max_batch_size = max_batch_size_k;
        graph.source_id_field = args.source.c_str();
        graph.target_id_field = args.target.c_str();
        graph.edge_id_field = args.edge.c_str();
        ukv_graph_import(&graph);

        state.PauseTiming();
        successes += status;
        if (status) {
            mtx.lock();
            if (scan_state)
                keys_in_bytes = get_keys_count() * sizeof(ukv_key_t), scan_state = false;
            mtx.unlock();
            size += keys_in_bytes;
        }
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
    for (const auto& entry : std::filesystem::directory_iterator(path_k)) {
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

static void bench_docs_export(bm::State& state, args_t const& args) {
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
        docs.paths_extension = args.extension.c_str();
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
    db.clear().throw_unhandled();
    state.counters["bytes/s"] = bm::Counter(size, bm::Counter::kIsRate);
    state.counters["fails,%"] = bm::Counter((state.iterations() - successes) * 100.0, bm::Counter::kAvgThreads);
}

static void bench_graph_export(bm::State& state, args_t const& args) {
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
        graph.paths_extension = args.extension.c_str();
        graph.max_batch_size = max_batch_size_k;
        graph.source_id_field = args.source.c_str();
        graph.target_id_field = args.target.c_str();
        graph.edge_id_field = args.edge.c_str();
        ukv_graph_export(&graph);

        state.PauseTiming();
        successes += status;
        if (status)
            size += find_and_delete();
        status.release_error();
        ++idx;
        state.ResumeTiming();
    }
    db.clear().throw_unhandled();
    state.counters["bytes/s"] = bm::Counter(size, bm::Counter::kIsRate);
    state.counters["fails,%"] = bm::Counter((state.iterations() - successes) * 100.0, bm::Counter::kAvgThreads);
}

void bench_docs(args_t const& args) {
    bm::RegisterBenchmark( //
        fmt::format("docs_import_{}", std::filesystem::path(args.path).extension().string().substr(1).c_str()).c_str(),
        [&](bm::State& s) { bench_docs_import(s, args); })
        ->Threads(args.threads_count)
        ->Iterations(1);

    bm::RegisterBenchmark(fmt::format("docs_export_{}", args.extension.substr(1)).c_str(),
                          [&](bm::State& s) { bench_docs_export(s, args); })
        ->Threads(args.threads_count)
        ->Iterations(1);
}

void bench_graph(args_t const& args) {
    bm::RegisterBenchmark( //
        fmt::format("graph_import_{}", std::filesystem::path(args.path).extension().string().substr(1).c_str()).c_str(),
        [&](bm::State& s) { bench_graph_import(s, args); })
        ->Threads(args.threads_count)
        ->Iterations(1);
    bm::RegisterBenchmark(fmt::format("graph_export_{}", args.extension.substr(1)).c_str(),
                          [&](bm::State& s) { bench_graph_export(s, args); })
        ->Threads(args.threads_count)
        ->Iterations(1);
}

int main(int argc, char** argv) {

    args_t args {};
    parse_args(argc, argv, args);
    make_bench_files(args);
    bm::Initialize(&argc, argv);
    db.open().throw_unhandled();

    bench_graph(args);
    bench_docs(args);

    bm::RunSpecifiedBenchmarks();
    bm::Shutdown();

    delete_bench_files();
    db.close();

    return 0;
}