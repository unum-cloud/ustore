
#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster

#include <cstring>     // `std::memchr`
#include <algorithm>   // `std::search`
#include <iostream>    // `std::cout`, we are not proud of this :)
#include <filesystem>  // Listing directories is too much pain in C
#include <string_view> //
#include <vector>      //
#include <thread>      //
#include <random>      // `std::random_device` for each thread

#include <benchmark/benchmark.h>

#include <ukv/ukv.hpp>

namespace bm = benchmark;
using namespace unum::ukv;
using uniform_idx_t = std::uniform_int_distribution<std::size_t>;

constexpr std::size_t id_str_max_length_k = 24;
constexpr std::size_t copies_per_tweet_k = 10;

constexpr std::size_t primes_k[copies_per_tweet_k] = {
    9223372036854777211ull,
    14223002033854726039ull,
    9223372036854777293ull,
    14223002033854726067ull,
    9223372036854777341ull,
    14223002033854726081ull,
    9223372036854777343ull,
    14223002033854726111ull,
    9223372036854777353ull,
    14223002033854726163ull,
};

struct tweet_t {
    char id_str[id_str_max_length_k] = {0};
    std::string_view body;
};

static std::string dataset_directory = "~/Datasets/Twitter/";
static std::vector<std::string> paths;
static std::vector<std::string_view> mapped_contents;
static std::vector<std::vector<tweet_t>> tweets_per_path;
static database_t db;
static std::size_t thread_count = 0;
static std::size_t tweet_count = 0;

static inline std::uint64_t ror64(std::uint64_t v, int r) {
    return (v >> r) | (v << (64 - r));
}

static inline std::uint64_t rrxmrrxmsx_0(std::uint64_t v) {
    v ^= ror64(v, 25) ^ ror64(v, 50);
    v *= 0xA24BAED4963EE407UL;
    v ^= ror64(v, 24) ^ ror64(v, 49);
    v *= 0x9FB21C651E98DF25UL;
    return v ^ v >> 28;
}

static inline std::size_t hash(tweet_t const& tweet) {
    auto u64s = reinterpret_cast<std::uint64_t const*>(tweet.id_str);
    auto mix = rrxmrrxmsx_0(u64s[0]) ^ rrxmrrxmsx_0(u64s[1]) ^ rrxmrrxmsx_0(u64s[2]);
    return mix;
}

static void batch_insert(bm::State& state) {
    status_t status;
    arena_t arena(db);

    // Locate the portion of tweets in a disjoint array.
    std::size_t const tweets_per_thread = tweet_count / thread_count;
    std::size_t global_tweet_idx = state.thread_index() * tweets_per_thread;
    std::size_t internal_tweet_idx = global_tweet_idx;
    std::size_t file_idx = 0;
    while (tweets_per_path[file_idx].size() <= internal_tweet_idx) {
        internal_tweet_idx -= tweets_per_path[file_idx].size();
        file_idx++;
    }

    std::size_t tweets_bytes = 0;
    for (auto _ : state) {

        // Detect if we need to jump to another file.
        if (internal_tweet_idx >= tweets_per_path.size()) {
            file_idx++;
            internal_tweet_idx = 0;
        }

        // Generate multiple IDs for each tweet, to augment the dataset.
        auto const& tweet = tweets_per_path[file_idx][internal_tweet_idx];
        auto const tweet_hash = hash(tweet);
        std::array<ukv_key_t, copies_per_tweet_k> keys;
        for (std::size_t copy_idx = 0; copy_idx != copies_per_tweet_k; ++copy_idx)
            keys[copy_idx] = static_cast<ukv_key_t>(tweet_hash * primes_k[copy_idx]);

        // Finally, import the data.
        ukv_bytes_cptr_t body = reinterpret_cast<ukv_bytes_cptr_t>(tweet.body.data());
        ukv_length_t length = static_cast<ukv_length_t>(tweet.body.size());
        ukv_write( //
            db,
            nullptr,
            copies_per_tweet_k,
            nullptr,
            0,
            keys.data(),
            sizeof(ukv_key_t),
            nullptr,
            nullptr,
            0,
            &length,
            0,
            &body,
            0,
            ukv_options_default_k,
            arena.member_ptr(),
            status.member_ptr());
        status.throw_unhandled();

        internal_tweet_idx++;
        global_tweet_idx++;
        tweets_bytes += tweet.body.size();
    }

    state.counters["docs/s"] = bm::Counter(tweets_per_thread * copies_per_tweet_k, bm::Counter::kIsRate);
    state.counters["batches/s"] = bm::Counter(tweets_per_thread, bm::Counter::kIsRate);
    state.counters["bytes/s"] = bm::Counter(tweets_bytes * copies_per_tweet_k, bm::Counter::kIsRate);
}

template <typename callback_at>
void sample_randomly(bm::State& state, callback_at callback) {

    std::random_device rd;
    std::mt19937 gen(rd());
    uniform_idx_t choose_file(0, tweets_per_path.size());
    uniform_idx_t choose_hash(0, copies_per_tweet_k);

    auto const batch_size = static_cast<ukv_size_t>(state.range(0));
    std::vector<ukv_key_t> batch_keys(batch_size);

    std::size_t iterations = 0;
    for (auto _ : state) {
        for (std::size_t key_idx = 0; key_idx != batch_size; ++key_idx) {
            std::size_t const file_idx = choose_file(gen);
            auto const& tweets = tweets_per_path[file_idx];
            uniform_idx_t choose_tweet(0, tweets.size());
            std::size_t const hash_idx = choose_hash(gen);
            std::size_t const tweet_idx = choose_tweet(gen);
            auto const& tweet = tweets[tweet_idx];
            auto const tweet_hash = hash(tweet);
            batch_keys[key_idx] = static_cast<ukv_key_t>(tweet_hash * primes_k[hash_idx]);
        }
        callback(batch_keys.data(), batch_size);
        iterations++;
    }

    state.counters["docs/s"] = bm::Counter(iterations * batch_size, bm::Counter::kIsRate);
    state.counters["batches/s"] = bm::Counter(iterations, bm::Counter::kIsRate);
}

static void sample_blobs(bm::State& state) {

    status_t status;
    arena_t arena(db);

    std::size_t received_bytes = 0;
    sample_randomly(state, [&](ukv_key_t const* keys, ukv_size_t count) {
        ukv_length_t* offsets = nullptr;
        ukv_byte_t* values = nullptr;
        ukv_read( //
            db,
            nullptr,
            count,
            nullptr,
            0,
            keys,
            sizeof(ukv_key_t),
            ukv_options_default_k,
            nullptr,
            &offsets,
            nullptr,
            &values,
            arena.member_ptr(),
            status.member_ptr());
        status.throw_unhandled();
        received_bytes += offsets[count];
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void sample_docs(bm::State& state) {

    // We want to trigger parsing and serialization
    ukv_format_t format = ukv_format_docs_internal_k == ukv_format_json_k ? ukv_format_msgpack_k : ukv_format_json_k;
    status_t status;
    arena_t arena(db);

    std::size_t received_bytes = 0;
    sample_randomly(state, [&](ukv_key_t const* keys, ukv_size_t count) {
        ukv_length_t* offsets = nullptr;
        ukv_byte_t* values = nullptr;
        ukv_docs_read( //
            db,
            nullptr,
            count,
            nullptr,
            0,
            keys,
            sizeof(ukv_key_t),
            nullptr,
            0,
            ukv_options_default_k,
            format,
            ukv_type_any_k,
            nullptr,
            &offsets,
            nullptr,
            &values,
            arena.member_ptr(),
            status.member_ptr());
        status.throw_unhandled();
        received_bytes += offsets[count];
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void sample_field(bm::State& state) {

    status_t status;
    arena_t arena(db);
    ukv_str_view_t field = "text";

    std::size_t received_bytes = 0;
    sample_randomly(state, [&](ukv_key_t const* keys, ukv_size_t count) {
        ukv_length_t* offsets = nullptr;
        ukv_byte_t* values = nullptr;
        ukv_docs_read( //
            db,
            nullptr,
            count,
            nullptr,
            0,
            keys,
            sizeof(ukv_key_t),
            &field,
            0,
            ukv_options_default_k,
            ukv_format_binary_k,
            ukv_type_str_k,
            nullptr,
            &offsets,
            nullptr,
            &values,
            arena.member_ptr(),
            status.member_ptr());
        status.throw_unhandled();
        received_bytes += offsets[count];
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void sample_tables(bm::State& state) {
    status_t status;
    arena_t arena(db);

    constexpr ukv_size_t fields_k = 4;
    ukv_str_view_t names[fields_k] {"timestamp_ms", "reply_count", "retweet_count", "favorite_count"};
    ukv_type_t types[fields_k] {ukv_type_str_k, ukv_type_u32_k, ukv_type_u32_k, ukv_type_u32_k};

    std::size_t received_bytes = 0;
    sample_randomly(state, [&](ukv_key_t const* keys, ukv_size_t count) {
        ukv_octet_t** validities = nullptr;
        ukv_byte_t** scalars = nullptr;
        ukv_length_t** offsets = nullptr;
        ukv_byte_t* strings = nullptr;
        ukv_docs_gather( //
            db,
            nullptr,
            count,
            fields_k,
            nullptr,
            0,
            keys,
            sizeof(ukv_key_t),
            names,
            sizeof(ukv_str_view_t),
            types,
            sizeof(ukv_type_t),
            ukv_options_default_k,
            &validities,
            nullptr,
            nullptr,
            &scalars,
            &offsets,
            nullptr,
            &strings,
            arena.member_ptr(),
            status.member_ptr());
        status.throw_unhandled();

        received_bytes += offsets[0][count];
        received_bytes += 3 * sizeof(std::uint32_t) * count;
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void index_file(std::string_view mapped_contents, std::vector<tweet_t>& tweets) {
    char const* id_key = "\"id\":";
    char const* line_begin = mapped_contents.begin();
    while (line_begin < mapped_contents.end()) {
        auto line_end = (char const*)std::memchr(line_begin, '\n', mapped_contents.end() - line_begin);
        auto id_key_begin = std::search(line_begin, line_end, id_key, id_key + 5);
        if (id_key_begin == line_end) {
            line_begin = line_end + 1;
            continue;
        }

        tweet_t tweet;
        tweet.body = std::string_view(line_begin, line_end - line_begin);

        auto id_begin = id_key_begin + 5;
        auto id_length = std::min<std::size_t>(id_str_max_length_k, line_end - id_begin);
        std::memcpy(tweet.id_str, id_begin, id_length);
        tweets.push_back(tweet);
    }
}

int main(int argc, char** argv) {
    bm::Initialize(&argc, argv);

    // 1. Find the dataset parts
    for (auto const& dir_entry : std::filesystem::directory_iterator {dataset_directory}) {
        if (dir_entry.path().extension() != "ndjson")
            continue;

        paths.push_back(dir_entry.path());
        std::cout << "- Found file: " << dir_entry.path().filename() //
                  << " ; size: " << dir_entry.file_size() << std::endl;
    }
    tweets_per_path.resize(paths.size());
    mapped_contents.resize(paths.size());

    // 2. Memory-map the contents
    // As we are closing the process after the benchmarks, we can avoid unmap them.
    for (std::size_t path_idx = 0; path_idx != paths.size(); ++path_idx) {
        auto const& path = paths[path_idx];
        auto handle = open(path.c_str(), O_RDONLY);
        if (handle == -1)
            throw std::runtime_error("Can't open file");

        struct stat metadata;
        if (fstat(handle, &metadata) == -1)
            throw std::runtime_error("Can't obtain size");

        auto begin = mmap(NULL, metadata.st_size, PROT_READ, MAP_PRIVATE, handle, 0);
        mapped_contents[path_idx] = std::string_view(reinterpret_cast<char const*>(begin), metadata.st_size);
        madvise(begin, metadata.st_size, MADV_SEQUENTIAL);
    }

    // 3. Index the dataset
    std::vector<std::thread> parsing_threads;
    for (std::size_t path_idx = 0; path_idx != paths.size(); ++path_idx)
        parsing_threads.push_back(std::thread( //
            &index_file,
            mapped_contents[path_idx],
            std::ref(tweets_per_path[path_idx])));
    for (auto& thread : parsing_threads)
        thread.join();
    thread_count = std::thread::hardware_concurrency() / 2;
    tweet_count = 0;
    for (auto const& tweets : tweets_per_path)
        tweet_count += tweets.size();

    // 4. Run the actual benchmarks
    bm::RegisterBenchmark("batch_insert", &batch_insert)->Iterations(tweet_count)->UseRealTime()->Threads(thread_count);
    bm::RegisterBenchmark("sample_blobs", &sample_blobs)
        ->MinTime(20)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(32)
        ->Arg(256);
    bm::RegisterBenchmark("sample_docs", &sample_docs)
        ->MinTime(20)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(32)
        ->Arg(256);
    bm::RegisterBenchmark("sample_field", &sample_field)
        ->MinTime(20)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(32)
        ->Arg(256);
    bm::RegisterBenchmark("sample_tables", &sample_tables)
        ->MinTime(20)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(32)
        ->Arg(256);

    bm::RunSpecifiedBenchmarks();
    bm::Shutdown();
    return 0;
}