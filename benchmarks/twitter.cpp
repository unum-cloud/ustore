
#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster

#include <cstring>     // `std::memchr`
#include <algorithm>   // `std::search`
#include <filesystem>  // Listing directories is too much pain in C
#include <string_view> //
#include <vector>      //
#include <thread>      //
#include <random>      // `std::random_device` for each thread

#include <benchmark/benchmark.h>

#include <ukv/ukv.hpp>
#include <ukv/cpp/ranges.hpp> // `sort_and_deduplicate`

namespace bm = benchmark;
using namespace unum::ukv;
using uniform_idx_t = std::uniform_int_distribution<std::size_t>;

constexpr std::size_t id_str_max_length_k = 24;
constexpr std::size_t copies_per_tweet_k = 10;
constexpr std::size_t tweet_file_size_k = 1; // GB

constexpr std::size_t primes_k[copies_per_tweet_k] = {
    12569589282558108893ull,
    10373281427301508897ull,
    10008795057561858269ull,
    7948791514834664467ull,
    3838954299457218127ull,
    3120785516547182557ull,
    4393300032555048899ull,
    7004376283452977123ull,
    9223372036854777211ull,
    14223002033854726039ull,
};

struct tweet_t {
    char id_str[id_str_max_length_k] = {0};
    char username[id_str_max_length_k] = {0};
    std::string_view body;
};

static std::string dataset_directory = "~/Datasets/Twitter/";
static std::vector<std::string> paths;
static std::vector<std::size_t> sizes;
static std::vector<std::string_view> mapped_contents;
static std::vector<std::vector<tweet_t>> tweets_per_path;
static database_t db;
static std::size_t thread_count = 0;
static std::size_t tweet_count = 0;

static ukv_collection_t collection_docs_k = ukv_collection_main_k;
static ukv_collection_t collection_graph_k = ukv_collection_main_k;
static ukv_collection_t collection_paths_k = ukv_collection_main_k;

static inline std::uint64_t hash_mix_ror64(std::uint64_t v, int r) {
    return (v >> r) | (v << (64 - r));
}

static inline std::uint64_t hash_mix_rrxmrrxmsx_0(std::uint64_t v) {
    v ^= hash_mix_ror64(v, 25) ^ hash_mix_ror64(v, 50);
    v *= 0xA24BAED4963EE407UL;
    v ^= hash_mix_ror64(v, 24) ^ hash_mix_ror64(v, 49);
    v *= 0x9FB21C651E98DF25UL;
    return v ^ v >> 28;
}

static inline std::size_t hash(tweet_t const& tweet) {
    auto u64s = reinterpret_cast<std::uint64_t const*>(tweet.id_str);
    auto mix = hash_mix_rrxmrrxmsx_0(u64s[0]) ^ hash_mix_rrxmrrxmsx_0(u64s[1]) ^ hash_mix_rrxmrrxmsx_0(u64s[2]);
    return mix;
}

class tweets_iterator_t {
    std::size_t internal_tweet_idx = 0;
    std::size_t file_idx = 0;

  public:
    tweets_iterator_t(std::size_t global_offset) noexcept {
        internal_tweet_idx = global_offset;
        while (tweets_per_path[file_idx].size() <= internal_tweet_idx) {
            internal_tweet_idx -= tweets_per_path[file_idx].size();
            file_idx++;
        }
    }
    tweets_iterator_t& operator++() noexcept {
        internal_tweet_idx++;
        while (file_idx < tweets_per_path.size() && internal_tweet_idx >= tweets_per_path[file_idx].size()) {
            file_idx++;
            internal_tweet_idx = 0;
        }
        return *this;
    }
    tweet_t const& operator*() const noexcept { return tweets_per_path[file_idx][internal_tweet_idx]; }
};

static void docs_upsert(bm::State& state) {
    status_t status;
    arena_t arena(db);

    // Locate the portion of tweets in a disjoint array.
    std::size_t const tweets_per_thread = tweet_count / thread_count;
    std::size_t const first_tweet_idx = state.thread_index() * tweets_per_thread;
    tweets_iterator_t tweets_iterator {first_tweet_idx};

    std::size_t tweets_bytes = 0;
    for (auto _ : state) {
        // TODO: Implement another way to select the batch size. Now it's equal copies_per_tweet_k
        // Generate multiple IDs for each tweet, to augment the dataset.
        auto const& tweet = *tweets_iterator;
        auto const tweet_hash = hash(tweet);
        std::array<ukv_key_t, copies_per_tweet_k> ids_tweets;
        for (std::size_t copy_idx = 0; copy_idx != copies_per_tweet_k; ++copy_idx)
            ids_tweets[copy_idx] = static_cast<ukv_key_t>(tweet_hash * primes_k[copy_idx]);

        // Finally, import the data.
        ukv_bytes_cptr_t body = reinterpret_cast<ukv_bytes_cptr_t>(tweet.body.data());
        ukv_length_t length = static_cast<ukv_length_t>(tweet.body.size());

        ukv_docs_write_t docs_write;
        docs_write.db = db;
        docs_write.error = status.member_ptr();
        docs_write.modification = ukv_doc_modify_upsert_k;
        docs_write.arena = arena.member_ptr();
        docs_write.type = ukv_doc_field_json_k;
        docs_write.tasks_count = copies_per_tweet_k;
        docs_write.collections = &collection_docs_k;
        docs_write.keys = ids_tweets.data();
        docs_write.keys_stride = sizeof(ukv_key_t);
        docs_write.lengths = &length;
        docs_write.values = &body;

        ukv_docs_write(&docs_write);
        status.throw_unhandled();

        ++tweets_iterator;
        tweets_bytes += tweet.body.size();
    }

    state.counters["docs/s"] = bm::Counter(tweets_per_thread * copies_per_tweet_k, bm::Counter::kIsRate);
    state.counters["batches/s"] = bm::Counter(tweets_per_thread, bm::Counter::kIsRate);
    state.counters["bytes/s"] = bm::Counter(tweets_bytes * copies_per_tweet_k, bm::Counter::kIsRate);
}

template <typename callback_at>
void sample_tweet_id_batches(bm::State& state, callback_at callback) {

    std::random_device rd;
    std::mt19937 gen(rd());
    uniform_idx_t choose_file(0, tweets_per_path.size() - 1);
    uniform_idx_t choose_hash(0, copies_per_tweet_k - 1);

    auto const batch_size = static_cast<ukv_size_t>(state.range(0));
    std::vector<ukv_key_t> batch_keys(batch_size);

    std::size_t iterations = 0;
    for (auto _ : state) {
        for (std::size_t key_idx = 0; key_idx != batch_size; ++key_idx) {
            std::size_t const file_idx = choose_file(gen);
            auto const& tweets = tweets_per_path[file_idx];
            uniform_idx_t choose_tweet(0, tweets.size() - 1);
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

static void docs_sample_blobs(bm::State& state) {

    status_t status;
    arena_t arena(db);

    std::size_t received_bytes = 0;
    sample_tweet_id_batches(state, [&](ukv_key_t const* ids_tweets, ukv_size_t count) {
        ukv_length_t* offsets = nullptr;
        ukv_byte_t* values = nullptr;
        ukv_read_t read;
        read.db = db;
        read.error = status.member_ptr();
        read.arena = arena.member_ptr();
        read.tasks_count = count;
        read.collections = &collection_docs_k;
        read.keys = ids_tweets;
        read.keys_stride = sizeof(ukv_key_t);
        read.offsets = &offsets;
        read.values = &values;

        ukv_read(&read);
        status.throw_unhandled();
        received_bytes += offsets[count];
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void docs_sample_objects(bm::State& state) {

    // We want to trigger parsing and serialization
    status_t status;
    arena_t arena(db);

    std::size_t received_bytes = 0;
    sample_tweet_id_batches(state, [&](ukv_key_t const* ids_tweets, ukv_size_t count) {
        ukv_length_t* offsets = nullptr;
        ukv_byte_t* values = nullptr;

        ukv_docs_read_t docs_read;
        docs_read.db = db;
        docs_read.error = status.member_ptr();
        docs_read.arena = arena.member_ptr();
        docs_read.type = ukv_doc_field_json_k;
        docs_read.tasks_count = count;
        docs_read.collections = &collection_docs_k;
        docs_read.keys = ids_tweets;
        docs_read.keys_stride = sizeof(ukv_key_t);
        docs_read.offsets = &offsets;
        docs_read.values = &values;

        ukv_docs_read(&docs_read);
        status.throw_unhandled();
        received_bytes += offsets[count];
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void docs_sample_field(bm::State& state) {

    status_t status;
    arena_t arena(db);
    ukv_str_view_t field = "text";

    std::size_t received_bytes = 0;
    sample_tweet_id_batches(state, [&](ukv_key_t const* ids_tweets, ukv_size_t count) {
        ukv_length_t* offsets = nullptr;
        ukv_byte_t* values = nullptr;

        ukv_docs_read_t docs_read;
        docs_read.db = db;
        docs_read.error = status.member_ptr();
        docs_read.arena = arena.member_ptr();
        docs_read.type = ukv_doc_field_str_k;
        docs_read.tasks_count = count;
        docs_read.collections = &collection_docs_k;
        docs_read.keys = ids_tweets;
        docs_read.keys_stride = sizeof(ukv_key_t);
        docs_read.fields = &field;
        docs_read.offsets = &offsets;
        docs_read.values = &values;

        ukv_docs_read(&docs_read);
        status.throw_unhandled();
        received_bytes += offsets[count];
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

static void docs_sample_table(bm::State& state) {
    status_t status;
    arena_t arena(db);

    constexpr ukv_size_t fields_k = 4;
    ukv_str_view_t names[fields_k] {"timestamp_ms", "reply_count", "retweet_count", "favorite_count"};
    ukv_doc_field_type_t types[fields_k] {
        ukv_doc_field_str_k,
        ukv_doc_field_u32_k,
        ukv_doc_field_u32_k,
        ukv_doc_field_u32_k,
    };

    std::size_t received_bytes = 0;
    sample_tweet_id_batches(state, [&](ukv_key_t const* ids_tweets, ukv_size_t count) {
        ukv_octet_t** validities = nullptr;
        ukv_byte_t** scalars = nullptr;
        ukv_length_t** offsets = nullptr;
        ukv_length_t** lengths = nullptr;
        ukv_byte_t* strings = nullptr;

        ukv_docs_gather_t docs_gather;
        docs_gather.db = db;
        docs_gather.error = status.member_ptr();
        docs_gather.arena = arena.member_ptr();
        docs_gather.docs_count = count;
        docs_gather.fields_count = fields_k;
        docs_gather.collections = &collection_docs_k;
        docs_gather.keys = ids_tweets;
        docs_gather.keys_stride = sizeof(ukv_key_t);
        docs_gather.fields = names;
        docs_gather.fields_stride = sizeof(ukv_str_view_t);
        docs_gather.types = types;
        docs_gather.types_stride = sizeof(ukv_doc_field_type_t);
        docs_gather.columns_validities = &validities;
        docs_gather.columns_scalars = &scalars;
        docs_gather.columns_offsets = &offsets;
        docs_gather.columns_lengths = &lengths;
        docs_gather.joined_strings = &strings;

        ukv_docs_gather(&docs_gather);
        status.throw_unhandled();

        // One column is just strings
        received_bytes += std::accumulate(&lengths[0][0], &lengths[0][0] + count - 1, 0ul);
        // Others are scalars
        received_bytes += (fields_k - 1) * sizeof(std::uint32_t) * count;
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
}

/**
 * @brief Two-step benchmark, that samples documents and constructs a graph between:
 * - Tweets and their Authors.
 * - Tweets and their Retweets.
 * - Authors and Retweeters labeled by Retweet IDs.
 *
 * Evaluates:
 * 1. Speed of documents Batch-Selections.
 * 2. Parsing and sampling their documents.
 * 3. Batch-upserts into Graph layout.
 */
static void graph_construct_from_docs(bm::State& state) {
    status_t status;
    arena_t arena(db);

    constexpr ukv_size_t fields_k = 3;
    ukv_str_view_t names[fields_k] {"/user/id", "/retweeted_status/id", "/retweeted_status/user/id"};
    ukv_doc_field_type_t types[fields_k] {ukv_doc_field_u64_k, ukv_doc_field_u64_k, ukv_doc_field_u64_k};
    std::vector<edge_t> edges_array;

    std::size_t received_bytes = 0;
    std::size_t added_edges = 0;
    sample_tweet_id_batches(state, [&](ukv_key_t const* ids_tweets, ukv_size_t count) {
        ukv_octet_t** validities = nullptr;
        ukv_byte_t** scalars = nullptr;
        ukv_byte_t* strings = nullptr;

        ukv_docs_gather_t docs_gather;
        docs_gather.db = db;
        docs_gather.error = status.member_ptr();
        docs_gather.arena = arena.member_ptr();
        docs_gather.docs_count = count;
        docs_gather.fields_count = fields_k;
        docs_gather.collections = &collection_docs_k;
        docs_gather.keys = ids_tweets;
        docs_gather.keys_stride = sizeof(ukv_key_t);
        docs_gather.fields = names;
        docs_gather.fields_stride = sizeof(ukv_str_view_t);
        docs_gather.types = types;
        docs_gather.types_stride = sizeof(ukv_doc_field_type_t);
        docs_gather.columns_validities = &validities;
        docs_gather.columns_scalars = &scalars;
        docs_gather.joined_strings = &strings;

        ukv_docs_gather(&docs_gather);
        status.throw_unhandled();

        // Check which edges can be constructed
        edges_array.clear();
        edges_array.reserve(count * 3);
        strided_iterator_gt<ukv_key_t> ids_users((ukv_key_t*)(scalars[0]), sizeof(ukv_key_t));
        strided_iterator_gt<ukv_key_t> ids_retweets((ukv_key_t*)(scalars[1]), sizeof(ukv_key_t));
        strided_iterator_gt<ukv_key_t> ids_retweeters((ukv_key_t*)(scalars[2]), sizeof(ukv_key_t));
        bits_span_t valid_users(validities[0]);
        bits_span_t valid_retweets(validities[1]);
        bits_span_t valid_retweeters(validities[2]);
        for (std::size_t i = 0; i != count; ++i) {
            // Tweet <-> Author
            edges_array.push_back(edge_t {.source_id = ids_tweets[i], .target_id = ids_users[i]});
            // Tweet <-> Retweet
            if (valid_retweets[i])
                edges_array.push_back(edge_t {.source_id = ids_tweets[i], .target_id = ids_retweets[i]});
            // Author <- Tweet -> Retweeter
            if (valid_retweeters[i])
                edges_array.push_back(edge_t {
                    .source_id = ids_users[i],
                    .target_id = ids_retweeters[i],
                    .id = ids_retweets[i],
                });
        }

        // Insert or update those edges
        auto strided = edges(edges_array);
        ukv_graph_upsert_edges_t graph_upsert_edges;
        graph_upsert_edges.db = db;
        graph_upsert_edges.error = status.member_ptr();
        graph_upsert_edges.arena = arena.member_ptr();
        graph_upsert_edges.tasks_count = count;
        graph_upsert_edges.collections = &collection_graph_k;
        graph_upsert_edges.edges_ids = strided.edge_ids.begin().get();
        graph_upsert_edges.edges_stride = strided.edge_ids.stride();
        graph_upsert_edges.sources_ids = strided.source_ids.begin().get();
        graph_upsert_edges.sources_stride = strided.source_ids.stride();
        graph_upsert_edges.targets_ids = strided.target_ids.begin().get();
        graph_upsert_edges.targets_stride = strided.target_ids.stride();

        ukv_graph_upsert_edges(&graph_upsert_edges);
        status.throw_unhandled();

        received_bytes += fields_k * sizeof(std::uint64_t) * count;
        added_edges += edges_array.size();
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
    state.counters["edges/s"] = bm::Counter(added_edges, bm::Counter::kIsRate);
}

/**
 * @brief Most Tweets in the graph have just one connection - to their Author.
 * That is why we make a two-hop benchmark. For every Tweet vertex we gather their
 * Authors and all the Retweets, as well as the connections of those Authors and
 * Retweets.
 */
static void graph_traverse_two_hops(bm::State& state) {
    status_t status;
    arena_t arena(db);
    std::plus plus;

    std::size_t received_bytes = 0;
    std::size_t received_edges = 0;
    sample_tweet_id_batches(state, [&](ukv_key_t const* ids_tweets, ukv_size_t count) {
        // First hop
        ukv_vertex_role_t const role = ukv_vertex_role_any_k;
        ukv_vertex_degree_t* degrees = nullptr;
        ukv_key_t* ids_in_edges = nullptr;

        ukv_graph_find_edges_t graph_find_edges_first;
        graph_find_edges_first.db = db;
        graph_find_edges_first.error = status.member_ptr();
        graph_find_edges_first.arena = arena.member_ptr();
        graph_find_edges_first.tasks_count = count;
        graph_find_edges_first.collections = &collection_graph_k;
        graph_find_edges_first.vertices = ids_tweets;
        graph_find_edges_first.vertices_stride = sizeof(ukv_key_t);
        graph_find_edges_first.roles = &role;
        graph_find_edges_first.degrees_per_vertex = &degrees;
        graph_find_edges_first.edges_per_vertex = &ids_in_edges;

        ukv_graph_find_edges(&graph_find_edges_first);
        status.throw_unhandled();

        // Now keep only the unique objects
        auto total_edges = std::transform_reduce(degrees, degrees + count, 0ul, plus, [](ukv_vertex_degree_t d) {
            return d != ukv_vertex_degree_missing_k ? d : 0;
        });
        auto total_ids = total_edges * 3;
        auto unique_ids = sort_and_deduplicate(ids_in_edges, ids_in_edges + total_ids);

        ukv_graph_find_edges_t graph_find_edges_second;
        graph_find_edges_second.db = db;
        graph_find_edges_second.error = status.member_ptr();
        graph_find_edges_second.arena = arena.member_ptr();
        graph_find_edges_second.options = ukv_option_dont_discard_memory_k;
        graph_find_edges_second.tasks_count = unique_ids;
        graph_find_edges_second.collections = &collection_graph_k;
        graph_find_edges_second.vertices = ids_in_edges;
        graph_find_edges_second.vertices_stride = sizeof(ukv_key_t);
        graph_find_edges_second.roles = &role;
        graph_find_edges_second.degrees_per_vertex = &degrees;
        graph_find_edges_second.edges_per_vertex = &ids_in_edges;

        // Second hop
        ukv_graph_find_edges(&graph_find_edges_second);
        status.throw_unhandled();

        total_edges += std::transform_reduce(degrees, degrees + unique_ids, 0ul, plus, [](ukv_vertex_degree_t d) {
            return d != ukv_vertex_degree_missing_k ? d : 0;
        });
        total_ids = total_edges * 3;

        received_bytes += total_ids * sizeof(ukv_key_t);
        received_edges += total_edges;
    });
    state.counters["bytes/s"] = bm::Counter(received_bytes, bm::Counter::kIsRate);
    state.counters["edges/s"] = bm::Counter(received_edges, bm::Counter::kIsRate);
}

static void paths_construct_from_nicknames(bm::State& state) {
    auto const batch_size = static_cast<ukv_size_t>(state.range(0));
    std::vector<ukv_str_view_t> batch_usernames(batch_size);
    std::vector<ukv_str_view_t> batch_id_strs(batch_size);

    status_t status;
    arena_t arena(db);
    ukv_char_t separator = 0;

    // Locate the portion of tweets in a disjoint array.
    std::size_t const tweets_per_thread = tweet_count / thread_count;
    std::size_t const batches_per_thread = tweets_per_thread / batch_size;
    std::size_t const first_tweet_idx = state.thread_index() * batches_per_thread * batch_size;
    tweets_iterator_t tweets_iterator {first_tweet_idx};

    std::size_t injected_bytes = 0;
    for (auto _ : state) {

        // Prepare a batch from multiple entries
        for (std::size_t i = 0; i != batch_size; ++i, ++tweets_iterator) {
            auto const& tweet = *tweets_iterator;
            batch_usernames[i] = tweet.username;
            batch_id_strs[i] = tweet.id_str;
            injected_bytes += std::strlen(tweet.username);
            injected_bytes += std::strlen(tweet.id_str);
        }

        ukv_paths_write_t paths_write;
        paths_write.db = db;
        paths_write.error = status.member_ptr();
        paths_write.arena = arena.member_ptr();
        paths_write.tasks_count = batch_size;
        paths_write.path_separator = separator;
        paths_write.paths = (ukv_str_view_t*)batch_usernames.data();
        paths_write.paths_stride = sizeof(ukv_str_view_t);
        paths_write.values_bytes = (ukv_bytes_cptr_t*)batch_id_strs.data();
        paths_write.values_bytes_stride = sizeof(ukv_str_view_t);

        // Finally, import the data.
        ukv_paths_write(&paths_write);
        status.throw_unhandled();
    }

    state.counters["docs/s"] = bm::Counter(tweets_per_thread, bm::Counter::kIsRate);
    state.counters["batches/s"] = bm::Counter(batches_per_thread, bm::Counter::kIsRate);
    state.counters["bytes/s"] = bm::Counter(injected_bytes, bm::Counter::kIsRate);
}

static void index_file(std::string_view mapped_contents, std::vector<tweet_t>& tweets) {
    constexpr char const* id_key = "\"id\":";
    constexpr char const* username_key = "\"screen_name\":";
    char const* line_begin = mapped_contents.begin();
    char const* const end = mapped_contents.end();
    while (line_begin < end) {
        auto line_end = (char const*)std::memchr(line_begin, '\n', end - line_begin) ?: end;
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

        auto username_key_begin = std::search(tweet.body.begin(), tweet.body.end(), username_key, username_key + 14);
        auto username_begin = username_key_begin + 14;
        auto username_end =
            std::find(username_begin, std::min(username_begin + sizeof(tweet.username), tweet.body.end()), ',');
        std::memcpy(tweet.username, username_begin, username_end - username_begin);

        tweets.push_back(tweet);
        line_begin = line_end + 1;
    }
}

int main(int argc, char** argv) {
    bm::Initialize(&argc, argv);

    std::size_t db_volume = 100; // GB
    thread_count = std::thread::hardware_concurrency() / 8;

    // 1. Find the dataset parts
    std::printf("Will search for .ndjson files...\n");
    auto dataset_path = dataset_directory;
    auto home_path = std::getenv("HOME");
    if (dataset_path.front() == '~')
        dataset_path = std::filesystem::path(home_path) / dataset_path.substr(2);
    auto opts = std::filesystem::directory_options::follow_directory_symlink;
    for (auto const& dir_entry : std::filesystem::directory_iterator(dataset_path, opts)) {
        if (dir_entry.path().extension() != ".ndjson")
            continue;

        paths.push_back(dir_entry.path());
        sizes.push_back(dir_entry.file_size());
    }
    std::printf("- found %i files\n", static_cast<int>(paths.size()));
    paths.resize(db_volume / (tweet_file_size_k * copies_per_tweet_k));
    std::printf("- kept only %i files\n", static_cast<int>(paths.size()));
    tweets_per_path.resize(paths.size());
    mapped_contents.resize(paths.size());

    // 2. Memory-map the contents
    // As we are closing the process after the benchmarks, we can avoid unmap them.
    std::printf("Will memory-map the files...\n");
    for (std::size_t path_idx = 0; path_idx != paths.size(); ++path_idx) {
        auto const& path = paths[path_idx];
        auto handle = open(path.c_str(), O_RDONLY);
        if (handle == -1)
            throw std::runtime_error("Can't open file");

        auto size = sizes[path_idx];
        auto begin = mmap(NULL, size, PROT_READ, MAP_PRIVATE, handle, 0);
        mapped_contents[path_idx] = std::string_view(reinterpret_cast<char const*>(begin), size);
        madvise(begin, size, MADV_SEQUENTIAL);
    }

    // 3. Index the dataset
    std::printf("Will index the files...\n");
    std::vector<std::thread> parsing_threads;
    for (std::size_t path_idx = 0; path_idx != paths.size(); ++path_idx)
        parsing_threads.push_back(std::thread( //
            &index_file,
            mapped_contents[path_idx],
            std::ref(tweets_per_path[path_idx])));
    for (auto& thread : parsing_threads)
        thread.join();
    tweet_count = 0;
    for (auto const& tweets : tweets_per_path)
        tweet_count += tweets.size();
    std::printf("- indexed %zu tweets\n", static_cast<size_t>(tweet_count));

// 4. Run the actual benchmarks
#if defined(UKV_ENGINE_IS_LEVELDB)
    db.open("/mnt/md0/Twitter/LevelDB").throw_unhandled();
#elif defined(UKV_ENGINE_IS_ROCKSDB)
    db.open("/mnt/md0/Twitter/RocksDB").throw_unhandled();
#elif defined(UKV_ENGINE_IS_UNUMDB)
    db.open("/mnt/md0/Twitter/UnumDB").throw_unhandled();
#else
    db.open().throw_unhandled();
#endif

    bool can_build_graph = false;
    bool can_build_paths = false;
    if (ukv_supports_named_collections_k) {
        status_t status;
        ukv_collection_create_t collection_init {
            .db = db,
            .error = status.member_ptr(),
            .name = "twitter.docs",
            .config = "",
            .id = &collection_docs_k,
        };

        ukv_collection_create(&collection_init);
        status.throw_unhandled();
        collection_init.name = "twitter.graph";
        collection_init.id = &collection_graph_k;
        ukv_collection_create(&collection_init);
        status.throw_unhandled();
        can_build_graph = true;
        collection_init.name = "twitter.nicks";
        collection_init.id = &collection_paths_k;
        ukv_collection_create(&collection_init);
        status.throw_unhandled();
        can_build_paths = true;
    }
    can_build_paths = false;

    std::printf("Will benchmark...\n");
    auto min_time = 10;
    auto small_batch_size = 32;
    auto big_batch_size = 256;

    bm::RegisterBenchmark("docs_upsert", &docs_upsert) //
        ->Iterations(tweet_count / thread_count)
        ->UseRealTime()
        ->Threads(thread_count);

    if (ukv_doc_field_default_k != ukv_doc_field_json_k)
        bm::RegisterBenchmark("docs_sample_blobs", &docs_sample_blobs) //
            ->MinTime(min_time)
            ->UseRealTime()
            ->Threads(thread_count)
            ->Arg(small_batch_size)
            ->Arg(big_batch_size);

    bm::RegisterBenchmark("docs_sample_objects", &docs_sample_objects) //
        ->MinTime(min_time)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(small_batch_size)
        ->Arg(big_batch_size);

    bm::RegisterBenchmark("docs_sample_field", &docs_sample_field) //
        ->MinTime(min_time)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(small_batch_size)
        ->Arg(big_batch_size);

    bm::RegisterBenchmark("docs_sample_table", &docs_sample_table) //
        ->MinTime(min_time)
        ->UseRealTime()
        ->Threads(thread_count)
        ->Arg(small_batch_size)
        ->Arg(big_batch_size);

    if (can_build_graph) {

        bm::RegisterBenchmark("graph_construct_from_docs", &graph_construct_from_docs) //
            ->MinTime(min_time)
            ->Threads(thread_count)
            ->Arg(small_batch_size)
            ->Arg(big_batch_size);

        bm::RegisterBenchmark("graph_traverse_two_hops", &graph_traverse_two_hops) //
            ->MinTime(min_time)
            ->Threads(thread_count)
            ->Arg(small_batch_size)
            ->Arg(big_batch_size);
    }

    if (can_build_paths) {

        bm::RegisterBenchmark("paths_construct_from_nicknames", &paths_construct_from_nicknames) //
            ->Iterations((tweet_count / thread_count) / big_batch_size)
            ->Threads(thread_count)
            ->Arg(big_batch_size);
    }

    bm::RunSpecifiedBenchmarks();
    bm::Shutdown();

    // To avoid sanitizer complaints, we should unmap the files:
    for (auto mapped_content : mapped_contents)
        munmap((void*)mapped_content.data(), mapped_content.size());
    db.clear().throw_unhandled();

    return 0;
}