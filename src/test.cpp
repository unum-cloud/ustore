/**
 * @file test.cpp
 * @author Ashot Vardanian
 * @date 2022-07-06
 *
 * @brief A set of tests implemented using Google Test.
 */

#include <unordered_set>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "ukv/ukv.hpp"

using namespace unum::ukv;
using namespace unum;

#define macro_concat_(prefix, suffix) prefix##suffix
#define macro_concat(prefix, suffix) macro_concat_(prefix, suffix)
#define _ [[maybe_unused]] auto macro_concat(_, __LINE__)

TEST(db, intro) {

    db_t db;
    EXPECT_TRUE(db.open());

    // Try getting the main collection
    EXPECT_TRUE(db.collection());
    collection_t main = *db.collection();

    // Single-element access
    main[42] = "purpose of life";
    main.at(42) = "purpose of life";
    EXPECT_EQ(main[42].value()->first, "purpose of life");
    _ = main[42].clear();

    // Mapping multiple keys to same values
    main[{43, 44}] = "same value";

    // Operations on smart-references
    _ = main[{43, 44}].clear();
    _ = main[{43, 44}].erase();
    _ = main[{43, 44}].present();
    _ = main[{43, 44}].length();
    _ = main[{43, 44}].value();
    _ = main[std::array<ukv_key_t, 3> {65, 66, 67}];
    _ = main[std::vector<ukv_key_t> {65, 66, 67, 68}];
    for (value_view_t value : main[{100, 101}].value()->first) {
    }

    // Accessing named collections
    collection_t prefixes = *db.collection("prefixes");
    prefixes.at(42) = "purpose";
    db["articles"]->at(42) = "of";
    db["suffixes"]->at(42) = "life";

    // Reusable memory
    // This interface not just more performant, but also provides nicer interface:
    //  expected_gt<taped_values_view_t> tapes = main[{100, 101}].on(arena);
    managed_arena_t arena(db);
    _ = main[{43, 44}].on(arena).clear();
    _ = main[{43, 44}].on(arena).erase();
    _ = main[{43, 44}].on(arena).present();
    _ = main[{43, 44}].on(arena).length();
    _ = main[{43, 44}].on(arena).value();

    // Batch-assignment: many keys to many values
    // main[std::array<ukv_key_t, 3> {65, 66, 67}] = std::array {"A", "B", "C"};
    // main[std::array {sub(prefixes, 65), sub(66), sub(67)}] = std::array {"A", "B", "C"};

    // Iterating over collections
    for (ukv_key_t key : main.keys()) {
    }
    for (ukv_key_t key : main.keys(100, 200)) {
    }
    _ = main.keys(100, 200).find_size()->cardinality;

    // Supporting options
    _ = main[{43, 44}].on(arena).clear(/*flush:*/ false);
    _ = main[{43, 44}].on(arena).erase(/*flush:*/ false);
    _ = main[{43, 44}].on(arena).present(/*track:*/ false);
    _ = main[{43, 44}].on(arena).length(/*format:*/ ukv_doc_format_binary_k, /*track:*/ false);
    _ = main[{43, 44}].on(arena).value(/*format:*/ ukv_doc_format_binary_k, /*track:*/ false);

    // Working with sub documents
    main[56] = R"( {"hello": "world", "answer": 42} )"_json.dump().c_str();
}

template <typename locations_at>
void check_length(member_refs_gt<locations_at>& ref, ukv_val_len_t expected_length) {

    EXPECT_TRUE(ref.value()) << "Failed to fetch missing keys";

    auto const expects_missing = expected_length == ukv_val_len_missing_k;

    // Validate that values match
    std::pair<taped_values_view_t, managed_arena_t> retrieved_and_arena = *ref.value();
    taped_values_view_t retrieved = retrieved_and_arena.first;
    ukv_size_t count = location_get_count(ref.locations());
    EXPECT_EQ(retrieved.size(), count);

    // Check views
    tape_iterator_t it = retrieved.begin();
    for (std::size_t i = 0; i != count; ++i, ++it) {
        EXPECT_EQ((*it).size(), expects_missing ? 0 : expected_length);
    }

    // Check boolean indicators
    auto maybe_indicators_and_arena = ref.present();
    EXPECT_TRUE(maybe_indicators_and_arena);
    for (std::size_t i = 0; i != count; ++i, ++it) {
        EXPECT_EQ(maybe_indicators_and_arena->first[i], !expects_missing);
    }

    // Check length estimates
    auto maybe_lengths_and_arena = ref.length();
    EXPECT_TRUE(maybe_lengths_and_arena);
    for (std::size_t i = 0; i != count; ++i, ++it) {
        EXPECT_EQ(maybe_lengths_and_arena->first[i], expected_length);
    }
}

template <typename locations_at>
void check_equalities(member_refs_gt<locations_at>& ref, values_arg_t values) {

    EXPECT_TRUE(ref.value()) << "Failed to fetch present keys";

    // Validate that values match
    std::pair<taped_values_view_t, managed_arena_t> retrieved_and_arena = *ref.value();
    taped_values_view_t retrieved = retrieved_and_arena.first;
    EXPECT_EQ(retrieved.size(), location_get_count(ref.locations()));

    tape_iterator_t it = retrieved.begin();
    for (std::size_t i = 0; i != location_get_count(ref.locations()); ++i, ++it) {
        auto expected_len = static_cast<std::size_t>(values.lengths_begin[i]);
        auto expected_begin = reinterpret_cast<byte_t const*>(values.contents_begin[i]) + values.offsets_begin[i];

        value_view_t val_view = *it;
        value_view_t expected_view(expected_begin, expected_begin + expected_len);
        EXPECT_EQ(val_view.size(), expected_len);
        EXPECT_EQ(val_view, expected_view);
    }
}

template <typename locations_at>
void round_trip(member_refs_gt<locations_at>& ref, values_arg_t values) {
    EXPECT_TRUE(ref.assign(values)) << "Failed to assign";
    check_equalities(ref, values);
}

TEST(db, basic) {

    db_t db;
    EXPECT_TRUE(db.open(""));

    // Try getting the main collection
    EXPECT_TRUE(db.collection());
    collection_t col = *db.collection();

    std::vector<ukv_key_t> keys {34, 35, 36};
    ukv_val_len_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {34, 35, 36};
    std::vector<ukv_val_len_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_val_ptr_t>(vals.data());

    auto ref = col[keys];
    values_arg_t values {
        .contents_begin = {&vals_begin, 0},
        .offsets_begin = {offs.data(), sizeof(ukv_val_len_t)},
        .lengths_begin = {&val_len, 0},
    };
    round_trip(ref, values);

    // Overwrite those values with same size integers and try again
    for (auto& val : vals)
        val += 100;
    round_trip(ref, values);

    // Overwrite with empty values, but check for existence
    EXPECT_TRUE(ref.clear());
    check_length(ref, 0);

    // Check scans
    keys_range_t present_keys = col.keys();
    keys_stream_t present_it = present_keys.begin();
    auto expected_it = keys.begin();
    for (; expected_it != keys.end(); ++present_it, ++expected_it) {
        EXPECT_EQ(*expected_it, *present_it);
    }
    EXPECT_TRUE(present_it.is_end());

    // Remove all of the values and check that they are missing
    EXPECT_TRUE(ref.erase());
    check_length(ref, ukv_val_len_missing_k);
}

TEST(db, named) {
    db_t db;
    EXPECT_TRUE(db.open(""));

    collection_t col1 = *(db["col1"]);
    collection_t col2 = *(db["col2"]);

    ukv_val_len_t val_len = sizeof(std::uint64_t);
    std::vector<ukv_key_t> keys {44, 45, 46};
    std::vector<std::uint64_t> vals {44, 45, 46};
    std::vector<ukv_val_len_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_val_ptr_t>(vals.data());

    values_arg_t values {
        .contents_begin = {&vals_begin, 0},
        .offsets_begin = {offs.data(), sizeof(ukv_val_len_t)},
        .lengths_begin = {&val_len, 0},
    };

    auto ref1 = col1[keys];
    auto ref2 = col2[keys];
    EXPECT_TRUE(*db.contains("col1"));
    EXPECT_TRUE(*db.contains("col2"));
    EXPECT_FALSE(*db.contains("unknown_col"));
    round_trip(ref1, values);
    round_trip(ref2, values);

    // Check scans
    keys_range_t present_keys1 = col1.keys();
    keys_range_t present_keys2 = col2.keys();
    keys_stream_t present_it1 = present_keys1.begin();
    keys_stream_t present_it2 = present_keys2.begin();
    auto expected_it1 = keys.begin();
    auto expected_it2 = keys.begin();
    for (; expected_it1 != keys.end() && expected_it2 != keys.end();
         ++present_it1, ++expected_it1, ++present_it2, ++expected_it2) {
        EXPECT_EQ(*expected_it1, *present_it1);
        EXPECT_EQ(*expected_it2, *present_it2);
    }
    EXPECT_TRUE(present_it1.is_end());
    EXPECT_TRUE(present_it2.is_end());

    _ = db.remove("col1");
    _ = db.remove("col2");
    EXPECT_FALSE(*db.contains("col1"));
    EXPECT_FALSE(*db.contains("col2"));
}

TEST(db, txn) {
#if 0
    db_t db;
    EXPECT_TRUE(db.open(""));
    EXPECT_TRUE(db.transact());
    txn_t txn = *db.transact();

    std::vector<ukv_key_t> keys {54, 55, 56};
    ukv_val_len_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {54, 55, 56};
    std::vector<ukv_val_len_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_val_ptr_t>(vals.data());

    values_arg_t values {
        .contents_begin = {&vals_begin, 0},
        .offsets_begin = {offs.data(), sizeof(ukv_val_len_t)},
        .lengths_begin = {&val_len, 0},
    };

    round_trip(txn[keys], values);

    EXPECT_TRUE(db.collection());
    collection_t col = *db.collection();
    ref = col[keys];

    // Check for missing values before commit
    check_length();

    txn.commit();
    txn.reset();

    // Validate that values match after commit
    check_equalities(ref, values);

    // Transaction with named collection
    EXPECT_TRUE(db.collection("named_col"));
    collection_t named_col = *db.collection("named_col");
    std::vector<sub_key_t> sub_keys {{named_col, 54}, {named_col, 55}, {named_col, 56}};
    ref = txn[sub_keys];
    round_trip(ref, values);

    // Check for missing values before commit
    ref = named_col[keys];
    check_length(ref);

    txn.commit();
    txn.reset();

    // Validate that values match after commit
    check_equalities(ref, values);
#endif
}

TEST(db, nested_docs) {
    db_t db;
    _ = db.open();
    collection_t col = *db.collection();
}

TEST(db, net) {

    db_t db;
    EXPECT_TRUE(db.open(""));

    collection_t col(db);
    graph_ref_t net(col);

    // triangle
    std::vector<edge_t> edge1 {{1, 2, 9}};
    std::vector<edge_t> edge2 {{2, 3, 10}};
    std::vector<edge_t> edge3 {{3, 1, 11}};

    EXPECT_TRUE(net.upsert(edge1));
    EXPECT_TRUE(net.upsert(edge2));
    EXPECT_TRUE(net.upsert(edge3));

    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_FALSE(*net.contains(9));
    EXPECT_FALSE(*net.contains(10));
    EXPECT_FALSE(*net.contains(1000));

    EXPECT_EQ(*net.degree(1), 2u);
    EXPECT_EQ(*net.degree(2), 2u);
    EXPECT_EQ(*net.degree(3), 2u);
    EXPECT_EQ(*net.degree(1, ukv_vertex_source_k), 1u);
    EXPECT_EQ(*net.degree(2, ukv_vertex_source_k), 1u);
    EXPECT_EQ(*net.degree(3, ukv_vertex_source_k), 1u);

    EXPECT_TRUE(net.edges(1));
    EXPECT_EQ(net.edges(1)->size(), 2ul);
    EXPECT_EQ(net.edges(1, ukv_vertex_source_k)->size(), 1ul);
    EXPECT_EQ(net.edges(1, ukv_vertex_target_k)->size(), 1ul);

    EXPECT_EQ(net.edges(3, ukv_vertex_target_k)->size(), 1ul);
    EXPECT_EQ(net.edges(2, ukv_vertex_source_k)->size(), 1ul);
    EXPECT_EQ((*net.edges(3, ukv_vertex_target_k))[0].source_id, 2);
    EXPECT_EQ((*net.edges(3, ukv_vertex_target_k))[0].target_id, 3);
    EXPECT_EQ((*net.edges(3, ukv_vertex_target_k))[0].id, 10);
    EXPECT_EQ(net.edges(3, 1)->size(), 1ul);
    EXPECT_EQ(net.edges(1, 3)->size(), 0ul);

    // Check scans
    EXPECT_TRUE(net.edges());
    {
        std::unordered_set<edge_t, edge_hash_t> expected_edges {edge1[0], edge2[0], edge3[0]};
        std::unordered_set<edge_t, edge_hash_t> exported_edges;

        auto present_edges = *net.edges();
        auto present_it = std::move(present_edges).begin();
        auto count_results = 0;
        while (!present_it.is_end()) {
            exported_edges.insert(*present_it);
            ++present_it;
            ++count_results;
        }
        EXPECT_EQ(count_results, 6);
        EXPECT_EQ(exported_edges, expected_edges);
    }

    // Remove a single edge, making sure that the nodes info persists
    EXPECT_TRUE(net.remove({
        {edge1.front().source_id},
        {edge1.front().target_id},
        {edge1.front().id},
    }));
    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_EQ(net.edges(1, 2)->size(), 0ul);

    // Bring that edge back
    EXPECT_TRUE(net.upsert({
        {edge1.front().source_id},
        {edge1.front().target_id},
        {edge1.front().id},
    }));
    EXPECT_EQ(net.edges(1, 2)->size(), 1ul);

    // Remove a vertex
    ukv_key_t vertex_to_remove = 2;
    EXPECT_TRUE(net.remove({vertex_to_remove}));
    EXPECT_FALSE(*net.contains(vertex_to_remove));
    EXPECT_EQ(net.edges(vertex_to_remove)->size(), 0ul);
    EXPECT_EQ(net.edges(1, vertex_to_remove)->size(), 0ul);
    EXPECT_EQ(net.edges(vertex_to_remove, 1)->size(), 0ul);

    // Bring back the whole graph
    EXPECT_TRUE(net.upsert(edge1));
    EXPECT_TRUE(net.upsert(edge2));
    EXPECT_TRUE(net.upsert(edge3));
    EXPECT_TRUE(*net.contains(vertex_to_remove));
    EXPECT_EQ(net.edges(vertex_to_remove)->size(), 2ul);
    EXPECT_EQ(net.edges(1, vertex_to_remove)->size(), 1ul);
    EXPECT_EQ(net.edges(vertex_to_remove, 1)->size(), 0ul);
}

TEST(db, net_batch) {

    db_t db;
    EXPECT_TRUE(db.open(""));

    collection_t col(db);
    graph_ref_t net(col);

    std::vector<edge_t> triangle {
        {1, 2, 9},
        {2, 3, 10},
        {3, 1, 11},
    };

    EXPECT_TRUE(net.upsert(triangle));
    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_FALSE(*net.contains(9));
    EXPECT_FALSE(*net.contains(10));
    EXPECT_FALSE(*net.contains(1000));

    EXPECT_EQ(*net.degree(1), 2u);
    EXPECT_EQ(*net.degree(2), 2u);
    EXPECT_EQ(*net.degree(3), 2u);
    EXPECT_EQ(*net.degree(1, ukv_vertex_source_k), 1u);
    EXPECT_EQ(*net.degree(2, ukv_vertex_source_k), 1u);
    EXPECT_EQ(*net.degree(3, ukv_vertex_source_k), 1u);

    EXPECT_TRUE(net.edges(1));
    EXPECT_EQ(net.edges(1)->size(), 2ul);
    EXPECT_EQ(net.edges(1, ukv_vertex_source_k)->size(), 1ul);
    EXPECT_EQ(net.edges(1, ukv_vertex_target_k)->size(), 1ul);

    EXPECT_EQ(net.edges(3, ukv_vertex_target_k)->size(), 1ul);
    EXPECT_EQ(net.edges(2, ukv_vertex_source_k)->size(), 1ul);
    EXPECT_EQ((*net.edges(3, ukv_vertex_target_k))[0].source_id, 2);
    EXPECT_EQ((*net.edges(3, ukv_vertex_target_k))[0].target_id, 3);
    EXPECT_EQ((*net.edges(3, ukv_vertex_target_k))[0].id, 10);
    EXPECT_EQ(net.edges(3, 1)->size(), 1ul);
    EXPECT_EQ(net.edges(1, 3)->size(), 0ul);

    // Check scans
    EXPECT_TRUE(net.edges());
    {
        std::unordered_set<edge_t, edge_hash_t> expected_edges {triangle.begin(), triangle.end()};
        std::unordered_set<edge_t, edge_hash_t> exported_edges;

        auto present_edges = *net.edges();
        auto present_it = std::move(present_edges).begin();
        auto count_results = 0;
        while (!present_it.is_end()) {
            exported_edges.insert(*present_it);
            ++present_it;
            ++count_results;
        }
        EXPECT_EQ(count_results, triangle.size() * 2);
        EXPECT_EQ(exported_edges, expected_edges);
    }

    // Remove a single edge, making sure that the nodes info persists
    EXPECT_TRUE(net.remove(edges_view_t {
        {triangle[0].source_id},
        {triangle[0].target_id},
        {triangle[0].id},
    }));
    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_EQ(net.edges(1, 2)->size(), 0ul);

    // Bring that edge back
    EXPECT_TRUE(net.upsert(edges_view_t {
        {triangle[0].source_id},
        {triangle[0].target_id},
        {triangle[0].id},
    }));
    EXPECT_EQ(net.edges(1, 2)->size(), 1ul);

    // Remove a vertex
    ukv_key_t vertex_to_remove = 2;
    EXPECT_TRUE(net.remove({vertex_to_remove}));
    EXPECT_FALSE(*net.contains(vertex_to_remove));
    EXPECT_EQ(net.edges(vertex_to_remove)->size(), 0ul);
    EXPECT_EQ(net.edges(1, vertex_to_remove)->size(), 0ul);
    EXPECT_EQ(net.edges(vertex_to_remove, 1)->size(), 0ul);

    // Bring back the whole graph
    EXPECT_TRUE(net.upsert(triangle));
    EXPECT_TRUE(*net.contains(vertex_to_remove));
    EXPECT_EQ(net.edges(vertex_to_remove)->size(), 2ul);
    EXPECT_EQ(net.edges(1, vertex_to_remove)->size(), 1ul);
    EXPECT_EQ(net.edges(vertex_to_remove, 1)->size(), 0ul);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}