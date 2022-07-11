/**
 * @file test.cpp
 * @author Ashot Vardanian
 * @date 2022-07-06
 *
 * @brief A set of tests implemented using Google Test.
 */

#include <gtest/gtest.h>

#include "ukv.hpp"
#include "ukv_graph.hpp"
#include "ukv_docs.hpp"

using namespace unum::ukv;
using namespace unum;

TEST(db, basic) {

    db_t db;
    EXPECT_FALSE(db.open(""));

    session_t session = db.session();

    std::vector<ukv_key_t> keys {34, 35, 36};
    ukv_val_len_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {34, 35, 36};
    std::vector<ukv_val_len_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_val_ptr_t>(vals.data());

    session[keys] = disjoint_values_view_t {
        .values_range = {&vals_begin, 0, 3},
        .offsets_range = offs,
        .lengths_range = {val_len, 1},
    };

    expected_gt<taped_values_view_t> maybe_retrieved = session[keys];
    EXPECT_TRUE(maybe_retrieved);

    taped_values_view_t retrieved = *maybe_retrieved;
    EXPECT_EQ(retrieved.size(), keys.size());
    tape_iterator_t it = retrieved.begin();
    for (std::size_t i = 0; i != keys.size(); ++i, ++it) {
        value_view_t val_view = *it;
        EXPECT_EQ(val_view.size(), val_len);
        auto casted = reinterpret_cast<std::uint64_t const*>(val_view.begin());
        EXPECT_EQ(casted[0], vals[i]);
    }
}

TEST(db, net) {

    db_t db;
    EXPECT_FALSE(db.open(""));

    graph_collection_session_t net {collection_t(db)};

    std::vector<edge_t> triangle {
        {1, 2, 9},
        {2, 3, 10},
        {3, 1, 11},
    };

    EXPECT_FALSE(net.upsert({triangle}));

    EXPECT_TRUE(net.edges(1));

    EXPECT_EQ(net.edges(1)->size(), 2ul);
    EXPECT_EQ(net.edges(1, ukv_vertex_source_k)->size(), 1ul);
    EXPECT_EQ(net.edges(1, ukv_vertex_target_k)->size(), 1ul);

    EXPECT_EQ(net.edges(3, ukv_vertex_target_k)->size(), 1ul);
    EXPECT_EQ(net.edges(2, ukv_vertex_source_k)->size(), 1ul);
    EXPECT_EQ(net.edges(3, ukv_vertex_source_k)->size(), 0ul);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}