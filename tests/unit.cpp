/**
 * @file units.cpp
 * @author Ashot Vardanian
 * @date 2022-07-06
 *
 * @brief A set of unit tests implemented using Google Test.
 */

#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "ukv/ukv.hpp"

using namespace unum::ukv;
using namespace unum;

template <typename container_at>
char const* str_begin(container_at const& container) {
    if constexpr (std::is_same_v<char const*, container_at>)
        return container;
    else if constexpr (std::is_same_v<value_view_t, container_at>)
        return reinterpret_cast<char const*>(container.begin());
    else
        return reinterpret_cast<char const*>(std::data(container));
}

template <typename container_at>
char const* str_end(container_at const& container) {
    if constexpr (std::is_same_v<char const*, container_at>)
        return container + std::strlen(container);
    else if constexpr (std::is_same_v<value_view_t, container_at>)
        return reinterpret_cast<char const*>(container.end());
    else
        return str_begin(container) + std::size(container);
}

using json_t = nlohmann::json;

static json_t json_parse(char const* begin, char const* end) {
    json_t result;
    auto adapter = nlohmann::detail::input_adapter(begin, end);
    auto parser = nlohmann::detail::parser<json_t, decltype(adapter)>(std::move(adapter), nullptr, true, true);
    parser.parse(false, result);
    return result;
    // return json_t::parse(begin, end, nullptr, true, true);
}

#define M_EXPECT_EQ_JSON(str1, str2) \
    EXPECT_EQ(json_parse(str_begin(str1), str_end(str1)), json_parse(str_begin(str2), str_end(str2)));
#define M_EXPECT_EQ_MSG(str1, str2) \
    EXPECT_EQ(json_t::from_msgpack(str_begin(str1), str_end(str1)), json_parse(str_begin(str2), str_end(str2)));

#if defined(UKV_ENGINE_IS_LEVELDB)
constexpr char const* path_k = "tmp/leveldb";
#elif defined(UKV_ENGINE_IS_ROCKSDB)
constexpr char const* path_k = "tmp/rocksdb";
#elif defined(UKV_ENGINE_IS_UNUMDB)
constexpr char const* path_k = "tmp/unumdb";
#else
constexpr char const* path_k = "";
#endif

#pragma region Binary Collections

template <typename locations_at>
void check_length(bins_ref_gt<locations_at>& ref, ukv_length_t expected_length) {

    EXPECT_TRUE(ref.value()) << "Failed to fetch missing keys";

    auto const expects_missing = expected_length == ukv_length_missing_k;
    using extractor_t = places_arg_extractor_gt<locations_at>;
    ukv_size_t count = extractor_t {}.count(ref.locations());

    // Validate that values match
    auto maybe_retrieved = ref.value();
    auto const& retrieved = *maybe_retrieved;
    EXPECT_EQ(retrieved.size(), count);

    // Check views
    auto it = retrieved.begin();
    for (std::size_t i = 0; i != count; ++i, ++it) {
        EXPECT_EQ((*it).size(), expects_missing ? 0 : expected_length);
    }

    // Check length estimates
    auto maybe_lengths = ref.length();
    EXPECT_TRUE(maybe_lengths);
    for (std::size_t i = 0; i != count; ++i) {
        EXPECT_EQ(maybe_lengths->at(i), expected_length);
    }

    // Check boolean indicators
    auto maybe_indicators = ref.present();
    EXPECT_TRUE(maybe_indicators);
    for (std::size_t i = 0; i != count; ++i) {
        EXPECT_EQ(maybe_indicators->at(i), !expects_missing);
    }
}

template <typename locations_at>
void check_equalities(bins_ref_gt<locations_at>& ref, contents_arg_t values) {

    EXPECT_TRUE(ref.value()) << "Failed to fetch present keys";
    using extractor_t = places_arg_extractor_gt<locations_at>;

    // Validate that values match
    auto maybe_retrieved = ref.value();
    auto const& retrieved = *maybe_retrieved;
    EXPECT_EQ(retrieved.size(), extractor_t {}.count(ref.locations()));

    auto it = retrieved.begin();
    for (std::size_t i = 0; i != extractor_t {}.count(ref.locations()); ++i, ++it) {
        auto expected_len = static_cast<std::size_t>(values.lengths_begin[i]);
        auto expected_begin = reinterpret_cast<byte_t const*>(values.contents_begin[i]) + values.offsets_begin[i];

        value_view_t val_view = *it;
        value_view_t expected_view(expected_begin, expected_begin + expected_len);
        EXPECT_EQ(val_view.size(), expected_len);
        EXPECT_EQ(val_view, expected_view);
    }
}

template <typename locations_at>
void round_trip(bins_ref_gt<locations_at>& ref, contents_arg_t values) {
    EXPECT_TRUE(ref.assign(values)) << "Failed to assign";
    check_equalities(ref, values);
}

void check_binary_collection(bins_collection_t& collection) {
    std::vector<ukv_key_t> keys {34, 35, 36};
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {34, 35, 36};
    std::vector<ukv_length_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(vals.data());

    auto ref = collection[keys];
    contents_arg_t values {
        .offsets_begin = {offs.data(), sizeof(ukv_length_t)},
        .lengths_begin = {&val_len, 0},
        .contents_begin = {&vals_begin, 0},
        .count = 3,
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
    keys_range_t present_keys = collection.keys();
    keys_stream_t present_it = present_keys.begin();
    auto expected_it = keys.begin();
    for (; expected_it != keys.end(); ++present_it, ++expected_it) {
        EXPECT_EQ(*expected_it, *present_it);
    }
    EXPECT_TRUE(present_it.is_end());

    // Remove all of the values and check that they are missing
    EXPECT_TRUE(ref.erase());
    check_length(ref, ukv_length_missing_k);
}

TEST(db, basic) {

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    // Try getting the main collection
    EXPECT_TRUE(db.collection());
    bins_collection_t collection = *db.collection();
    check_binary_collection(collection);
    EXPECT_TRUE(db.clear());
}

TEST(db, named) {

    if (!ukv_supports_named_collections_k)
        return;

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    EXPECT_TRUE(db["col1"]);
    EXPECT_TRUE(db["col2"]);

    bins_collection_t col1 = *db.add_collection("col1");
    bins_collection_t col2 = *db.add_collection("col2");

    check_binary_collection(col1);
    check_binary_collection(col2);

    EXPECT_TRUE(db.drop("col1"));
    EXPECT_TRUE(db.drop("col2"));
    EXPECT_TRUE(*db.contains(""));
    EXPECT_FALSE(*db.contains("col1"));
    EXPECT_FALSE(*db.contains("col2"));
    EXPECT_TRUE(db.clear());
    EXPECT_TRUE(*db.contains(""));
}

TEST(db, collection_list) {

    database_t db;

    EXPECT_TRUE(db.open(path_k));

    if (!ukv_supports_named_collections_k) {
        EXPECT_FALSE(*db["name"]);
        EXPECT_FALSE(*db.contains("name"));
        EXPECT_FALSE(db.drop("name"));
        EXPECT_FALSE(db.drop(""));
        EXPECT_TRUE(db.collection()->clear());
        return;
    }
    else {
        bins_collection_t col1 = *db.add_collection("col1");
        bins_collection_t col2 = *db.add_collection("col2");
        bins_collection_t col3 = *db.add_collection("col3");
        bins_collection_t col4 = *db.add_collection("col4");

        EXPECT_TRUE(*db.contains("col1"));
        EXPECT_TRUE(*db.contains("col2"));
        EXPECT_FALSE(*db.contains("unknown_col"));

        auto maybe_txn = db.transact();
        EXPECT_TRUE(maybe_txn);
        auto maybe_cols = maybe_txn->collections();
        EXPECT_TRUE(maybe_cols);

        size_t count = 0;
        std::vector<std::string> collections;
        auto cols = *maybe_cols;
        while (!cols.names.is_end()) {
            collections.push_back(std::string(*cols.names));
            ++cols.names;
            ++count;
        }
        EXPECT_EQ(count, 4);
        std::sort(collections.begin(), collections.end());
        EXPECT_EQ(collections[0], "col1");
        EXPECT_EQ(collections[1], "col2");
        EXPECT_EQ(collections[2], "col3");
        EXPECT_EQ(collections[3], "col4");

        EXPECT_TRUE(db.drop("col1"));
        EXPECT_FALSE(*db.contains("col1"));
        EXPECT_FALSE(db.drop(""));
        EXPECT_TRUE(db.collection()->clear());
    }
    EXPECT_TRUE(db.clear());
}

TEST(db, paths) {

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    char const* keys[] {"Facebook", "Apple", "Amazon", "Netflix", "Google"};
    char const* vals[] {"F", "A", "A", "N", "G"};

    arena_t arena(db);
    status_t status;
    ukv_paths_write( //
        db,
        nullptr,
        5,
        nullptr,
        0,
        nullptr,
        0,
        nullptr,
        0,
        keys,
        sizeof(char const*),
        nullptr,
        nullptr,
        0,
        nullptr,
        0,
        reinterpret_cast<ukv_bytes_cptr_t*>(vals),
        sizeof(char const*),
        ukv_options_default_k,
        arena.member_ptr(),
        status.member_ptr());

    ukv_key_t* key_hashes = nullptr;
    char* vals_recovered = nullptr;
    ukv_paths_read( //
        db,
        nullptr,
        5,
        nullptr,
        0,
        nullptr,
        0,
        nullptr,
        0,
        keys,
        sizeof(char const*),
        ukv_options_default_k,
        nullptr,
        &key_hashes,
        nullptr,
        nullptr,
        reinterpret_cast<ukv_bytes_ptr_t*>(&vals_recovered),
        arena.member_ptr(),
        status.member_ptr());

    EXPECT_TRUE(status);
    EXPECT_EQ(std::string_view(vals_recovered, 5), "FAANG");
    EXPECT_TRUE(db.clear());
}

TEST(db, unnamed_and_named) {

    if (!ukv_supports_named_collections_k)
        return;

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    std::vector<ukv_key_t> keys {54, 55, 56};
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {1, 2, 3};
    std::vector<ukv_length_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(vals.data());

    contents_arg_t values {
        .offsets_begin = {offs.data(), sizeof(ukv_length_t)},
        .lengths_begin = {&val_len, 0},
        .contents_begin = {&vals_begin, 0},
        .count = 3,
    };

    for (auto&& i : {"one", "", "three"}) {
        for (auto& j : vals)
            j += 7;

        bins_collection_t collection = *db.add_collection(i);
        auto collection_ref = collection[keys];
        check_length(collection_ref, ukv_length_missing_k);
        round_trip(collection_ref, values);
        check_length(collection_ref, 8);
    }
    EXPECT_TRUE(db.clear());
}

TEST(db, txn) {

    if (!ukv_supports_transactions_k)
        return;

    database_t db;
    EXPECT_TRUE(db.open(path_k));
    EXPECT_TRUE(db.transact());
    transaction_t txn = *db.transact();

    std::vector<ukv_key_t> keys {54, 55, 56};
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {54, 55, 56};
    std::vector<ukv_length_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(vals.data());

    contents_arg_t values {
        .offsets_begin = {offs.data(), sizeof(ukv_length_t)},
        .lengths_begin = {&val_len, 0},
        .contents_begin = {&vals_begin, 0},
        .count = 3,
    };

    auto txn_ref = txn[keys];
    round_trip(txn_ref, values);

    EXPECT_TRUE(db.collection());
    bins_collection_t collection = *db.collection();
    auto collection_ref = collection[keys];

    // Check for missing values before commit
    check_length(collection_ref, ukv_length_missing_k);

    auto status = txn.commit();
    status.throw_unhandled();
    status = txn.reset();
    status.throw_unhandled();

    // Validate that values match after commit
    check_equalities(collection_ref, values);
    EXPECT_TRUE(db.clear());
}

TEST(db, txn_named) {

    if (!ukv_supports_transactions_k)
        return;
    if (!ukv_supports_named_collections_k)
        return;

    database_t db;
    EXPECT_TRUE(db.open(path_k));
    EXPECT_TRUE(db.transact());
    transaction_t txn = *db.transact();

    std::vector<ukv_key_t> keys {54, 55, 56};
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {54, 55, 56};
    std::vector<ukv_length_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(vals.data());

    contents_arg_t values {
        .offsets_begin = {offs.data(), sizeof(ukv_length_t)},
        .lengths_begin = {&val_len, 0},
        .contents_begin = {&vals_begin, 0},
        .count = 3,
    };

    // Transaction with named collection
    EXPECT_TRUE(db.collection("named_col"));
    bins_collection_t named_collection = *db.collection("named_col");
    std::vector<collection_key_t> sub_keys {{named_collection, 54}, {named_collection, 55}, {named_collection, 56}};
    auto txn_named_collection_ref = txn[sub_keys];
    round_trip(txn_named_collection_ref, values);

    // Check for missing values before commit
    auto named_collection_ref = named_collection[keys];
    check_length(named_collection_ref, ukv_length_missing_k);

    auto status = txn.commit();
    status.throw_unhandled();
    status = txn.reset();
    status.throw_unhandled();

    // Validate that values match after commit
    check_equalities(named_collection_ref, values);
    EXPECT_TRUE(db.clear());
}

TEST(db, txn_unnamed_then_named) {

    if (!ukv_supports_transactions_k)
        return;
    if (!ukv_supports_named_collections_k)
        return;

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    EXPECT_TRUE(db.transact());
    transaction_t txn = *db.transact();

    std::vector<ukv_key_t> keys {54, 55, 56};
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<std::uint64_t> vals {54, 55, 56};
    std::vector<ukv_length_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(vals.data());

    contents_arg_t values {
        .offsets_begin = {offs.data(), sizeof(ukv_length_t)},
        .lengths_begin = {&val_len, 0},
        .contents_begin = {&vals_begin, 0},
        .count = 3,
    };

    auto txn_ref = txn[keys];
    round_trip(txn_ref, values);

    EXPECT_TRUE(db.collection());
    bins_collection_t collection = *db.collection();
    auto collection_ref = collection[keys];

    // Check for missing values before commit
    check_length(collection_ref, ukv_length_missing_k);

    auto status = txn.commit();
    status.throw_unhandled();
    status = txn.reset();
    status.throw_unhandled();

    // Validate that values match after commit
    check_equalities(collection_ref, values);

    // Transaction with named collection
    EXPECT_TRUE(db.add_collection("named_col"));
    bins_collection_t named_collection = *db.add_collection("named_col");
    std::vector<collection_key_t> sub_keys {{named_collection, 54}, {named_collection, 55}, {named_collection, 56}};
    auto txn_named_collection_ref = txn[sub_keys];
    round_trip(txn_named_collection_ref, values);

    // Check for missing values before commit
    auto named_collection_ref = named_collection[keys];
    check_length(named_collection_ref, ukv_length_missing_k);

    status = txn.commit();
    status.throw_unhandled();
    status = txn.reset();
    status.throw_unhandled();

    // Validate that values match after commit
    check_equalities(named_collection_ref, values);
    EXPECT_TRUE(db.clear());
}

#pragma region Document Collections

TEST(db, docs) {

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    // JSON
    docs_collection_t collection = *db.collection<docs_collection_t>();
    auto json = R"( {"person": "Carl", "age": 24} )"_json.dump();
    collection[1] = json.c_str();
    M_EXPECT_EQ_JSON(*collection[1].value(), json);
    M_EXPECT_EQ_JSON(*collection[ckf(1, "person")].value(), "\"Carl\"");
    M_EXPECT_EQ_JSON(*collection[ckf(1, "age")].value(), "24");

    // Binary
    auto maybe_person = collection[ckf(1, "person")].value(ukv_doc_field_str_k);
    EXPECT_EQ(std::string_view(maybe_person->c_str(), maybe_person->size()), std::string_view("Carl"));

#if 0
    // JSON-Patch Merging
    auto json_to_merge = R"( {"person": "Bob", "age": 28} )"_json.dump();
    auto expected_json = R"( {"person": "Bob", "hello": ["world"], "age": 28} )"_json.dump();
    collection[1].merge(json_to_merge.c_str());
    auto merge_result = collection[1].value();
    M_EXPECT_EQ_JSON(merge_result->c_str(), expected_json.c_str());
    M_EXPECT_EQ_JSON(collection[ckf(1, "person")].value()->c_str(), "\"Bob\"");
    M_EXPECT_EQ_JSON(collection[ckf(1, "/hello/0")].value()->c_str(), "\"world\"");
    M_EXPECT_EQ_JSON(collection[ckf(1, "age")].value()->c_str(), "28");

    // JSON-Patching
    auto json_patch =
        R"( [
            { "op": "replace", "path_k": "/person", "value": "Alice" },
            { "op": "add", "path_k": "/hello", "value": ["world"] },
            { "op": "remove", "path_k": "/age" }
            ] )"_json.dump();
    expected_json = R"( {"person": "Alice", "hello": ["world"]} )"_json.dump();
    collection[1].patch(json_patch.c_str());
    auto patch_result = collection[1].value();
    M_EXPECT_EQ_JSON(patch_result->c_str(), expected_json.c_str());
    M_EXPECT_EQ_JSON(collection[ckf(1, "person")].value()->c_str(), "\"Alice\"");
    M_EXPECT_EQ_JSON(collection[ckf(1, "/hello/0")].value()->c_str(), "\"world\"");

    // MsgPack
    collection.as(ukv_format_msgpack_k);
    value_view_t val = *collection[1].value();
    M_EXPECT_EQ_MSG(val, json.c_str());
    val = *collection[ckf(1, "person")].value();
    M_EXPECT_EQ_MSG(val, "\"Carl\"");
    val = *collection[ckf(1, "age")].value();
    M_EXPECT_EQ_MSG(val, "24");
#endif

    EXPECT_TRUE(db.clear());
}
#if 0
TEST(db, docs_merge_and_patch) {
    using json_t = nlohmann::json;
    database_t db;
    EXPECT_TRUE(db.open(path_k));
    docs_collection_t collection = *db.collection<docs_collection_t>();

    std::ifstream f_patch("tests/patch.json");
    json_t j_object = json_t::parse(f_patch);
    for (auto it : j_object) {
        auto doc = it["doc"].dump();
        auto patch = it["patch"].dump();
        auto expected = it["expected"].dump();
        collection[1] = doc.c_str();
        collection[1] = patch.c_str();
        M_EXPECT_EQ_JSON(collection[1].value()->c_str(), expected.c_str());
    }

    std::ifstream f_merge("tests/merge.json");
    j_object = json_t::parse(f_merge);
    for (auto it : j_object) {
        auto doc = it["doc"].dump();
        auto merge = it["merge"].dump();
        auto expected = it["expected"].dump();
        collection[1] = doc.c_str();
        collection[1] = merge.c_str();
        M_EXPECT_EQ_JSON(collection[1].value()->c_str(), expected.c_str());
    }

    EXPECT_TRUE(db.clear());
}

TEST(db, doc_fields_update) {
    using json_t = nlohmann::json;
    database_t db;
    EXPECT_TRUE(db.open(path_k));

    docs_collection_t collection = *db.collection<docs_collection_t>();
    auto json1 = R"( {"person": "Carl", "age": 24} )"_json.dump();
    collection[1] = json1.c_str();
    M_EXPECT_EQ_JSON(collection[1].value()->c_str(), json1.c_str());
    M_EXPECT_EQ_JSON(collection[ckf(1, "person")].value()->c_str(), "\"Carl\"");
    M_EXPECT_EQ_JSON(collection[ckf(1, "age")].value()->c_str(), "24");

    collection[ckf(1, "person")] = "\"Charls\"";
    collection[ckf(1, "age")] = "25";
    M_EXPECT_EQ_JSON(collection[ckf(1, "person")].value()->c_str(), "\"Charls\"");
    M_EXPECT_EQ_JSON(collection[ckf(1, "age")].value()->c_str(), "25");

    EXPECT_TRUE(db.clear());
}
#endif
TEST(db, docs_table) {

    using json_t = nlohmann::json;
    database_t db;
    EXPECT_TRUE(db.open(path_k));

    // Inject basic data
    docs_collection_t collection = *db.collection<docs_collection_t>();
    auto json_alice = R"( { "person": "Alice", "age": 27, "height": 1 } )"_json.dump();
    auto json_bob = R"( { "person": "Bob", "age": "27", "weight": 2 } )"_json.dump();
    auto json_carl = R"( { "person": "Carl", "age": 24 } )"_json.dump();
    collection[1] = json_alice.c_str();
    collection[2] = json_bob.c_str();
    collection[3] = json_carl.c_str();
    M_EXPECT_EQ_JSON(*collection[1].value(), json_alice.c_str());
    M_EXPECT_EQ_JSON(*collection[2].value(), json_bob.c_str());

    // Just column names
    {
        auto maybe_fields = collection[1].gist();
        auto fields = *maybe_fields;

        std::vector<std::string> parsed;
        for (auto field : fields)
            parsed.emplace_back(field.data());

        EXPECT_NE(std::find(parsed.begin(), parsed.end(), "/person"), parsed.end());
        EXPECT_NE(std::find(parsed.begin(), parsed.end(), "/height"), parsed.end());
        EXPECT_NE(std::find(parsed.begin(), parsed.end(), "/age"), parsed.end());
        EXPECT_EQ(std::find(parsed.begin(), parsed.end(), "/weight"), parsed.end());
    }

    // Single cell
    {
        auto header = table_header().with<std::uint32_t>("age");
        auto maybe_table = collection[1].gather(header);
        auto table = *maybe_table;
        auto col0 = table.column<0>();

        EXPECT_EQ(col0[0].value, 27);
        EXPECT_FALSE(col0[0].converted);
    }

    // Single row
    {
        auto header = table_header() //
                          .with<std::uint32_t>("age")
                          .with<std::int32_t>("age")
                          .with<std::string_view>("age");

        auto maybe_table = collection[1].gather(header);
        auto table = *maybe_table;
        auto col0 = table.column<0>();
        auto col1 = table.column<1>();
        auto col2 = table.column<2>();

        EXPECT_EQ(col0[0].value, 27);
        EXPECT_FALSE(col0[0].converted);
        EXPECT_EQ(col1[0].value, 27);
        EXPECT_TRUE(col1[0].converted);
        EXPECT_STREQ(col2[0].value.data(), "27");
        EXPECT_TRUE(col2[0].converted);
    }

    // Single column
    {
        auto header = table_header().with<std::int32_t>("age");
        auto maybe_table = collection[{1, 2, 3, 123456}].gather(header);
        auto table = *maybe_table;
        auto col0 = table.column<0>();

        EXPECT_EQ(col0[0].value, 27);
        EXPECT_EQ(col0[1].value, 27);
        EXPECT_TRUE(col0[1].converted);
        EXPECT_EQ(col0[2].value, 24);
    }

    // Single strings column
    {
        auto header = table_header().with<std::string_view>("age");
        auto maybe_table = collection[{1, 2, 3, 123456}].gather(header);
        auto table = *maybe_table;
        auto col0 = table.column<0>();

        EXPECT_STREQ(col0[0].value.data(), "27");
        EXPECT_TRUE(col0[0].converted);
        EXPECT_STREQ(col0[1].value.data(), "27");
        EXPECT_STREQ(col0[2].value.data(), "24");
    }

    // Multi-column
    {
        auto header = table_header() //
                          .with<std::int32_t>("age")
                          .with<std::string_view>("age")
                          .with<std::string_view>("person")
                          .with<float>("person")
                          .with<std::int32_t>("height")
                          .with<std::uint64_t>("weight");

        auto maybe_table = collection[{1, 2, 3, 123456, 654321}].gather(header);
        auto table = *maybe_table;
        auto col0 = table.column<0>();
        auto col1 = table.column<1>();
        auto col2 = table.column<2>();
        auto col3 = table.column<3>();
        auto col4 = table.column<4>();
        auto col5 = table.column<5>();

        EXPECT_EQ(col0[0].value, 27);
        EXPECT_EQ(col0[1].value, 27);
        EXPECT_TRUE(col0[1].converted);
        EXPECT_EQ(col0[2].value, 24);

        EXPECT_STREQ(col1[0].value.data(), "27");
        EXPECT_TRUE(col1[0].converted);
        EXPECT_STREQ(col1[1].value.data(), "27");
        EXPECT_STREQ(col1[2].value.data(), "24");
    }

    // Multi-column Type-punned exports
    {
        table_header_t header {{
            field_type_t {"age", ukv_doc_field_i32_k},
            field_type_t {"age", ukv_doc_field_str_k},
            field_type_t {"person", ukv_doc_field_str_k},
            field_type_t {"person", ukv_doc_field_f32_k},
            field_type_t {"height", ukv_doc_field_i32_k},
            field_type_t {"weight", ukv_doc_field_u64_k},
        }};

        auto maybe_table = collection[{1, 2, 3, 123456, 654321}].gather(header);
        auto table = *maybe_table;
        auto col0 = table.column(0).as<std::int32_t>();
        auto col1 = table.column(1).as<value_view_t>();
        auto col2 = table.column(2).as<value_view_t>();
        auto col3 = table.column(3).as<float>();
        auto col4 = table.column(4).as<std::int32_t>();
        auto col5 = table.column(5).as<std::uint64_t>();

        EXPECT_EQ(col0[0].value, 27);
        EXPECT_EQ(col0[1].value, 27);
        EXPECT_TRUE(col0[1].converted);
        EXPECT_EQ(col0[2].value, 24);

        EXPECT_STREQ(col1[0].value.c_str(), "27");
        EXPECT_TRUE(col1[0].converted);
        EXPECT_STREQ(col1[1].value.c_str(), "27");
        EXPECT_STREQ(col1[2].value.c_str(), "24");
    }

    EXPECT_TRUE(db.clear());
}

#pragma region Graph Collections

TEST(db, graph_triangle) {

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    graph_collection_t net = *db.collection<graph_collection_t>();

    // triangle
    edge_t edge1 {1, 2, 9};
    edge_t edge2 {2, 3, 10};
    edge_t edge3 {3, 1, 11};

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
        std::unordered_set<edge_t, edge_hash_t> expected_edges {edge1, edge2, edge3};
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
        {{&edge1.source_id}, 1},
        {{&edge1.target_id}, 1},
        {{&edge1.id}, 1},
    }));
    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_EQ(net.edges(1, 2)->size(), 0ul);

    // Bring that edge back
    EXPECT_TRUE(net.upsert({
        {{&edge1.source_id}, 1},
        {{&edge1.target_id}, 1},
        {{&edge1.id}, 1},
    }));
    EXPECT_EQ(net.edges(1, 2)->size(), 1ul);

    // Remove a vertex
    ukv_key_t vertex_to_remove = 2;
    EXPECT_TRUE(net.remove(vertex_to_remove));
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
    EXPECT_TRUE(db.clear());
}

TEST(db, graph_triangle_batch_api) {

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    bins_collection_t main = *db.collection();
    graph_collection_t net = *db.collection<graph_collection_t>();

    std::vector<edge_t> triangle {
        {1, 2, 9},
        {2, 3, 10},
        {3, 1, 11},
    };

    EXPECT_TRUE(net.upsert(edges(triangle)));
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
        size_t count_results = 0ul;
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
        {{&triangle[0].source_id}, 1},
        {{&triangle[0].target_id}, 1},
        {{&triangle[0].id}, 1},
    }));
    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_EQ(net.edges(1, 2)->size(), 0ul);

    // Bring that edge back
    EXPECT_TRUE(net.upsert(edges_view_t {
        {{&triangle[0].source_id}, 1},
        {{&triangle[0].target_id}, 1},
        {{&triangle[0].id}, 1},
    }));
    EXPECT_EQ(net.edges(1, 2)->size(), 1ul);

    // Remove a vertex
    ukv_key_t vertex_to_remove = 2;
    EXPECT_TRUE(net.remove(vertex_to_remove));
    EXPECT_FALSE(*net.contains(vertex_to_remove));
    EXPECT_EQ(net.edges(vertex_to_remove)->size(), 0ul);
    EXPECT_EQ(net.edges(1, vertex_to_remove)->size(), 0ul);
    EXPECT_EQ(net.edges(vertex_to_remove, 1)->size(), 0ul);

    // Bring back the whole graph
    EXPECT_TRUE(net.upsert(edges(triangle)));
    EXPECT_TRUE(*net.contains(vertex_to_remove));
    EXPECT_EQ(net.edges(vertex_to_remove)->size(), 2ul);
    EXPECT_EQ(net.edges(1, vertex_to_remove)->size(), 1ul);
    EXPECT_EQ(net.edges(vertex_to_remove, 1)->size(), 0ul);
    EXPECT_TRUE(db.clear());
}

edge_t make_edge(ukv_key_t edge_id, ukv_key_t v1, ukv_key_t v2) {
    return {v1, v2, edge_id};
}

std::vector<edge_t> make_edges(std::size_t vertices_count = 2, std::size_t next_connect = 1) {
    std::vector<edge_t> es;
    ukv_key_t edge_id = 0;
    for (ukv_key_t vertex_id = 0; vertex_id != vertices_count; ++vertex_id) {
        ukv_key_t connect_with = vertex_id + next_connect;
        while (connect_with < vertices_count) {
            edge_id++;
            es.push_back(make_edge(edge_id, vertex_id, connect_with));
            connect_with = connect_with + next_connect;
        }
    }
    return es;
}

TEST(db, graph_random_fill) {
    database_t db;
    EXPECT_TRUE(db.open(path_k));

    bins_collection_t main = *db.collection();
    graph_collection_t graph = *db.collection<graph_collection_t>();

    constexpr std::size_t vertices_count = 1000;
    auto edges_vec = make_edges(vertices_count, 100);
    EXPECT_TRUE(graph.upsert(edges(edges_vec)));

    for (ukv_key_t vertex_id = 0; vertex_id != vertices_count; ++vertex_id) {
        EXPECT_TRUE(graph.contains(vertex_id));
        EXPECT_EQ(*graph.degree(vertex_id), 9u);
    }

    EXPECT_TRUE(db.clear());
}

TEST(db, graph_conflicting_transactions) {

    if (!ukv_supports_transactions_k)
        return;

    database_t db;
    EXPECT_TRUE(db.open(path_k));

    graph_collection_t net = *db.collection<graph_collection_t>();

    transaction_t txn = *db.transact();
    graph_collection_t txn_net = *txn.collection<graph_collection_t>();

    // triangle
    edge_t edge1 {1, 2, 9};
    edge_t edge2 {2, 3, 10};
    edge_t edge3 {3, 1, 11};

    EXPECT_TRUE(txn_net.upsert(edge1));
    EXPECT_TRUE(txn_net.upsert(edge2));
    EXPECT_TRUE(txn_net.upsert(edge3));

    EXPECT_TRUE(*txn_net.contains(1));
    EXPECT_TRUE(*txn_net.contains(2));
    EXPECT_TRUE(*txn_net.contains(3));

    EXPECT_FALSE(*net.contains(1));
    EXPECT_FALSE(*net.contains(2));
    EXPECT_FALSE(*net.contains(3));

    auto status = txn.commit();
    status.throw_unhandled();
    EXPECT_TRUE(*net.contains(1));
    EXPECT_TRUE(*net.contains(2));
    EXPECT_TRUE(*net.contains(3));

    EXPECT_TRUE(txn.reset());
    txn_net = *txn.collection<graph_collection_t>();

    transaction_t txn2 = *db.transact();
    graph_collection_t txn_net2 = *txn2.collection<graph_collection_t>();

    edge_t edge4 {4, 5, 15};
    edge_t edge5 {5, 6, 16};

    EXPECT_TRUE(txn_net.upsert(edge4));
    EXPECT_TRUE(txn_net2.upsert(edge5));

    EXPECT_TRUE(txn.commit());
    EXPECT_FALSE(txn2.commit());

    EXPECT_TRUE(db.clear());
}

TEST(db, graph_remove_vertices) {
    database_t db;
    EXPECT_TRUE(db.open(path_k));

    graph_collection_t graph = *db.collection<graph_collection_t>();

    constexpr std::size_t vertices_count = 1000;
    auto edges_vec = make_edges(vertices_count, 100);
    EXPECT_TRUE(graph.upsert(edges(edges_vec)));

    for (ukv_key_t vertex_id = 0; vertex_id != vertices_count; ++vertex_id) {
        EXPECT_TRUE(graph.contains(vertex_id));
        EXPECT_TRUE(*graph.contains(vertex_id));
        EXPECT_TRUE(graph.remove(vertex_id));
        EXPECT_TRUE(graph.contains(vertex_id));
        EXPECT_FALSE(*graph.contains(vertex_id));
    }

    EXPECT_TRUE(db.clear());
}

TEST(db, graph_remove_edges_keep_vertices) {
    database_t db;
    EXPECT_TRUE(db.open(path_k));

    graph_collection_t graph = *db.collection<graph_collection_t>();

    constexpr std::size_t vertices_count = 1000;
    auto edges_vec = make_edges(vertices_count, 100);
    EXPECT_TRUE(graph.upsert(edges(edges_vec)));
    EXPECT_TRUE(graph.remove(edges(edges_vec)));

    for (ukv_key_t vertex_id = 0; vertex_id != vertices_count; ++vertex_id) {
        EXPECT_TRUE(graph.contains(vertex_id));
        EXPECT_TRUE(*graph.contains(vertex_id));
    }

    EXPECT_TRUE(db.clear());
}

int main(int argc, char** argv) {
    std::filesystem::create_directory("./tmp");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}