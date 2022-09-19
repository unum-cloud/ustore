#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

#include <ukv/ukv.hpp>

using namespace unum::ukv;
using namespace unum;

TEST(db, validation) {

    database_t db;
    EXPECT_TRUE(db.open("./tmp/stl"));
    collection_t collection = *db.collection();
    transaction_t txn = *db.transact();
    std::vector<ukv_key_t> keys {34, 35, 36};
    std::vector<std::uint64_t> vals {34, 35, 36};
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<ukv_length_t> offs {0, val_len, val_len * 2};
    auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(vals.data());
    ukv_length_t count = 3;

    contents_arg_t values {
        .offsets_begin = {offs.data(), sizeof(ukv_length_t)},
        .lengths_begin = {&val_len, 0},
        .contents_begin = {&vals_begin, 0},
        .count = count,
    };

    using value_extractor_t = contents_arg_extractor_gt<std::remove_reference_t<contents_arg_t>>;
    auto contents = value_extractor_t {}.contents(values);
    auto offsets = value_extractor_t {}.offsets(values);
    auto lengths = value_extractor_t {}.lengths(values);

    status_t status;
    std::vector<ukv_options_t> options {ukv_options_default_k, ukv_option_write_flush_k};
    for (auto& option : options) {
        ukv_write(db,
                  nullptr,
                  count,
                  collection.member_ptr(),
                  0,
                  keys.data(),
                  sizeof(ukv_key_t),
                  nullptr,
                  offsets.get(),
                  offsets.stride(),
                  lengths.get(),
                  lengths.stride(),
                  contents.get(),
                  contents.stride(),
                  option,
                  collection.member_arena(),
                  status.member_ptr());

        EXPECT_TRUE(status);
    }

    ukv_write(db,
              txn,
              count,
              collection.member_ptr(),
              0,
              keys.data(),
              sizeof(ukv_key_t),
              nullptr,
              offsets.get(),
              offsets.stride(),
              lengths.get(),
              lengths.stride(),
              contents.get(),
              contents.stride(),
              ukv_options_default_k,
              collection.member_arena(),
              status.member_ptr());

    EXPECT_TRUE(status);

    // Transaction With Flush
    ukv_write(db,
              txn,
              count,
              collection.member_ptr(),
              0,
              keys.data(),
              sizeof(ukv_key_t),
              nullptr,
              offsets.get(),
              offsets.stride(),
              lengths.get(),
              lengths.stride(),
              contents.get(),
              contents.stride(),
              ukv_option_write_flush_k,
              collection.member_arena(),
              status.member_ptr());

    EXPECT_FALSE(status);
    status.release_error();

    // Count = 0, Keys!= nullptr
    ukv_write(db,
              nullptr,
              0,
              collection.member_ptr(),
              0,
              keys.data(),
              sizeof(ukv_key_t),
              nullptr,
              offsets.get(),
              offsets.stride(),
              lengths.get(),
              lengths.stride(),
              contents.get(),
              contents.stride(),
              ukv_options_default_k,
              collection.member_arena(),
              status.member_ptr());

    EXPECT_FALSE(status);
    status.release_error();

    // Count > 0; Keys == nullptr
    ukv_write(db,
              nullptr,
              count,
              collection.member_ptr(),
              0,
              nullptr,
              sizeof(ukv_key_t),
              nullptr,
              offsets.get(),
              offsets.stride(),
              lengths.get(),
              lengths.stride(),
              contents.get(),
              contents.stride(),
              ukv_options_default_k,
              collection.member_arena(),
              status.member_ptr());

    EXPECT_FALSE(status);
    status.release_error();

    // Wrong Write Options
    std::vector<ukv_options_t> wrong_write_options {
        ukv_option_read_track_k,
        ukv_option_txn_snapshot_k,
    };

    for (auto& option : wrong_write_options) {
        ukv_write(db,
                  nullptr,
                  count,
                  collection.member_ptr(),
                  0,
                  keys.data(),
                  sizeof(ukv_key_t),
                  nullptr,
                  offsets.get(),
                  offsets.stride(),
                  lengths.get(),
                  lengths.stride(),
                  contents.get(),
                  contents.stride(),
                  option,
                  collection.member_arena(),
                  status.member_ptr());

        EXPECT_FALSE(status);
        status.release_error();
    }

    ukv_length_t* found_offsets = nullptr;
    ukv_length_t* found_lengths = nullptr;
    ukv_bytes_ptr_t found_values = nullptr;

    ukv_read(db,
             nullptr,
             count,
             collection.member_ptr(),
             0,
             keys.data(),
             sizeof(ukv_key_t),
             ukv_options_default_k,
             nullptr,
             &found_offsets,
             &found_lengths,
             &found_values,
             collection.member_arena(),
             status.member_ptr());

    EXPECT_TRUE(status);

    ukv_read(db,
             txn,
             count,
             collection.member_ptr(),
             0,
             keys.data(),
             sizeof(ukv_key_t),
             ukv_option_read_track_k,
             nullptr,
             &found_offsets,
             &found_lengths,
             &found_values,
             collection.member_arena(),
             status.member_ptr());

    EXPECT_TRUE(status);

    ukv_read(db,
             txn,
             count,
             collection.member_ptr(),
             0,
             keys.data(),
             sizeof(ukv_key_t),
             ukv_option_txn_snapshot_k,
             nullptr,
             &found_offsets,
             &found_lengths,
             &found_values,
             collection.member_arena(),
             status.member_ptr());

    EXPECT_TRUE(status);

    // Wrong Read Options
    std::vector<ukv_options_t> wrong_read_options {
        ukv_option_write_flush_k,
        ukv_option_read_track_k,
        ukv_option_txn_snapshot_k,
    };

    for (auto& option : wrong_read_options) {
        ukv_read(db,
                 nullptr,
                 count,
                 collection.member_ptr(),
                 0,
                 keys.data(),
                 sizeof(ukv_key_t),
                 option,
                 nullptr,
                 &found_offsets,
                 &found_lengths,
                 &found_values,
                 collection.member_arena(),
                 status.member_ptr());

        EXPECT_FALSE(status);
        status.release_error();
    }

    // Transaction

    ukv_transaction_t ukv_txn = nullptr;
    ukv_transaction_begin(db, 0, ukv_options_default_k, &ukv_txn, status.member_ptr());
    EXPECT_TRUE(status);

    ukv_transaction_begin(db, 0, ukv_options_default_k, nullptr, status.member_ptr());
    EXPECT_FALSE(status);
    status.release_error();

    // Wrong Transaction Begin Options
    std::vector<ukv_options_t> wrong_txn_begin_options {
        ukv_option_write_flush_k,
        ukv_option_nodiscard_k,
    };

    for (auto& option : wrong_txn_begin_options) {
        ukv_transaction_begin(db, 0, option, &ukv_txn, status.member_ptr());
        EXPECT_FALSE(status);
        status.release_error();
    }

    // Wrong Transaction Commit Options
    std::vector<ukv_options_t> wrong_txn_commit_options {
        ukv_option_txn_snapshot_k,
        ukv_option_nodiscard_k,
    };

    for (auto& option : wrong_txn_commit_options) {
        ukv_transaction_commit(db, txn, option, status.member_ptr());
        EXPECT_FALSE(status);
        status.release_error();
    }

    // Scans
    ukv_key_t* found_keys = nullptr;
    ukv_length_t* found_counts = nullptr;

    ukv_scan(db,
             txn,
             1,
             collection.member_ptr(),
             0,
             keys.data(),
             0,
             keys.data() + count - 1,
             0,
             &count,
             0,
             ukv_options_default_k,
             &found_offsets,
             &found_counts,
             &found_keys,
             collection.member_arena(),
             status.member_ptr());

    EXPECT_TRUE(status);

    // Count > 0, Keys = nullptr
    ukv_scan(db,
             txn,
             1,
             collection.member_ptr(),
             0,
             nullptr,
             0,
             nullptr,
             0,
             &count,
             0,
             ukv_options_default_k,
             &found_offsets,
             &found_counts,
             &found_keys,
             collection.member_arena(),
             status.member_ptr());

    EXPECT_FALSE(status);
    status.release_error();

    // Limits == nullptr
    ukv_scan(db,
             txn,
             1,
             collection.member_ptr(),
             0,
             keys.data(),
             0,
             keys.data() + count - 1,
             0,
             nullptr,
             0,
             ukv_options_default_k,
             &found_offsets,
             &found_counts,
             &found_keys,
             collection.member_arena(),
             status.member_ptr());

    EXPECT_FALSE(status);
    status.release_error();
}

int main(int argc, char** argv) {
    std::filesystem::create_directory("./tmp/stl");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}