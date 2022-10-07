#include <vector>
#include <numeric>
#include <thread>
#include <mutex>
#include <charconv>
#include <filesystem>

#include <gtest/gtest.h>
#include "ukv/ukv.hpp"

using namespace unum::ukv;
using namespace unum;

static char const* path() {
    char* path = std::getenv("UKV_BACKEND_PATH");
    if (path)
        return path;

#if defined(UKV_FLIGHT_CLIENT)
    return nullptr;
#elif defined(UKV_TEST_PATH)
    return UKV_TEST_PATH;
#else
    return nullptr;
#endif
}

template <std::size_t threads_cnt, std::size_t txn_cnt, std::size_t op_per_txn>
void insert_concurrent_transactions() {
    database_t db;
    EXPECT_TRUE(db.open(path()));
    EXPECT_TRUE(db.clear());

    constexpr std::size_t keys_size = txn_cnt * op_per_txn;
    std::vector<ukv_key_t> keys(keys_size);
    std::iota(std::begin(keys), std::end(keys), 0);

    auto task = [&](size_t thread_idx) {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt; ++txn_idx) {
            transaction_t txn = *db.transact();
            auto txn_collection = *txn.collection();

            std::size_t offset = txn_idx * op_per_txn;
            std::uint64_t num_value = txn_idx * threads_cnt + thread_idx;
            value_view_t value((byte_t*)&num_value, sizeof(num_value));
            for (std::size_t key_idx = offset; key_idx != offset + op_per_txn; ++key_idx)
                txn_collection[keys[key_idx]] = value.c_str();
            status_t status = txn.commit();
        }
    };

    std::vector<std::thread> threads(threads_cnt);
    for (std::size_t i = 0; i < threads_cnt; ++i)
        threads[i] = std::thread(task, i);
    for (std::size_t i = 0; i < threads_cnt; ++i)
        threads[i].join();

    bins_collection_t collection = *db.collection();
    auto ref = collection[keys];
    auto maybe_retrieved = ref.value();
    auto const& retrieved = *maybe_retrieved;

    for (std::size_t i = 0; i != retrieved.size() - 1; ++i)
        if ((i + 1) % op_per_txn)
            EXPECT_EQ(retrieved[i], retrieved[i + 1]);

    EXPECT_TRUE(db.clear());
    db.close();
}

struct operation_t {
    enum op_code { select, insert, remove };

    op_code type;
    std::vector<ukv_key_t> keys;
    std::vector<std::uint64_t> values;

    inline operation_t(op_code op, std::vector<ukv_key_t>&& moved_keys, std::vector<std::uint64_t>&& moved_values)
        : type(op), keys(std::move(moved_keys)), values(std::move(moved_values)) {};
};

template <std::size_t threads_cnt, std::size_t txn_cnt, std::size_t max_op_per_txn>
void time_point_concurrent_transactions() {

    database_t db;
    EXPECT_TRUE(db.open(path()));
    EXPECT_TRUE(db.clear());
    std::mutex mutex;

    using time_point_t = std::uint64_t;
    std::map<time_point_t, operation_t> operations;

    auto task_insert = [&]() {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt; ++txn_idx) {

            std::size_t op_cnt = std::rand() % max_op_per_txn;
            std::vector<ukv_key_t> keys(op_cnt);
            std::vector<std::uint64_t> values(op_cnt);
            std::vector<ukv_length_t> offsets(op_cnt);
            ukv_length_t val_len = sizeof(std::uint64_t);
            auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(values.data());
            std::srand(std::time(nullptr));
            std::generate(keys.begin(), keys.end(), std::rand);
            std::generate(values.begin(), values.end(), std::rand);
            for (std::size_t i = 0; i != op_cnt; ++i)
                offsets[i] = i * val_len;

            contents_arg_t contents {
                .offsets_begin = {offsets.data(), sizeof(ukv_length_t)},
                .lengths_begin = {&val_len, 0},
                .contents_begin = {&vals_begin, 0},
                .count = op_cnt,
            };

            transaction_t txn = *db.transact();
            auto txn_ref = txn[keys];
            txn_ref.assign(contents);
            status_t status = txn.commit();
            auto time =
                std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();
            if (!status)
                continue;

            mutex.lock();
            operations.insert(std::pair<time_point_t, operation_t>(
                time,
                operation_t(operation_t::insert, std::move(keys), std::move(values))));
            mutex.unlock();
        }
    };

    auto task_remove = [&]() {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt; ++txn_idx) {

            std::size_t op_cnt = std::rand() % max_op_per_txn;
            std::vector<ukv_key_t> keys(op_cnt);
            std::srand(std::time(nullptr));
            std::generate(keys.begin(), keys.end(), std::rand);

            transaction_t txn = *db.transact();
            auto txn_ref = txn[keys];
            txn_ref.erase();
            status_t status = txn.commit();
            auto time =
                std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();
            if (!status)
                continue;

            mutex.lock();
            operations.insert(std::pair<time_point_t, operation_t>(
                time,
                operation_t(operation_t::remove, std::move(keys), std::vector<std::uint64_t>())));
            mutex.unlock();
        }
    };

    auto task_select = [&]() {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt; ++txn_idx) {

            std::size_t op_cnt = std::rand() % max_op_per_txn;
            std::vector<ukv_key_t> keys(op_cnt);
            std::srand(std::time(nullptr));
            std::generate(keys.begin(), keys.end(), std::rand);

            transaction_t txn = *db.transact();
            auto txn_ref = txn[keys];
            auto const& retrieved = *txn_ref.value();
            status_t status = txn.commit();
            auto time =
                std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();
            if (!status)
                continue;

            std::vector<std::uint64_t> values(op_cnt);
            auto it = retrieved.begin();
            for (std::size_t i = 0; i != op_cnt; ++i, ++it) {
                value_view_t val_view = *it;
                if (val_view)
                    std::memcpy((void*)&values[i], (const void*)val_view.data(), sizeof(std::uint64_t));
            }

            mutex.lock();
            operations.insert(std::pair<time_point_t, operation_t>(
                time,
                operation_t(operation_t::select, std::move(keys), std::move(values))));
            mutex.unlock();
        }
    };

    std::vector<std::thread> threads(threads_cnt);
    for (std::size_t i = 0; i != (threads_cnt * 30) / 100; ++i)
        threads[i] = std::thread(task_insert);
    for (std::size_t i = (threads_cnt * 30) / 100; i != (threads_cnt * 30) / 100 + threads_cnt / 10; ++i)
        threads[i] = std::thread(task_remove);
    for (std::size_t i = (threads_cnt * 30) / 100 + threads_cnt / 10; i != threads_cnt; ++i)
        threads[i] = std::thread(task_select);

    for (std::size_t i = 0; i != threads_cnt; ++i)
        threads[i].join();

    std::filesystem::path second_db_path(path());
    if (second_db_path.has_filename())
        second_db_path += "_copy";
    else {
        second_db_path = second_db_path.parent_path();
        second_db_path += "_copy/";
    }

    database_t db_copy;
    EXPECT_TRUE(db_copy.open(second_db_path.c_str()));
    EXPECT_TRUE(db_copy.clear());

    bins_collection_t collection = *db_copy.collection();
    for (auto& time_and_operation : operations) {
        auto operation = time_and_operation.second;
        auto ref = collection[operation.keys];
        if (operation.type == operation_t::remove) {
            EXPECT_TRUE(ref.erase());
        }
        else if (operation.type == operation_t::insert) {
            ukv_length_t val_len = sizeof(std::uint64_t);
            auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(operation.values.data());
            std::vector<ukv_length_t> offsets(operation.values.size());
            for (std::size_t i = 0; i != operation.values.size(); ++i)
                offsets[i] = i * val_len;

            contents_arg_t contents {
                .offsets_begin = {offsets.data(), sizeof(ukv_length_t)},
                .lengths_begin = {&val_len, 0},
                .contents_begin = {&vals_begin, 0},
                .count = operation.values.size(),
            };

            EXPECT_TRUE(ref.assign(contents));
        }
        else {
            ukv_length_t val_len = sizeof(std::uint64_t);
            auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(operation.values.data());
            std::vector<ukv_length_t> offsets(operation.keys.size());
            for (std::size_t i = 0; i != operation.values.size(); ++i)
                offsets[i] = i * val_len;

            contents_arg_t contents {
                .offsets_begin = {offsets.data(), sizeof(ukv_length_t)},
                .lengths_begin = {&val_len, 0},
                .contents_begin = {&vals_begin, 0},
                .count = operation.values.size(),
            };

            auto const& retrieved = *ref.value();
            auto it = retrieved.begin();
            for (std::size_t i = 0; i != operation.values.size(); ++i, ++it) {
                value_view_t val_view = *it;
                if (!val_view) {
                    EXPECT_EQ(operation.values[i], 0);
                    continue;
                }

                auto expected_len = static_cast<std::size_t>(contents.lengths_begin[i]);
                auto expected_begin =
                    reinterpret_cast<byte_t const*>(contents.contents_begin[i]) + contents.offsets_begin[i];
                value_view_t expected_view(expected_begin, expected_begin + expected_len);
                EXPECT_EQ(val_view.size(), expected_len);
                EXPECT_EQ(val_view, expected_view);
            }
        }
    }

    // TODO: Check equality of db and db_copy
}

TEST(db, insert_concurrent_transactions) {
    insert_concurrent_transactions<4, 10, 100>();
    insert_concurrent_transactions<8, 100, 100>();
    insert_concurrent_transactions<16, 100, 1000>();
}

TEST(db, time_point_concurrent_transactions) {
    time_point_concurrent_transactions<4, 10, 100>();
    time_point_concurrent_transactions<8, 100, 100>();
    time_point_concurrent_transactions<16, 100, 1000>();
}

int main(int argc, char** argv) {
    std::filesystem::create_directory("./tmp");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}