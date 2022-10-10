#include <numeric>
#include <random>
#include <thread>
#include <mutex>
#include <chrono>
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

thread_local std::random_device random_device;
thread_local std::mt19937 gen(random_device());

template <std::size_t threads_cnt_ak, std::size_t txn_cnt_ak, std::size_t op_per_txn_ak>
void serializability_insert() {
    database_t db;
    EXPECT_TRUE(db.open(path()));
    EXPECT_TRUE(db.clear());

    constexpr std::size_t keys_size = txn_cnt_ak * op_per_txn_ak;
    std::vector<ukv_key_t> keys(keys_size);
    std::iota(std::begin(keys), std::end(keys), 0);

    auto task = [&](size_t thread_idx) {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt_ak; ++txn_idx) {
            transaction_t txn = db.transact().throw_or_release();
            auto txn_collection = txn.collection().throw_or_release();

            std::size_t offset = txn_idx * op_per_txn_ak;
            std::uint64_t num_value = txn_idx * threads_cnt_ak + thread_idx;
            value_view_t value((byte_t*)&num_value, sizeof(num_value));
            for (std::size_t key_idx = offset; key_idx != offset + op_per_txn_ak; ++key_idx)
                txn_collection[keys[key_idx]] = value.c_str();
            status_t status = txn.commit();
        }
    };

    std::vector<std::thread> threads(threads_cnt_ak);
    for (std::size_t i = 0; i < threads_cnt_ak; ++i)
        threads[i] = std::thread(task, i);
    for (std::size_t i = 0; i < threads_cnt_ak; ++i)
        threads[i].join();

    bins_collection_t collection = db.collection().throw_or_release();
    auto ref = collection[keys];
    auto const& retrieved = ref.value().throw_or_release();

    for (std::size_t i = 0; i != retrieved.size() - 1; ++i)
        if ((i + 1) % op_per_txn_ak)
            EXPECT_EQ(retrieved[i], retrieved[i + 1]);

    EXPECT_TRUE(db.clear());
    db.close();
}

template <std::size_t array_size_ak>
struct operation_gt {
    enum op_code_t { select_k, insert_k, remove_k };

    op_code_t type;
    std::size_t count;
    std::array<ukv_key_t, array_size_ak> keys;
    std::array<std::uint64_t, array_size_ak> values;

    inline operation_gt(op_code_t op_type,
                        std::size_t op_count,
                        std::array<ukv_key_t, array_size_ak>&& op_keys,
                        std::array<std::uint64_t, array_size_ak>&& op_values)
        : type(op_type), count(op_count), keys(std::move(op_keys)), values((std::move(op_values))) {}

    inline operation_gt(op_code_t op_type, std::size_t op_count, std::array<ukv_key_t, array_size_ak>&& op_keys)
        : type(op_type), count(op_count), keys(std::move(op_keys)) {}
};

template <typename element_at, std::size_t arr_size>
void random_fill(std::array<element_at, arr_size>& arr, std::size_t size) {
    std::uniform_int_distribution<element_at> dist(std::numeric_limits<element_at>::min());
    std::generate(arr.begin(), arr.begin() + size, [&dist]() { return dist(gen); });
}

template <std::size_t threads_cnt_ak, std::size_t txn_cnt_ak, std::size_t max_op_per_txn_ak>
void serializability_mix() {

    database_t db;
    EXPECT_TRUE(db.open(path()));
    EXPECT_TRUE(db.clear());
    std::mutex mutex;

    using time_point_t = std::uint64_t;
    using operation_t = operation_gt<max_op_per_txn_ak>;

    std::vector<std::pair<time_point_t, operation_t>> operations;
    ukv_length_t val_len = sizeof(std::uint64_t);
    std::vector<ukv_length_t> offsets(max_op_per_txn_ak);
    for (std::size_t i = 0; i != max_op_per_txn_ak; ++i)
        offsets[i] = i * val_len;
    std::uniform_int_distribution<> dist(1, max_op_per_txn_ak);

    auto task_insert = [&]() {
        contents_arg_t contents {
            .offsets_begin = {offsets.data(), sizeof(ukv_length_t)},
            .lengths_begin = {&val_len, 0},
            .count = max_op_per_txn_ak,
        };

        for (std::size_t txn_idx = 0; txn_idx != txn_cnt_ak; ++txn_idx) {
            std::array<ukv_key_t, max_op_per_txn_ak> keys;
            std::array<std::uint64_t, max_op_per_txn_ak> values;
            std::size_t op_cnt = dist(gen);
            random_fill(keys, op_cnt);
            random_fill(values, op_cnt);

            transaction_t txn = db.transact().throw_or_release();
            auto txn_ref = txn[strided_range_gt<ukv_key_t>(keys.data(), op_cnt)];
            auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(values.data());
            contents.contents_begin = {&vals_begin, 0};
            status_t status = txn_ref.assign(contents);
            if (!status)
                continue;
            status = txn.commit();
            time_point_t time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            if (!status)
                continue;
            operation_t operation(operation_t::insert_k, op_cnt, std::move(keys), std::move(values));

            mutex.lock();
            operations.push_back(std::pair<time_point_t, operation_t>(time, operation));
            mutex.unlock();
        }
    };

    auto task_remove = [&]() {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt_ak; ++txn_idx) {
            std::size_t op_cnt = dist(gen);
            std::array<ukv_key_t, max_op_per_txn_ak> keys;
            random_fill(keys, op_cnt);

            transaction_t txn = db.transact().throw_or_release();
            auto txn_ref = txn[strided_range_gt<ukv_key_t>(keys.data(), op_cnt)];
            status_t status = txn_ref.erase();
            if (!status)
                continue;
            status = txn.commit();
            time_point_t time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            if (!status)
                continue;
            operation_t operation(operation_t::remove_k, op_cnt, std::move(keys));

            mutex.lock();
            operations.push_back(std::pair<time_point_t, operation_t>(time, operation));
            mutex.unlock();
        }
    };

    auto task_select = [&]() {
        for (std::size_t txn_idx = 0; txn_idx != txn_cnt_ak; ++txn_idx) {
            std::size_t op_cnt = dist(gen);
            std::array<ukv_key_t, max_op_per_txn_ak> keys;
            std::array<std::uint64_t, max_op_per_txn_ak> values;
            random_fill(keys, op_cnt);

            transaction_t txn = db.transact().throw_or_release();
            auto txn_ref = txn[strided_range_gt<ukv_key_t>(keys.data(), op_cnt)];
            auto const& retrieved = txn_ref.value().throw_or_release();
            status_t status = txn.commit();
            time_point_t time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            if (!status)
                continue;

            auto it = retrieved.begin();
            for (std::size_t i = 0; i != op_cnt; ++i, ++it) {
                value_view_t val_view = *it;
                if (val_view)
                    std::memcpy((void*)&values[i], (const void*)val_view.data(), sizeof(std::uint64_t));
                else
                    values[i] = 0;
            }
            operation_t operation(operation_t::select_k, op_cnt, std::move(keys), std::move(values));

            mutex.lock();
            operations.push_back(std::pair<time_point_t, operation_t>(time, operation));
            mutex.unlock();
        }
    };

    std::vector<std::thread> threads(threads_cnt_ak);
    for (std::size_t i = 0; i != (threads_cnt_ak * 30) / 100; ++i)
        threads[i] = std::thread(task_insert);
    for (std::size_t i = (threads_cnt_ak * 30) / 100; i != (threads_cnt_ak * 30) / 100 + threads_cnt_ak / 10; ++i)
        threads[i] = std::thread(task_remove);
    for (std::size_t i = (threads_cnt_ak * 30) / 100 + threads_cnt_ak / 10; i != threads_cnt_ak; ++i)
        threads[i] = std::thread(task_select);

    for (std::size_t i = 0; i != threads_cnt_ak; ++i)
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

    bins_collection_t collection_copy = db_copy.collection().throw_or_release();
    for (auto& time_and_operation : operations) {
        auto operation = time_and_operation.second;
        auto ref = collection_copy[strided_range_gt<ukv_key_t>(operation.keys.data(), operation.count)];
        contents_arg_t contents {
            .offsets_begin = {offsets.data(), sizeof(ukv_length_t)},
            .lengths_begin = {&val_len, 0},
            .count = operation.count,
        };

        if (operation.type == operation_t::remove_k)
            EXPECT_TRUE(ref.erase());
        else if (operation.type == operation_t::insert_k) {
            auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(operation.values.data());
            contents.contents_begin = {&vals_begin, 0};
            EXPECT_TRUE(ref.assign(contents));
        }
        else {
            auto vals_begin = reinterpret_cast<ukv_bytes_ptr_t>(operation.values.data());
            contents.contents_begin = {&vals_begin, 0};
            auto const& retrieved = ref.value().throw_or_release();
            auto it = retrieved.begin();
            for (std::size_t i = 0; i != operation.count; ++i, ++it) {
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

    bins_collection_t collection = db.collection().throw_or_release();
    keys_range_t present_keys = collection.keys();
    keys_stream_t present_it = present_keys.begin();
    keys_range_t present_keys_copy = collection_copy.keys();
    keys_stream_t present_it_copy = present_keys_copy.begin();

    for (; !present_it.is_end(); ++present_it, ++present_it_copy)
        EXPECT_EQ(*present_it, *present_it_copy);
    EXPECT_TRUE(present_it_copy.is_end());
}

TEST(db, serializability_insert) {
    serializability_insert<4, 10, 100>();
    serializability_insert<8, 100, 100>();
    serializability_insert<16, 100, 1000>();
}

TEST(db, serializability_mix) {
    serializability_mix<4, 10, 100>();
    serializability_mix<8, 100, 100>();
    serializability_mix<16, 100, 1000>();
}

int main(int argc, char** argv) {
    std::filesystem::create_directory("./tmp");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}