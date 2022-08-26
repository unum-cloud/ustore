/**
 * @file arrow_client.cpp
 * @author Ashot Vardanian
 *
 * @brief Client library for Apache Arrow RPC server.
 * Converts native UKV operations into Arrows classical `DoPut`, `DoExchange`...
 * Understanding the costs of remote communication, might keep a cache.
 * ? Intended for single-thread use?
 */

#include <unordered_map>

#include <fmt/core.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#include <arrow/type.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/memory_pool.h>
#include <arrow/flight/client.h>
#include <arrow/c/bridge.h> // `ExportSchema`
#pragma GCC diagnostic pop

#define ARROW_C_DATA_INTERFACE 1
#define ARROW_C_STREAM_INTERFACE 1
#include "ukv/db.h"
#include "ukv/arrow.h"

#include "helpers.hpp"
#include "arrow.hpp"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_col_t ukv_col_main_k = 0;
ukv_val_len_t ukv_val_len_missing_k = std::numeric_limits<ukv_val_len_t>::max();
ukv_key_t ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

namespace arf = arrow::flight;
namespace ar = arrow;

using namespace unum::ukv;
using namespace unum;

struct rpc_client_t {
    std::unique_ptr<arf::FlightClient> flight;
};

class arrow_mem_pool_t final : public ar::MemoryPool {
    monotonic_resource_t resource_;

  public:
    arrow_mem_pool_t(stl_arena_t& arena) : resource_(&arena.resource) {}
    ~arrow_mem_pool_t() {}

    ar::Status Allocate(int64_t size, uint8_t** ptr) override {
        auto new_ptr = resource_.allocate(size);
        if (!new_ptr)
            return ar::Status::OutOfMemory("");

        *ptr = reinterpret_cast<uint8_t*>(new_ptr);
        return ar::Status::OK();
    }
    ar::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
        auto new_ptr = resource_.allocate(new_size);
        if (!new_ptr)
            return ar::Status::OutOfMemory("");

        std::memcpy(new_ptr, *ptr, old_size);
        resource_.deallocate(*ptr, old_size);
        *ptr = reinterpret_cast<uint8_t*>(new_ptr);
        return ar::Status::OK();
    }
    void Free(uint8_t* buffer, int64_t size) override { resource_.deallocate(buffer, size); }
    void ReleaseUnused() override {}
    int64_t bytes_allocated() const override { return static_cast<int64_t>(resource_.used()); }
    int64_t max_memory() const override { return static_cast<int64_t>(resource_.capacity()); }
    std::string backend_name() const { return "ukv"; }
};

arf::FlightCallOptions call_options(arrow_mem_pool_t& pool) {
    arf::FlightCallOptions options;
    options.read_options.memory_pool = &pool;
    options.read_options.use_threads = false;
    options.read_options.max_recursion_depth = 1;
    options.write_options.memory_pool = &pool;
    options.write_options.use_threads = false;
    options.write_options.max_recursion_depth = 1;
    options.memory_manager;
    return options;
}

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_db_open( //
    ukv_str_view_t c_config,
    ukv_t* c_db,
    ukv_error_t* c_error) {

#ifdef DEBUG
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(5s);
#endif

    safe_section("Starting client", c_error, [&] {
        if (!c_config || !std::strlen(c_config))
            c_config = "grpc://0.0.0.0:38709";

        auto db_ptr = new rpc_client_t {};
        auto maybe_location = arf::Location::Parse(c_config);
        return_if_error(maybe_location.ok(), c_error, args_wrong_k, "Server URI");

        auto maybe_flight_ptr = arf::FlightClient::Connect(*maybe_location);
        return_if_error(maybe_flight_ptr.ok(), c_error, network_k, "Flight Client Connection");

        db_ptr->flight = maybe_flight_ptr.MoveValueUnsafe();
        *c_db = db_ptr;
    });
}

void ukv_read( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_val_ptr_t* c_found_values,
    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    ukv_1x8_t** c_found_presences,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = clean_arena(c_arena, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {cols, keys, {}, c_tasks_count};

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = call_options(pool);

    // Configure the `cmd` descriptor
    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && places.same_collections_are_named();
    bool const request_only_present = c_found_presences && !c_found_lengths && !c_found_values;
    bool const request_length = c_found_lengths && !c_found_values;
    char const* partial_mode = !request_length && !request_length //
                                   ? nullptr
                                   : request_length ? "length" : "present";
    bool const read_shared = c_options & ukv_option_read_shared_k;
    bool const read_track = c_options & ukv_option_read_track_k;
    arf::FlightDescriptor descriptor;
    descriptor.cmd = "read?";
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd), "txn={:x}&", std::uintptr_t(c_txn));
    if (same_named_collection)
        fmt::format_to(std::back_inserter(descriptor.cmd), "col={:x}&", cols[0]);
    if (partial_mode)
        fmt::format_to(std::back_inserter(descriptor.cmd), "part={}&", partial_mode);
    if (read_shared)
        descriptor.cmd.append("shared&");
    if (read_track)
        descriptor.cmd.append("track&");

    bool const has_cols_column = !same_collection;
    constexpr bool has_keys_column = true;

    // If all requests map to the same collection, we can avoid passing its ID
    if (has_cols_column && !cols.is_continuous()) {
        auto continuous = arena.alloc<ukv_col_t>(places.count, c_error);
        return_on_error(c_error);
        transform_n(keys, places.count, continuous.begin());
        cols = {continuous.begin(), sizeof(ukv_col_t)};
    }

    // When exporting keys, make sure they are properly strided
    if (has_keys_column && !keys.is_continuous()) {
        auto continuous = arena.alloc<ukv_key_t>(places.count, c_error);
        return_on_error(c_error);
        transform_n(keys, places.count, continuous.begin());
        keys = {continuous.begin(), sizeof(ukv_key_t)};
    }

    // Now build-up the Arrow representation
    ArrowArray array_c;
    ArrowSchema schema_c;
    auto count_cols = has_cols_column + has_keys_column;
    ukv_to_arrow_schema(places.count, count_cols, &schema_c, &array_c, c_error);
    return_on_error(c_error);

    if (has_cols_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "cols",
            ukv_type<ukv_col_t>(),
            nullptr,
            nullptr,
            cols.get(),
            schema_c.children[0],
            array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "keys",
            ukv_type<ukv_key_t>(),
            nullptr,
            nullptr,
            keys.get(),
            schema_c.children[has_cols_column],
            array_c.children[has_cols_column],
            c_error);
    return_on_error(c_error);

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&array_c, &schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    ar::Result<arf::FlightClient::DoExchangeResult> result = db.flight->DoExchange(options, descriptor);
    return_if_error(result.ok(), c_error, network_k, "Failed to exchange with Arrow server");

    ar_status = result->writer->Begin(batch_ptr->schema());
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing schema");

    ar_status = result->writer->WriteRecordBatch(*batch_ptr);
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing request");

    // ar_status = result->writer->DoneWriting();
    // return_if_error(ar_status.ok(), c_error, error_unknown_k, "Submitting request");

    ar_status = result->writer->Close();
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Closing the channel");

    // Fetch the responses
    // Requesting `ToTable` might be more efficient than concatenating and
    // reallocating directly from our arena, as the underlying Arrow implementation
    // may know the length of the entire dataset.
    arrow::Result<arf::FlightStreamChunk> read_chunk = result->reader->Next();
    return_if_error(read_chunk.ok(), c_error, network_k, "No response");

    // Convert the responses in Arrow C form
    batch_ptr = read_chunk->data;
    return_if_error(batch_ptr->columns().size() == 1, c_error, error_unknown_k, "Expecting one column");

    std::shared_ptr<ar::Array> array_ptr = batch_ptr->column(0);
    ar_status = ar::ExportArray(*array_ptr, &array_c, &schema_c);
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Can't parse Arrow response");

    // Export the results into out expected form
    auto bitmap_slots = divide_round_up<std::size_t>(array_c.length, CHAR_BIT);
    if (request_only_present) {
        *c_found_presences = (ukv_1x8_t*)array_c.buffers[1];
    }
    else if (request_length) {
        auto presences = (ukv_1x8_t*)array_c.buffers[0];
        auto lens = (ukv_val_len_t*)array_c.buffers[1];
        if (!c_found_presences)
            *c_found_lengths = normalize_lengths_with_bitmap(presences, lens, array_c.length);
        else {
            *c_found_presences = presences;
            *c_found_lengths = lens;
        }
    }
    else {
        auto presences = (ukv_1x8_t*)array_c.buffers[0];
        auto offs = (ukv_val_len_t*)array_c.buffers[1];
        auto data = (ukv_val_ptr_t)array_c.buffers[2];

        if (c_found_presences)
            *c_found_presences = presences;
        if (c_found_offsets)
            *c_found_offsets = offs;
        if (c_found_values)
            *c_found_values = data;

        if (c_found_lengths) {
            auto presences_range = strided_iterator_gt<ukv_1x8_t>(presences, sizeof(ukv_1x8_t));
            auto lens = *c_found_lengths = arena.alloc<ukv_val_len_t>(places.count, c_error).begin();
            return_on_error(c_error);

            for (std::size_t i = 0; i != places.count; ++i)
                lens[i] = presences_range[i] ? ukv_val_len_missing_k : (offs[i + 1] - offs[i]);
        }
    }
}

void ukv_write( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_val_ptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_val_len_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_val_len_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_1x8_t const* c_presences,

    ukv_options_t const c_options,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = clean_arena(c_arena, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    strided_iterator_gt<ukv_1x8_t const> presences {c_presences, sizeof(ukv_1x8_t)};

    places_arg_t places {cols, keys, {}, c_tasks_count};
    contents_arg_t contents {vals, offs, lens, presences, c_tasks_count};

    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && places.same_collections_are_named();
    bool const write_flush = c_options & ukv_option_write_flush_k;

    bool const has_cols_column = !same_collection;
    constexpr bool has_keys_column = true;
    bool const has_contents_column = vals != nullptr;

    if (has_cols_column && !cols.is_continuous()) {
        auto continuous = arena.alloc<ukv_col_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(cols, places.size(), continuous.begin());
        cols = {continuous.begin(), places.size()};
    }

    if (has_keys_column && !keys.is_continuous()) {
        auto continuous = arena.alloc<ukv_key_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(keys, places.size(), continuous.begin());
        keys = {continuous.begin(), places.size()};
    }

    // Check if the input is continuous and is already in an Arrow-compatible form
    ukv_val_ptr_t joined_vals_begin = vals ? vals[0] : nullptr;
    if (has_contents_column && !contents.is_continuous()) {
        auto total = transform_reduce_n(contents, places.size(), 0ul, std::mem_fn(&value_view_t::size));
        auto joined_vals = arena.alloc<byte_t>(total, c_error);
        return_on_error(c_error);
        auto joined_offs = arena.alloc<ukv_val_len_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        auto joined_presences = arena.alloc<ukv_1x8_t>(divide_round_up<std::size_t>(places.size(), CHAR_BIT), c_error);
        return_on_error(c_error);

        // Exports into the Arrow-compatible form
        ukv_val_len_t exported_bytes = 0;
        for (std::size_t i = 0; i != c_tasks_count; ++i) {
            auto value = contents[i];
            joined_presences[i] = !value;
            joined_offs[i] = exported_bytes;
            std::memcpy(joined_vals.begin() + exported_bytes, value.begin(), value.size());
            exported_bytes += value.size();
        }
        joined_offs[places.size()] = exported_bytes;

        joined_vals_begin = (ukv_val_ptr_t)joined_vals.begin();
        vals = {&joined_vals_begin, 0};
        offs = {joined_offs.begin(), sizeof(ukv_key_t)};
        presences = {joined_presences.begin(), sizeof(ukv_1x8_t)};
    }
    // It may be the case, that we only have `c_tasks_count` offsets instead of `c_tasks_count+1`,
    // which won't be enough for Arrow.
    else if (!contents.is_arrow()) {
        auto joined_offs = arena.alloc<ukv_val_len_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        auto joined_presences = arena.alloc<ukv_1x8_t>(divide_round_up<std::size_t>(places.size(), CHAR_BIT), c_error);
        return_on_error(c_error);

        // Exports into the Arrow-compatible form
        ukv_val_len_t exported_bytes = 0;
        for (std::size_t i = 0; i != c_tasks_count; ++i) {
            auto value = contents[i];
            joined_presences[i] = !value;
            joined_offs[i] = exported_bytes;
            exported_bytes += value.size();
        }
        joined_offs[places.size()] = exported_bytes;

        vals = {&joined_vals_begin, 0};
        offs = {joined_offs.begin(), sizeof(ukv_key_t)};
        presences = {joined_presences.begin(), sizeof(ukv_1x8_t)};
    }

    // Now build-up the Arrow representation
    ArrowArray array_c;
    ArrowSchema schema_c;
    auto count_cols = has_cols_column + has_keys_column + has_contents_column;
    ukv_to_arrow_schema(c_tasks_count, count_cols, &schema_c, &array_c, c_error);
    return_on_error(c_error);

    if (has_cols_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "cols",
            ukv_type<ukv_col_t>(),
            nullptr,
            nullptr,
            cols.get(),
            schema_c.children[0],
            array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "keys",
            ukv_type<ukv_key_t>(),
            nullptr,
            nullptr,
            keys.get(),
            schema_c.children[has_cols_column],
            array_c.children[has_cols_column],
            c_error);
    return_on_error(c_error);

    if (has_contents_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "vals",
            ukv_type<value_view_t>(),
            presences.get(),
            offs.get(),
            joined_vals_begin,
            schema_c.children[has_cols_column + has_keys_column],
            array_c.children[has_cols_column + has_keys_column],
            c_error);
    return_on_error(c_error);

    // Send everything over the network and wait for the response
    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = call_options(pool);

    // Configure the `cmd` descriptor
    arf::FlightDescriptor descriptor;
    descriptor.cmd = "write?";
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd), "txn={:x}&", std::uintptr_t(c_txn));
    if (!has_cols_column && cols)
        fmt::format_to(std::back_inserter(descriptor.cmd), "col={:x}&", cols[0]);
    if (write_flush)
        descriptor.cmd.append("flush&");

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&array_c, &schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    ar::Result<arf::FlightClient::DoPutResult> result = db.flight->DoPut(options, descriptor, batch_ptr->schema());
    return_if_error(result.ok(), c_error, network_k, "Failed to exchange with Arrow server");

    // This writer has already been started!
    // ar_status = result->writer->Begin(batch_ptr->schema());
    // return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing schema");

    ar_status = result->writer->WriteRecordBatch(*batch_ptr);
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing request");

    ar_status = result->writer->DoneWriting();
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Submitting request");

    // Fetch the responses
    // std::shared_ptr<ar::Buffer> response;
    // ar_status = result->reader->ReadMetadata(&response);
    // return_if_error(ar_status.ok(), c_error, network_k, "No response");
}

void ukv_scan( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const c_min_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_min_keys,
    ukv_size_t const c_min_keys_stride,

    ukv_size_t const* c_scan_lengths,
    ukv_size_t const c_scan_lengths_stride,

    ukv_options_t const c_options,

    ukv_size_t** c_found_counts,
    ukv_key_t*** c_found_keys,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = clean_arena(c_arena, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_min_keys, c_min_keys_stride};
    strided_iterator_gt<ukv_size_t const> lens {c_scan_lengths, c_scan_lengths_stride};
    scans_arg_t places {cols, keys, lens, c_min_tasks_count};
}

void ukv_size( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const n,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_min_keys,
    ukv_size_t const c_min_keys_stride,

    ukv_key_t const* c_max_keys,
    ukv_size_t const c_max_keys_stride,

    ukv_options_t const,

    ukv_size_t** c_found_estimates,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = clean_arena(c_arena, c_error);
    return_on_error(c_error);
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ukv_col_open(
    // Inputs:
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_str_view_t,
    // Outputs:
    ukv_col_t* c_col,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    auto name_len = std::strlen(c_col_name);
    if (!name_len) {
        *c_col = ukv_col_main_k;
        return;
    }

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    auto const col_name = std::string_view(c_col_name, name_len);
}

void ukv_col_remove(
    // Inputs:
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    // Outputs:
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
}

void ukv_col_list( //
    ukv_t const c_db,
    ukv_size_t* c_count,
    ukv_col_t** c_ids,
    ukv_val_len_t** c_offsets,
    ukv_str_view_t* c_names,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = clean_arena(c_arena, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
}

void ukv_db_control( //
    ukv_t const c_db,
    ukv_str_view_t c_request,
    ukv_str_view_t* c_response,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c_request, c_error, uninitialized_state_k, "Request is uninitialized");

    *c_response = NULL;
    log_error(c_error, missing_feature_k, "Controls aren't supported in this implementation!");
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_txn_begin(
    // Inputs:
    ukv_t const c_db,
    ukv_size_t const c_generation,
    ukv_options_t const,
    // Outputs:
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    return_if_error(c_txn, c_error, uninitialized_state_k, "Transaction is uninitialized");
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_t const, ukv_arena_t c_arena) {
    if (!c_arena)
        return;
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(c_arena);
    delete &arena;
}

void ukv_txn_free(ukv_t const, ukv_txn_t const c_txn) {
    if (!c_txn)
        return;
}

void ukv_db_free(ukv_t c_db) {
    if (!c_db)
        return;
    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    delete &db;
}

void ukv_col_free(ukv_t const, ukv_col_t const) {
    // In this in-memory freeing the col handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
