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

class UKVMemoryPool final : public ar::MemoryPool {
    monotonic_resource_t resource_;

  public:
    UKVMemoryPool(stl_arena_t& arena) : resource_(&arena.resource) {}
    ~UKVMemoryPool() {}

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

/*********************************************************/
/*****************	    C Interface 	  ****************/
/*********************************************************/

void ukv_db_open( //
    ukv_str_view_t c_config,
    ukv_t* c_db,
    ukv_error_t* c_error) {

    try {
        auto db_ptr = new rpc_client_t {};
        auto maybe_location = arf::Location::Parse(c_config);
        if (!maybe_location.ok()) {
            *c_error = "Couldn't understand server location URL";
            return;
        }
        auto maybe_flight_ptr = arf::FlightClient::Connect(*maybe_location);
        if (!maybe_flight_ptr.ok()) {
            *c_error = "Couldn't connect to server";
            return;
        }
        db_ptr->flight = maybe_flight_ptr.MoveValueUnsafe();
        *c_db = db_ptr;
    }
    catch (...) {
        *c_error = "Failed to initialize the database";
    }
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

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    read_tasks_soa_t tasks {cols, keys, c_tasks_count};

    UKVMemoryPool pool(arena);
    arf::FlightCallOptions options;
    options.read_options.memory_pool = &pool;
    options.read_options.use_threads = false;
    options.read_options.max_recursion_depth = 1;
    options.write_options.memory_pool = &pool;
    options.write_options.use_threads = false;
    options.write_options.max_recursion_depth = 1;
    options.memory_manager;

    // This will return
    bool read_lengths = c_options & ukv_option_read_lengths_k;
    bool read_shared = c_options & ukv_option_read_shared_k;
    bool read_track = c_options & ukv_option_read_track_k;
    arf::FlightDescriptor descriptor;
    descriptor.cmd = fmt::format( //
        "read?{}{:{}}&{}&{}&{}",
        // transactions:
        c_txn ? "txn=" : "",
        std::uintptr_t(c_txn),
        c_txn ? "x" : "",
        // options:
        read_lengths ? "lengths" : "",
        read_shared ? "shared" : "",
        read_track ? "track" : "");

    // Submit the read operations
    std::shared_ptr<ar::RecordBatch> batch_ptr;
    ar::Status ar_status;
    ar::Result<arf::FlightClient::DoExchangeResult> result = db.flight->DoExchange(options, descriptor);
    ar_status = result.status();
    ar_status = result->writer->WriteRecordBatch(*batch_ptr);
    ar_status = result->writer->Close();

    // Fetch the responses
    // Requesting `ToTable` might be more efficient than concatenating and
    // reallocating directly from our arena, as the underlying Arrow implementation
    // may know the length of the entire dataset.
    arrow::Result<arf::FlightStreamChunk> read_chunk = result->reader->Next();
    ar_status = result.status();
    if (!ar_status.ok()) {
        *c_error = "Didn't receive a response";
        return;
    }

    batch_ptr = read_chunk->data;
    if (batch_ptr->columns().size() != 1) {
        *c_error = "Received an unexpected number of columns";
        return;
    }

    //
    ArrowArray array_c;
    ArrowSchema schema_c;
    std::shared_ptr<ar::Array> array_ptr = batch_ptr->column(0);
    ar_status = ar::ExportArray(*array_ptr, &array_c, &schema_c);
    if (!ar_status.ok()) {
        *c_error = "Couldn't parse the response";
        return;
    }

    if (read_lengths) {
        auto nulls = (ukv_1x8_t*)array_c.buffers[0];
        auto offs = (ukv_val_len_t*)array_c.buffers[1];
        auto data = (ukv_val_ptr_t)array_c.buffers[2];

        if (c_found_offsets)
            *c_found_offsets = normalize_lengths_with_bitmap(nulls, offs, array_c.length);
        if (c_found_values)
            *c_found_values = data;
        if (c_found_lengths) {
            // TODO: We are forced to allocate and reconstruct the lengths if those were requested
        }
    }
    else {
        auto nulls = (ukv_1x8_t*)array_c.buffers[0];
        auto lens = (ukv_val_len_t*)array_c.buffers[1];
        *c_found_lengths = normalize_lengths_with_bitmap(nulls, lens, array_c.length);
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

    ukv_options_t const c_options,
    ukv_arena_t*,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_val_ptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_val_len_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_val_len_t const> lens {c_lens, c_lens_stride};
    write_tasks_soa_t tasks {cols, keys, vals, offs, lens, c_tasks_count};
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

    ukv_key_t** c_found_keys,
    ukv_val_len_t** c_found_lengths,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_min_keys, c_min_keys_stride};
    strided_iterator_gt<ukv_size_t const> lens {c_scan_lengths, c_scan_lengths_stride};
    scan_tasks_soa_t tasks {cols, keys, lens, c_min_tasks_count};
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

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;
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

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

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

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

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

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    stl_arena_t arena = clean_arena(c_arena, c_error);
    if (*c_error)
        return;

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
}

void ukv_db_control( //
    ukv_t const c_db,
    ukv_str_view_t c_request,
    ukv_str_view_t* c_response,
    ukv_error_t* c_error) {

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    if (!c_request && (*c_error = "Request is NULL!"))
        return;

    *c_response = NULL;
    *c_error = "Controls aren't supported in this implementation!";
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

    if (!c_db && (*c_error = "DataBase is NULL!"))
        return;

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
}

void ukv_txn_commit( //
    ukv_txn_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    if (!c_txn && (*c_error = "Transaction is NULL!"))
        return;
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
