/**
 * @file arrow_client.cpp
 * @author Ashot Vardanian
 *
 * @brief Client library for Apache Arrow RPC server.
 * Converts native UKV operations into Arrows classical `DoPut`, `DoExchange`...
 * Understanding the costs of remote communication, might keep a cache.
 */

#include <unordered_map>

#include <fmt/core.h>
#include <arrow/flight/client.h>

#include "ukv/db.h"

#include "helpers.hpp"
#include "arrow_helpers.hpp"
#include "ukv/arrow.h"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_col_t ukv_col_main_k = 0;
ukv_val_len_t ukv_val_len_missing_k = std::numeric_limits<ukv_val_len_t>::max();
ukv_key_t ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

using namespace unum::ukv;
using namespace unum;

struct rpc_client_t {
    std::unique_ptr<arf::FlightClient> flight;
};

arf::FlightCallOptions arrow_call_options(arrow_mem_pool_t& pool) {
    arf::FlightCallOptions options;
    options.read_options = arrow_read_options(pool);
    options.write_options = arrow_write_options(pool);
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

    ukv_1x8_t** c_found_presences,

    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    ukv_val_ptr_t* c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {cols, keys, {}, c_tasks_count};

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && places.same_collections_are_named();
    bool const request_only_presences = c_found_presences && !c_found_lengths && !c_found_values;
    bool const request_only_lengths = c_found_lengths && !c_found_values;
    char const* partial_mode = !request_only_presences && !request_only_lengths //
                                   ? nullptr
                                   : request_only_lengths ? "lengths" : "presences";
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
    ArrowArray input_array_c, output_array_c;
    ArrowSchema input_schema_c, output_schema_c;
    auto count_cols = has_cols_column + has_keys_column;
    ukv_to_arrow_schema(places.count, count_cols, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_cols_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_type<ukv_col_t>(),
            nullptr,
            nullptr,
            cols.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
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
            input_schema_c.children[has_cols_column],
            input_array_c.children[has_cols_column],
            c_error);
    return_on_error(c_error);

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    ar::Result<arf::FlightClient::DoExchangeResult> result = db.flight->DoExchange(options, descriptor);
    return_if_error(result.ok(), c_error, network_k, "Failed to exchange with Arrow server");

    ar_status = result->writer->Begin(batch_ptr->schema());
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing schema");

    auto table = ar::Table::Make(batch_ptr->schema(), batch_ptr->columns(), static_cast<int64_t>(places.size()));
    ar_status = result->writer->WriteTable(*table);
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing request");

    ar_status = result->writer->DoneWriting();
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Submitting request");

    // Fetch the responses
    // Requesting `ToTable` might be more efficient than concatenating and
    // reallocating directly from our arena, as the underlying Arrow implementation
    // may know the length of the entire dataset.
    ar_status = unpack_table(result->reader->ToTable(), output_schema_c, output_array_c);
    return_if_error(ar_status.ok(), c_error, network_k, "No response");

    // Convert the responses in Arrow C form
    return_if_error(output_schema_c.n_children == 1, c_error, error_unknown_k, "Expecting one column");

    // Export the results into out expected form
    if (request_only_presences) {
        *c_found_presences = (ukv_1x8_t*)output_array_c.children[0]->buffers[1];
    }
    else if (request_only_lengths) {
        auto presences_ptr = (ukv_1x8_t*)output_array_c.children[0]->buffers[0];
        auto lens_ptr = (ukv_val_len_t*)output_array_c.children[0]->buffers[1];
        if (c_found_lengths)
            *c_found_lengths = presences_ptr //
                                   ? arrow_replace_missing_scalars(presences_ptr,
                                                                   lens_ptr,
                                                                   output_array_c.length,
                                                                   ukv_val_len_missing_k)
                                   : lens_ptr;
        if (c_found_presences)
            *c_found_presences = presences_ptr;
    }
    else {
        auto presences_ptr = (ukv_1x8_t*)output_array_c.children[0]->buffers[0];
        auto offs_ptr = (ukv_val_len_t*)output_array_c.children[0]->buffers[1];
        auto data_ptr = (ukv_val_ptr_t)output_array_c.children[0]->buffers[2];

        if (c_found_presences)
            *c_found_presences = presences_ptr;
        if (c_found_offsets)
            *c_found_offsets = offs_ptr;
        if (c_found_values)
            *c_found_values = data_ptr;

        if (c_found_lengths) {
            auto lens = *c_found_lengths = arena.alloc<ukv_val_len_t>(places.count, c_error).begin();
            return_on_error(c_error);
            if (presences_ptr) {
                auto presences = strided_iterator_gt<ukv_1x8_t const>(presences_ptr, sizeof(ukv_1x8_t));
                for (std::size_t i = 0; i != places.count; ++i)
                    lens[i] = presences[i] ? (offs_ptr[i + 1] - offs_ptr[i]) : ukv_val_len_missing_k;
            }
            else {
                for (std::size_t i = 0; i != places.count; ++i)
                    lens[i] = offs_ptr[i + 1] - offs_ptr[i];
            }
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

    ukv_1x8_t const* c_presences,

    ukv_val_len_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_val_len_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_val_ptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
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
        auto slots_count = divide_round_up<std::size_t>(places.size(), CHAR_BIT);
        auto slots_presences = arena.alloc<ukv_1x8_t>(slots_count, c_error);
        return_on_error(c_error);
        std::memset(slots_presences.begin(), 0, slots_count);
        auto joined_presences = strided_iterator_gt<ukv_1x8_t>(slots_presences.begin(), sizeof(ukv_1x8_t));

        // Exports into the Arrow-compatible form
        ukv_val_len_t exported_bytes = 0;
        for (std::size_t i = 0; i != c_tasks_count; ++i) {
            auto value = contents[i];
            joined_presences[i] = value;
            joined_offs[i] = exported_bytes;
            std::memcpy(joined_vals.begin() + exported_bytes, value.begin(), value.size());
            exported_bytes += value.size();
        }
        joined_offs[places.size()] = exported_bytes;

        joined_vals_begin = (ukv_val_ptr_t)joined_vals.begin();
        vals = {&joined_vals_begin, 0};
        offs = {joined_offs.begin(), sizeof(ukv_key_t)};
        presences = {slots_presences.begin(), sizeof(ukv_1x8_t)};
    }
    // It may be the case, that we only have `c_tasks_count` offsets instead of `c_tasks_count+1`,
    // which won't be enough for Arrow.
    else if (has_contents_column && !contents.is_arrow()) {
        auto joined_offs = arena.alloc<ukv_val_len_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        auto slots_count = divide_round_up<std::size_t>(places.size(), CHAR_BIT);
        auto slots_presences = arena.alloc<ukv_1x8_t>(slots_count, c_error);
        return_on_error(c_error);
        std::memset(slots_presences.begin(), 0, slots_count);
        auto joined_presences = strided_iterator_gt<ukv_1x8_t>(slots_presences.begin(), sizeof(ukv_1x8_t));

        // Exports into the Arrow-compatible form
        ukv_val_len_t exported_bytes = 0;
        for (std::size_t i = 0; i != c_tasks_count; ++i) {
            auto value = contents[i];
            joined_presences[i] = value;
            joined_offs[i] = exported_bytes;
            exported_bytes += value.size();
        }
        joined_offs[places.size()] = exported_bytes;

        vals = {&joined_vals_begin, 0};
        offs = {joined_offs.begin(), sizeof(ukv_key_t)};
        presences = {slots_presences.begin(), sizeof(ukv_1x8_t)};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c;
    ArrowSchema input_schema_c;
    auto count_cols = has_cols_column + has_keys_column + has_contents_column;
    ukv_to_arrow_schema(c_tasks_count, count_cols, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_cols_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_type<ukv_col_t>(),
            nullptr,
            nullptr,
            cols.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
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
            input_schema_c.children[has_cols_column],
            input_array_c.children[has_cols_column],
            c_error);
    return_on_error(c_error);

    if (has_contents_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgVals.c_str(),
            ukv_type<value_view_t>(),
            presences.get(),
            offs.get(),
            joined_vals_begin,
            input_schema_c.children[has_cols_column + has_keys_column],
            input_array_c.children[has_cols_column + has_keys_column],
            c_error);
    return_on_error(c_error);

    // Send everything over the network and wait for the response
    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

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
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    ar::Result<arf::FlightClient::DoPutResult> result = db.flight->DoPut(options, descriptor, batch_ptr->schema());
    return_if_error(result.ok(), c_error, network_k, "Failed to exchange with Arrow server");

    // This writer has already been started!
    // ar_status = result->writer->Begin(batch_ptr->schema());
    // return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing schema");

    auto table = ar::Table::Make(batch_ptr->schema(), batch_ptr->columns(), static_cast<int64_t>(places.size()));
    ar_status = result->writer->WriteTable(*table);
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
    ukv_size_t const c_tasks_count,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_start_keys,
    ukv_size_t const c_start_keys_stride,

    ukv_val_len_t const* c_scan_lengths,
    ukv_size_t const c_scan_lengths_stride,

    ukv_options_t const c_options,

    ukv_val_len_t** c_found_offsets,
    ukv_val_len_t** c_found_lengths,
    ukv_key_t** c_found_keys,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_col_t const> cols {c_cols, c_cols_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_val_len_t const> lens {c_scan_lengths, c_scan_lengths_stride};
    scans_arg_t scans {cols, keys, lens, c_tasks_count};
    places_arg_t places {cols, keys, {}, c_tasks_count};

    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && places.same_collections_are_named();
    bool const write_flush = c_options & ukv_option_write_flush_k;

    bool const has_cols_column = !same_collection;
    constexpr bool has_keys_column = true;
    constexpr bool has_lens_column = true;

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

    if (has_lens_column && !lens.is_continuous()) {
        auto continuous = arena.alloc<ukv_val_len_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(lens, places.size(), continuous.begin());
        lens = {continuous.begin(), places.size()};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c, output_array_c;
    ArrowSchema input_schema_c, output_schema_c;
    auto count_cols = has_cols_column + has_keys_column + has_lens_column;
    ukv_to_arrow_schema(c_tasks_count, count_cols, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_cols_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_type<ukv_col_t>(),
            nullptr,
            nullptr,
            cols.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgScanStarts.c_str(),
            ukv_type<ukv_key_t>(),
            nullptr,
            nullptr,
            keys.get(),
            input_schema_c.children[has_cols_column],
            input_array_c.children[has_cols_column],
            c_error);
    return_on_error(c_error);

    if (has_lens_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgScanLengths.c_str(),
            ukv_type<ukv_val_len_t>(),
            nullptr,
            nullptr,
            lens.get(),
            input_schema_c.children[has_cols_column + has_keys_column],
            input_array_c.children[has_cols_column + has_keys_column],
            c_error);
    return_on_error(c_error);

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    bool const read_shared = c_options & ukv_option_read_shared_k;
    bool const read_track = c_options & ukv_option_read_track_k;
    arf::FlightDescriptor descriptor;
    descriptor.cmd = "scan?";
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd), "txn={:x}&", std::uintptr_t(c_txn));
    if (same_named_collection)
        fmt::format_to(std::back_inserter(descriptor.cmd), "col={:x}&", cols[0]);
    if (read_shared)
        descriptor.cmd.append("shared&");
    if (read_track)
        descriptor.cmd.append("track&");

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    ar::Result<arf::FlightClient::DoExchangeResult> result = db.flight->DoExchange(options, descriptor);
    return_if_error(result.ok(), c_error, network_k, "Failed to exchange with Arrow server");

    ar_status = result->writer->Begin(batch_ptr->schema());
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing schema");

    auto table = ar::Table::Make(batch_ptr->schema(), batch_ptr->columns(), static_cast<int64_t>(places.size()));
    ar_status = result->writer->WriteTable(*table);
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Serializing request");

    ar_status = result->writer->DoneWriting();
    return_if_error(ar_status.ok(), c_error, error_unknown_k, "Submitting request");

    // Fetch the responses
    // Requesting `ToTable` might be more efficient than concatenating and
    // reallocating directly from our arena, as the underlying Arrow implementation
    // may know the length of the entire dataset.
    ar_status = unpack_table(result->reader->ToTable(), output_schema_c, output_array_c);
    return_if_error(ar_status.ok(), c_error, network_k, "No response");

    // Convert the responses in Arrow C form
    return_if_error(output_schema_c.n_children == 1, c_error, error_unknown_k, "Expecting one column");
    return_if_error(output_schema_c.children[0]->n_children == 1, c_error, error_unknown_k, "Expecting one sub-column");

    auto offs_ptr = (ukv_val_len_t*)output_array_c.children[0]->buffers[1];
    auto data_ptr = (ukv_key_t*)output_array_c.children[0]->children[0]->buffers[1];

    if (c_found_offsets)
        *c_found_offsets = offs_ptr;
    if (c_found_keys)
        *c_found_keys = data_ptr;
    if (c_found_lengths) {
        auto lens = *c_found_lengths = arena.alloc<ukv_val_len_t>(places.count, c_error).begin();
        return_on_error(c_error);
        for (std::size_t i = 0; i != places.count; ++i)
            lens[i] = offs_ptr[i + 1] - offs_ptr[i];
    }
}

void ukv_size( //
    ukv_t const c_db,
    ukv_txn_t const c_txn,
    ukv_size_t const n,

    ukv_col_t const* c_cols,
    ukv_size_t const c_cols_stride,

    ukv_key_t const* c_start_keys,
    ukv_size_t const c_start_keys_stride,

    ukv_key_t const* c_end_keys,
    ukv_size_t const c_end_keys_stride,

    ukv_options_t const,

    ukv_size_t** c_found_estimates,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ukv_col_upsert(
    // Inputs:
    ukv_t const c_db,
    ukv_str_view_t c_col_name,
    ukv_str_view_t c_col_config,
    // Outputs:
    ukv_col_t* c_col,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    if (!c_col_name || !std::strlen(c_col_name)) {
        *c_col = ukv_col_main_k;
        return;
    }

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    // TODO: Can we somehow reuse the IPC-needed memory?
    // Do we need to add that arena argument to every call?
    // ar::Status ar_status;
    // arrow_mem_pool_t pool(arena);
    // arf::FlightCallOptions options = arrow_call_options(pool);

    arf::Action action;
    fmt::format_to(std::back_inserter(action.type), "col_upsert?col={}", c_col_name);
    if (c_col_config)
        action.body = std::make_shared<ar::Buffer>(ar::util::string_view {c_col_config});

    ar::Result<std::unique_ptr<arf::ResultStream>> maybe_stream = db.flight->DoAction(action);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");

    auto& stream_ptr = maybe_stream.ValueUnsafe();
    ar::Result<std::unique_ptr<arf::Result>> maybe_id = stream_ptr->Next();
    return_if_error(maybe_id.ok(), c_error, network_k, "No response received");

    auto& id_ptr = maybe_id.ValueUnsafe();
    std::memcpy(c_col, id_ptr->body->data(), sizeof(ukv_col_t));
}

void ukv_col_drop(
    // Inputs:
    ukv_t const c_db,
    ukv_col_t c_col_id,
    ukv_str_view_t c_col_name,
    ukv_col_drop_mode_t c_mode,
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

    stl_arena_t arena = prepare_arena(c_arena, {}, c_error);
    return_on_error(c_error);

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);

    arf::Ticket ticket {kFlightListCols};
    ar::Result<std::unique_ptr<arf::FlightStreamReader>> maybe_stream = db.flight->DoGet(ticket);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");

    auto& stream_ptr = maybe_stream.ValueUnsafe();
    ar::Result<std::shared_ptr<ar::Table>> maybe_table = stream_ptr->ToTable();

    ArrowSchema schema_c;
    ArrowArray batch_c;
    ar_status = unpack_table(maybe_table, schema_c, batch_c);
    return_if_error(ar_status.ok(), c_error, args_combo_k, "Failed to unpack list of columns");

    auto ids_column_idx = column_idx(schema_c, "cols");
    auto names_column_idx = column_idx(schema_c, "names");
    return_if_error(ids_column_idx && names_column_idx, c_error, args_combo_k, "Expecting two columns");

    if (c_count)
        *c_count = static_cast<ukv_size_t>(batch_c.length);
    if (c_ids)
        *c_ids = (ukv_col_t*)batch_c.children[*ids_column_idx]->buffers[1];
    if (c_offsets)
        *c_offsets = (ukv_val_len_t*)batch_c.children[*names_column_idx]->buffers[1];
    if (c_names)
        *c_names = (ukv_str_view_t)batch_c.children[*names_column_idx]->buffers[2];
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
