/**
 * @file arrow_client.cpp
 * @author Ashot Vardanian
 *
 * @brief Client library for Apache Arrow RPC server.
 * Converts native UKV operations into Arrows classical `DoPut`, `DoExchange`...
 * Understanding the costs of remote communication, might keep a cache.
 */

#include <thread> // `std::this_thread`

#include <fmt/core.h> // `fmt::format_to`
#include <arrow/c/abi.h>
#include <arrow/flight/client.h>

#include "ukv/db.h"
#include "ukv/arrow.h"
#include "ukv/cpp/types.hpp" // `ukv_doc_field`
#include "helpers/arrow.hpp"

/*********************************************************/
/*****************   Structures & Consts  ****************/
/*********************************************************/

ukv_collection_t const ukv_collection_main_k = 0;
ukv_length_t const ukv_length_missing_k = std::numeric_limits<ukv_length_t>::max();
ukv_key_t const ukv_key_unknown_k = std::numeric_limits<ukv_key_t>::max();
bool const ukv_supports_transactions_k = true;
bool const ukv_supports_named_collections_k = true;
bool const ukv_supports_snapshots_k = true;

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

void ukv_database_init( //
    ukv_str_view_t c_config,
    ukv_database_t* c_db,
    ukv_error_t* c_error) {

#ifdef UKV_DEBUG
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
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_options_t const c_options,

    ukv_octet_t** c_found_presences,

    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_lengths,
    ukv_bytes_ptr_t* c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    places_arg_t places {collections, keys, {}, c_tasks_count};

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && same_collections_are_named(places.collections_begin);
    bool const request_only_presences = c_found_presences && !c_found_lengths && !c_found_values;
    bool const request_only_lengths = c_found_lengths && !c_found_values;
    char const* partial_mode = request_only_presences //
                                   ? kParamReadPartPresences.c_str()
                                   : request_only_lengths //
                                         ? kParamReadPartLengths.c_str()
                                         : nullptr;

    bool const read_shared = c_options & ukv_option_read_shared_memory_k;
    bool const dont_watch = c_options & ukv_option_transaction_dont_watch_k;
    arf::FlightDescriptor descriptor;
    fmt::format_to(std::back_inserter(descriptor.cmd), "{}?", kFlightRead);
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd),
                       "{}=0x{:0>16x}&",
                       kParamTransactionID,
                       std::uintptr_t(c_txn));
    if (same_named_collection)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}=0x{:0>16x}&", kParamCollectionID, collections[0]);
    if (partial_mode)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}={}&", kParamReadPart, partial_mode);
    if (read_shared)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagSharedMemRead);
    if (dont_watch)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagDontWatch);

    bool const has_collections_column = collections && !same_collection;
    constexpr bool has_keys_column = true;

    // If all requests map to the same collection, we can avoid passing its ID
    if (has_collections_column && !collections.is_continuous()) {
        auto continuous = arena.alloc<ukv_collection_t>(places.count, c_error);
        return_on_error(c_error);
        transform_n(collections, places.count, continuous.begin());
        collections = {continuous.begin(), sizeof(ukv_collection_t)};
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
    auto count_collections = has_collections_column + has_keys_column;
    ukv_to_arrow_schema(places.count, count_collections, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_collections_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_doc_field<ukv_collection_t>(),
            nullptr,
            nullptr,
            collections.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "keys",
            ukv_doc_field<ukv_key_t>(),
            nullptr,
            nullptr,
            keys.get(),
            input_schema_c.children[has_collections_column],
            input_array_c.children[has_collections_column],
            c_error);
    return_on_error(c_error);

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    if (batch_ptr->num_rows() == 0)
        return;
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
        *c_found_presences = (ukv_octet_t*)output_array_c.children[0]->buffers[1];
    }
    else if (request_only_lengths) {
        auto presences_ptr = (ukv_octet_t*)output_array_c.children[0]->buffers[0];
        auto lens_ptr = (ukv_length_t*)output_array_c.children[0]->buffers[1];
        if (c_found_lengths)
            *c_found_lengths = presences_ptr //
                                   ? arrow_replace_missing_scalars(presences_ptr,
                                                                   lens_ptr,
                                                                   output_array_c.length,
                                                                   ukv_length_missing_k)
                                   : lens_ptr;
        if (c_found_presences)
            *c_found_presences = presences_ptr;
    }
    else {
        auto presences_ptr = (ukv_octet_t*)output_array_c.children[0]->buffers[0];
        auto offs_ptr = (ukv_length_t*)output_array_c.children[0]->buffers[1];
        auto data_ptr = (ukv_bytes_ptr_t)output_array_c.children[0]->buffers[2];

        if (c_found_presences)
            *c_found_presences = presences_ptr;
        if (c_found_offsets)
            *c_found_offsets = offs_ptr;
        if (c_found_values)
            *c_found_values = data_ptr;

        if (c_found_lengths) {
            auto lens = *c_found_lengths = arena.alloc<ukv_length_t>(places.count, c_error).begin();
            return_on_error(c_error);
            if (presences_ptr) {
                auto presences = bits_view_t(presences_ptr);
                for (std::size_t i = 0; i != places.count; ++i)
                    lens[i] = presences[i] ? (offs_ptr[i + 1] - offs_ptr[i]) : ukv_length_missing_k;
            }
            else {
                for (std::size_t i = 0; i != places.count; ++i)
                    lens[i] = offs_ptr[i + 1] - offs_ptr[i];
            }
        }
    }
}

void ukv_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_keys,
    ukv_size_t const c_keys_stride,

    ukv_octet_t const* c_presences,

    ukv_length_t const* c_offs,
    ukv_size_t const c_offs_stride,

    ukv_length_t const* c_lens,
    ukv_size_t const c_lens_stride,

    ukv_bytes_cptr_t const* c_vals,
    ukv_size_t const c_vals_stride,

    ukv_options_t const c_options,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> keys {c_keys, c_keys_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c_vals, c_vals_stride};
    strided_iterator_gt<ukv_length_t const> offs {c_offs, c_offs_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_lens, c_lens_stride};
    bits_view_t presences {c_presences};

    places_arg_t places {collections, keys, {}, c_tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c_tasks_count};

    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && same_collections_are_named(places.collections_begin);
    bool const write_flush = c_options & ukv_option_write_flush_k;

    bool const has_collections_column = collections && !same_collection;
    constexpr bool has_keys_column = true;
    bool const has_contents_column = vals != nullptr;

    if (has_collections_column && !collections.is_continuous()) {
        auto continuous = arena.alloc<ukv_collection_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(collections, places.size(), continuous.begin());
        collections = {continuous.begin(), places.size()};
    }

    if (has_keys_column && !keys.is_continuous()) {
        auto continuous = arena.alloc<ukv_key_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(keys, places.size(), continuous.begin());
        keys = {continuous.begin(), places.size()};
    }

    // Check if the input is continuous and is already in an Arrow-compatible form
    ukv_bytes_cptr_t joined_vals_begin = vals ? vals[0] : nullptr;
    if (has_contents_column && !contents.is_continuous()) {
        size_t total = transform_reduce_n(contents, places.size(), 0ul, std::mem_fn(&value_view_t::size));
        auto joined_vals = arena.alloc<byte_t>(total, c_error);
        return_on_error(c_error);
        auto joined_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        size_t slots_count = divide_round_up<std::size_t>(places.size(), CHAR_BIT);
        auto slots_presences = arena.alloc<ukv_octet_t>(slots_count, c_error);
        return_on_error(c_error);
        std::memset(slots_presences.begin(), 0, slots_count);
        auto joined_presences = bits_span_t(slots_presences.begin());

        // Exports into the Arrow-compatible form
        ukv_length_t exported_bytes = 0;
        for (std::size_t i = 0; i != c_tasks_count; ++i) {
            auto value = contents[i];
            joined_presences[i] = value;
            joined_offs[i] = exported_bytes;
            std::memcpy(joined_vals.begin() + exported_bytes, value.begin(), value.size());
            exported_bytes += value.size();
        }
        joined_offs[places.size()] = exported_bytes;

        joined_vals_begin = (ukv_bytes_cptr_t)joined_vals.begin();
        vals = {&joined_vals_begin, 0};
        offs = {joined_offs.begin(), sizeof(ukv_key_t)};
        presences = {slots_presences.begin()};
    }
    // It may be the case, that we only have `c_tasks_count` offsets instead of `c_tasks_count+1`,
    // which won't be enough for Arrow.
    else if (has_contents_column && !contents.is_arrow()) {
        auto joined_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        size_t slots_count = divide_round_up<std::size_t>(places.size(), CHAR_BIT);
        auto slots_presences = arena.alloc<ukv_octet_t>(slots_count, c_error);
        return_on_error(c_error);
        std::memset(slots_presences.begin(), 0, slots_count);
        auto joined_presences = bits_span_t(slots_presences.begin());

        // Exports into the Arrow-compatible form
        ukv_length_t exported_bytes = 0;
        for (std::size_t i = 0; i != c_tasks_count; ++i) {
            auto value = contents[i];
            joined_presences[i] = value;
            joined_offs[i] = exported_bytes;
            exported_bytes += value.size();
        }
        joined_offs[places.size()] = exported_bytes;

        vals = {&joined_vals_begin, 0};
        offs = {joined_offs.begin(), sizeof(ukv_key_t)};
        presences = {slots_presences.begin()};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c;
    ArrowSchema input_schema_c;
    auto count_collections = has_collections_column + has_keys_column + has_contents_column;
    ukv_to_arrow_schema(c_tasks_count, count_collections, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_collections_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_doc_field<ukv_collection_t>(),
            nullptr,
            nullptr,
            collections.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            "keys",
            ukv_doc_field<ukv_key_t>(),
            nullptr,
            nullptr,
            keys.get(),
            input_schema_c.children[has_collections_column],
            input_array_c.children[has_collections_column],
            c_error);
    return_on_error(c_error);

    if (has_contents_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgVals.c_str(),
            ukv_doc_field<value_view_t>(),
            presences.get(),
            offs.get(),
            joined_vals_begin,
            input_schema_c.children[has_collections_column + has_keys_column],
            input_array_c.children[has_collections_column + has_keys_column],
            c_error);
    return_on_error(c_error);

    // Send everything over the network and wait for the response
    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    arf::FlightDescriptor descriptor;
    fmt::format_to(std::back_inserter(descriptor.cmd), "{}?", kFlightWrite);
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd),
                       "{}=0x{:0>16x}&",
                       kParamTransactionID,
                       std::uintptr_t(c_txn));
    if (!has_collections_column && collections)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}=0x{:0>16x}&", kParamCollectionID, collections[0]);
    if (write_flush)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagFlushWrite);

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

void ukv_paths_write( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_length_t const* c_paths_offsets,
    ukv_size_t const c_paths_offsets_stride,

    ukv_length_t const* c_paths_lengths,
    ukv_size_t const c_paths_lengths_stride,

    ukv_str_view_t const* c_paths,
    ukv_size_t const c_paths_stride,

    ukv_octet_t const* c_values_presences,

    ukv_length_t const* c_values_offsets,
    ukv_size_t const c_values_offsets_stride,

    ukv_length_t const* c_values_lengths,
    ukv_size_t const c_values_lengths_stride,

    ukv_bytes_cptr_t const* c_values_bytes,
    ukv_size_t const c_values_bytes_stride,

    ukv_options_t const c_options,
    ukv_char_t const c_separator,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_length_t const> path_offs {c_paths_offsets, c_paths_offsets_stride};
    strided_iterator_gt<ukv_length_t const> path_lens {c_paths_lengths, c_paths_lengths_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> paths {reinterpret_cast<ukv_bytes_cptr_t const*>(c_paths),
                                                       c_paths_stride};

    strided_iterator_gt<ukv_bytes_cptr_t const> vals {c_values_bytes, c_values_bytes_stride};
    strided_iterator_gt<ukv_length_t const> offs {c_values_offsets, c_values_offsets_stride};
    strided_iterator_gt<ukv_length_t const> lens {c_values_lengths, c_values_lengths_stride};
    strided_iterator_gt<ukv_octet_t const> presences {c_values_presences, sizeof(ukv_octet_t)};

    places_arg_t places {collections, {}, {}, c_tasks_count};
    contents_arg_t contents {presences, offs, lens, vals, c_tasks_count};
    contents_arg_t path_contents {nullptr, path_offs, path_lens, paths, c_tasks_count, c_separator};

    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && same_collections_are_named(places.collections_begin);
    bool const write_flush = c_options & ukv_option_write_flush_k;

    bool const has_collections_column = collections && !same_collection;
    constexpr bool has_paths_column = true;
    bool const has_contents_column = vals != nullptr;

    if (has_collections_column && !collections.is_continuous()) {
        auto continuous = arena.alloc<ukv_collection_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(collections, places.size(), continuous.begin());
        collections = {continuous.begin(), places.size()};
    }

    ukv_bytes_cptr_t joined_vals_begin = vals ? vals[0] : nullptr;
    if (has_contents_column) {
        auto joined_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        ukv_to_continous_bin(contents, places.size(), c_tasks_count, &joined_vals_begin, joined_offs, arena, c_error);
        offs = {joined_offs.begin(), sizeof(ukv_length_t)};
    }

    ukv_bytes_cptr_t joined_paths_begin = paths[0];
    if (has_paths_column) {
        auto joined_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        ukv_to_continous_bin(path_contents,
                             places.size(),
                             c_tasks_count,
                             &joined_paths_begin,
                             joined_offs,
                             arena,
                             c_error);
        path_offs = {joined_offs.begin(), sizeof(ukv_length_t)};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c;
    ArrowSchema input_schema_c;
    auto count_collections = has_collections_column + has_paths_column + has_contents_column;
    ukv_to_arrow_schema(c_tasks_count, count_collections, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_collections_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_doc_field<ukv_collection_t>(),
            nullptr,
            nullptr,
            collections.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_paths_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgPaths.c_str(),
            ukv_doc_field<ukv_str_view_t>(),
            nullptr,
            path_offs.get(),
            joined_paths_begin,
            input_schema_c.children[has_collections_column],
            input_array_c.children[has_collections_column],
            c_error);
    return_on_error(c_error);

    if (has_contents_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgVals.c_str(),
            ukv_doc_field<value_view_t>(),
            presences.get(),
            offs.get(),
            joined_vals_begin,
            input_schema_c.children[has_collections_column + has_paths_column],
            input_array_c.children[has_collections_column + has_paths_column],
            c_error);
    return_on_error(c_error);

    // Send everything over the network and wait for the response
    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    arf::FlightDescriptor descriptor;
    fmt::format_to(std::back_inserter(descriptor.cmd), "{}?", kFlightWritePath);
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd),
                       "{}=0x{:0>16x}&",
                       kParamTransactionID,
                       std::uintptr_t(c_txn));
    if (!has_collections_column && collections)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}=0x{:0>16x}&", kParamCollectionID, collections[0]);
    if (write_flush)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagFlushWrite);

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

void ukv_paths_match( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_length_t const* c_patterns_offsets,
    ukv_size_t const c_patterns_offsets_stride,

    ukv_length_t const* c_patterns_lengths,
    ukv_size_t const c_patterns_lengths_stride,

    ukv_str_view_t const* c_patterns_strings,
    ukv_size_t const c_patterns_strings_stride,

    ukv_length_t const* c_previous_offsets,
    ukv_size_t const c_previous_offsets_stride,

    ukv_length_t const* c_previous_lengths,
    ukv_size_t const c_previous_lengths_stride,

    ukv_str_view_t const* c_previous,
    ukv_size_t const c_previous_stride,

    ukv_length_t const* c_scan_limits,
    ukv_size_t const c_scan_limits_stride,

    ukv_options_t const c_options,
    ukv_char_t const c_separator,

    ukv_length_t** c_found_counts,
    ukv_length_t** c_found_offsets,
    ukv_char_t** c_found_paths,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_length_t const> scan_limits {c_scan_limits, c_scan_limits_stride};

    strided_iterator_gt<ukv_length_t const> pattern_offs {c_patterns_offsets, c_patterns_offsets_stride};
    strided_iterator_gt<ukv_length_t const> pattern_lens {c_patterns_lengths, c_patterns_lengths_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> patterns {reinterpret_cast<ukv_bytes_cptr_t const*>(c_patterns_strings),
                                                          c_patterns_strings_stride};

    strided_iterator_gt<ukv_length_t const> previous_offs {c_previous_offsets, c_previous_offsets_stride};
    strided_iterator_gt<ukv_length_t const> previous_lens {c_previous_lengths, c_previous_lengths_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> previous {reinterpret_cast<ukv_bytes_cptr_t const*>(c_previous),
                                                          c_previous_stride};

    places_arg_t places {collections, {}, {}, c_tasks_count};
    contents_arg_t pattern_contents {nullptr, pattern_offs, pattern_lens, patterns, c_tasks_count, c_separator};
    contents_arg_t previous_contents {nullptr, previous_offs, previous_lens, previous, c_tasks_count, c_separator};

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && same_collections_are_named(places.collections_begin);
    bool const request_only_counts = c_found_counts && !c_found_paths;
    char const* partial_mode = request_only_counts //
                                   ? kParamReadPartPresences.c_str()
                                   : nullptr;

    bool const read_shared = c_options & ukv_option_read_shared_memory_k;
    bool const dont_watch = c_options & ukv_option_transaction_dont_watch_k;
    arf::FlightDescriptor descriptor;
    fmt::format_to(std::back_inserter(descriptor.cmd), "{}?", kFlightMatchPath);
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd),
                       "{}=0x{:0>16x}&",
                       kParamTransactionID,
                       std::uintptr_t(c_txn));
    if (same_named_collection)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}=0x{:0>16x}&", kParamCollectionID, collections[0]);
    if (partial_mode)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}={}&", kParamReadPart, partial_mode);
    if (read_shared)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagSharedMemRead);
    if (dont_watch)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagDontWatch);

    bool const has_collections_column = collections && !same_collection;
    bool const has_previous_column = previous != nullptr;
    bool const has_limits_column = scan_limits != nullptr;

    // If all requests map to the same collection, we can avoid passing its ID
    if (has_collections_column && !collections.is_continuous()) {
        auto continuous = arena.alloc<ukv_collection_t>(places.count, c_error);
        return_on_error(c_error);
        transform_n(collections, places.count, continuous.begin());
        collections = {continuous.begin(), sizeof(ukv_collection_t)};
    }

    if (has_limits_column && !scan_limits.is_continuous()) {
        auto continuous = arena.alloc<ukv_length_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(scan_limits, places.size(), continuous.begin());
        scan_limits = {continuous.begin(), places.size()};
    }

    ukv_bytes_cptr_t joined_patrns_begin = patterns[0];
    auto joined_patrns_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
    return_on_error(c_error);
    ukv_to_continous_bin(pattern_contents,
                         places.size(),
                         c_tasks_count,
                         &joined_patrns_begin,
                         joined_patrns_offs,
                         arena,
                         c_error);
    pattern_offs = {joined_patrns_offs.begin(), sizeof(ukv_length_t)};

    ukv_bytes_cptr_t joined_prevs_begin;
    if (has_previous_column) {
        joined_prevs_begin = previous[0];
        auto joined_prevs_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        ukv_to_continous_bin(previous_contents,
                             places.size(),
                             c_tasks_count,
                             &joined_prevs_begin,
                             joined_prevs_offs,
                             arena,
                             c_error);
        previous_offs = {joined_prevs_offs.begin(), sizeof(ukv_length_t)};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c, output_array_c;
    ArrowSchema input_schema_c, output_schema_c;
    auto count_collections = has_collections_column + has_limits_column + has_previous_column + 1;
    ukv_to_arrow_schema(places.count, count_collections, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_collections_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_doc_field<ukv_collection_t>(),
            nullptr,
            nullptr,
            collections.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_limits_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgScanLengths.c_str(),
            ukv_doc_field<ukv_length_t>(),
            nullptr,
            nullptr,
            scan_limits.get(),
            input_schema_c.children[has_collections_column],
            input_array_c.children[has_collections_column],
            c_error);
    return_on_error(c_error);

    if (has_previous_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgPrevPatterns.c_str(),
            ukv_doc_field<ukv_str_view_t>(),
            nullptr,
            previous_offs.get(),
            joined_prevs_begin,
            input_schema_c.children[has_collections_column + has_limits_column],
            input_array_c.children[has_collections_column + has_limits_column],
            c_error);
    return_on_error(c_error);

    ukv_to_arrow_column( //
        c_tasks_count,
        kArgPatterns.c_str(),
        ukv_doc_field<ukv_str_view_t>(),
        nullptr,
        pattern_offs.get(),
        joined_patrns_begin,
        input_schema_c.children[has_collections_column + has_limits_column + has_previous_column],
        input_array_c.children[has_collections_column + has_limits_column + has_previous_column],
        c_error);
    return_on_error(c_error);

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    if (batch_ptr->num_rows() == 0)
        return;
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
    return_if_error(output_schema_c.n_children >= 1, c_error, error_unknown_k, "Expecting one or two columns");

    // Export the results into out expected form
    *c_found_counts = (ukv_length_t*)output_array_c.children[0]->buffers[1];
    if (!request_only_counts) {
        auto presences_ptr = (ukv_octet_t*)output_array_c.children[1]->buffers[0];
        auto offs_ptr = (ukv_length_t*)output_array_c.children[1]->buffers[1];
        auto data_ptr = (ukv_bytes_ptr_t)output_array_c.children[1]->buffers[2];

        if (c_found_offsets)
            *c_found_offsets = offs_ptr;
        if (c_found_paths)
            *c_found_paths = reinterpret_cast<ukv_char_t*>(data_ptr);
    }
}

void ukv_paths_read( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_length_t const* c_paths_offsets,
    ukv_size_t const c_paths_offsets_stride,

    ukv_length_t const* c_paths_lengths,
    ukv_size_t const c_paths_lengths_stride,

    ukv_str_view_t const* c_paths,
    ukv_size_t const c_paths_stride,

    ukv_options_t const c_options,
    ukv_char_t const c_separator,

    ukv_octet_t** c_found_presences,
    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_lengths,
    ukv_byte_t** c_found_values,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = prepare_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_length_t const> path_offs {c_paths_offsets, c_paths_offsets_stride};
    strided_iterator_gt<ukv_length_t const> path_lens {c_paths_lengths, c_paths_lengths_stride};
    strided_iterator_gt<ukv_bytes_cptr_t const> paths {reinterpret_cast<ukv_bytes_cptr_t const*>(c_paths),
                                                       c_paths_stride};

    places_arg_t places {collections, {}, {}, c_tasks_count};
    contents_arg_t path_contents {nullptr, path_offs, path_lens, paths, c_tasks_count, c_separator};

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && same_collections_are_named(places.collections_begin);
    bool const request_only_presences = c_found_presences && !c_found_lengths && !c_found_values;
    bool const request_only_lengths = c_found_lengths && !c_found_values;
    char const* partial_mode = request_only_presences //
                                   ? kParamReadPartPresences.c_str()
                                   : request_only_lengths //
                                         ? kParamReadPartLengths.c_str()
                                         : nullptr;

    bool const read_shared = c_options & ukv_option_read_shared_memory_k;
    bool const dont_watch = c_options & ukv_option_transaction_dont_watch_k;
    arf::FlightDescriptor descriptor;
    fmt::format_to(std::back_inserter(descriptor.cmd), "{}?", kFlightReadPath);
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd),
                       "{}=0x{:0>16x}&",
                       kParamTransactionID,
                       std::uintptr_t(c_txn));
    if (same_named_collection)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}=0x{:0>16x}&", kParamCollectionID, collections[0]);
    if (partial_mode)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}={}&", kParamReadPart, partial_mode);
    if (read_shared)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagSharedMemRead);
    if (dont_watch)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagDontWatch);

    bool const has_collections_column = collections && !same_collection;
    constexpr bool has_paths_column = true;

    // If all requests map to the same collection, we can avoid passing its ID
    if (has_collections_column && !collections.is_continuous()) {
        auto continuous = arena.alloc<ukv_collection_t>(places.count, c_error);
        return_on_error(c_error);
        transform_n(collections, places.count, continuous.begin());
        collections = {continuous.begin(), sizeof(ukv_collection_t)};
    }

    // Check if the paths are continuous and are already in an Arrow-compatible form
    ukv_bytes_cptr_t joined_paths_begin = paths[0];
    if (has_paths_column) {
        auto joined_offs = arena.alloc<ukv_length_t>(places.size() + 1, c_error);
        return_on_error(c_error);
        ukv_to_continous_bin(path_contents,
                             places.size(),
                             c_tasks_count,
                             &joined_paths_begin,
                             joined_offs,
                             arena,
                             c_error);
        path_offs = {joined_offs.begin(), sizeof(ukv_length_t)};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c, output_array_c;
    ArrowSchema input_schema_c, output_schema_c;
    auto count_collections = has_collections_column + has_paths_column;
    ukv_to_arrow_schema(places.count, count_collections, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_collections_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_doc_field<ukv_collection_t>(),
            nullptr,
            nullptr,
            collections.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_paths_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgPaths.c_str(),
            ukv_doc_field<ukv_str_view_t>(),
            nullptr,
            path_offs.get(),
            joined_paths_begin,
            input_schema_c.children[has_collections_column],
            input_array_c.children[has_collections_column],
            c_error);
    return_on_error(c_error);

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    if (batch_ptr->num_rows() == 0)
        return;
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
        *c_found_presences = (ukv_octet_t*)output_array_c.children[0]->buffers[1];
    }
    else if (request_only_lengths) {
        auto presences_ptr = (ukv_octet_t*)output_array_c.children[0]->buffers[0];
        auto lens_ptr = (ukv_length_t*)output_array_c.children[0]->buffers[1];
        if (c_found_lengths)
            *c_found_lengths = presences_ptr //
                                   ? arrow_replace_missing_scalars(presences_ptr,
                                                                   lens_ptr,
                                                                   output_array_c.length,
                                                                   ukv_length_missing_k)
                                   : lens_ptr;
        if (c_found_presences)
            *c_found_presences = presences_ptr;
    }
    else {
        auto presences_ptr = (ukv_octet_t*)output_array_c.children[0]->buffers[0];
        auto offs_ptr = (ukv_length_t*)output_array_c.children[0]->buffers[1];
        auto data_ptr = (ukv_bytes_ptr_t)output_array_c.children[0]->buffers[2];

        if (c_found_presences)
            *c_found_presences = presences_ptr;
        if (c_found_offsets)
            *c_found_offsets = offs_ptr;
        if (c_found_values)
            *c_found_values = data_ptr;

        if (c_found_lengths) {
            auto lens = *c_found_lengths = arena.alloc<ukv_length_t>(places.count, c_error).begin();
            return_on_error(c_error);
            if (presences_ptr) {
                auto presences = strided_iterator_gt<ukv_octet_t const>(presences_ptr, sizeof(ukv_octet_t));
                for (std::size_t i = 0; i != places.count; ++i)
                    lens[i] = presences[i] ? (offs_ptr[i + 1] - offs_ptr[i]) : ukv_length_missing_k;
            }
            else {
                for (std::size_t i = 0; i != places.count; ++i)
                    lens[i] = offs_ptr[i + 1] - offs_ptr[i];
            }
        }
    }
}

void ukv_scan( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const c_tasks_count,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_start_keys,
    ukv_size_t const c_start_keys_stride,

    ukv_key_t const* c_end_keys,
    ukv_size_t const c_end_keys_stride,

    ukv_length_t const* c_scan_limits,
    ukv_size_t const c_scan_limits_stride,

    ukv_options_t const c_options,

    ukv_length_t** c_found_offsets,
    ukv_length_t** c_found_lengths,
    ukv_key_t** c_found_keys,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    strided_iterator_gt<ukv_collection_t const> collections {c_collections, c_collections_stride};
    strided_iterator_gt<ukv_key_t const> start_keys {c_start_keys, c_start_keys_stride};
    strided_iterator_gt<ukv_key_t const> end_keys {c_end_keys, c_end_keys_stride};
    strided_iterator_gt<ukv_length_t const> limits {c_scan_limits, c_scan_limits_stride};
    scans_arg_t scans {collections, start_keys, end_keys, limits, c_tasks_count};
    places_arg_t places {collections, start_keys, {}, c_tasks_count};

    bool const same_collection = places.same_collection();
    bool const same_named_collection = same_collection && same_collections_are_named(places.collections_begin);
    bool const write_flush = c_options & ukv_option_write_flush_k;

    bool const has_collections_column = !same_collection;
    constexpr bool has_start_keys_column = true;
    bool const has_end_keys_column = static_cast<bool>(end_keys);
    constexpr bool has_lens_column = true;

    if (has_collections_column && !collections.is_continuous()) {
        auto continuous = arena.alloc<ukv_collection_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(collections, places.size(), continuous.begin());
        collections = {continuous.begin(), places.size()};
    }

    if (has_start_keys_column && !start_keys.is_continuous()) {
        auto continuous = arena.alloc<ukv_key_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(start_keys, places.size(), continuous.begin());
        start_keys = {continuous.begin(), places.size()};
    }

    if (has_end_keys_column && !end_keys.is_continuous()) {
        auto continuous = arena.alloc<ukv_key_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(end_keys, places.size(), continuous.begin());
        end_keys = {continuous.begin(), places.size()};
    }

    if (has_lens_column && !limits.is_continuous()) {
        auto continuous = arena.alloc<ukv_length_t>(places.size(), c_error);
        return_on_error(c_error);
        transform_n(limits, places.size(), continuous.begin());
        limits = {continuous.begin(), places.size()};
    }

    // Now build-up the Arrow representation
    ArrowArray input_array_c, output_array_c;
    ArrowSchema input_schema_c, output_schema_c;
    auto count_collections = has_collections_column + has_start_keys_column + has_lens_column;
    ukv_to_arrow_schema(c_tasks_count, count_collections, &input_schema_c, &input_array_c, c_error);
    return_on_error(c_error);

    if (has_collections_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgCols.c_str(),
            ukv_doc_field<ukv_collection_t>(),
            nullptr,
            nullptr,
            collections.get(),
            input_schema_c.children[0],
            input_array_c.children[0],
            c_error);
    return_on_error(c_error);

    if (has_start_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgScanStarts.c_str(),
            ukv_doc_field<ukv_key_t>(),
            nullptr,
            nullptr,
            start_keys.get(),
            input_schema_c.children[has_collections_column],
            input_array_c.children[has_collections_column],
            c_error);
    return_on_error(c_error);

    if (has_end_keys_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgScanEnds.c_str(),
            ukv_doc_field<ukv_key_t>(),
            nullptr,
            nullptr,
            end_keys.get(),
            input_schema_c.children[has_collections_column + has_start_keys_column],
            input_array_c.children[has_collections_column + has_start_keys_column],
            c_error);
    return_on_error(c_error);

    if (has_lens_column)
        ukv_to_arrow_column( //
            c_tasks_count,
            kArgScanLengths.c_str(),
            ukv_doc_field<ukv_length_t>(),
            nullptr,
            nullptr,
            limits.get(),
            input_schema_c.children[has_collections_column + has_start_keys_column + has_end_keys_column],
            input_array_c.children[has_collections_column + has_start_keys_column + has_end_keys_column],
            c_error);
    return_on_error(c_error);

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    // Configure the `cmd` descriptor
    bool const read_shared = c_options & ukv_option_read_shared_memory_k;
    bool const dont_watch = c_options & ukv_option_transaction_dont_watch_k;
    arf::FlightDescriptor descriptor;
    fmt::format_to(std::back_inserter(descriptor.cmd), "{}?", kFlightScan);
    if (c_txn)
        fmt::format_to(std::back_inserter(descriptor.cmd),
                       "{}=0x{:0>16x}&",
                       kParamTransactionID,
                       std::uintptr_t(c_txn));
    if (same_named_collection)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}=0x{:0>16x}&", kParamCollectionID, collections[0]);
    if (read_shared)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagSharedMemRead);
    if (dont_watch)
        fmt::format_to(std::back_inserter(descriptor.cmd), "{}&", kParamFlagDontWatch);

    // Send the request to server
    ar::Result<std::shared_ptr<ar::RecordBatch>> maybe_batch = ar::ImportRecordBatch(&input_array_c, &input_schema_c);
    return_if_error(maybe_batch.ok(), c_error, error_unknown_k, "Can't pack RecordBatch");

    std::shared_ptr<ar::RecordBatch> batch_ptr = maybe_batch.ValueUnsafe();
    if (batch_ptr->num_rows() == 0)
        return;
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

    auto offs_ptr = (ukv_length_t*)output_array_c.children[0]->buffers[1];
    auto data_ptr = (ukv_key_t*)output_array_c.children[0]->children[0]->buffers[1];

    if (c_found_offsets)
        *c_found_offsets = offs_ptr;
    if (c_found_keys)
        *c_found_keys = data_ptr;
    if (c_found_lengths) {
        auto lens = *c_found_lengths = arena.alloc<ukv_length_t>(places.count, c_error).begin();
        return_on_error(c_error);
        for (std::size_t i = 0; i != places.count; ++i)
            lens[i] = offs_ptr[i + 1] - offs_ptr[i];
    }
}

void ukv_size( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_size_t const n,

    ukv_collection_t const* c_collections,
    ukv_size_t const c_collections_stride,

    ukv_key_t const* c_start_keys,
    ukv_size_t const c_start_keys_stride,

    ukv_key_t const* c_end_keys,
    ukv_size_t const c_end_keys_stride,

    ukv_options_t const c_options,

    ukv_size_t** c_min_cardinalities,
    ukv_size_t** c_max_cardinalities,
    ukv_size_t** c_min_value_bytes,
    ukv_size_t** c_max_value_bytes,
    ukv_size_t** c_min_space_usages,
    ukv_size_t** c_max_space_usages,

    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);
}

/*********************************************************/
/*****************	Collections Management	****************/
/*********************************************************/

void ukv_collection_init(
    // Inputs:
    ukv_database_t const c_db,
    ukv_str_view_t c_collection_name,
    ukv_str_view_t c_collection_config,
    // Outputs:
    ukv_collection_t* c_collection,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    if (!c_collection_name || !std::strlen(c_collection_name)) {
        *c_collection = ukv_collection_main_k;
        return;
    }

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    // TODO: Can we somehow reuse the IPC-needed memory?
    // Do we need to add that arena argument to every call?
    // ar::Status ar_status;
    // arrow_mem_pool_t pool(arena);
    // arf::FlightCallOptions options = arrow_call_options(pool);

    arf::Action action;
    fmt::format_to(std::back_inserter(action.type),
                   "{}?{}={}",
                   kFlightColOpen,
                   kParamCollectionName,
                   c_collection_name);
    if (c_collection_config)
        action.body = std::make_shared<ar::Buffer>(ar::util::string_view {c_collection_config});

    ar::Result<std::unique_ptr<arf::ResultStream>> maybe_stream = db.flight->DoAction(action);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");

    auto& stream_ptr = maybe_stream.ValueUnsafe();
    ar::Result<std::unique_ptr<arf::Result>> maybe_id = stream_ptr->Next();
    return_if_error(maybe_id.ok(), c_error, network_k, "No response received");

    auto& id_ptr = maybe_id.ValueUnsafe();
    return_if_error(id_ptr->body->size() == sizeof(ukv_collection_t), c_error, error_unknown_k, "Inadequate response");
    std::memcpy(c_collection, id_ptr->body->data(), sizeof(ukv_collection_t));
}

void ukv_collection_drop(
    // Inputs:
    ukv_database_t const c_db,
    ukv_collection_t c_collection_id,
    ukv_drop_mode_t c_mode,
    // Outputs:
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    std::string_view mode;
    switch (c_mode) {
    case ukv_drop_vals_k: mode = kParamDropModeValues; break;
    case ukv_drop_keys_vals_k: mode = kParamDropModeContents; break;
    case ukv_drop_keys_vals_handle_k: mode = kParamDropModeCollection; break;
    }

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    // TODO: Can we somehow reuse the IPC-needed memory?
    // Do we need to add that arena argument to every call?
    // ar::Status ar_status;
    // arrow_mem_pool_t pool(arena);
    // arf::FlightCallOptions options = arrow_call_options(pool);

    arf::Action action;
    fmt::format_to(std::back_inserter(action.type),
                   "{}?{}=0x{:0>16x}&{}={}",
                   kFlightColDrop,
                   kParamCollectionID,
                   c_collection_id,
                   kParamDropMode,
                   mode);

    ar::Result<std::unique_ptr<arf::ResultStream>> maybe_stream = db.flight->DoAction(action);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");
}

void ukv_collection_list( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_options_t const c_options,
    ukv_size_t* c_count,
    ukv_collection_t** c_ids,
    ukv_length_t** c_offsets,
    ukv_char_t** c_names,
    ukv_arena_t* c_arena,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");

    stl_arena_t arena = make_stl_arena(c_arena, c_options, c_error);
    return_on_error(c_error);

    ar::Status ar_status;
    arrow_mem_pool_t pool(arena);
    arf::FlightCallOptions options = arrow_call_options(pool);

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);

    arf::Ticket ticket {kFlightListCols};
    if (c_txn)
        fmt::format_to(std::back_inserter(ticket.ticket), "?{}=0x{:0>16x}", kParamTransactionID, std::uintptr_t(c_txn));
    ar::Result<std::unique_ptr<arf::FlightStreamReader>> maybe_stream = db.flight->DoGet(ticket);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");

    auto& stream_ptr = maybe_stream.ValueUnsafe();
    ar::Result<std::shared_ptr<ar::Table>> maybe_table = stream_ptr->ToTable();

    ArrowSchema schema_c;
    ArrowArray batch_c;
    ar_status = unpack_table(maybe_table, schema_c, batch_c);
    return_if_error(ar_status.ok(), c_error, args_combo_k, "Failed to unpack list of columns");

    auto ids_column_idx = column_idx(schema_c, kArgCols);
    auto names_column_idx = column_idx(schema_c, kArgNames);
    return_if_error(ids_column_idx && names_column_idx, c_error, args_combo_k, "Expecting two columns");

    if (c_count)
        *c_count = static_cast<ukv_size_t>(batch_c.length);
    if (c_ids)
        *c_ids = (ukv_collection_t*)batch_c.children[*ids_column_idx]->buffers[1];
    if (c_offsets)
        *c_offsets = (ukv_length_t*)batch_c.children[*names_column_idx]->buffers[1];
    if (c_names)
        *c_names = (ukv_str_span_t)batch_c.children[*names_column_idx]->buffers[2];
}

void ukv_database_control( //
    ukv_database_t const c_db,
    ukv_str_view_t c_request,
    ukv_char_t** c_response,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c_request, c_error, uninitialized_state_k, "Request is uninitialized");

    *c_response = NULL;
    log_error(c_error, missing_feature_k, "Controls aren't supported in this implementation!");
}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_transaction_init(
    // Inputs:
    ukv_database_t const c_db,
    ukv_options_t const c_options,
    // Outputs:
    ukv_transaction_t* c_txn,
    ukv_error_t* c_error) {

    return_if_error(c_db, c_error, uninitialized_state_k, "DataBase is uninitialized");
    return_if_error(c_txn, c_error, uninitialized_state_k, "Transaction is uninitialized");

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    // TODO: Can we somehow reuse the IPC-needed memory?
    // Do we need to add that arena argument to every call?
    // ar::Status ar_status;
    // arrow_mem_pool_t pool(arena);
    // arf::FlightCallOptions options = arrow_call_options(pool);

    arf::Action action;
    ukv_size_t txn_id = *reinterpret_cast<ukv_size_t*>(c_txn);
    fmt::format_to(std::back_inserter(action.type), "{}?", kFlightTxnBegin);
    if (txn_id != 0)
        fmt::format_to(std::back_inserter(action.type), "{}=0x{:0>16x}&", kParamTransactionID, txn_id);
    if (c_options & ukv_option_transaction_snapshot_k)
        fmt::format_to(std::back_inserter(action.type), "{}&", kParamFlagSnapshotTxn);

    ar::Result<std::unique_ptr<arf::ResultStream>> maybe_stream = db.flight->DoAction(action);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");

    auto& stream_ptr = maybe_stream.ValueUnsafe();
    ar::Result<std::unique_ptr<arf::Result>> maybe_id = stream_ptr->Next();
    return_if_error(maybe_id.ok(), c_error, network_k, "No response received");

    auto& id_ptr = maybe_id.ValueUnsafe();
    return_if_error(id_ptr->body->size() == sizeof(ukv_transaction_t), c_error, error_unknown_k, "Inadequate response");
    std::memcpy(c_txn, id_ptr->body->data(), sizeof(ukv_transaction_t));
}

void ukv_transaction_commit( //
    ukv_database_t const c_db,
    ukv_transaction_t const c_txn,
    ukv_options_t const c_options,
    ukv_error_t* c_error) {

    return_if_error(c_txn, c_error, uninitialized_state_k, "Transaction is uninitialized");

    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    // TODO: Can we somehow reuse the IPC-needed memory?
    // Do we need to add that arena argument to every call?
    // ar::Status ar_status;
    // arrow_mem_pool_t pool(arena);
    // arf::FlightCallOptions options = arrow_call_options(pool);

    arf::Action action;
    fmt::format_to(std::back_inserter(action.type),
                   "{}?{}=0x{:0>16x}&",
                   kFlightTxnCommit,
                   kParamTransactionID,
                   std::uintptr_t(c_txn));
    if (c_options & ukv_option_write_flush_k)
        fmt::format_to(std::back_inserter(action.type), "{}&", kParamFlagFlushWrite);

    ar::Result<std::unique_ptr<arf::ResultStream>> maybe_stream = db.flight->DoAction(action);
    return_if_error(maybe_stream.ok(), c_error, network_k, "Failed to act on Arrow server");
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_database_t const, ukv_arena_t c_arena) {
    if (!c_arena)
        return;
    stl_arena_t& arena = *reinterpret_cast<stl_arena_t*>(c_arena);
    delete &arena;
}

void ukv_transaction_free(ukv_database_t const, ukv_transaction_t const c_txn) {
    if (!c_txn)
        return;
}

void ukv_database_free(ukv_database_t c_db) {
    if (!c_db)
        return;
    rpc_client_t& db = *reinterpret_cast<rpc_client_t*>(c_db);
    delete &db;
}

void ukv_collection_free(ukv_database_t const, ukv_collection_t const) {
    // In this in-memory freeing the collection handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_error_free(ukv_error_t) {
}
