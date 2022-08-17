/**
 * @file arrow_server.cpp
 * @author Ashot Vardanian
 * @date 2022-07-18
 *
 * @brief An server implementing Apache Arrow Flight RPC protocol.
 *
 * Links:
 * https://arrow.apache.org/cookbook/cpp/flight.html
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#include <arrow/type.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/pretty_print.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/c/bridge.h> // `ExportSchema`
#pragma GCC diagnostic pop

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>
#include <iostream>

#define ARROW_C_DATA_INTERFACE 1
#define ARROW_C_STREAM_INTERFACE 1
#include "ukv/arrow.h"
#include "ukv/cpp/db.hpp"

namespace arf = arrow::flight;
namespace ar = arrow;

using namespace unum::ukv;
using namespace unum;

expected_gt<std::size_t> column_idx(ArrowSchema* schema_ptr, std::string_view name) {
    auto begin = schema_ptr->children;
    auto end = begin + schema_ptr->n_children;
    auto it = std::find_if(begin, end, [=](ArrowSchema* column_schema) {
        return std::string_view {column_schema->name} == name;
    });
    if (it == end)
        return status_t {"Column not found!"};
    return static_cast<std::size_t>(it - begin);
}

bool validate_column_cols(ArrowSchema* schema_ptr, ArrowArray* column_ptr) {
    if (schema_ptr->format != ukv_type_to_arrow_format(ukv_type<ukv_col_t>()))
        return false;
    if (column_ptr->null_count != 0)
        return false;
    return true;
}

bool validate_column_keys(ArrowSchema* schema_ptr, ArrowArray* column_ptr) {
    if (schema_ptr->format != ukv_type_to_arrow_format(ukv_type<ukv_key_t>()))
        return false;
    if (column_ptr->null_count != 0)
        return false;
    return true;
}

bool validate_column_vals(ArrowSchema* schema_ptr, ArrowArray* column_ptr) {
    if (schema_ptr->format != ukv_type_to_arrow_format(ukv_type<value_view_t>()))
        return false;
    if (column_ptr->null_count != 0)
        return false;
    return true;
}

class UKVService : public arf::FlightServerBase {
  public:
    inline static arf::ActionType const kActionColOpen {"col_open", "Find a collection descriptor by name."};
    inline static arf::ActionType const kActionColRemove {"col_remove", "Delete a named collection."};
    inline static arf::ActionType const kActionColList {"col_list", "Lists all named collections."};
    inline static arf::ActionType const kActionTxnBegin {"txn_begin", "Starts an ACID transaction and returns its ID."};
    inline static arf::ActionType const kActionTxnCommit {"txn_commit", "Commit a previously started transaction."};

    inline static std::string const kOpRead = "read";
    inline static std::string const kOpWrite = "write";
    inline static std::string const kOpScan = "scan";
    inline static std::string const kOpSize = "size";

    inline static std::string const kArgCols = "cols";
    inline static std::string const kArgKeys = "keys";
    inline static std::string const kArgVals = "vals";
    inline static std::string const kArgFields = "fields";

    db_t db_;

    UKVService(db_t&& db) noexcept : db_(std::move(db)) {}

    ar::Status ListActions( //
        arf::ServerCallContext const&,
        std::vector<arf::ActionType>* actions) override {
        *actions = {kActionColOpen, kActionColRemove, kActionColList, kActionTxnBegin, kActionTxnCommit};
        return ar::Status::OK();
    }

    ar::Status ListFlights( //
        arf::ServerCallContext const&,
        arf::Criteria const*,
        std::unique_ptr<arf::FlightListing>*) override {
        return ar::Status::OK();
    }

    ar::Status GetFlightInfo( //
        arf::ServerCallContext const&,
        arf::FlightDescriptor const&,
        std::unique_ptr<arf::FlightInfo>*) override {
        // ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
        // ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
        // *info = std::make_unique<arf::FlightInfo>(std::move(flight_info));
        return ar::Status::OK();
    }

    ar::Status GetSchema( //
        arf::ServerCallContext const&,
        arf::FlightDescriptor const&,
        std::unique_ptr<arf::SchemaResult>*) override {
        return ar::Status::OK();
    }

    ar::Status DoAction( //
        arf::ServerCallContext const&,
        arf::Action const& action,
        std::unique_ptr<arf::ResultStream>*) override {

        if (action.type == kActionColOpen.type) {
        }
        return ar::Status::NotImplemented("Unknown action type: ", action.type);
    }

    ar::Status DoExchange( //
        arf::ServerCallContext const& context,
        std::unique_ptr<arf::FlightMessageReader> request_ptr,
        std::unique_ptr<arf::FlightMessageWriter> response_ptr) {

        arf::FlightMessageReader& request = *request_ptr;
        arf::FlightMessageWriter& response = *response_ptr;
        arf::FlightDescriptor const& desc = request.descriptor();
        std::string const& cmd = desc.cmd;

        std::shared_ptr<ar::Table> const& full = request.ToTable().ValueOrDie();
        std::vector<std::shared_ptr<ar::RecordBatch>> const& batches = request.ToRecordBatches().ValueOrDie();

        if (desc.cmd == kOpRead) {
        }
        return ar::Status::OK();
    }

    ar::Status DoPut( //
        arf::ServerCallContext const& context,
        std::unique_ptr<arf::FlightMessageReader> request_ptr,
        std::unique_ptr<arf::FlightMetadataWriter> response_ptr) override {

        arf::FlightMessageReader& request = *request_ptr;
        arf::FlightMetadataWriter& response = *response_ptr;
        arf::FlightDescriptor const& desc = request.descriptor();
        std::string const& cmd = desc.cmd;
        std::shared_ptr<ar::Schema> const& schema_ptr = request.GetSchema().ValueOrDie();
        ArrowSchema schema_c;
        auto _ = ar::ExportSchema(*schema_ptr, &schema_c);

        std::optional<std::size_t> idx_cols = column_idx(&schema_c, kArgCols);
        std::optional<std::size_t> idx_keys = column_idx(&schema_c, kArgKeys);
        std::optional<std::size_t> idx_vals = column_idx(&schema_c, kArgVals);

        if (desc.cmd == kOpWrite) {

            while (true) {
                arf::FlightStreamChunk const& chunk = request.Next().ValueOrDie();
                std::shared_ptr<ar::RecordBatch> const& batch_ptr = chunk.data;
                if (!batch_ptr)
                    break;

                ArrowArray batch_c;
                auto _ = ar::ExportRecordBatch(*batch_ptr, &batch_c, nullptr);

                ArrowArray* keys_c = batch_c.children[*idx_keys];
                ArrowArray* vals_c = batch_c.children[*idx_vals];
                status_t status;
                arena_t arena(db_);
                ukv_write( //
                    db_,
                    nullptr,
                    keys_c->length,
                    nullptr,
                    0,
                    (ukv_key_t const*)keys_c->buffers[1],
                    sizeof(ukv_key_t),
                    (ukv_val_ptr_t const*)&vals_c->buffers[2],
                    0,
                    (ukv_val_len_t const*)vals_c->buffers[1],
                    sizeof(ukv_val_len_t),
                    nullptr,
                    0,
                    ukv_options_default_k,
                    arena.member_ptr(),
                    status.member_ptr());
            }
        }

        return ar::Status::OK();
    }

    ar::Status DoGet( //
        arf::ServerCallContext const&,
        arf::Ticket const&,
        std::unique_ptr<arf::FlightDataStream>*) override {
        return ar::Status::OK();
    }
};

ar::Status run_server() {
    db_t db;
    db.open().throw_unhandled();

    arf::Location server_location = arf::Location::ForGrpcTcp("0.0.0.0", 38709).ValueOrDie();
    arf::FlightServerOptions options(server_location);
    auto server = std::make_unique<UKVService>(std::move(db));
    ARROW_RETURN_NOT_OK(server->Init(options));
    std::cout << "Listening on port " << server->port() << std::endl;
    return server->Serve();
}

//------------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    return run_server().ok() ? EXIT_SUCCESS : EXIT_FAILURE;
}
