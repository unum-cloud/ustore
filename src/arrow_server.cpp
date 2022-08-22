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

#include <unordered_map>
#include <unordered_set>
#include <chrono> // `std::time_point`
#include <mutex>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#include <arrow/type.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/flight/server.h>
#include <arrow/c/bridge.h> // `ExportSchema`
#pragma GCC diagnostic pop

#include <boost/lexical_cast.hpp> // HEX conversion
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/compute/detail/lru_cache.hpp>

#define ARROW_C_DATA_INTERFACE 1
#define ARROW_C_STREAM_INTERFACE 1
#include "ukv/arrow.h"
#include "ukv/cpp/db.hpp"
#include "ukv/cpp/types.hpp" // `hash_combine`

namespace arf = arrow::flight;
namespace ar = arrow;

using namespace unum::ukv;
using namespace unum;

using sys_clock_t = std::chrono::system_clock;
using sys_time_t = std::chrono::time_point<sys_clock_t>;

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

/**
 * @brief Searches for a "value" among key-value pairs passed in URI after path.
 * @param query_params  Must begin with "?" or "/".
 * @param param_name    Must end with "=".
 */
std::optional<std::string_view> param_value(std::string_view query_params, std::string_view param_name) {

    auto key_begin = std::search(query_params.begin(), query_params.end(), param_name.begin(), param_name.end());
    if (key_begin == query_params.end())
        return std::nullopt;

    char preceding_char = *(key_begin - 1);
    bool isnt_part_of_bigger_key = (preceding_char != '?') & (preceding_char != '&') & (preceding_char != '/');
    if (!isnt_part_of_bigger_key)
        return std::nullopt;

    auto value_begin = key_begin + param_name.size();
    auto value_end = std::find(value_begin, query_params.end(), '&');
    return std::string_view {value_begin, static_cast<size_t>(value_end - value_begin)};
}

bool is_query(std::string_view uri, std::string_view name) {
    if (uri.size() > name.size())
        return uri.substr(0, name.size()) == name && uri[name.size()] == '?';
    return uri == name;
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

class SingleResultStream : public arf::ResultStream {
    std::unique_ptr<arf::Result> one_;

  public:
    SingleResultStream(std::unique_ptr<arf::Result>&& ptr) : one_(std::move(ptr)) {}
    ~SingleResultStream() {}

    ar::Result<std::unique_ptr<arf::Result>> Next() override {
        if (one_)
            return std::exchange(one_, {});
        else
            return {nullptr};
    }
};

/**
 * @brief Wraps a single scalar into a Arrow-compatible `ResultStream`.
 * @section Critique
 * This function marks the pinnacle of ugliness of most modern C++ interfaces.
 * Wrapping an `int` causes 2x `unique_ptr` and a `shared_ptr` allocation!
 */
template <typename scalar_at>
std::unique_ptr<arf::ResultStream> return_scalar(scalar_at scalar) {
    static_assert(!std::is_reference_v<scalar_at>);
    auto result = std::make_unique<arf::Result>();
    result->body = std::make_shared<ar::Buffer>( //
        reinterpret_cast<uint8_t const*>(&scalar),
        sizeof(scalar));
    auto results = std::make_unique<SingleResultStream>(std::move(result));
    return std::unique_ptr<arf::ResultStream>(results.release());
}

using base_id_t = std::uint64_t;
enum client_id_t : base_id_t {};
enum txn_id_t : base_id_t {};

client_id_t parse_parse_client_id(arf::ServerCallContext const& ctx) {
    std::string const& peer_addr = ctx.peer();
    return static_cast<client_id_t>(std::hash<std::string> {}(peer_addr));
}

txn_id_t parse_txn_id(std::string_view str) {
    if (str.size() != 16)
        return txn_id_t {0};
    return txn_id_t {boost::lexical_cast<base_id_t>(str.data(), str.size())};
}

struct session_id_t {
    client_id_t client_id {0};
    txn_id_t txn_id {0};

    bool operator==(session_id_t const& other) const noexcept {
        return (client_id == other.client_id) & (txn_id == other.txn_id);
    }
    bool operator!=(session_id_t const& other) const noexcept {
        return (client_id != other.client_id) | (txn_id != other.txn_id);
    }
};

struct session_id_hash_t {
    std::size_t operator()(session_id_t const& id) const noexcept {
        std::size_t result = SIZE_MAX;
        hash_combine(result, static_cast<base_id_t>(id.client_id));
        hash_combine(result, static_cast<base_id_t>(id.txn_id));
        return result;
    }
};

/**
 * @section Critique
 * Using `shared_ptr`s inside is not the best design decision,
 * but it boils down to having a good LRU-cache implementation
 * with copy-less lookup possibilities. Neither Boost, nor other
 * popular FOSS C++ implementations have that.
 */
struct running_txn_t {
    ukv_txn_t txn = nullptr;
    ukv_arena_t arena = nullptr;
    sys_time_t last_access;
    bool executing = false;
};

using client_to_txn_t = std::unordered_map<session_id_t, running_txn_t, session_id_hash_t>;

struct aging_txn_order_t {
    client_to_txn_t const& sessions;

    bool operator()(session_id_t const& a, session_id_t const& b) const noexcept {
        return sessions.at(a).last_access > sessions.at(b).last_access;
    }
};

/**
 * @brief Resource-Allocation control mechanism, that makes sure that no single client
 * holds ownership of any "transaction handle" or "memory arena" for too long. So if
 * a client goes mute or disconnects, we can reuse same memory for other connections
 * and clients.
 */
class sessions_t {
    std::mutex mutex_;
    // Reusable object handles:
    std::vector<ukv_arena_t> free_arenas_;
    std::vector<ukv_txn_t> free_txns_;
    /// Links each session to memory used for its operations:
    std::unordered_map<session_id_t, running_txn_t, session_id_hash_t> client_to_txn_;
    std::vector<session_id_t> txns_aging_heap_;
    ukv_t db_ = nullptr;
    // On Postgre 9.6+ is set to same 30 seconds.
    std::size_t milliseconds_timeout = 30'000;

    aging_txn_order_t order() const noexcept { return aging_txn_order_t {client_to_txn_}; }

    running_txn_t pop(ukv_error_t* c_error) noexcept {
        session_id_t session_id = txns_aging_heap_.front();
        auto it = client_to_txn_.find(session_id);
        auto age = std::chrono::duration_cast<std::chrono::milliseconds>(it->second.last_access - sys_clock_t::now());
        if (age.count() < milliseconds_timeout) {
            *c_error = "Too many concurrent sessions";
            return {};
        }

        std::pop_heap(txns_aging_heap_.begin(), txns_aging_heap_.end(), order());
        txns_aging_heap_.pop_back(); // Resize by removing one last slot
        running_txn_t released = it->second;
        client_to_txn_.erase(it);
        return released;
    }

    void submit(session_id_t session_id, running_txn_t running_txn) noexcept {
        client_to_txn_.emplace(session_id, running_txn);
        txns_aging_heap_.push_back(session_id);
        std::push_heap(txns_aging_heap_.begin(), txns_aging_heap_.end(), order());
    }

  public:
    sessions_t(ukv_t db, std::size_t n)
        : db_(db), free_arenas_(n), free_txns_(n), client_to_txn_(n), txns_aging_heap_(n) {
        std::fill_n(free_arenas_.begin(), n, nullptr);
        std::fill_n(free_txns_.begin(), n, nullptr);
        txns_aging_heap_.clear();
    }

    ~sessions_t() noexcept {
        for (auto a : free_arenas_)
            ukv_arena_free(db_, a);
        for (auto t : free_txns_)
            ukv_arena_free(db_, t);
    }

    running_txn_t continue_txn(session_id_t session_id, ukv_error_t* c_error) noexcept {
        std::unique_lock _ {mutex_};

        auto it = client_to_txn_.find(session_id);
        if (it == client_to_txn_.end()) {
            *c_error = "Transaction was terminated, start a new one.";
            return {};
        }

        running_txn_t& running = it->second;
        if (running.executing) {
            *c_error = "Transaction can't be modified concurrently.";
            return {};
        }

        running.executing = true;
        running.last_access = sys_clock_t::now();

        // Update the heap order.
        // With a single change shouldn't take more than `log2(n)` operations.
        std::make_heap(txns_aging_heap_.begin(), txns_aging_heap_.end(), order());
        return running;
    }

    running_txn_t request_txn(session_id_t session_id, ukv_error_t* c_error) noexcept {
        std::unique_lock _ {mutex_};

        auto it = client_to_txn_.find(session_id);
        if (it != client_to_txn_.end()) {
            *c_error = "Such transaction is already running, just continue using it.";
            return {};
        }

        // Consider evicting some of the old sessions, if there are no more empty slots
        if (free_txns_.empty() || free_arenas_.empty()) {
            running_txn_t running = pop(c_error);
            if (*c_error)
                return {};
            running.executing = true;
            running.last_access = sys_clock_t::now();
            return running;
        }

        // If we have free slots
        running_txn_t running;
        running.arena = free_arenas_.back();
        running.txn = free_txns_.back();
        running.executing = true;
        running.last_access = sys_clock_t::now();
        free_arenas_.pop_back();
        free_txns_.pop_back();
        return running;
    }

    void hold_txn(session_id_t session_id, running_txn_t running_txn) noexcept {
        std::unique_lock _ {mutex_};
        submit(session_id, running_txn);
    }

    void release_txn(running_txn_t running_txn) noexcept {
        std::unique_lock _ {mutex_};
        free_arenas_.push_back(running_txn.arena);
        free_txns_.push_back(running_txn.txn);
    }

    ukv_arena_t request_arena(ukv_error_t* c_error) noexcept {
        std::unique_lock _ {mutex_};
        // Consider evicting some of the old sessions, if there are no more empty slots
        if (free_arenas_.empty()) {
            running_txn_t running = pop(c_error);
            if (*c_error)
                return nullptr;
            free_txns_.push_back(running.txn);
            return running.arena;
        }

        ukv_arena_t arena = free_arenas_.back();
        free_arenas_.pop_back();
        return arena;
    }

    void release_arena(ukv_arena_t arena) noexcept {
        std::unique_lock _ {mutex_};
        free_arenas_.push_back(arena);
    }
};

struct session_params_t {
    session_id_t session_id;
    std::optional<std::string_view> txn;
    std::optional<std::string_view> col;
    std::optional<std::string_view> opt_lengths;
    std::optional<std::string_view> opt_snapshot;
    std::optional<std::string_view> opt_flush;
    std::optional<std::string_view> opt_track;
};

session_params_t session_params(arf::ServerCallContext const& server_call, std::string_view uri) {
    auto params = uri.substr(uri.find('?'));

    session_params_t result;
    result.session_id.client_id = parse_parse_client_id(server_call);
    result.txn = param_value(params, "txn=");
    if (result.txn)
        result.session_id.txn_id = parse_txn_id(*result.txn);
    result.col = param_value(params, "col=");

    result.opt_lengths = param_value(params, "lengths=");
    result.opt_snapshot = param_value(params, "snapshot=");
    result.opt_flush = param_value(params, "flush=");
    result.opt_track = param_value(params, "track=");

    return result;
}

ukv_str_view_t get_null_terminated(ar::Buffer const& buf) {
    ukv_str_view_t col_config = reinterpret_cast<ukv_str_view_t>(buf.data());
    auto end_config = col_config + buf.capacity();
    return std::find(col_config, end_config, '\0') == end_config ? nullptr : col_config;
}

ukv_str_view_t get_null_terminated(std::shared_ptr<ar::Buffer> const& buf_ptr) {
    return buf_ptr ? get_null_terminated(*buf_ptr) : nullptr;
}

/**
 * @brief Remote Procedure Call implementation on top of Apache Arrow Flight RPC.
 * Currently only implements only the binary interface, which is enough even for
 * Document and Graph logic to work properly with most of encoding/decoding
 * shifted to client side.
 *
 * @section Endpoints
 *
 * > write?col=x&txn=y&lengths&track&shared (DoPut)
 * > read?col=x&txn=y&flush (DoExchange)
 * > col_open?col=x (DoAction): Returns collection ID
 *   Payload buffer: Collection opening config.
 * > col_remove?col=x (DoAction): Drops a collection
 * > txn_begin?txn=y (DoAction): Starts a transaction with a potentially custom ID
 * > txn_commit?txn=y (DoAction): Commits a transaction with a given ID
 *
 * @section Concurrency
 *
 * Flight RPC allows concurrent calls from the same client.
 * In our implementation things are trickier, as transactions are not thread-safe.
 */
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
    sessions_t sessions_;

    UKVService(db_t&& db, std::size_t capacity = 4096) : db_(std::move(db)), sessions_(db_, capacity) {}

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
        arf::ServerCallContext const& server_call,
        arf::Action const& action,
        std::unique_ptr<arf::ResultStream>* results_ptr) override {

        ar::Status ar_status;
        session_params_t params = session_params(server_call, action.type);
        status_t status;

        // Locating the collection ID
        if (is_query(action.type, kActionColOpen.type)) {
            if (!params.col)
                return ar::Status::Invalid("Missing collection name argument");

            // Upsert and fetch collection ID
            ukv_col_t col_id = db_.collection(params.col->data()).throw_or_release();
            ukv_str_view_t col_config = get_null_terminated(action.body);
            ukv_col_open(db_, params.col->begin(), col_config, &col_id, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());
            *results_ptr = return_scalar(col_id);
            return ar::Status::OK();
        }

        // Dropping a collection
        if (is_query(action.type, kActionColRemove.type)) {
            if (!params.col)
                return ar::Status::Invalid("Missing collection name argument");

            ukv_col_remove(db_, params.col->begin(), status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());
            return ar::Status::OK();
        }

        // Listing all available collections
        if (is_query(action.type, kActionColList.type)) {

            // We will need some temporary memory for exports
            ukv_arena_t arena = sessions_.request_arena(status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_size_t count = 0;
            ukv_col_t* collections = nullptr;
            ukv_val_len_t* offsets = nullptr;
            ukv_str_view_t names = nullptr;

            ukv_col_list( //
                db_,
                &count,
                &collections,
                &offsets,
                &names,
                &arena,
                status.member_ptr());
            if (!status) {
                sessions_.release_arena(arena);
                return ar::Status::ExecutionError(status.message());
            }

            // Pack two columns into a Table
            ArrowSchema schema_c;
            ArrowArray array_c;
            ukv_to_arrow_schema(count, 2, &schema_c, &array_c, status.member_ptr());
            if (!status) {
                sessions_.release_arena(arena);
                return ar::Status::ExecutionError(status.message());
            }

            ukv_to_arrow_column( //
                count,
                "cols",
                ukv_type<ukv_col_t>(),
                nullptr,
                nullptr,
                ukv_val_ptr_t(collections),
                schema_c.children[0],
                array_c.children[0],
                status.member_ptr());
            if (!status) {
                sessions_.release_arena(arena);
                return ar::Status::ExecutionError(status.message());
            }

            ukv_to_arrow_column( //
                count,
                "names",
                ukv_type_str_k,
                nullptr,
                offsets,
                ukv_val_ptr_t(names),
                schema_c.children[1],
                array_c.children[1],
                status.member_ptr());
            if (!status) {
                sessions_.release_arena(arena);
                return ar::Status::ExecutionError(status.message());
            }

            //
            auto table = ar::ImportRecordBatch(&array_c, &schema_c).ValueOrDie();
            auto result = std::make_unique<arf::Result>();
            result->body = std::dynamic_pointer_cast<ar::Buffer>(table);
            auto results = std::make_unique<SingleResultStream>(std::move(result));
            *results_ptr = std::unique_ptr<arf::ResultStream>(results.release());
            sessions_.release_arena(arena);
            return ar::Status::OK();
        }

        // Starting a transaction
        if (is_query(action.type, kActionTxnBegin.type)) {
            ukv_options_t options = ukv_options_default_k;
            if (params.opt_snapshot)
                options = ukv_option_txn_snapshot_k;
            if (!params.txn)
                params.session_id.txn_id = static_cast<txn_id_t>(std::rand());

            // Check if collection configuration string was also provided
            ukv_str_view_t col_config = nullptr;
            if (action.body) {
                col_config = reinterpret_cast<ukv_str_view_t>(action.body->data());
                auto end_config = col_config + action.body->capacity();
                if (std::find(col_config, end_config, '\0') == end_config)
                    return ar::Status::Invalid("Collection config must be Null-terminated");
            }

            // Request handles for memory
            running_txn_t session = sessions_.request_txn(params.session_id, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            // Cleanup internal state
            ukv_txn_begin( //
                db_,
                static_cast<ukv_size_t>(params.session_id.txn_id),
                options,
                &session.txn,
                status.member_ptr());
            if (!status) {
                sessions_.release_txn(session);
                return ar::Status::ExecutionError(status.message());
            }

            // Don't forget to add the transaction to active sessions
            sessions_.hold_txn(params.session_id, session);
            *results_ptr = return_scalar(params.session_id.txn_id);
            return ar::Status::OK();
        }

        if (is_query(action.type, kActionTxnCommit.type)) {
            if (!params.txn)
                return ar::Status::Invalid("Missing transaction ID argument");
            ukv_options_t options = ukv_options_default_k;
            if (params.opt_flush)
                options = ukv_option_write_flush_k;

            running_txn_t session = sessions_.continue_txn(params.session_id, status.member_ptr());
            if (!status) {
                sessions_.hold_txn(params.session_id, session);
                return ar::Status::ExecutionError(status.message());
            }

            ukv_txn_commit(session.txn, options, status.member_ptr());
            if (!status) {
                sessions_.hold_txn(params.session_id, session);
                return ar::Status::ExecutionError(status.message());
            }

            sessions_.release_txn(session);
            return ar::Status::OK();
        }

        return ar::Status::NotImplemented("Unknown action type: ", action.type);
    }

    ar::Status DoExchange( //
        arf::ServerCallContext const& server_call,
        std::unique_ptr<arf::FlightMessageReader> request_ptr,
        std::unique_ptr<arf::FlightMessageWriter> response_ptr) {

        ar::Status ar_status;
        arf::FlightMessageReader& request = *request_ptr;
        arf::FlightMessageWriter& response = *response_ptr;
        arf::FlightDescriptor const& desc = request.descriptor();
        std::string const& cmd = desc.cmd;
        session_params_t params = session_params(server_call, cmd);
        status_t status;

        std::shared_ptr<ar::Schema> const& schema_ptr = request.GetSchema().ValueOrDie();
        ArrowSchema schema_c;
        if (ar_status = ar::ExportSchema(*schema_ptr, &schema_c); !ar_status.ok())
            return ar_status;

        if (desc.cmd == kOpRead) {
            std::optional<std::size_t> idx_cols = column_idx(&schema_c, kArgCols);
            std::optional<std::size_t> idx_keys = column_idx(&schema_c, kArgKeys);

            while (true) {
                arf::FlightStreamChunk const& chunk = request.Next().ValueOrDie();
                std::shared_ptr<ar::RecordBatch> const& batch_ptr = chunk.data;
                if (!batch_ptr)
                    break;

                ArrowArray batch_c;
                if (ar_status = ar::ExportRecordBatch(*batch_ptr, &batch_c, nullptr); !ar_status.ok())
                    return ar_status;

                ukv_arena_t arena = sessions_.request_arena(status.member_ptr());
                if (!status)
                    return ar::Status::ExecutionError(status.message());

                ArrowArray& keys_c = *batch_c.children[*idx_keys];
                ukv_val_ptr_t found_values = nullptr;
                ukv_val_len_t* found_offsets = nullptr;
                ukv_read( //
                    db_,
                    nullptr,
                    keys_c.length,
                    nullptr,
                    0,
                    (ukv_key_t const*)keys_c.buffers[1],
                    sizeof(ukv_key_t),
                    ukv_options_default_k,
                    &found_values,
                    &found_offsets,
                    nullptr,
                    &arena,
                    status.member_ptr());

                ArrowSchema vals_schema_c;
                ArrowArray vals_c;
                ukv_to_arrow_column( //
                    keys_c.length,
                    "vals",
                    ukv_type_bin_k,
                    nullptr,
                    found_offsets,
                    found_values,
                    &vals_schema_c,
                    &vals_c,
                    status.member_ptr());

                auto packed = ar::ImportRecordBatch(&vals_c, &vals_schema_c).ValueOrDie();
                ar_status = response.WriteRecordBatch(*packed);
                sessions_.release_arena(arena);
                if (!ar_status.ok())
                    return ar_status;
            }
        }

        return ar::Status::OK();
    }

    ar::Status DoPut( //
        arf::ServerCallContext const& server_call,
        std::unique_ptr<arf::FlightMessageReader> request_ptr,
        std::unique_ptr<arf::FlightMetadataWriter> response_ptr) override {

        ar::Status ar_status;
        arf::FlightMessageReader& request = *request_ptr;
        arf::FlightMetadataWriter& response = *response_ptr;
        arf::FlightDescriptor const& desc = request.descriptor();
        std::string const& cmd = desc.cmd;
        session_params_t params = session_params(server_call, cmd);
        status_t status;

        ArrowSchema schema_c;
        std::shared_ptr<ar::Schema> const& schema_ptr = request.GetSchema().ValueOrDie();
        ar_status = ar::ExportSchema(*schema_ptr, &schema_c);
        if (!ar_status.ok())
            return ar_status;

        if (desc.cmd == kOpWrite) {
            std::optional<std::size_t> idx_cols = column_idx(&schema_c, kArgCols);
            std::optional<std::size_t> idx_keys = column_idx(&schema_c, kArgKeys);
            std::optional<std::size_t> idx_vals = column_idx(&schema_c, kArgVals);

            while (true) {
                arf::FlightStreamChunk const& chunk = request.Next().ValueOrDie();
                std::shared_ptr<ar::RecordBatch> const& batch_ptr = chunk.data;
                if (!batch_ptr)
                    break;

                ArrowArray batch_c;
                ar_status = ar::ExportRecordBatch(*batch_ptr, &batch_c, nullptr);
                if (!ar_status.ok())
                    return ar_status;

                ukv_arena_t arena = sessions_.request_arena(status.member_ptr());
                if (!status)
                    return ar::Status::ExecutionError(status.message());

                ArrowArray& keys_c = *batch_c.children[*idx_keys];
                ArrowArray& vals_c = *batch_c.children[*idx_vals];
                ukv_write( //
                    db_,
                    nullptr,
                    keys_c.length,
                    nullptr,
                    0,
                    (ukv_key_t const*)keys_c.buffers[1],
                    sizeof(ukv_key_t),
                    (ukv_val_ptr_t const*)&vals_c.buffers[2],
                    0,
                    (ukv_val_len_t const*)vals_c.buffers[1],
                    sizeof(ukv_val_len_t),
                    nullptr,
                    0,
                    ukv_options_default_k,
                    &arena,
                    status.member_ptr());

                sessions_.release_arena(arena);
                if (!status)
                    return ar::Status::ExecutionError(status.message());
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
