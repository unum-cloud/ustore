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

#include <arrow/flight/server.h>
#include <boost/lexical_cast.hpp> // HEX conversion
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/compute/detail/lru_cache.hpp>

#include "ukv/cpp/db.hpp"
#include "ukv/cpp/types.hpp" // `hash_combine`

#include "arrow_helpers.hpp"
#include "ukv/arrow.h"

using namespace unum::ukv;
using namespace unum;

using sys_clock_t = std::chrono::system_clock;
using sys_time_t = std::chrono::time_point<sys_clock_t>;

inline static arf::ActionType const kActionColOpen {kFlightColOpen, "Find a collection descriptor by name."};
inline static arf::ActionType const kActionColDrop {kFlightColDrop, "Delete a named collection."};
inline static arf::ActionType const kActionTxnBegin {kFlightTxnBegin, "Starts an ACID transaction and returns its ID."};
inline static arf::ActionType const kActionTxnCommit {kFlightTxnCommit, "Commit a previously started transaction."};

/**
 * @brief Searches for a "value" among key-value pairs passed in URI after path.
 * @param query_params  Must begin with "?" or "/".
 * @param param_name    The name of the URI parameter to match.
 */
std::optional<std::string_view> param_value(std::string_view query_params, std::string_view param_name) {

    char const* key_begin = query_params.begin();
    do {
        key_begin = std::search(key_begin, query_params.end(), param_name.begin(), param_name.end());
        if (key_begin == query_params.end())
            return std::nullopt;
        bool is_suffix = key_begin + param_name.size() == query_params.end();
        if (is_suffix)
            return std::string_view {};

        // Check if we have matched a part of bigger key.
        // In that case skip to next starting point.
        auto prev_character = *(key_begin - 1);
        if (prev_character != '?' && prev_character != '&' && prev_character != '/') {
            key_begin += 1;
            continue;
        }

        auto next_character = key_begin[param_name.size()];
        if (next_character == '&')
            return std::string_view {};

        if (next_character == '=') {
            auto value_begin = key_begin + param_name.size() + 1;
            auto value_end = std::find(value_begin, query_params.end(), '&');
            return std::string_view {value_begin, static_cast<size_t>(value_end - value_begin)};
        }

        key_begin += 1;
    } while (true);

    return std::nullopt;
}

bool is_query(std::string_view uri, std::string_view name) {
    if (uri.size() > name.size())
        return uri.substr(0, name.size()) == name && uri[name.size()] == '?';
    return uri == name;
}

bool validate_column_cols(ArrowSchema* schema_ptr, ArrowArray* column_ptr) {
    if (schema_ptr->format != ukv_type_to_arrow_format(ukv_type<ukv_collection_t>()))
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

class EmptyResultStream : public arf::ResultStream {

  public:
    EmptyResultStream() {}
    ~EmptyResultStream() {}

    ar::Result<std::unique_ptr<arf::Result>> Next() override { return {nullptr}; }
};

/**
 * @brief Wraps a single scalar into a Arrow-compatible `ResultStream`.
 * @section Critique
 * This function marks the pinnacle of ugliness of most modern C++ interfaces.
 * Wrapping an `int` causes 2x `unique_ptr` and a `shared_ptr` allocation!
 */
template <typename scalar_at>
std::unique_ptr<arf::ResultStream> return_scalar(scalar_at const& scalar) {
    static_assert(!std::is_reference_v<scalar_at>);
    auto result = std::make_unique<arf::Result>();
    result->body = std::make_shared<ar::Buffer>( //
        reinterpret_cast<uint8_t const*>(&scalar),
        sizeof(scalar));
    auto results = std::make_unique<SingleResultStream>(std::move(result));
    return std::unique_ptr<arf::ResultStream>(results.release());
}

std::unique_ptr<arf::ResultStream> return_empty() {
    auto results = std::make_unique<EmptyResultStream>();
    return std::unique_ptr<arf::ResultStream>(results.release());
}

using base_id_t = std::uint64_t;
enum client_id_t : base_id_t {};
enum txn_id_t : base_id_t {};
static_assert(sizeof(txn_id_t) == sizeof(ukv_transaction_t));

client_id_t parse_parse_client_id(arf::ServerCallContext const& ctx) {
    std::string const& peer_addr = ctx.peer();
    return static_cast<client_id_t>(std::hash<std::string> {}(peer_addr));
}

base_id_t parse_u64_hex(std::string_view str, base_id_t default_ = 0) {
    if (str.size() != 16)
        return default_;
    return boost::lexical_cast<base_id_t>(str.data(), str.size());
}

txn_id_t parse_txn_id(std::string_view str) {
    return txn_id_t {parse_u64_hex(str)};
}

struct session_id_t {
    client_id_t client_id {0};
    txn_id_t txn_id {0};

    bool is_txn() const noexcept { return txn_id; }
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
    ukv_transaction_t txn = nullptr;
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

class sessions_t;
struct session_lock_t {
    sessions_t& sessions;
    session_id_t session_id;
    ukv_transaction_t txn = nullptr;
    ukv_arena_t arena = nullptr;

    inline bool is_txn() const noexcept { return txn; }
    ~session_lock_t();
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
    std::vector<ukv_transaction_t> free_txns_;
    /// Links each session to memory used for its operations:
    std::unordered_map<session_id_t, running_txn_t, session_id_hash_t> client_to_txn_;
    std::vector<session_id_t> txns_aging_heap_;
    ukv_database_t db_ = nullptr;
    // On Postgre 9.6+ is set to same 30 seconds.
    std::size_t milliseconds_timeout = 30'000;

    aging_txn_order_t order() const noexcept { return aging_txn_order_t {client_to_txn_}; }

    running_txn_t pop(ukv_error_t* c_error) noexcept {
        session_id_t session_id = txns_aging_heap_.front();
        auto it = client_to_txn_.find(session_id);
        auto age = std::chrono::duration_cast<std::chrono::milliseconds>(it->second.last_access - sys_clock_t::now());
        if (age.count() < milliseconds_timeout) {
            log_error(c_error, error_unknown_k, "Too many concurrent sessions");
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
    sessions_t(ukv_database_t db, std::size_t n)
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
            log_error(c_error, args_wrong_k, "Transaction was terminated, start a new one");
            return {};
        }

        running_txn_t& running = it->second;
        if (running.executing) {
            log_error(c_error, args_wrong_k, "Transaction can't be modified concurrently.");
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
            log_error(c_error, args_wrong_k, "Such transaction is already running, just continue using it.");
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

    session_lock_t lock(session_id_t id, ukv_error_t* c_error) noexcept {
        if (id.is_txn()) {
            running_txn_t running = continue_txn(id, c_error);
            return {*this, id, running.txn, running.arena};
        }
        else
            return {*this, id, nullptr, request_arena(c_error)};
    }
};

session_lock_t::~session_lock_t() {
    if (is_txn())
        sessions.hold_txn( //
            session_id,
            running_txn_t {
                .txn = txn,
                .arena = arena,
                .last_access = sys_clock_t::now(),
                .executing = true,
            });
    else
        sessions.release_arena(arena);
}

struct session_params_t {
    session_id_t session_id;
    std::optional<std::string_view> transaction_id;
    std::optional<std::string_view> collection_name;
    std::optional<std::string_view> collection_id;

    std::optional<std::string_view> opt_read_part;
    std::optional<std::string_view> opt_drop_mode;
    std::optional<std::string_view> opt_snapshot;
    std::optional<std::string_view> opt_flush;
    std::optional<std::string_view> opt_track;
    std::optional<std::string_view> opt_shared_mem;
};

session_params_t session_params(arf::ServerCallContext const& server_call, std::string_view uri) {

    session_params_t result;
    result.session_id.client_id = parse_parse_client_id(server_call);

    auto params_offs = uri.find('?');
    if (params_offs == std::string_view::npos)
        return result;

    auto params = uri.substr(params_offs);
    result.transaction_id = param_value(params, kParamTransactionID);
    if (result.transaction_id)
        result.session_id.txn_id = parse_txn_id(*result.transaction_id);

    result.collection_name = param_value(params, kParamCollectionName);
    result.collection_id = param_value(params, kParamCollectionID);

    result.opt_read_part = param_value(params, kParamReadPart);
    result.opt_drop_mode = param_value(params, kParamDropMode);
    result.opt_snapshot = param_value(params, kParamFlagSnapshotTxn);
    result.opt_flush = param_value(params, kParamFlagFlushWrite);
    result.opt_track = param_value(params, kParamFlagTrackRead);
    result.opt_shared_mem = param_value(params, kParamFlagSharedMemRead);

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
 * > col_upsert?col=x (DoAction): Returns collection ID
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
    database_t db_;
    sessions_t sessions_;

  public:
    UKVService(database_t&& db, std::size_t capacity = 4096) : db_(std::move(db)), sessions_(db_, capacity) {}

    ar::Status ListActions( //
        arf::ServerCallContext const&,
        std::vector<arf::ActionType>* actions) override {
        *actions = {kActionColOpen, kActionColDrop, kActionTxnBegin, kActionTxnCommit};
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
            if (!params.collection_name)
                return ar::Status::Invalid("Missing collection name argument");

            // Upsert and fetch collection ID
            auto maybe_col = db_.collection(params.collection_name->data());
            if (!maybe_col)
                return ar::Status::ExecutionError(maybe_col.release_status().message());

            ukv_collection_t col_id = maybe_col.throw_or_ref();
            ukv_str_view_t col_config = get_null_terminated(action.body);
            ukv_collection_open(db_, params.collection_name->begin(), col_config, &col_id, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());
            *results_ptr = return_scalar<ukv_collection_t>(col_id);
            return ar::Status::OK();
        }

        // Dropping a collection
        if (is_query(action.type, kActionColDrop.type)) {
            if (!params.collection_id && !params.collection_name)
                return ar::Status::Invalid("Missing collection name argument");

            ukv_drop_mode_t mode = //
                params.opt_drop_mode == kParamDropModeValues     ? ukv_drop_vals_k
                : params.opt_drop_mode == kParamDropModeContents ? ukv_drop_keys_vals_k
                                                                 : ukv_drop_keys_vals_handle_k;

            ukv_collection_drop(db_,
                                params.collection_id ? parse_u64_hex(*params.collection_id, ukv_collection_main_k)
                                                     : ukv_collection_main_k,
                                params.collection_name ? params.collection_name->begin() : nullptr,
                                mode,
                                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());
            *results_ptr = return_empty();
            return ar::Status::OK();
        }

        // Starting a transaction
        if (is_query(action.type, kActionTxnBegin.type)) {
            ukv_options_t options = ukv_options_default_k;
            if (params.opt_snapshot)
                options = ukv_option_txn_snapshot_k;
            if (!params.transaction_id)
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
            ukv_transaction_begin( //
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
            *results_ptr = return_scalar<txn_id_t>(params.session_id.txn_id);
            return ar::Status::OK();
        }

        if (is_query(action.type, kActionTxnCommit.type)) {
            if (!params.transaction_id)
                return ar::Status::Invalid("Missing transaction ID argument");
            ukv_options_t options = ukv_options_default_k;
            if (params.opt_flush)
                options = ukv_option_write_flush_k;

            running_txn_t session = sessions_.continue_txn(params.session_id, status.member_ptr());
            if (!status) {
                sessions_.hold_txn(params.session_id, session);
                return ar::Status::ExecutionError(status.message());
            }

            ukv_transaction_commit(db_, session.txn, options, status.member_ptr());
            if (!status) {
                sessions_.hold_txn(params.session_id, session);
                return ar::Status::ExecutionError(status.message());
            }

            sessions_.release_txn(session);
            *results_ptr = return_empty();
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
        session_params_t params = session_params(server_call, desc.cmd);
        status_t status;

        ArrowSchema input_schema_c, output_schema_c;
        ArrowArray input_batch_c, output_batch_c;
        if (ar_status = unpack_table(request.ToTable(), input_schema_c, input_batch_c); !ar_status.ok())
            return ar_status;

        if (is_query(desc.cmd, kFlightRead)) {

            /// @param `keys`
            auto input_keys = get_keys(input_schema_c, input_batch_c, kArgKeys);
            if (!input_keys)
                return ar::Status::Invalid("Keys must have been provided for reads");

            /// @param `cols`
            auto input_cols = get_collections(input_schema_c, input_batch_c, kArgCols);
            bool const request_only_presences = params.opt_read_part == kParamReadPartPresences;
            bool const request_only_lengths = params.opt_read_part == kParamReadPartLengths;
            bool const request_content = !request_only_lengths && !request_only_presences;

            // Reserve resources for the execution of this request
            auto session = sessions_.lock(params.session_id, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            // As we are immediately exporting in the Arrow format,
            // we don't need the lengths, just the NULL indicators
            ukv_bytes_ptr_t found_values = nullptr;
            ukv_length_t* found_offsets = nullptr;
            ukv_length_t* found_lengths = nullptr;
            ukv_octet_t* found_presences = nullptr;
            ukv_size_t tasks_count = static_cast<ukv_size_t>(input_batch_c.length);
            ukv_read( //
                db_,
                session.txn,
                tasks_count,
                input_cols.get(),
                input_cols.stride(),
                input_keys.get(),
                input_keys.stride(),
                ukv_options_default_k,
                &found_presences,
                request_content ? &found_offsets : nullptr,
                request_only_lengths ? &found_lengths : nullptr,
                request_content ? &found_values : nullptr,
                &session.arena,
                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_size_t result_length =
                request_only_presences ? divide_round_up<ukv_size_t>(tasks_count, CHAR_BIT) : tasks_count;
            ukv_to_arrow_schema(result_length, 1, &output_schema_c, &output_batch_c, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            if (request_content)
                ukv_to_arrow_column( //
                    result_length,
                    kArgVals.c_str(),
                    ukv_type_bin_k,
                    found_presences,
                    found_offsets,
                    found_values,
                    output_schema_c.children[0],
                    output_batch_c.children[0],
                    status.member_ptr());
            else if (request_only_lengths)
                ukv_to_arrow_column( //
                    result_length,
                    kArgLengths.c_str(),
                    ukv_type<ukv_length_t>(),
                    found_presences,
                    nullptr,
                    found_lengths,
                    output_schema_c.children[0],
                    output_batch_c.children[0],
                    status.member_ptr());
            else if (request_only_presences)
                ukv_to_arrow_column( //
                    result_length,
                    kArgPresences.c_str(),
                    ukv_type<ukv_octet_t>(),
                    nullptr,
                    nullptr,
                    found_presences,
                    output_schema_c.children[0],
                    output_batch_c.children[0],
                    status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());
        }
        else if (is_query(desc.cmd, kFlightScan)) {

            /// @param `start_keys`
            auto input_start_keys = get_keys(input_schema_c, input_batch_c, kArgScanStarts);
            /// @param `start_keys`
            auto input_end_keys = get_keys(input_schema_c, input_batch_c, kArgScanEnds);
            /// @param `lengths`
            auto input_lengths = get_lengths(input_schema_c, input_batch_c, kArgScanLengths);
            /// @param `cols`
            auto input_cols = get_collections(input_schema_c, input_batch_c, kArgCols);

            if (!input_start_keys || !input_lengths)
                return ar::Status::Invalid("Keys and lengths must have been provided for scans");

            // Reserve resources for the execution of this request
            auto session = sessions_.lock(params.session_id, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            // As we are immediately exporting in the Arrow format,
            // we don't need the lengths, just the NULL indicators
            ukv_length_t* found_offsets = nullptr;
            ukv_length_t* found_lengths = nullptr;
            ukv_key_t* found_keys = nullptr;
            ukv_size_t tasks_count = static_cast<ukv_size_t>(input_batch_c.length);
            ukv_scan( //
                db_,
                session.txn,
                tasks_count,
                input_cols.get(),
                input_cols.stride(),
                input_start_keys.get(),
                input_start_keys.stride(),
                input_end_keys.get(),
                input_end_keys.stride(),
                input_lengths.get(),
                input_lengths.stride(),
                ukv_options_default_k,
                &found_offsets,
                nullptr,
                &found_keys,
                &session.arena,
                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_to_arrow_schema(tasks_count, 1, &output_schema_c, &output_batch_c, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_to_arrow_list( //
                tasks_count,
                kArgKeys.c_str(),
                ukv_type<ukv_key_t>(),
                nullptr,
                found_offsets,
                found_keys,
                output_schema_c.children[0],
                output_batch_c.children[0],
                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());
        }

        auto maybe_table = ar::ImportRecordBatch(&output_batch_c, &output_schema_c);
        if (!maybe_table.ok())
            return maybe_table.status();

        auto table = maybe_table.ValueUnsafe();
        ar_status = table->ValidateFull();
        if (!ar_status.ok())
            return ar_status;

        ar_status = response.Begin(table->schema());
        if (!ar_status.ok())
            return ar_status;

        ar_status = response.WriteRecordBatch(*table);
        if (!ar_status.ok())
            return ar_status;

        return response.Close();
    }

    ar::Status DoPut( //
        arf::ServerCallContext const& server_call,
        std::unique_ptr<arf::FlightMessageReader> request_ptr,
        std::unique_ptr<arf::FlightMetadataWriter> response_ptr) override {

        ar::Status ar_status;
        arf::FlightMessageReader& request = *request_ptr;
        arf::FlightMetadataWriter& response = *response_ptr;
        arf::FlightDescriptor const& desc = request.descriptor();
        session_params_t params = session_params(server_call, desc.cmd);
        status_t status;

        ArrowSchema input_schema_c;
        ArrowArray input_batch_c;
        if (ar_status = unpack_table(request.ToTable(), input_schema_c, input_batch_c); !ar_status.ok())
            return ar_status;

        if (is_query(desc.cmd, kFlightWrite)) {

            /// @param `keys`
            auto input_keys = get_keys(input_schema_c, input_batch_c, kArgKeys);
            if (!input_keys)
                return ar::Status::Invalid("Keys must have been provided for reads");

            /// @param `cols`
            auto input_cols = get_collections(input_schema_c, input_batch_c, kArgCols);
            auto input_vals = get_contents(input_schema_c, input_batch_c, kArgVals);

            auto session = sessions_.lock(params.session_id, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_size_t tasks_count = static_cast<ukv_size_t>(input_batch_c.length);
            ukv_write( //
                db_,
                session.txn,
                tasks_count,
                input_cols.get(),
                input_cols.stride(),
                input_keys.get(),
                input_keys.stride(),
                input_vals.presences_begin.get(),
                input_vals.offsets_begin.get(),
                input_vals.offsets_begin.stride(),
                input_vals.lengths_begin.get(),
                input_vals.lengths_begin.stride(),
                input_vals.contents_begin.get(),
                input_vals.contents_begin.stride(),
                ukv_options_default_k,
                &session.arena,
                status.member_ptr());

            if (!status)
                return ar::Status::ExecutionError(status.message());
        }

        return ar::Status::OK();
    }

    ar::Status DoGet( //
        arf::ServerCallContext const& server_call,
        arf::Ticket const& ticket,
        std::unique_ptr<arf::FlightDataStream>* response_ptr) override {

        ar::Status ar_status;
        session_params_t params = session_params(server_call, ticket.ticket);
        status_t status;

        if (is_query(ticket.ticket, kFlightListCols)) {

            // We will need some temporary memory for exports
            auto session = sessions_.lock({.client_id = params.session_id.client_id}, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_size_t count = 0;
            ukv_collection_t* collections = nullptr;
            ukv_length_t* offsets = nullptr;
            ukv_str_span_t names = nullptr;

            ukv_collection_list( //
                db_,
                &count,
                &collections,
                &offsets,
                &names,
                &session.arena,
                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            // Pack two columns into a Table
            ArrowSchema schema_c;
            ArrowArray array_c;
            ukv_to_arrow_schema(count, 2, &schema_c, &array_c, status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_to_arrow_column( //
                count,
                kArgCols.c_str(),
                ukv_type<ukv_collection_t>(),
                nullptr,
                nullptr,
                ukv_bytes_ptr_t(collections),
                schema_c.children[0],
                array_c.children[0],
                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            ukv_to_arrow_column( //
                count,
                kArgNames.c_str(),
                ukv_type_str_k,
                nullptr,
                offsets,
                ukv_bytes_ptr_t(names),
                schema_c.children[1],
                array_c.children[1],
                status.member_ptr());
            if (!status)
                return ar::Status::ExecutionError(status.message());

            auto maybe_batch = ar::ImportRecordBatch(&array_c, &schema_c);
            if (!maybe_batch.ok())
                return maybe_batch.status();

            auto batch = maybe_batch.ValueUnsafe();
            auto maybe_reader = ar::RecordBatchReader::Make({batch});
            if (!maybe_reader.ok())
                return maybe_reader.status();

            // TODO: Pass right IPC options
            auto stream = std::make_unique<arf::RecordBatchStream>(maybe_reader.ValueUnsafe());
            *response_ptr = std::move(stream);
            return ar::Status::OK();
        }

        return ar::Status::OK();
    }
};

ar::Status run_server() {
    database_t db;
    db.open().throw_unhandled();

    arf::Location server_location = arf::Location::ForGrpcTcp("0.0.0.0", 38709).ValueUnsafe();
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
