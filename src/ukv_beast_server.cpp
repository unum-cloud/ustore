/**
 * @file ukv_beast_server.cpp
 * @author Ashot Vardanian
 * @date 2022-06-18
 *
 * @brief A web server implementint @b REST backend on top of any other
 * UKV implementation using pre-release draft of C++23 Networking TS,
 * through the means of @b Boost.Beast and @b Boost.ASIO.
 *
 * @section Supported Endpoints
 *
 * Modifying single entries:
 * > POST /one/id?col=str&txn=int&field=str:    Upserts data.
 * > GET /one/id?col=str&txn=int&field=str:     Retrieves data.
 * > HEAD /one/id?col=str&txn=int&field=str:    Retrieves data length.
 * > DELETE /one/id?col=str&txn=int&field=str:  Deletes data.
 *
 * Modifying collections:
 * > POST /col/name:    Upserts a collection.
 * > DELETE /col/name:  Drops the entire collection.
 * > DELETE /col:       Clears the main collection.
 *
 * Global operations:
 * > DELETE /all/:              Clears the entire DB.
 * > GET /all/meta?query=str:   Retrieves DB metadata.
 *
 * Supporting transactions:
 * > GET /txn/client:   Returns: {id?: int, error?: str}
 * > DELETE /txn/id:    Drops the transaction and it's contents.
 * > POST /txn/id:      Commits and drops the transaction.
 *
 *  @section Upcoming Endpoints
 *
 * Working with @b batched data in tape-like SOA:
 * > POST /soa/:    Receives: {cols?: [str], keys: [int], txn?: int, lens: [int], tape: str}
 *                  Returns: {error?: str}
 * > GET /soa/:     Receives: {cols?: [str], keys: [int], fields?: [str], txn?: int}
 *                  Returns: {lens?: [int], tape?: str, error?: str}
 * > DELETE /soa/:  Receives: {cols?: [str], keys: [int], fields?: [str], txn?: int}
 *                  Returns: {error?: str}
 * > HEAD /soa/:    Receives: {col?: str, key: int, fields?: [str], txn?: int}
 *                  Returns: {len?: int, error?: str}
 *
 * Working with @b batched data in AOS:
 * > POST /aos/:    Receives: {colocated?: str, objs: [obj], txn?: int}
 *                  Returns: {error?: str}
 * > GET /aos/:     Receives: {colocated?: str, keys: [int], fields?: [str], txn?: int}
 *                  Returns: {objs?: [obj], error?: str}
 * > DELETE /aos/:  Receives: {colocated?: str, keys: [int], fields?: [str], txn?: int}
 *                  Returns: {error?: str}
 * > HEAD /aos/:    Receives: {colocated?: str, keys: [int], fields?: [str], txn?: int}
 *                  Returns: {len?: int, error?: str}
 *
 * Working with @b batched data in Apache Arrow format:
 * > GET /arrow/:   Receives: {cols?: [str], keys: [int], fields: [str], txn?: int}
 *                  Returns: Apache Arrow buffers
 */

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <string_view>
#include <charconv>

// Boost files are quite noisy in terms of warnings,
// so let's silence them a bit.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Weverything"
#elif defined(__GNUC__) || defined(__GNUG__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#elif defined(_MSC_VER)
#endif

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/config.hpp>

#include <fmt/core.h>

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__) || defined(__GNUG__)
#pragma GCC diagnostic pop
#elif defined(_MSC_VER)
#endif

#include "ukv.h"

namespace beast = boost::beast;   // from <boost/beast.hpp>
namespace http = beast::http;     // from <boost/beast/http.hpp>
namespace net = boost::asio;      // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp; // from <boost/asio/ip/tcp.hpp>

static constexpr char const* server_name_k = "unum-cloud/ukv/beast_server";
static constexpr char const* binary_mime_k = "application/octet-stream";

struct db_t : public std::enable_shared_from_this<db_t> {

    ukv_t raw = NULL;
    int running_transactions;

    ~db_t() noexcept { ukv_free(raw); }
};

struct raii_tape_t {
    ukv_t db = NULL;
    ukv_tape_ptr_t ptr = NULL;
    size_t capacity = 0;

    raii_tape_t(ukv_t db_ptr) noexcept : db(db_ptr) {}
    ~raii_tape_t() noexcept { ukv_tape_free(db, ptr, capacity); }
};

struct raii_error_t {
    ukv_error_t raw = NULL;

    ~raii_error_t() noexcept { ukv_error_free(raw); }
};

void log_failure(beast::error_code ec, char const* what) {
    std::cerr << what << ": " << ec.message() << "\n";
}

template <typename body_at, typename allocator_at>
http::response<http::string_body> make_error(http::request<body_at, http::basic_fields<allocator_at>> const& req,
                                             http::status status,
                                             beast::string_view why) {

    http::response<http::string_body> res {status, req.version()};
    res.set(http::field::server, server_name_k);
    res.set(http::field::content_type, "text/html");
    res.keep_alive(req.keep_alive());
    res.body() = std::string(why);
    res.prepare_payload();
    return res;
}

/**
 * @brief Searches for a "value" among key-value pairs passed in URI after path.
 * @param query_params  Must begin with "?".
 * @param param_name    Must end with "=".
 */
std::optional<beast::string_view> param_value(beast::string_view query_params, beast::string_view param_name) {

    auto key_begin = std::search(query_params.begin(), query_params.end(), param_name.begin(), param_name.end());
    if (key_begin == query_params.end())
        return std::nullopt;

    char preceding_char = *(key_begin - 1);
    bool isnt_part_of_bigger_key = (preceding_char != '?') & (preceding_char != '&');
    if (!isnt_part_of_bigger_key)
        return std::nullopt;

    auto value_begin = key_begin + param_name.size();
    auto value_end = std::find(value_begin, query_params.end(), '&');
    return beast::string_view {value_begin, static_cast<size_t>(value_end - value_begin)};
}

/**
 * @brief Primary dispatch point, rounting incoming HTTP requests
 *        into underlying UKV calls, preparing results and sending back.
 */
template <typename body_at, typename allocator_at, typename send_response_at>
void handle_request(db_t& db,
                    http::request<body_at, http::basic_fields<allocator_at>>&& req,
                    send_response_at&& send_response) {

    http::verb received_verb = req.method();
    beast::string_view received_path = req.target();
    fmt::print("Received path:{}\n", std::string(received_path));

    // Modifying single entries:
    if (received_path.starts_with("/one/")) {

        ukv_key_t key = 0;
        ukv_txn_t txn = NULL;
        ukv_collection_t collection = NULL;
        ukv_options_read_t options = NULL;

        // Parse the `key`
        auto key_begin = received_path.substr(5).begin();
        auto key_end = std::find(key_begin, received_path.end(), '?');
        auto key_parse_result = std::from_chars(key_begin, key_end, key);
        if (key_parse_result.ec != std::errc())
            return send_response(make_error(req, http::status::bad_request, "Couldn't parse the integer key"));

        // Parse the following free-order parameters, starting with transaction identifier.
        auto params_str = beast::string_view {key_end, static_cast<size_t>(received_path.end() - key_end)};
        if (auto txn_val = param_value(params_str, "txn="); txn_val) {
            auto txn_id = size_t {0};
            auto txn_parse_result = std::from_chars(txn_val->data(), txn_val->data() + txn_val->size(), txn_id);
            if (txn_parse_result.ec != std::errc())
                return send_response(make_error(req, http::status::bad_request, "Couldn't parse the transaction id"));
        }

        // Parse the collection name string.
        if (auto col_val = param_value(params_str, "col="); col_val) {
            char col_name_buffer[65] = {0};
            std::memcpy(col_name_buffer, col_val->data(), std::min(col_val->size(), 64ul));

            raii_error_t error;
            ukv_collection_upsert(db.raw, col_name_buffer, &collection, &error.raw);
            if (error.raw)
                return send_response(make_error(req, http::status::internal_server_error, error.raw));
        }

        // Once we know, which collection, key and transation user is
        // interested in - perform the actions depending on verbs.
        switch (received_verb) {

            // Read the data:
        case http::verb::get: {

            raii_tape_t tape(db.raw);
            raii_error_t error;
            ukv_read(db.raw, txn, &key, 1, &collection, options, &tape.ptr, &tape.capacity, &error.raw);
            if (error.raw)
                return send_response(make_error(req, http::status::internal_server_error, error.raw));

            ukv_tape_ptr_t begin = tape.ptr + sizeof(ukv_val_len_t);
            ukv_val_len_t len = reinterpret_cast<ukv_val_len_t*>(tape.ptr)[0];
            if (!len)
                return send_response(make_error(req, http::status::not_found, "Missing key"));

            http::buffer_body::value_type body;
            body.data = begin;
            body.size = len;
            body.more = false;

            http::response<http::buffer_body> res {
                std::piecewise_construct,
                std::make_tuple(std::move(body)),
                std::make_tuple(http::status::ok, req.version()),
            };
            res.set(http::field::server, server_name_k);
            res.set(http::field::content_type, binary_mime_k);
            res.content_length(len);
            res.keep_alive(req.keep_alive());
            return send_response(std::move(res));
        }

            // Check the data:
        case http::verb::head: {

            raii_tape_t tape(db.raw);
            raii_error_t error;
            ukv_option_read_lengths(&options, true);
            ukv_read(db.raw, txn, &key, 1, &collection, options, &tape.ptr, &tape.capacity, &error.raw);
            if (error.raw)
                return send_response(make_error(req, http::status::internal_server_error, error.raw));

            ukv_val_len_t len = reinterpret_cast<ukv_val_len_t*>(tape.ptr)[0];
            if (!len)
                return send_response(make_error(req, http::status::not_found, "Missing key"));

            http::response<http::empty_body> res;
            res.set(http::field::server, server_name_k);
            res.set(http::field::content_type, binary_mime_k);
            res.content_length(len);
            res.keep_alive(req.keep_alive());
            return send_response(std::move(res));
        }

            // Upsert data:
        case http::verb::post: {

            auto payload_len = req.payload_size();
            if (!payload_len)
                return send_response(
                    make_error(req, http::status::length_required, "Chunk Transfer Encoding isn't supported"));

            raii_error_t error;
            auto value_ptr = reinterpret_cast<ukv_tape_ptr_t>(req.body().data());
            auto value_len = static_cast<ukv_val_len_t>(*payload_len);
            ukv_write(db.raw, txn, &key, 1, &collection, options, value_ptr, &value_len, &error.raw);
            if (error.raw)
                return send_response(make_error(req, http::status::internal_server_error, error.raw));

            http::response<http::empty_body> res;
            res.set(http::field::server, server_name_k);
            res.set(http::field::content_type, binary_mime_k);
            res.keep_alive(req.keep_alive());
            return send_response(std::move(res));
        }

            // Upsert data:
        case http::verb::delete_: {

            raii_error_t error;
            ukv_val_len_t value_len = 0;
            ukv_write(db.raw, txn, &key, 1, &collection, options, NULL, &value_len, &error.raw);
            if (error.raw)
                return send_response(make_error(req, http::status::internal_server_error, error.raw));

            http::response<http::empty_body> res;
            res.set(http::field::server, server_name_k);
            res.set(http::field::content_type, binary_mime_k);
            res.keep_alive(req.keep_alive());
            return send_response(std::move(res));
        }

        //
        default: return send_response(make_error(req, http::status::bad_request, "Unsupported HTTP verb"));
        }
    }

    // Modifying collections:
    else if (received_path.starts_with("/col/")) {
    }

    // Global operations:
    else if (received_path.starts_with("/all/")) {
    }

    // Supporting transactions:
    else if (received_path.starts_with("/txn/")) {
    }

    // Structure-of-Arrays:
    else if (received_path.starts_with("/soa/"))
        return send_response(make_error(req, http::status::bad_request, "Batch API aren't implemented yet"));

    // Array-of-Structures:
    else if (received_path.starts_with("/aos/"))
        return send_response(make_error(req, http::status::bad_request, "Batch API aren't implemented yet"));

    // Array-of-Structures:
    else if (received_path.starts_with("/arrow/"))
        return send_response(make_error(req, http::status::bad_request, "Batch API aren't implemented yet"));

    else
        return send_response(make_error(req, http::status::bad_request, "Unknown request"));
}

/**
 * @brief A communication channel/session for a single client.
 */
class session_t : public std::enable_shared_from_this<session_t> {
    // This is the C++11 equivalent of a generic lambda.
    // The function object is used to send an HTTP message.
    struct send_request_t {
        session_t& self_;

        explicit send_request_t(session_t& self) noexcept : self_(self) {}

        template <bool ir_request_ak, typename body_at, typename fields_at>
        void operator()(http::message<ir_request_ak, body_at, fields_at>&& msg) const {
            // The lifetime of the message has to extend
            // for the duration of the async operation so
            // we use a `std::shared_ptr` to manage it.
            auto sp = std::make_shared<http::message<ir_request_ak, body_at, fields_at>>(std::move(msg));

            // Store a type-erased version of the shared
            // pointer in the class to keep it alive.
            self_.res_ = sp;

            // Write the response
            http::async_write(
                self_.stream_,
                *sp,
                beast::bind_front_handler(&session_t::on_write, self_.shared_from_this(), sp->need_eof()));
        }
    };

    beast::tcp_stream stream_;
    beast::flat_buffer buffer_;
    std::shared_ptr<db_t> db_;
    http::request<http::string_body> req_;
    std::shared_ptr<void> res_;
    send_request_t send_request_;

  public:
    session_t(tcp::socket&& socket, std::shared_ptr<db_t> const& db)
        : stream_(std::move(socket)), db_(db), send_request_(*this) {}

    /**
     * @brief Start the asynchronous operation.
     */
    void run() {
        // We need to be executing within a strand to perform async operations
        // on the I/O objects in this session_t. Although not strictly necessary
        // for single-threaded contexts, this example code is written to be
        // thread-safe by default.
        net::dispatch(stream_.get_executor(), beast::bind_front_handler(&session_t::do_read, shared_from_this()));
    }

    void do_read() {
        // Make the request empty before reading,
        // otherwise the operation behavior is undefined.
        req_ = {};

        // Set the timeout.
        stream_.expires_after(std::chrono::seconds(30));

        // Read a request
        http::async_read(stream_, buffer_, req_, beast::bind_front_handler(&session_t::on_read, shared_from_this()));
    }

    void on_read(beast::error_code ec, std::size_t bytes_transferred) {
        boost::ignore_unused(bytes_transferred);

        if (ec == http::error::end_of_stream)
            // This means they closed the connection
            return do_close();

        if (ec)
            return log_failure(ec, "read");

        // send_at the response
        handle_request(*db_, std::move(req_), send_request_);
    }

    void on_write(bool close, beast::error_code ec, std::size_t bytes_transferred) {
        boost::ignore_unused(bytes_transferred);

        if (ec)
            return log_failure(ec, "write");

        if (close)
            // This means we should close the connection, usually because
            // the response indicated the "Connection: close" semantic.
            return do_close();

        // We're done with the response so delete it
        res_ = nullptr;

        // Read another request
        do_read();
    }

    void do_close() {
        beast::error_code ec;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
    }
};

//------------------------------------------------------------------------------

/**
 * @brief Spins on sockets, listening for new connection requests.
 *        Once accepted, allocates and dispatches a new @c `session_t`.
 */
class listener_t : public std::enable_shared_from_this<listener_t> {
    net::io_context& io_context_;
    tcp::acceptor acceptor_;
    std::shared_ptr<db_t> db_;

  public:
    listener_t(net::io_context& io_context, tcp::endpoint endpoint, std::shared_ptr<db_t> const& db)
        : io_context_(io_context), acceptor_(net::make_strand(io_context)), db_(db) {
        connect_to(endpoint);
    }

    // Start accepting incoming connections
    void run() { do_accept(); }

  private:
    void do_accept() {
        // The new connection gets its own strand
        acceptor_.async_accept(net::make_strand(io_context_),
                               beast::bind_front_handler(&listener_t::on_accept, shared_from_this()));
    }

    void on_accept(beast::error_code ec, tcp::socket socket) {
        if (ec)
            // To avoid infinite loop
            return log_failure(ec, "accept");

        // Create the session_t and run it
        std::make_shared<session_t>(std::move(socket), db_)->run();
        // Accept another connection
        do_accept();
    }

    void connect_to(tcp::endpoint endpoint) {
        beast::error_code ec;

        // Open the acceptor
        acceptor_.open(endpoint.protocol(), ec);
        if (ec)
            return log_failure(ec, "open");

        // Allow address reuse
        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        if (ec)
            return log_failure(ec, "set_option");

        // Bind to the server address
        acceptor_.bind(endpoint, ec);
        if (ec)
            return log_failure(ec, "bind");

        // Start listening for connections
        acceptor_.listen(net::socket_base::max_listen_connections, ec);
        if (ec)
            return log_failure(ec, "listen");
    }
};

//------------------------------------------------------------------------------

int main(int argc, char* argv[]) {

    // Check command line arguments
    if (argc != 5) {
        std::cerr << "Usage: ukv_beast_server <address> <port> <threads> <db_config_path>\n"
                  << "Example:\n"
                  << "    ukv_beast_server 0.0.0.0 8080 1 config.json\n";
        return EXIT_FAILURE;
    }

    // Parse the arguments
    auto const address = net::ip::make_address(argv[1]);
    auto const port = static_cast<unsigned short>(std::atoi(argv[2]));
    auto const threads = std::max<int>(1, std::atoi(argv[3]));
    auto const db_config_path = std::string(argv[4]);

    // Check if we can initialize the DB
    auto db = std::make_shared<db_t>();
    raii_error_t error;
    ukv_open("", &db->raw, &error.raw);
    if (error.raw) {
        std::cerr << "Couldn't initialize DB: " << error.raw << std::endl;
        return EXIT_FAILURE;
    }

    // Create and launch a listening port
    auto io_context = net::io_context {threads};
    std::make_shared<listener_t>(io_context, tcp::endpoint {address, port}, db)->run();

    // Run the I/O service on the requested number of threads
    std::vector<std::thread> v;
    v.reserve(threads - 1);
    for (auto i = threads - 1; i > 0; --i)
        v.emplace_back([&io_context] { io_context.run(); });
    io_context.run();

    return EXIT_SUCCESS;
}