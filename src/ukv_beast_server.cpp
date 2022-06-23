/**
 * @file ukv_beast_server.cpp
 * @author Ashot Vardanian
 * @date 2022-06-18
 *
 * @brief A web server implementint REST backend on top of any other
 * UKV implementation using pre-release draft of C++23 Networking TS,
 * through the means of Boost.Beast and Boost.ASIO.
 *
 * @section Supported Endpoints
 *
 * Modifying single entries:
 * > POST /one/name?/id/field?:    Imports binary data under given key in a named collection.
 * > GET /one/name?/id/field?:     Returns binary data under given key in a named collection.
 * > HEAD /one/name?/id/field?:    Returns binary data under given key in a named collection.
 * > DELETE /one/name?/id/field?:  Returns binary data under given key in a named collection.
 *
 * Modifying collections:
 * > POST /col/name:    Upserts a collection.
 * > DELETE /col/name:  Drops the entire collection.
 * > DELETE /col:       Clears the main collection.
 * > DELETE /:          Clears the entire DB.
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
 * > GET /soa/:     Receives: {cols?: [str], keys: [int], fields: [str], txn?: int}
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

struct db_t : public std::enable_shared_from_this<db_t> {

    ukv_t raw;
    int running_transactions;

    ~db_t() { ukv_free(raw); }
};

void log_failure(beast::error_code ec, char const* what) {
    std::cerr << what << ": " << ec.message() << "\n";
}

/**
 * @brief Primary dispatch point, rounting incoming HTTP requests
 *        into underlying UKV calls, preparing results and sending back.
 */
template <typename body_at, typename allocator_at, typename send_response_at>
void handle_request(db_t& db,
                    http::request<body_at, http::basic_fields<allocator_at>>&& req,
                    send_response_at&& send_response) {

    // Returns a bad request response
    auto const bad_request = [&req](beast::string_view why) {
        http::response<http::string_body> res {http::status::bad_request, req.version()};
        res.set(http::field::server, server_name_k);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = std::string(why);
        res.prepare_payload();
        return res;
    };

    // Returns a not found response
    auto const not_found = [&req](beast::string_view target) {
        http::response<http::string_body> res {http::status::not_found, req.version()};
        res.set(http::field::server, server_name_k);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = "The resource '" + std::string(target) + "' was not found.";
        res.prepare_payload();
        return res;
    };

    // Returns a server error response
    auto const server_error = [&req](beast::string_view what) {
        http::response<http::string_body> res {http::status::internal_server_error, req.version()};
        res.set(http::field::server, server_name_k);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = "An error occurred: '" + std::string(what) + "'";
        res.prepare_payload();
        return res;
    };

    // Make sure we can handle the method
    if (req.method() != http::verb::get && req.method() != http::verb::head)
        return send_response(bad_request("Unknown HTTP-method"));

    // Request path must be absolute and not contain "..".
    if (req.target().empty() || req.target()[0] != '/' || req.target().find("..") != beast::string_view::npos)
        return send_response(bad_request("Illegal request-target"));

    // Build the path to the requested file
    std::string path = ""; // path_cat(db, req.target());
    if (req.target().back() == '/')
        path.append("index.html");

    // Attempt to open the file
    beast::error_code ec;
    http::file_body::value_type body;
    body.open(path.c_str(), beast::file_mode::scan, ec);

    // Handle the case where the file doesn't exist
    if (ec == beast::errc::no_such_file_or_directory)
        return send_response(not_found(req.target()));

    // Handle an unknown error
    if (ec)
        return send_response(server_error(ec.message()));

    // Cache the size since we need it after the move
    auto const size = body.size();

    // Respond to HEAD request
    if (req.method() == http::verb::head) {
        http::response<http::empty_body> res {http::status::ok, req.version()};
        res.set(http::field::server, server_name_k);
        // res.set(http::field::content_type, mime_type(path));
        res.content_length(size);
        res.keep_alive(req.keep_alive());
        return send_response(std::move(res));
    }

    // Respond to GET request
    http::response<http::file_body> res {
        std::piecewise_construct,
        std::make_tuple(std::move(body)),
        std::make_tuple(http::status::ok, req.version()),
    };
    res.set(http::field::server, server_name_k);
    // res.set(http::field::content_type, mime_type(path));
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    return send_response(std::move(res));
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

    // Check command line arguments.
    if (argc != 5) {
        std::cerr << "Usage: ukv_beast_server <address> <port> <threads> <db_config_path>\n"
                  << "Example:\n"
                  << "    ukv_beast_server 0.0.0.0 8080 1 config.json\n";
        return EXIT_FAILURE;
    }

    auto const address = net::ip::make_address(argv[1]);
    auto const port = static_cast<unsigned short>(std::atoi(argv[2]));
    auto const threads = std::max<int>(1, std::atoi(argv[3]));
    auto const db_config_path = std::string(argv[4]);

    auto io_context = net::io_context {threads};
    auto db = std::make_shared<db_t>();

    // Create and launch a listening port
    std::make_shared<listener_t>(io_context, tcp::endpoint {address, port}, db)->run();

    // Run the I/O service on the requested number of threads
    std::vector<std::thread> v;
    v.reserve(threads - 1);
    for (auto i = threads - 1; i > 0; --i)
        v.emplace_back([&io_context] { io_context.run(); });
    io_context.run();

    return EXIT_SUCCESS;
}