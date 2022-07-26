
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/util/logging.h> // `FlightInfo`

using namespace arrow::flight;
using namespace arrow;

/**
 * @brief
 *
 * Examples:
 * https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
 *
 */
struct MyFlightServer : public FlightServerBase {

    Status ListFlights(ServerCallContext const& context,
                       Criteria const* criteria,
                       std::unique_ptr<FlightListing>* listings) override {

        std::vector<FlightInfo> flights;

        FlightDescriptor::Path({"gyumri", ""});
        auto f = FlightInfo::Make(Schema(), {}, {}, 0, 0);
        *listings = std::unique_ptr<FlightListing>(new SimpleFlightListing(flights));
        return Status::OK();
    }

    Status DoGet(ServerCallContext const& context,
                 Ticket const& request,
                 std::unique_ptr<FlightDataStream>* stream) override;

    Status DoPut(ServerCallContext const& context,
                 std::unique_ptr<FlightMessageReader> reader,
                 std::unique_ptr<FlightMetadataWriter> writer) override;
};

int main(int argc, char** argv) {

    std::unique_ptr<FlightServerBase> server;
    // Initialize server
    Location location;
    // Listen to all interfaces on a free port
    ARROW_CHECK_OK(Location::ForGrpcTcp("0.0.0.0", 0, &location));
    FlightServerOptions options(location);

    // Start the server
    ARROW_CHECK_OK(server->Init(options));
    // Exit with a clean error code (0) on SIGTERM
    ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));

    std::cout << "Server listening on localhost:" << server->port() << std::endl;
    ARROW_CHECK_OK(server->Serve());

    return 0;
}
