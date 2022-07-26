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

class ParquetStorageService : public arrow::flight::FlightServerBase {
  public:
    const arrow::flight::ActionType kActionDropDataset {"drop_dataset", "Delete a dataset."};

    explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> root) : root_(std::move(root)) {}

    arrow::Status ListFlights(const arrow::flight::ServerCallContext&,
                              const arrow::flight::Criteria*,
                              std::unique_ptr<arrow::flight::FlightListing>* listings) override {
        arrow::fs::FileSelector selector;
        selector.base_dir = "/";
        ARROW_ASSIGN_OR_RAISE(auto listing, root_->GetFileInfo(selector));

        std::vector<arrow::flight::FlightInfo> flights;
        for (const auto& file_info : listing) {
            if (!file_info.IsFile() || file_info.extension() != "parquet")
                continue;
            ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo(file_info));
            flights.push_back(std::move(info));
        }

        *listings =
            std::unique_ptr<arrow::flight::FlightListing>(new arrow::flight::SimpleFlightListing(std::move(flights)));
        return arrow::Status::OK();
    }

    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                                const arrow::flight::FlightDescriptor& descriptor,
                                std::unique_ptr<arrow::flight::FlightInfo>* info) override {
        ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
        ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
        *info = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(std::move(flight_info)));
        return arrow::Status::OK();
    }

    arrow::Status DoPut(const arrow::flight::ServerCallContext&,
                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                        std::unique_ptr<arrow::flight::FlightMetadataWriter>) override {
        ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(reader->descriptor()));
        ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_info.path()));
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());

        ARROW_RETURN_NOT_OK(
            parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
        return arrow::Status::OK();
    }

    arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                        const arrow::flight::Ticket& request,
                        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
        ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

        std::shared_ptr<arrow::Table> table;
        ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
        // Note that we can't directly pass TableBatchReader to
        // RecordBatchStream because TableBatchReader keeps a non-owning
        // reference to the underlying Table, which would then get freed
        // when we exit this function
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        arrow::TableBatchReader batch_reader(*table);
        ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());

        ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(std::move(batches), table->schema()));
        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(new arrow::flight::RecordBatchStream(owning_reader));

        return arrow::Status::OK();
    }

    arrow::Status ListActions(const arrow::flight::ServerCallContext&,
                              std::vector<arrow::flight::ActionType>* actions) override {
        *actions = {kActionDropDataset};
        return arrow::Status::OK();
    }

    arrow::Status DoAction(const arrow::flight::ServerCallContext&,
                           const arrow::flight::Action& action,
                           std::unique_ptr<arrow::flight::ResultStream>* result) override {
        if (action.type == kActionDropDataset.type) {
            *result = std::unique_ptr<arrow::flight::ResultStream>(new arrow::flight::SimpleResultStream({}));
            return DoActionDropDataset(action.body->ToString());
        }
        return arrow::Status::NotImplemented("Unknown action type: ", action.type);
    }

  private:
    arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(const arrow::fs::FileInfo& file_info) {
        ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(file_info));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

        auto descriptor = arrow::flight::FlightDescriptor::Path({file_info.base_name()});

        arrow::flight::FlightEndpoint endpoint;
        endpoint.ticket.ticket = file_info.base_name();
        arrow::flight::Location location;
        ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp("localhost", port()));
        endpoint.locations.push_back(location);

        int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
        int64_t total_bytes = file_info.size();

        return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, total_records, total_bytes);
    }

    arrow::Result<arrow::fs::FileInfo> FileInfoFromDescriptor(const arrow::flight::FlightDescriptor& descriptor) {
        if (descriptor.type != arrow::flight::FlightDescriptor::PATH) {
            return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor");
        }
        else if (descriptor.path.size() != 1) {
            return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor with one path component");
        }
        return root_->GetFileInfo(descriptor.path[0]);
    }

    arrow::Status DoActionDropDataset(const std::string& key) { return root_->DeleteFile(key); }

    std::shared_ptr<arrow::fs::FileSystem> root_;
}; // end ParquetStorageService

//------------------------------------------------------------------------------

int main(int argc, char* argv[]) {

    // Check command line arguments
    if (argc < 4) {
        std::cerr << "Usage: ukv_beast_server <address> <port> <threads> <db_config_path>?\n"
                  << "Example:\n"
                  << "    ukv_beast_server 0.0.0.0 8080 1\n"
                  << "    ukv_beast_server 0.0.0.0 8080 1 ./config.json\n"
                  << "";
        return EXIT_FAILURE;
    }

    // Parse the arguments
    auto const address = net::ip::make_address(argv[1]);
    auto const port = static_cast<unsigned short>(std::atoi(argv[2]));
    auto const threads = std::max<int>(1, std::atoi(argv[3]));
    auto db_config = std::string();

    // Read the configuration file
    if (argc >= 5) {
        auto const db_config_path = std::string(argv[4]);
        if (!db_config_path.empty()) {
            std::ifstream ifs(db_config_path);
            db_config = std::string(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
        }
    }

    // Check if we can initialize the DB
    auto session = std::make_shared<db_w_clients_t>();
    status_t status;
    ukv_open(db_config.c_str(), &session->raw, error.member_ptr());
    if (!status) {
        std::cerr << "Couldn't initialize DB: " << error.raw << std::endl;
        return EXIT_FAILURE;
    }

    // Create and launch a listening port
    auto io_context = net::io_context {threads};
    std::make_shared<listener_t>(io_context, tcp::endpoint {address, port}, session)->run();

    // Run the I/O service on the requested number of threads
    std::vector<std::thread> v;
    v.reserve(threads - 1);
    for (auto i = threads - 1; i > 0; --i)
        v.emplace_back([&io_context] { io_context.run(); });
    io_context.run();

    return EXIT_SUCCESS;
}
