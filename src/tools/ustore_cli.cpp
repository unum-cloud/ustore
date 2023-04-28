#include <iostream>

#include <clipp.h>
#include <fmt/format.h>

#include "ustore/cpp/db.hpp"

using namespace unum::ustore;
using namespace unum;

int main(int argc, char* argv[]) {
    using namespace clipp;

    std::string url = "";
    std::string drop_collection;
    std::string create_collection;
    bool help = false;
    bool list_collections = false;

    auto cli = ( //
        (required("--url") & value("URL", url)).doc("Server URL"),
        (option("--list_collections").set(list_collections)).doc("Print list of collections"),
        (option("--drop_collection") & value("collection name", drop_collection)).doc("Drop collection"),
        (option("--create_collection") & value("collection name", create_collection)).doc("Create collection"),
        option("-h", "--help").set(help).doc("Print this help information on this tool and exit"));

    if (!parse(argc, argv, cli)) {
        std::cerr << make_man_page(cli, argv[0]);
        return 1;
    }
    if (help) {
        std::cout << make_man_page(cli, argv[0]);
        return 0;
    }

    database_t db;
    db.open(url.c_str()).throw_unhandled();

    if (list_collections) {
        auto context = context_t {db, nullptr};
        auto collections = context.collections().throw_or_release();
        while (!collections.names.is_end()) {
            fmt::print("{}\n", *collections.names);
            ++collections.names;
        }
    }
    if (!drop_collection.empty()) {
        auto maybe_collection = db.find(drop_collection);
        if (maybe_collection) {
            auto collection = *maybe_collection;
            collection.drop();
        }
    }
    if (!create_collection.empty()) {
        db.find_or_create(create_collection.c_str());
    }

    return 0;
}
