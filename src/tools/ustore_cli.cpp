#include <iostream>
#include <regex>

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

    bool close = false;
    if (list_collections) {
        close = true;
        auto context = context_t {db, nullptr};
        auto collections = context.collections().throw_or_release();
        while (!collections.names.is_end()) {
            fmt::print("{}\n", *collections.names);
            ++collections.names;
        }
    }
    if (!drop_collection.empty()) {
        close = true;
        auto maybe_collection = db.find(drop_collection);
        if (maybe_collection) {
            auto collection = *maybe_collection;
            collection.drop();
        }
    }
    if (!create_collection.empty()) {
        close = true;
        db.find_or_create(create_collection.c_str());
    }

    if (close)
        return 0;

    std::string input;
    std::vector<std::string> commands;
    std::regex const reg_exp(R"(\s+)");
    std::regex_token_iterator<std::string::iterator> const end_tokens;

    while (true) {
        commands.clear();
        fmt::print(">>> ");

        std::getline(std::cin, input);
        std::regex_token_iterator<std::string::iterator> it(input.begin(), input.end(), reg_exp, -1);
        while (it != end_tokens)
            commands.emplace_back(*it++);
        if (!commands[0].size())
            commands.erase(commands.begin());
        if (commands[0] == "exit" && commands.size() == 1)
            break;

        if (commands[0] == "create") {
            if (commands.size() != 3) {
                fmt::print("Invalid input\n");
                continue;
            }

            auto& argument = commands[1];
            if (argument == "--collection") {
                auto collection_name = commands[2];
                auto maybe_collection = db.find_or_create(collection_name.c_str());
                if (maybe_collection)
                    fmt::print("Succesfully created collection {}\n", collection_name);
                else
                    fmt::print("Failed to created collection {}\n", collection_name);
            }
            else
                fmt::print("Invalid create argument {}\n", argument);
        }
        else if (commands[0] == "drop") {
            if (commands.size() != 3) {
                fmt::print("Invalid input\n");
                continue;
            }

            auto& argument = commands[1];
            if (argument == "--collection") {
                auto collection_name = commands[2];
                auto maybe_collection = db.find(collection_name);
                if (maybe_collection) {
                    auto collection = *maybe_collection;
                    auto status = collection.drop();
                    if (status)
                        fmt::print("Succesfully droped collection {}\n", collection_name);
                    else
                        fmt::print("Failed to drop collection {}\n", collection_name);
                }
                else
                    fmt::print("Collection {} not found\n", collection_name);
            }
            else
                fmt::print("Invalid drop argument {}\n", argument);
        }
        else if (commands[0] == "list") {
            if (commands.size() != 2) {
                fmt::print("Invalid input\n");
                continue;
            }

            auto& argument = commands[1];
            if (argument == "--collections") {
                auto context = context_t {db, nullptr};
                auto collections = context.collections();
                if (!collections) {
                    fmt::print("Failed to list collections\n");
                    continue;
                }
                while (!collections->names.is_end()) {
                    fmt::print("{}\n", *collections->names);
                    ++collections->names;
                }
            }
            else
                fmt::print("Invalid list argument {}\n", argument);
        }
        else
            fmt::print("Invalid input\n");
    };

    return 0;
}
