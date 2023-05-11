#include <iostream>
#include <regex>

#include <fmt/format.h>

#include <clipp.h>
#include "readline/readline.h"
#include "readline/history.h"

#include "ustore/cpp/db.hpp"
#include "dataset.h"

using namespace unum::ustore;
using namespace unum;

#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\x1B[33m"
#define RESET "\033[0m"

std::string remove_quotes(std::string str) {
    if (str[0] == '\"' && str[str.size() - 1] == '\"') {
        str.erase(0, 1);
        str.erase(str.size() - 1);
    }
    return str;
}

template<typename... args_at>
void print(char const* color, std::string message, args_at&&... args) {
    if (sizeof...(args) != 0)
        message = fmt::format(message, std::forward<args_at>(args)...);
    fmt::print("{}{}{}\n", color, message, RESET);
}

status_t collection_create(database_t& db, std::string const& name) {
    auto maybe_collection = db.find_or_create(name.c_str());
    if (maybe_collection)
        print(GREEN, "Collection '{}' created", name);
    else
        fmt::print("{}Failed to create collection '{}'{}\n", RED, name, RESET);

    return maybe_collection.release_status();
}

status_t collection_drop(database_t& db, std::string const& name) {
    status_t status;
    auto maybe_collection = db.find(name);
    if (!maybe_collection) {
        status = maybe_collection.release_status();
        fmt::print("{}Collection '{}' not found{}\n", RED, name, RESET);
        return maybe_collection.release_status();
    }

    if (status)
        fmt::print("{}Collection '{}' dropped{}\n", GREEN, name, RESET);
    else
        fmt::print("{}Failed to drop collection '{}'{}\n", RED, name, RESET);
}

void docs_import(database_t& db,
                 std::string const& collection_name,
                 std::string const& input_file,
                 std::string const& id_field,
                 std::size_t max_batch_size) {
    status_t status;
    arena_t arena(db);
    ustore_collection_t collection = db.find(collection_name);

    ustore_docs_import_t docs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .options = ustore_options_default_k,
        .collection = collection,
        .paths_pattern = input_file.c_str(),
        .max_batch_size = max_batch_size,
        .id_field = id_field.c_str(),
    };
    ustore_docs_import(&docs);

    if (status)
        fmt::print("{}Successfully imported{}\n", GREEN, RESET);
    else
        fmt::print("{}Failed to import: {}{}\n", RED, status.message(), RESET);
}

void docs_export(database_t& db,
                 std::string const& collection_name,
                 std::string const& output_ext,
                 std::size_t max_batch_size) {
    status_t status;
    arena_t arena(db);
    ustore_collection_t collection = db.find(collection_name);

    ustore_docs_export_t docs {
        .db = db,
        .error = status.member_ptr(),
        .arena = arena.member_ptr(),
        .options = ustore_options_default_k,
        .collection = collection,
        .paths_extension = output_ext.c_str(),
        .max_batch_size = max_batch_size,
    };
    ustore_docs_export(&docs);
    if (status)
        fmt::print("{}Successfully exported{}\n", GREEN, RESET);
    else
        fmt::print("Failed to export: {}\n", RED, status.message(), RESET);
}

int main(int argc, char* argv[]) {
    using namespace clipp;

    std::string url = "";
    std::string input_file;
    std::string output_ext;
    std::string id_field;
    std::string coll_name;
    std::size_t max_batch_size;
    bool help = false;

    std::string selected;
    std::string action;
    std::string name;
    std::string export_path;
    ustore_snapshot_t snap_id;

    auto collection =
        (option("collection").set(selected, std::string("collection")) &
         ((required("create").set(action, std::string("create")) & required("--name") &
           value("collection name", name)) |
          (required("drop").set(action, std::string("drop")) & required("--name") & value("collection name", name)) |
          required("list").set(action, std::string("list")) |
          ((required("import").set(action, std::string("import")) &
            (required("--input") & value("input", input_file)).doc("Input file name") &
            (required("--id") & value("id field", id_field)).doc("The field which data will use as key(s)")) |
           (required("export").set(action, std::string("export")) &
            (required("--output") & value("output", output_ext)).doc("Output file extension"))) &
              ((required("--max_batch_size") & value("max batch size", max_batch_size)).doc("Size of available RAM"),
               (option("--name") & value("collection name", coll_name)))));

    auto snapshot = (option("snapshot").set(selected, std::string("snapshot")) &
                         (required("create").set(action, std::string("create"))) |
                     (required("export").set(action, std::string("export")) & value("path", export_path)) |
                     (required("drop").set(action, std::string("drop")) & value("snapshot id", snap_id)) |
                     (required("list").set(action, std::string("list"))));

    auto cli = ( //
        (required("--url") & value("URL", url)).doc("Server URL"),
        (collection | snapshot),
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
    if (selected == "collection") {
        close = true;
        if (action == "create") {
            collection_create(db, name);
        }
        else if (action == "drop") {
            collection_drop(db, name);
        }
        else if (action == "list") {
            auto context = context_t {db, nullptr};
            auto collections = context.collections().throw_or_release();
            while (!collections.names.is_end()) {
                fmt::print("{}\n", *collections.names);
                ++collections.names;
            }
        }
        else if (action == "import")
            docs_import(db, coll_name, input_file, id_field, max_batch_size);
        else if (action == "export")
            docs_export(db, coll_name, output_ext, max_batch_size);
    }
    else if (selected == "snapshot") {
        close = true;
        if (action == "create") {
            db.snapshot();
        }
        else if (action == "export") {
            auto context = context_t {db, nullptr};
            context.export_to(export_path.c_str());
        }
        else if (action == "drop") {
            // TODO
        }
        else if (action == "list") {
            auto context = context_t {db, nullptr};
            auto snapshots = context.snapshots().throw_or_release();
            auto it = snapshots.begin();
            while (it != snapshots.end()) {
                fmt::print("{}\n", *it);
                ++it;
            }
        }
    }

    if (close)
        return 0;

    std::string input;
    std::vector<std::string> commands;
    std::regex const reg_exp(" +(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    std::regex_token_iterator<std::string::iterator> const end_tokens;

    while (true) {
        commands.clear();
        input = readline(">>> ");
        add_history(input.c_str());
        std::regex_token_iterator<std::string::iterator> it(input.begin(), input.end(), reg_exp, -1);
        while (it != end_tokens)
            commands.emplace_back(*it++);
        if (!commands[0].size())
            commands.erase(commands.begin());

        if (commands[0] == "exit" && commands.size() == 1)
            break;

        if (commands[0] == "clear" && commands.size() == 1) {
            system("clear");
            continue;
        }

        if (commands[0] == "collection") {

            auto& action = commands[1];
            if (action == "create") {

                if (commands.size() != 3) {
                    fmt::print("{}Invalid input{}\n", RED, RESET);
                    continue;
                }

                name = remove_quotes(commands[2]);
                collection_create(db, name);
            }
            else if (action == "drop") {

                if (commands.size() != 3) {
                    fmt::print("{}Invalid input{}\n", RED, RESET);
                    continue;
                }

                name = remove_quotes(commands[2]);
                collection_drop(db, name);
            }
            else if (action == "list") {

                if (commands.size() != 2) {
                    fmt::print("{}Invalid input{}\n", RED, RESET);
                    continue;
                }

                auto context = context_t {db, nullptr};
                auto collections = context.collections();
                if (!collections) {
                    fmt::print("{}Failed to list collections{}\n", RED, RESET);
                    continue;
                }
                while (!collections->names.is_end()) {
                    fmt::print("{}{}{}\n", YELLOW, *collections->names, RESET);
                    ++collections->names;
                }
            }
            else
                fmt::print("{}Invalid collection action {}{}\n", RED, action, RESET);
        }
        else if (commands[0] == "snapshot") {

            auto& action = commands[1];
            if (action == "create") {

                if (commands.size() != 2) {
                    fmt::print("{}Invalid input{}\n", RED, RESET);
                    continue;
                }

                auto snapshot = db.snapshot();
                if (snapshot)
                    fmt::print("{}Snapshot created{}\n", GREEN);
                else
                    fmt::print("{}Failed to created snapshot{}\n", RED);
            }
            else if (action == "export") {

                if (commands.size() != 3) {
                    fmt::print("{}Invalid input{}\n", RED, RESET);
                    continue;
                }

                auto context = context_t {db, nullptr};
                auto status = context.export_to(export_path.c_str());
                if (status)
                    fmt::print("{}Snapshot exported{}\n", GREEN);
                else
                    fmt::print("{}Failed to export snapshot{}\n", RED);
            }
            else if (action == "drop") {
                // TODO
            }
            else if (action == "list") {

                if (commands.size() != 2) {
                    fmt::print("{}Invalid input{}\n", RED, RESET);
                    continue;
                }

                auto context = context_t {db, nullptr};
                auto snapshots = context.snapshots().throw_or_release();
                auto it = snapshots.begin();
                while (it != snapshots.end()) {
                    fmt::print("{}{}{}\n", YELLOW, *it, RESET);
                    ++it;
                }
            }
        }
        else if (commands[0] == "import") {
            if (commands.size() != 9 && commands.size() != 7) {
                fmt::print("{}Invalid input{}\n", RED, RESET);
                continue;
            }

            auto& argument = commands[1];
            if (argument == "--input")
                input_file = commands[2];
            else {
                fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                continue;
            }

            argument = commands[3];
            if (argument == "--id")
                id_field = commands[4];
            else {
                fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                continue;
            }

            argument = commands[5];
            if (argument == "--max_batch_size")
                max_batch_size = std::stoi(commands[6]);
            else {
                fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                continue;
            }

            if (commands.size() == 9) {
                argument = commands[7];
                if (argument == "--collection")
                    coll_name = commands[8];
                else {
                    fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                    continue;
                }
            }
            else
                coll_name = "";

            docs_import(db, coll_name, input_file, id_field, max_batch_size);
        }
        else if (commands[0] == "export") {
            if (commands.size() != 7 && commands.size() != 5) {
                fmt::print("{}Invalid input{}\n", RED, RESET);
                continue;
            }

            auto& argument = commands[1];
            if (argument == "--output")
                output_ext = commands[2];
            else {
                fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                continue;
            }

            argument = commands[3];
            if (argument == "--max_batch_size")
                max_batch_size = std::stoi(commands[4]);
            else {
                fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                continue;
            }

            if (commands.size() == 7) {
                argument = commands[5];
                if (argument == "--collection")
                    coll_name = commands[6];
                else {
                    fmt::print("{}Invalid list argument {}{}\n", RED, argument, RESET);
                    continue;
                }
            }
            else
                coll_name = "";

            docs_export(db, coll_name, output_ext, max_batch_size);
        }
        else
            fmt::print("{}Invalid input{}\n", RED, RESET);
    };

    return 0;
}
