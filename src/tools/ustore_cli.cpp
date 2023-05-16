#include <iostream>
#include <regex>

#include <fmt/format.h>

#include <clipp.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "ustore/cpp/db.hpp"
#include "dataset.h"

using namespace unum::ustore;
using namespace unum;

#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\x1B[33m"
#define RESET "\033[0m"

#pragma region Helpers
inline std::string remove_quotes(std::string str) {
    if (str[0] == '\"' && str[str.size() - 1] == '\"') {
        str.erase(0, 1);
        str.erase(str.size() - 1);
    }
    return str;
}

template <typename... args_at>
inline void print(char const* color, std::string message, args_at&&... args) {
    if (sizeof...(args) != 0)
        message = fmt::format(message, std::forward<args_at>(args)...);
    fmt::print("{}{}{}\n", color, message, RESET);
}
#pragma endregion Helpers

#pragma region Collection
inline void collection_create(database_t& db, std::string const& name) {
    auto maybe_collection = db.find_or_create(name.c_str());
    if (maybe_collection)
        print(GREEN, "Collection '{}' created", name);
    else
        print(RED, "Failed to create collection '{}'", name);
}

inline void collection_drop(database_t& db, std::string const& name) {
    auto status = db.drop(name);
    if (status)
        print(GREEN, "Collection '{}' dropped", name);
    else
        print(RED, "Failed to drop collection '{}'", name);
}

void collection_list(database_t& db) {
    auto context = context_t {db, nullptr};
    auto collections = context.collections();

    if (!collections) {
        print(RED, "Failed to list collections");
        return;
    }

    while (!collections->names.is_end()) {
        print(YELLOW, "{}", *collections->names);
        ++collections->names;
    }
}
#pragma endregion Collection

#pragma region Snapshot
inline void snapshot_create(database_t& db) {
    auto snapshot = db.snapshot();
    if (snapshot)
        print(GREEN, "Snapshot created");
    else
        print(RED, "Failed to create snapshot");
}

inline void snapshot_export(database_t& db, std::string const& path) {
    auto context = context_t {db, nullptr};
    auto status = context.export_to(path.c_str());
    if (status)
        print(GREEN, "Snapshot exported");
    else
        print(RED, "Failed to export snapshot");
}

void snapshot_drop(database_t& /* db */, ustore_snapshot_t const& /* id */) {
    // TODO
}

void snapshot_list(database_t& db) {
    auto context = context_t {db, nullptr};
    auto snapshots = context.snapshots().throw_or_release();

    for (auto it = snapshots.begin(); it != snapshots.end(); ++it)
        print(YELLOW, "{}", *it);
}
#pragma endregion Snapshot

#pragma region Import/Export
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
        print(GREEN, "Successfully imported");
    else
        print(RED, "Failed to import: {}", status.message());
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
        print(GREEN, "Successfully exported");
    else
        print(RED, "Failed to export: {}", status.message());
}
#pragma endregion Import / Export

#pragma region Interface

#pragma endregion Interface

int main(int argc, char* argv[]) {
    using namespace clipp;

    bool help = false;
    std::string url = "";
    std::string name;
    std::string db_object;
    std::string action;
    std::string id_field;
    std::string input_file;
    std::string output_ext;
    std::string export_path;
    std::size_t memory_limit;
    ustore_snapshot_t snap_id;

    auto collection =
        (option("collection").set(db_object, std::string("collection")) &
         ((required("create").set(action, std::string("create")) & required("--name") &
           value("collection name", name)) |
          (required("drop").set(action, std::string("drop")) & required("--name") & value("collection name", name)) |
          required("list").set(action, std::string("list")) |
          ((required("import").set(action, std::string("import")) &
            (required("--input") & value("input", input_file)).doc("Input file path") &
            (required("--id") & value("id field", id_field)).doc("The field which data will use as key(s)")) |
           (required("export").set(action, std::string("export")) &
            (required("--output") & value("output", output_ext)).doc("Output file path"))) &
              ((required("--mlimit") & value("memory limit", memory_limit))
                   .doc("Size of available RAM for a specific operation in bytes"),
               (option("--name") & value("collection name", name)))));

    auto snapshot = (option("snapshot").set(db_object, std::string("snapshot")) &
                     ((required("create").set(action, std::string("create"))) |
                      (required("export").set(action, std::string("export")) & value("path", export_path)) |
                      (required("drop").set(action, std::string("drop")) & value("snapshot id", snap_id)) |
                      (required("list").set(action, std::string("list")))));

    auto cli = ((required("--url") & value("URL", url)).doc("Server URL"),
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

    if (db_object == "collection") {
        if (action == "create")
            collection_create(db, name);
        else if (action == "drop")
            collection_drop(db, name);
        else if (action == "list")
            collection_list(db);
        else if (action == "import")
            docs_import(db, name, input_file, id_field, memory_limit);
        else if (action == "export")
            docs_export(db, name, output_ext, memory_limit);
        return 0;
    }
    else if (db_object == "snapshot") {
        if (action == "create")
            snapshot_create(db);
        else if (action == "export")
            snapshot_export(db, export_path);
        else if (action == "drop")
            snapshot_drop(db, snap_id);
        else if (action == "list")
            snapshot_list(db);
        return 0;
    }

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
            std::string const& action = commands[1];
            if (action == "create") {

                if (commands.size() != 3) {
                    print(RED, "Invalid input");
                    continue;
                }

                name = remove_quotes(commands[2]);
                collection_create(db, name);
            }
            else if (action == "drop") {

                if (commands.size() != 3) {
                    print(RED, "Invalid input");
                    continue;
                }

                name = remove_quotes(commands[2]);
                collection_drop(db, name);
            }
            else if (action == "list") {

                if (commands.size() != 2) {
                    print(RED, "Invalid input");
                    continue;
                }
                collection_list(db);
            }
            else
                print(RED, "Invalid collection action {}", action);
        }
        else if (commands[0] == "snapshot") {

            std::string const& action = commands[1];
            if (action == "create") {

                if (commands.size() != 2) {
                    print(RED, "Invalid input");
                    continue;
                }
                snapshot_create(db);
            }
            else if (action == "export") {

                if (commands.size() != 3) {
                    print(RED, "Invalid input");
                    continue;
                }
                snapshot_export(db, export_path);
            }
            else if (action == "drop") {

                if (commands.size() != 3) {
                    print(RED, "Invalid input");
                    continue;
                }
                snapshot_drop(db, snap_id);
            }
            else if (action == "list") {

                if (commands.size() != 2) {
                    print(RED, "Invalid input");
                    continue;
                }
                snapshot_list(db);
            }
            else
                print(RED, "Invalid snapshot action {}", action);
        }
        else if (commands[0] == "import") {

            if (commands.size() != 9 && commands.size() != 7) {
                print(RED, "Invalid input");
                continue;
            }

            std::string argument = commands[1];
            if (argument == "--input")
                input_file = commands[2];
            else {
                print(RED, "Invalid list argument {}", argument);
                continue;
            }

            argument = commands[3];
            if (argument == "--id")
                id_field = commands[4];
            else {
                print(RED, "Invalid list argument {}", argument);
                continue;
            }

            argument = commands[5];
            if (argument == "--mlimit")
                memory_limit = std::stoi(commands[6]);
            else {
                print(RED, "Invalid list argument {}", argument);
                continue;
            }

            if (commands.size() == 9) {
                argument = commands[7];
                if (argument == "--collection")
                    name = commands[8];
                else {
                    print(RED, "Invalid list argument {}", argument);
                    continue;
                }
            }
            else
                name = "";

            docs_import(db, name, input_file, id_field, memory_limit);
        }
        else if (commands[0] == "export") {
            if (commands.size() != 7 && commands.size() != 5) {
                print(RED, "Invalid input");
                continue;
            }

            std::string argument = commands[1];
            if (argument == "--output")
                output_ext = commands[2];
            else {
                print(RED, "Invalid list argument {}", argument);
                continue;
            }

            argument = commands[3];
            if (argument == "--mlimit")
                memory_limit = std::stoi(commands[4]);
            else {
                print(RED, "Invalid list argument {}", argument);
                continue;
            }

            if (commands.size() == 7) {
                argument = commands[5];
                if (argument == "--collection")
                    name = commands[6];
                else {
                    print(RED, "Invalid list argument {}", argument);
                    continue;
                }
            }
            else
                name = "";

            docs_export(db, name, output_ext, memory_limit);
        }
        else
            print(RED, "Invalid input");
    };

    return 0;
}
