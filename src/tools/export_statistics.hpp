#include <fcntl.h>    // `open` files
#include <sys/stat.h> // `stat` to obtain file metadata
#include <sys/mman.h> // `mmap` to read datasets faster
#include <unistd.h>   // `close` files

#include <string>

#include <fmt/format.h>

#include "ustore/ustore.hpp"

using namespace unum::ustore;
using namespace unum;

static char const* db_path() {
    char* path = std::getenv("USTORE_TEST_PATH");
    if (path)
        return std::strlen(path) ? path : nullptr;

#if defined(USTORE_TEST_PATH)
    return USTORE_TEST_PATH;
#else
    return nullptr;
#endif
}

static std::string db_config() {
    auto dir = db_path();
    if (!dir)
        return {};
    return fmt::format(R"({{"version": "1.0", "directory": "{}"}})", dir);
}

bool export_statistics() {

    database_t db;
    db.open(db_config().c_str()).throw_unhandled();

    status_t status;
    arena_t arena(db);

    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_char_t* names = nullptr;
    ustore_size_t* values = nullptr;
    ustore_size_t count = 0;

    ustore_statistics_list_t stats {};
    stats.db = db;
    stats.error = status.member_ptr();
    stats.arena = arena.member_ptr();
    stats.offsets = &offsets;
    stats.lengths = &lengths;
    stats.names = &names;
    stats.values = &values;
    stats.count = &count;
    ustore_statistics_list(&stats);
    if (!status)
        return false;

    std::remove("statistics.json");
    auto fd = open("statistics.json", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd == -1)
        return false;

    write(fd, "{\n", 2);
    for (std::size_t idx = 0; idx < count - 1; ++idx) {
        auto str = fmt::format("    \"{}\":{},\n", std::string_view {names + offsets[idx], lengths[idx]}, values[idx]);
        write(fd, str.data(), str.size());
    }
    auto str = fmt::format("    \"{}\":{}\n}}\n",
                           std::string_view {names + offsets[count - 1], lengths[count - 1]},
                           values[count - 1]);
    write(fd, str.data(), str.size());

    close(fd);
    db.close();
    return true;
}