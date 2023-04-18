#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#include <map>
#include <cstring>
#include <fstream>
#include <numeric>
#include <algorithm>
#include <filesystem>

#include <simdjson.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <ukv/ukv.hpp>

using namespace unum::ukv;

constexpr ukv_str_view_t dataset_path_k = "~/Datasets/tweets32K-clean.ndjson";
constexpr size_t docs_count = 1000;
static database_t db;

simdjson::ondemand::object& rewinded(simdjson::ondemand::object& doc) noexcept {
    doc.reset();
    return doc;
}

std::vector<ukv_doc_field_type_t> types;
std::vector<edge_t> vtx_n_edges;
std::vector<std::string> fields;
std::vector<value_view_t> docs;
std::vector<ukv_key_t> keys;

constexpr ukv_str_view_t id = "id";

void make_batch() {

    bool state = true;

    std::string dataset_path = dataset_path_k;
    dataset_path = std::filesystem::path(std::getenv("HOME")) / dataset_path.substr(2);

    auto handle = open(dataset_path.c_str(), O_RDONLY);
    ukv_size_t file_size = std::filesystem::file_size(std::filesystem::path(dataset_path.c_str()));
    auto begin = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, handle, 0);
    std::string_view mapped_content = std::string_view(reinterpret_cast<ukv_char_t const*>(begin), file_size);
    madvise(begin, file_size, MADV_SEQUENTIAL);

    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream documents = parser.iterate_many( //
        mapped_content.data(),
        mapped_content.size(),
        1000000ul);

    docs.reserve(docs_count);
    keys.reserve(docs_count);
    vtx_n_edges.reserve(docs_count);
    size_t count = 0;
    size_t idx = 0;
    for (auto doc : documents) {
        simdjson::ondemand::object object = doc.get_object().value();
        if (state) {
            simdjson::dom::parser dom_parser;
            simdjson::dom::element doc_ = dom_parser.parse(rewinded(object).raw_json().value());
            simdjson::dom::object obj = doc_.get_object();

            for (auto [key, value] : obj)
                fields.push_back(key.data());
        }
        if (state) {
            types.reserve(fields.size());
            for (auto field : fields) {
                switch (rewinded(object)[field].type()) {
                case simdjson::ondemand::json_type::array: types.push_back(ukv_doc_field_str_k); break;
                case simdjson::ondemand::json_type::object: types.push_back(ukv_doc_field_json_k); break;
                case simdjson::ondemand::json_type::number: {
                    if (rewinded(object)[field].is_integer())
                        types.push_back(ukv_doc_field_i64_k);
                    else
                        types.push_back(ukv_doc_field_f64_k);
                } break;
                case simdjson::ondemand::json_type::string: types.push_back(ukv_doc_field_str_k); break;
                case simdjson::ondemand::json_type::boolean: types.push_back(ukv_doc_field_bool_k); break;
                case simdjson::ondemand::json_type::null: types.push_back(ukv_doc_field_null_k); break;
                default: break;
                }
            }
            state = false;
        }
        docs.push_back(rewinded(object).raw_json().value());
        keys.push_back(rewinded(object)[id]);
        vtx_n_edges.push_back(edge_t {idx, idx + 1, idx + 2});
        ++count;
        if (count == docs_count)
            break;
        idx += 3;
    }
    close(handle);
}

void test_single_read_n_write() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    ukv_key_t key = std::rand();
    auto str = fmt::format("{{\"_id\":{},\"doc\":\"abcdefghijklmnop\"}}", key);
    value_view_t write_value = str.c_str();

    ukv_docs_write_t write {};
    write.db = db;
    write.error = status.member_ptr();
    write.arena = arena.member_ptr();
    write.collections = &collection;
    write.options = ukv_options_default_k;
    write.tasks_count = 1;
    write.type = ukv_doc_field_json_k;
    write.modification = ukv_doc_modify_upsert_k;
    write.lengths = write_value.member_length();
    write.values = write_value.member_ptr();
    write.id_field = "_id";
    ukv_docs_write(&write);
    EXPECT_TRUE(status);

    ukv_bytes_ptr_t read_value = nullptr;
    ukv_docs_read_t read {};
    read.db = db;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = ukv_options_default_k;
    read.type = ukv_doc_field_json_k;
    read.tasks_count = 1;
    read.collections = &collection;
    read.keys = &key;
    read.values = &read_value;
    ukv_docs_read(&read);
    EXPECT_TRUE(status);
    EXPECT_EQ(std::strcmp(write_value.c_str(), (char const*)read_value), 0);

    db.clear().throw_unhandled();

    write.keys = &key;
    write.id_field = nullptr;
    ukv_docs_write(&write);
    EXPECT_TRUE(status);

    read_value = nullptr;
    ukv_docs_read(&read);
    EXPECT_TRUE(status);
    EXPECT_EQ(std::strcmp(write_value.c_str(), (char const*)read_value), 0);
    db.clear().throw_unhandled();
}

void test_batch_read_n_write() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    ukv_docs_write_t write {};
    write.db = db;
    write.error = status.member_ptr();
    write.arena = arena.member_ptr();
    write.collections = &collection;
    write.options = ukv_options_default_k;
    write.tasks_count = keys.size();
    write.type = ukv_doc_field_json_k;
    write.modification = ukv_doc_modify_upsert_k;
    write.keys = keys.data();
    write.keys_stride = sizeof(ukv_key_t);
    write.lengths = docs.front().member_length();
    write.lengths_stride = sizeof(value_view_t);
    write.values = docs.front().member_ptr();
    write.values_stride = sizeof(value_view_t);
    ukv_docs_write(&write);
    EXPECT_TRUE(status);

    ukv_octet_t* presences = nullptr;
    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_bytes_ptr_t values = nullptr;

    ukv_docs_read_t read {};
    read.db = db;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = ukv_options_default_k;
    read.type = ukv_doc_field_json_k;
    read.tasks_count = keys.size();
    read.collections = &collection;
    read.keys = keys.data();
    read.keys_stride = sizeof(ukv_key_t);
    read.presences = &presences;
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values;
    ukv_docs_read(&read);
    EXPECT_TRUE(status);

    strided_iterator_gt<ukv_length_t const> offs {offsets, sizeof(ukv_length_t)};
    strided_iterator_gt<ukv_length_t const> lens {lengths, sizeof(ukv_length_t)};
    strided_iterator_gt<ukv_bytes_cptr_t const> vals {&values, 0};
    bits_view_t preses {presences};

    contents_arg_t contents {preses, offs, lens, vals, keys.size()};

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        EXPECT_EQ(std::strncmp(docs[idx].c_str(), contents[idx].c_str(), docs[idx].size()), 0);
    }

    db.clear().throw_unhandled();

    write.keys = nullptr;
    write.id_field = id;
    ukv_docs_write(&write);
    EXPECT_TRUE(status);

    presences = nullptr;
    offsets = nullptr;
    lengths = nullptr;
    values = nullptr;
    ukv_docs_read(&read);
    EXPECT_TRUE(status);

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        EXPECT_EQ(std::strncmp(docs[idx].c_str(), contents[idx].c_str(), docs[idx].size()), 0);
    }
    db.clear().throw_unhandled();
}

void test_gist() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    ukv_docs_write_t write {};
    write.db = db;
    write.error = status.member_ptr();
    write.arena = arena.member_ptr();
    write.collections = &collection;
    write.options = ukv_options_default_k;
    write.tasks_count = keys.size();
    write.type = ukv_doc_field_json_k;
    write.modification = ukv_doc_modify_upsert_k;
    write.keys = keys.data();
    write.keys_stride = sizeof(ukv_key_t);
    write.lengths = docs.front().member_length();
    write.lengths_stride = sizeof(value_view_t);
    write.values = docs.front().member_ptr();
    write.values_stride = sizeof(value_view_t);
    ukv_docs_write(&write);
    EXPECT_TRUE(status);

    ukv_size_t fields_count = 0;
    ukv_length_t* offsets = nullptr;
    ukv_char_t* fields_ptr = nullptr;

    ukv_docs_gist_t gist {};
    gist.db = db;
    gist.error = status.member_ptr();
    gist.arena = arena.member_ptr();
    gist.docs_count = keys.size();
    gist.collections = &collection;
    gist.keys = keys.data();
    gist.keys_stride = sizeof(ukv_key_t);
    gist.fields_count = &fields_count;
    gist.offsets = &offsets;
    gist.fields = &fields_ptr;
    ukv_docs_gist(&gist);

    EXPECT_TRUE(status);
    EXPECT_EQ(fields_count, fields.size());
    for (size_t idx = 0; idx < fields.size(); ++idx) {
        EXPECT_EQ(std::strcmp(fields_ptr + offsets[idx] + 1, fields[idx].c_str()), 0);
    }
    db.clear().throw_unhandled();
}

void test_graph_single_upsert() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    ukv_key_t source = std::rand();
    ukv_key_t target = std::rand();
    ukv_key_t edge = std::rand();

    ukv_graph_upsert_edges_t upsert {};
    upsert.db = db;
    upsert.error = status.member_ptr();
    upsert.arena = arena.member_ptr();
    upsert.options = ukv_options_default_k;
    upsert.tasks_count = 1;
    upsert.collections = &collection;
    upsert.edges_ids = &edge;
    upsert.sources_ids = &source;
    upsert.targets_ids = &target;
    ukv_graph_upsert_edges(&upsert);

    EXPECT_TRUE(status);

    ukv_vertex_role_t role = ukv_vertex_role_any_k;
    ukv_vertex_degree_t* degrees = nullptr;
    ukv_key_t* ids = nullptr;
    ukv_key_t keys[2] = {source, target};

    ukv_graph_find_edges_t find {};
    find.db = db;
    find.error = status.member_ptr();
    find.arena = arena.member_ptr();
    find.options = ukv_options_default_k;
    find.tasks_count = 2;
    find.collections = &collection;
    find.vertices = keys;
    find.vertices_stride = sizeof(ukv_key_t);
    find.roles = &role;
    find.degrees_per_vertex = &degrees;
    find.edges_per_vertex = &ids;
    ukv_graph_find_edges(&find);
    EXPECT_TRUE(status);

    ukv_key_t expected[2][3] = {{source, target, edge}, {target, source, edge}};

    size_t ids_count = std::transform_reduce(degrees, degrees + 2, 0ul, std::plus {}, [](ukv_vertex_degree_t d) {
        return d != ukv_vertex_degree_missing_k ? d : 0;
    });
    ids_count *= 3;
    EXPECT_EQ(ids_count, 6);
    for (size_t i = 0, idx = 0; i < 2; ++i) {
        for (size_t j = 0; j < 3; ++j, ++idx) {
            EXPECT_EQ(ids[idx], expected[i][j]);
        }
    }
    db.clear().throw_unhandled();
}

void test_graph_batch_upsert_edges() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    auto strided = edges(vtx_n_edges);
    ukv_graph_upsert_edges_t upsert {};
    upsert.db = db;
    upsert.error = status.member_ptr();
    upsert.arena = arena.member_ptr();
    upsert.options = ukv_options_default_k;
    upsert.tasks_count = vtx_n_edges.size();
    upsert.collections = &collection;
    upsert.edges_ids = strided.edge_ids.begin().get();
    upsert.edges_stride = strided.edge_ids.stride();
    upsert.sources_ids = strided.source_ids.begin().get();
    upsert.sources_stride = strided.source_ids.stride();
    upsert.targets_ids = strided.target_ids.begin().get();
    upsert.targets_stride = strided.target_ids.stride();
    ukv_graph_upsert_edges(&upsert);
    EXPECT_TRUE(status);

    ukv_vertex_role_t role = ukv_vertex_source_k;
    ukv_vertex_degree_t* degrees = nullptr;
    ukv_key_t* ids = nullptr;

    ukv_graph_find_edges_t find {};
    find.db = db;
    find.error = status.member_ptr();
    find.arena = arena.member_ptr();
    find.options = ukv_options_default_k;
    find.tasks_count = strided.source_ids.size();
    find.collections = &collection;
    find.vertices = strided.source_ids.begin().get();
    find.vertices_stride = strided.source_ids.stride();
    find.roles = &role;
    find.degrees_per_vertex = &degrees;
    find.edges_per_vertex = &ids;
    ukv_graph_find_edges(&find);
    EXPECT_TRUE(status);

    size_t ids_count =
        std::transform_reduce(degrees,
                              degrees + strided.source_ids.size(),
                              0ul,
                              std::plus {},
                              [](ukv_vertex_degree_t d) { return d != ukv_vertex_degree_missing_k ? d : 0; });
    ids_count *= 3;

    EXPECT_EQ(ids_count, vtx_n_edges.size() * 3);
    for (size_t idx = 0; idx < ids_count; idx += 3) {
        EXPECT_EQ(ids[idx], vtx_n_edges[idx / 3].source_id);
        EXPECT_EQ(ids[idx + 1], vtx_n_edges[idx / 3].target_id);
        EXPECT_EQ(ids[idx + 2], vtx_n_edges[idx / 3].id);
    }
    db.clear().throw_unhandled();
}

void test_graph_batch_upsert_vtx() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    auto strided = edges(vtx_n_edges);
    ukv_graph_upsert_vertices_t upsert {};
    upsert.db = db;
    upsert.error = status.member_ptr();
    upsert.arena = arena.member_ptr();
    upsert.options = ukv_options_default_k;
    upsert.tasks_count = vtx_n_edges.size();
    upsert.collections = &collection;
    upsert.vertices = strided.source_ids.begin().get();
    upsert.vertices_stride = strided.source_ids.stride();
    ukv_graph_upsert_vertices(&upsert);
    EXPECT_TRUE(status);

    ukv_length_t count_limits = vtx_n_edges.size();
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* found_keys = nullptr;
    ukv_key_t start_keys = 0;

    ukv_scan_t scan {};
    scan.db = db;
    scan.error = status.member_ptr();
    scan.arena = arena.member_ptr();
    scan.tasks_count = 1;
    scan.collections = &collection;
    scan.start_keys = &start_keys;
    scan.count_limits = &count_limits;
    scan.counts = &found_counts;
    scan.keys = &found_keys;
    ukv_scan(&scan);
    EXPECT_TRUE(status);

    EXPECT_EQ(*found_counts, vtx_n_edges.size());
    for (size_t idx = 0; idx < *found_counts; ++idx)
        EXPECT_EQ(found_keys[idx], vtx_n_edges[idx].source_id);
    db.clear().throw_unhandled();
}

void test_graph_find() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    auto strided = edges(vtx_n_edges);
    ukv_graph_upsert_edges_t upsert {};
    upsert.db = db;
    upsert.error = status.member_ptr();
    upsert.arena = arena.member_ptr();
    upsert.options = ukv_options_default_k;
    upsert.tasks_count = vtx_n_edges.size();
    upsert.collections = &collection;
    upsert.edges_ids = strided.edge_ids.begin().get();
    upsert.edges_stride = strided.edge_ids.stride();
    upsert.sources_ids = strided.source_ids.begin().get();
    upsert.sources_stride = strided.source_ids.stride();
    upsert.targets_ids = strided.target_ids.begin().get();
    upsert.targets_stride = strided.target_ids.stride();
    ukv_graph_upsert_edges(&upsert);

    EXPECT_TRUE(status);

    ukv_vertex_role_t role = ukv_vertex_source_k;
    ukv_vertex_degree_t* degrees = nullptr;
    ukv_key_t* ids = nullptr;

    EXPECT_TRUE(status);

    ukv_graph_find_edges_t find {};
    find.db = db;
    find.error = status.member_ptr();
    find.arena = arena.member_ptr();
    find.options = ukv_options_default_k;
    find.tasks_count = strided.source_ids.size();
    find.collections = &collection;
    find.vertices = strided.source_ids.begin().get();
    find.vertices_stride = strided.source_ids.stride();
    find.roles = &role;
    find.degrees_per_vertex = &degrees;
    find.edges_per_vertex = &ids;
    ukv_graph_find_edges(&find);
    EXPECT_TRUE(status);

    size_t ids_count =
        std::transform_reduce(degrees,
                              degrees + strided.source_ids.size(),
                              0ul,
                              std::plus {},
                              [](ukv_vertex_degree_t d) { return d != ukv_vertex_degree_missing_k ? d : 0; });
    ids_count *= 3;

    EXPECT_EQ(ids_count, vtx_n_edges.size() * 3);
    for (size_t idx = 0; idx < ids_count; idx += 3) {
        EXPECT_EQ(ids[idx], vtx_n_edges[idx / 3].source_id);
        EXPECT_EQ(ids[idx + 1], vtx_n_edges[idx / 3].target_id);
        EXPECT_EQ(ids[idx + 2], vtx_n_edges[idx / 3].id);
    }

    role = ukv_vertex_target_k;
    degrees = nullptr;
    ids = nullptr;
    find.tasks_count = strided.target_ids.size();
    find.vertices = strided.target_ids.begin().get();
    find.vertices_stride = strided.target_ids.stride();
    ukv_graph_find_edges(&find);
    EXPECT_TRUE(status);

    ids_count = std::transform_reduce(degrees,
                                      degrees + strided.target_ids.size(),
                                      0ul,
                                      std::plus {},
                                      [](ukv_vertex_degree_t d) { return d != ukv_vertex_degree_missing_k ? d : 0; });
    ids_count *= 3;

    EXPECT_EQ(ids_count, vtx_n_edges.size() * 3);
    for (size_t idx = 0; idx < ids_count; idx += 3) {
        EXPECT_EQ(ids[idx], vtx_n_edges[idx / 3].target_id);
        EXPECT_EQ(ids[idx + 1], vtx_n_edges[idx / 3].source_id);
        EXPECT_EQ(ids[idx + 2], vtx_n_edges[idx / 3].id);
    }

    std::vector<edge_t> expected = vtx_n_edges;
    for (auto _ : vtx_n_edges)
        expected.push_back(edge_t {_.target_id, _.source_id, _.id});

    std::sort(expected.begin(), expected.end(), [](auto const& lhs, auto const& rhs) {
        return lhs.source_id < rhs.source_id;
    });

    auto exp_strided = edges(expected);
    role = ukv_vertex_role_any_k;
    degrees = nullptr;
    ids = nullptr;
    find.tasks_count = exp_strided.source_ids.size();
    find.vertices = exp_strided.source_ids.begin().get();
    find.vertices_stride = exp_strided.source_ids.stride();
    ukv_graph_find_edges(&find);
    EXPECT_TRUE(status);

    ids_count = std::transform_reduce(degrees,
                                      degrees + exp_strided.source_ids.size(),
                                      0ul,
                                      std::plus {},
                                      [](ukv_vertex_degree_t d) { return d != ukv_vertex_degree_missing_k ? d : 0; });
    ids_count *= 3;

    EXPECT_EQ(ids_count, expected.size() * 3);
    for (size_t idx = 0; idx < ids_count; idx += 3) {
        EXPECT_EQ(ids[idx], expected[idx / 3].source_id);
        EXPECT_EQ(ids[idx + 1], expected[idx / 3].target_id);
        EXPECT_EQ(ids[idx + 2], expected[idx / 3].id);
    }
    db.clear().throw_unhandled();
}

void test_graph_remove_edges() {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    auto strided = edges(vtx_n_edges);
    ukv_graph_upsert_edges_t upsert {};
    upsert.db = db;
    upsert.error = status.member_ptr();
    upsert.arena = arena.member_ptr();
    upsert.options = ukv_options_default_k;
    upsert.tasks_count = vtx_n_edges.size();
    upsert.collections = &collection;
    upsert.edges_ids = strided.edge_ids.begin().get();
    upsert.edges_stride = strided.edge_ids.stride();
    upsert.sources_ids = strided.source_ids.begin().get();
    upsert.sources_stride = strided.source_ids.stride();
    upsert.targets_ids = strided.target_ids.begin().get();
    upsert.targets_stride = strided.target_ids.stride();
    ukv_graph_upsert_edges(&upsert);
    EXPECT_TRUE(status);

    ukv_graph_remove_edges_t remove {};
    remove.db = db;
    remove.error = status.member_ptr();
    remove.arena = arena.member_ptr();
    remove.options = ukv_options_default_k;
    remove.tasks_count = vtx_n_edges.size();
    remove.collections = &collection;
    remove.edges_ids = strided.edge_ids.begin().get();
    remove.edges_stride = strided.edge_ids.stride();
    remove.sources_ids = strided.source_ids.begin().get();
    remove.sources_stride = strided.source_ids.stride();
    remove.targets_ids = strided.target_ids.begin().get();
    remove.targets_stride = strided.target_ids.stride();
    ukv_graph_remove_edges(&remove);
    EXPECT_TRUE(status);

    std::vector<ukv_key_t> all_keys;
    all_keys.reserve(vtx_n_edges.size() * 2);
    for (auto key : strided.source_ids)
        all_keys.push_back(key);

    for (auto key : strided.target_ids)
        all_keys.push_back(key);
    std::sort(all_keys.begin(), all_keys.end());

    ukv_vertex_role_t role = ukv_vertex_role_any_k;
    ukv_key_t* ids = nullptr;

    ukv_graph_find_edges_t find {};
    find.db = db;
    find.error = status.member_ptr();
    find.arena = arena.member_ptr();
    find.options = ukv_options_default_k;
    find.tasks_count = all_keys.size();
    find.collections = &collection;
    find.vertices = all_keys.data();
    find.vertices_stride = sizeof(ukv_key_t);
    find.roles = &role;
    find.edges_per_vertex = &ids;
    ukv_graph_find_edges(&find);
    EXPECT_TRUE(status);
    EXPECT_EQ(ids, nullptr);
    db.clear().throw_unhandled();
}

void test_graph_remove_vertices(ukv_vertex_role_t role) {
    status_t status;
    arena_t arena(db);
    ukv_collection_t collection = db.main();

    auto strided = edges(vtx_n_edges);
    ukv_graph_upsert_edges_t upsert {};
    upsert.db = db;
    upsert.error = status.member_ptr();
    upsert.arena = arena.member_ptr();
    upsert.options = ukv_options_default_k;
    upsert.tasks_count = vtx_n_edges.size();
    upsert.collections = &collection;
    upsert.edges_ids = strided.edge_ids.begin().get();
    upsert.edges_stride = strided.edge_ids.stride();
    upsert.sources_ids = strided.source_ids.begin().get();
    upsert.sources_stride = strided.source_ids.stride();
    upsert.targets_ids = strided.target_ids.begin().get();
    upsert.targets_stride = strided.target_ids.stride();
    ukv_graph_upsert_edges(&upsert);
    EXPECT_TRUE(status);

    std::vector<ukv_key_t> all_keys;
    all_keys.reserve(vtx_n_edges.size() * 2);
    if (role == ukv_vertex_role_any_k || role == ukv_vertex_source_k)
        for (auto key : strided.source_ids)
            all_keys.push_back(key);

    if (role == ukv_vertex_role_any_k || role == ukv_vertex_target_k) {
        for (auto key : strided.target_ids)
            all_keys.push_back(key);
    }

    ukv_graph_remove_vertices_t remove {};
    remove.db = db;
    remove.error = status.member_ptr();
    remove.arena = arena.member_ptr();
    remove.options = ukv_options_default_k;
    remove.tasks_count = all_keys.size();
    remove.collections = &collection;
    remove.vertices = all_keys.data();
    remove.vertices_stride = sizeof(ukv_key_t);
    remove.roles = &role;
    ukv_graph_remove_vertices(&remove);
    EXPECT_TRUE(status);

    ukv_length_t count_limits = vtx_n_edges.size() * 2;
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* found_keys = nullptr;
    ukv_key_t start_keys = 0;

    ukv_scan_t scan {};
    scan.db = db;
    scan.error = status.member_ptr();
    scan.arena = arena.member_ptr();
    scan.tasks_count = 1;
    scan.collections = &collection;
    scan.start_keys = &start_keys;
    scan.count_limits = &count_limits;
    scan.counts = &found_counts;
    scan.keys = &found_keys;
    ukv_scan(&scan);
    EXPECT_TRUE(status);
    if (role == ukv_vertex_role_any_k)
        EXPECT_EQ(*found_counts, 0);
    else if (role == ukv_vertex_source_k) {
        EXPECT_EQ(*found_counts, vtx_n_edges.size());
        size_t idx = 0;
        for (auto key : strided.target_ids) {
            EXPECT_EQ(key, found_keys[idx]);
            ++idx;
        }
    }
    else if (role == ukv_vertex_target_k) {
        EXPECT_EQ(*found_counts, vtx_n_edges.size());
        size_t idx = 0;
        for (auto key : strided.source_ids) {
            EXPECT_EQ(key, found_keys[idx]);
            ++idx;
        }
    }
    db.clear().throw_unhandled();
}

TEST(docs, read_n_write) {
    test_single_read_n_write();
    test_batch_read_n_write();
}

TEST(docs, gist) {
    test_gist();
}

TEST(grpah, upsert) {
    test_graph_single_upsert();
    test_graph_batch_upsert_vtx();
    test_graph_batch_upsert_edges();
}

TEST(grpah, find) {
    test_graph_find();
}

TEST(grpah, remove) {
    test_graph_remove_edges();
    test_graph_remove_vertices(ukv_vertex_role_any_k);
    test_graph_remove_vertices(ukv_vertex_source_k);
    test_graph_remove_vertices(ukv_vertex_target_k);
}

int main(int argc, char** argv) {
    make_batch();
    db.open().throw_unhandled();
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}