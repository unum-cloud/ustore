
#pragma once
#include <utility> // `std::pair`

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include "ukv/ukv.hpp"

namespace unum::ukv {

namespace py = pybind11;

struct py_db_t;
struct py_txn_t;
struct py_col_t;

struct py_graph_t;
struct py_frame_t;

struct py_task_ctx_t;

/**
 * @brief Python tasks are generally called for a single collection.
 * That greatly simplifies the implementation.
 */
struct py_task_ctx_t {
    ukv_t db = nullptr;
    ukv_txn_t txn = nullptr;
    ukv_collection_t* col = nullptr;
    ukv_arena_t* arena = nullptr;
    ukv_options_t options = ukv_options_default_k;
};

/**
 * @brief Wrapper for `ukv::db_t`.
 * Assumes that the Python client won't use more than one
 * concurrent session, as multithreading in Python is
 * prohibitively expensive.
 * We need to preserve the `config`, to allow re-opening.
 */

template <typename stream_t>
struct py_stream_gt : public std::enable_shared_from_this<py_stream_gt<stream_t>> {
    stream_t native;
    ukv_key_t stop_point;
    bool last = false;

    py_stream_gt(stream_t&& stream, ukv_key_t max) : native(std::move(stream)), stop_point(max) {}
    py_stream_gt(py_stream_gt const&) = delete;
    py_stream_gt(py_stream_gt&& other) noexcept
        : native(std::move(other.native)), stop_point(std::move(other.stop_point)) {}
};

template <typename range_t>
struct py_range_gt : public std::enable_shared_from_this<py_range_gt<range_t>> {
    range_t native;
    ukv_key_t min_key;
    ukv_key_t max_key;

    py_range_gt(range_t&& range,
                ukv_key_t min = std::numeric_limits<ukv_key_t>::min(),
                ukv_key_t max = ukv_key_unknown_k)
        : native(std::move(range)), min_key(min), max_key(max) {}
    py_range_gt(py_range_gt const&) = delete;
    py_range_gt(py_range_gt&& other) noexcept
        : native(std::move(other.native)), min_key(std::move(other.min_key)), max_key(std::move(other.max_key)) {}
};

struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    db_t native;
    arena_t arena;
    std::string config;

    py_db_t(db_t&& n, std::string const& c) : native(std::move(n)), arena(native), config(c) {}
    py_db_t(py_db_t const&) = delete;
    py_db_t(py_db_t&& other) noexcept
        : native(std::move(other.native)), arena(std::move(other.arena)), config(std::move(config)) {}

    operator py_task_ctx_t() & noexcept {
        return {native, nullptr, nullptr, arena.member_ptr(), ukv_options_default_k};
    }
};

/**
 * @brief Only adds reference counting to the native C++ interface.
 */
struct py_txn_t : public std::enable_shared_from_this<py_txn_t> {
    std::shared_ptr<py_db_t> db_ptr;
    txn_t native;
    arena_t arena;
    bool track_reads = false;
    bool flush_writes = false;

    py_txn_t(std::shared_ptr<py_db_t>&& d, txn_t&& t) noexcept
        : db_ptr(std::move(d)), native(std::move(t)), arena(db_ptr->native) {}
    py_txn_t(py_txn_t const&) = delete;
    py_txn_t(py_txn_t&& other) noexcept
        : db_ptr(std::move(other.db_ptr)), native(std::move(other.native)), arena(std::move(other.arena)) {}

    operator py_task_ctx_t() & noexcept {
        auto options = static_cast<ukv_options_t>( //
            ukv_options_default_k |                //
            (track_reads ? ukv_option_read_track_k : ukv_options_default_k) |
            (flush_writes ? ukv_option_write_flush_k : ukv_options_default_k));
        return {db_ptr->native, native, nullptr, arena.member_ptr(), options};
    }
};

/**
 * @brief Wrapper for `ukv::collection_t`.
 * We need to preserve the `name`, to upsert again, after removing it in `clear`.
 * We also keep the transaction pointer, to persist the context of operation.
 */
struct py_col_t : public std::enable_shared_from_this<py_col_t> {
    std::shared_ptr<py_db_t> db_ptr;
    std::shared_ptr<py_txn_t> txn_ptr;
    collection_t native;
    std::string name;

    py_col_t() {}
    py_col_t(py_col_t const&) = delete;
    py_col_t(py_col_t&& other) noexcept
        : db_ptr(std::move(other.db_ptr)), txn_ptr(std::move(other.txn_ptr)), native(std::move(other.native)),
          name(std::move(other.name)) {}

    operator py_task_ctx_t() & noexcept {
        py_task_ctx_t result = txn_ptr ? py_task_ctx_t(*txn_ptr) : py_task_ctx_t(*db_ptr);
        result.col = native.member_ptr();
        return result;
    }

    inline collection_t replicate() { return *db_ptr->native.collection(name.c_str()); }
};

/**
 * @brief A generalization of the graph supported by NetworkX.
 *
 * Sources and targets can match.
 * Relations attrs can be banned all together.
 *
 * Example for simple non-attributed undirected graphs:
 * > relations_name: ".graph"
 * > attrs_name: ""
 * > sources_name: ""
 * > targets_name: ""
 *
 * Example for recommender systems
 * > relations_name: "views.graph"
 * > attrs_name: "views.docs"
 * > sources_name: "people.docs"
 * > targets_name: "movies.docs"
 */
struct py_graph_t : public std::enable_shared_from_this<py_graph_t> {

    std::shared_ptr<py_db_t> db_ptr;
    collection_t index;
    collection_t sources_attrs;
    collection_t targets_attrs;
    collection_t relations_attrs;

    bool is_directed_ = false;
    bool is_multi_ = false;
    bool allow_self_loops_ = false;

    Py_buffer last_buffer;
    Py_ssize_t last_buffer_shape[3];
    Py_ssize_t last_buffer_strides[3];

    py_graph_t() {}
    py_graph_t(py_graph_t&&) = delete;
    py_graph_t(py_graph_t const&) = delete;
    ~py_graph_t() {}

    graph_ref_t ref() { return index.as_graph(); }
};

struct py_col_name_t {
    std::string owned;
    ukv_str_view_t view;
};

struct py_col_keys_range_t {
    ukv_collection_t col = ukv_default_collection_k;
    ukv_key_t min = std::numeric_limits<ukv_key_t>::min();
    ukv_key_t max = std::numeric_limits<ukv_key_t>::max();
    std::size_t limit = std::numeric_limits<std::size_t>::max();
};

/**
 * @brief Materialized view over a specific subset of documents
 * UIDs (potentially, in different collections) and column (field) names.
 */
struct py_frame_t : public std::enable_shared_from_this<py_frame_t> {

    ukv_t db = NULL;

    std::variant<std::monostate, py_col_name_t, std::vector<py_col_name_t>> fields;
    std::variant<std::monostate, py_col_keys_range_t, std::vector<col_key_t>> docs;

    py_frame_t() = default;
    py_frame_t(py_frame_t&&) = delete;
    py_frame_t(py_frame_t const&) = delete;
};

/**
 * @brief Binds DBMS to Python, as if it was `dict[str, dict[int, bytes]]`.
 *
 * @section Interface
 *
 * DataBase Methods:
 *      * main ~ Accesses the default collection
 *      * __getitem__(collection: str) ~ Accesses a named collection
 *      * clear() ~ Clears all the data from DB
 *      * transact() - Starts a new transaction (supports context managers)
 *
 * Collection Methods:
 *      * __in__(key), has_key(...) ~ Single & Batch Contains
 *      * __getitem__(key: int), get(...) ~ Value Lookup
 *      * __setitem__(key: int, value), set(...) ~ Value Upserts
 *      * __delitem__(key), pop(...) ~ Removes a key
 * All those CRUD operations can be submitted in batches in forms of
 * Python `tuple`s, `list`s, NumPy arrays, or anything that supports buffer
 * protocol. Remaining collection methods include:
 *      * update(mapping: dict) ~ Batch Insert/Put
 *      * clear() ~ Removes all items in collection
 *      * tensor(collection?, keys, max_length: int, padding: byte)
 * All in all, collections mimic Python @c `dict` API, but some funcs were skipped:
 *      * __len__() ~ It's hard to consistently estimate the collection.
 *      * popitem() ~ We can't guarantee Last-In First-Out semantics.
 *      * setdefault(key[, default]) ~ As default values are useless in DBs.
 * To access typed collections following computable properties are provided:
 *      * docs  ~ Unpack objects into `dict`/`list`s and supports field-level ops
 *      * table ~ Accesses Docs in a Pandas-like fashion
 *      * graph ~ Accesses relations/links in NetworkX fashion
 *      * media ~ Unpacks and converts to Tensors on lookups
 *
 * https://python-reference.readthedocs.io/en/latest/docs/dict/
 * https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
 */
void wrap_database(py::module&);

/**
 * @brief Python bindings for a Graph index, that mimics NetworkX.
 * Is similar in it's purpose to a pure-Python NetworkXum:
 * https://github.com/unum-cloud/NetworkXum
 *
 * @section Supported Graph Types
 * We support all the NetworkX graph kinds and more:
 * https://networkx.org/documentation/stable/reference/classes/index.html#which-graph-class-should-i-use
 *
 *      | Class          | Type         | Self-loops | Parallel edges |
 *      | Graph          | undirected   | Yes        | No             |
 *      | DiGraph        | directed     | Yes        | No             |
 *      | MultiGraph     | undirected   | Yes        | Yes            |
 *      | MultiDiGraph   | directed     | Yes        | Yes            |
 *
 * Aside from those, you can instantiate the most generic `ukv.Network`,
 * controlling whether graph should be directed, allow loops, or have
 * attrs in source/target vertices or edges.
 *
 * @section Interface
 * Primary single element methods:
 *      * add_edge(first, second, key?, attrs?)
 *      * remove_edge(first, second, key?, attrs?)
 * Additional batch methods:
 *      * add_edges_from(firsts, seconds, keys?, attrs?)
 *      * remove_edges_from(firsts, seconds, keys?, attrs?)
 * Intentionally not implemented:
 *      * __len__() ~ It's hard to consistently estimate the collection size.
 */
void wrap_networkx(py::module&);

/**
 * @brief Python bindings for a Document Store, that mimics Pandas.
 * Mostly intended for usage with NumPy and Arrow buffers.
 */
void wrap_pandas(py::module&);

} // namespace unum::ukv