
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
    ukv_col_t* col = nullptr;
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
struct py_db_t : public std::enable_shared_from_this<py_db_t> {
    db_t native;
    std::string config;

    py_db_t(db_t&& n, std::string const& c) : native(std::move(n)), config(c) {}
    py_db_t(py_db_t&& other) noexcept : native(std::move(other.native)), config(std::move(other.config)) {}
    py_db_t(py_db_t const&) = delete;
};

/**
 * @brief Only adds reference counting to the native C++ interface.
 */
struct py_txn_t : public std::enable_shared_from_this<py_txn_t> {
    txn_t native;

    bool track_reads = false;
    bool flush_writes = false;

    py_txn_t(std::shared_ptr<py_db_t>&& d, txn_t&& t) noexcept : db_ptr(std::move(d)), native(std::move(t)) {}
    py_txn_t(py_txn_t&& other) noexcept : db_ptr(std::move(other.db_ptr)), native(std::move(other.native)) {}
    py_txn_t(py_txn_t const&) = delete;
};

/**
 * @brief Wrapper for `ukv::col_t`.
 * We need to preserve the `name`, to upsert again, after removing it in `clear`.
 * We also keep the transaction pointer, to persist the context of operation.
 */
struct py_col_t {
    col_t native;

    std::shared_ptr<py_db_t> db_ptr;
    std::shared_ptr<py_txn_t> txn_ptr;
    std::string name;

    py_col_t() {}
    py_col_t(py_col_t const&) = delete;
    py_col_t(py_col_t&& other) noexcept
        : db_ptr(std::move(other.db_ptr)), txn_ptr(std::move(other.txn_ptr)), native(std::move(other.native)),
          name(std::move(other.name)) {}

    operator py_task_ctx_t() & noexcept {
        py_task_ctx_t result;
        if (txn_ptr) {
            result.txn = txn_ptr->native;
            result.options = static_cast<ukv_options_t>( //
                ukv_options_default_k |                  //
                (txn_ptr->track_reads ? ukv_option_read_track_k : ukv_options_default_k) |
                (txn_ptr->flush_writes ? ukv_option_write_flush_k : ukv_options_default_k));
        }
        result.db = native.db();
        result.col = native.member_ptr();
        result.arena = native.member_arena();
        return result;
    }
};

struct py_graph_t {

    std::shared_ptr<py_db_t> db_ptr;
    col_t index;
    col_t sources_attrs;
    col_t targets_attrs;
    col_t relations_attrs;

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
    ukv_col_t col = ukv_col_main_k;
    ukv_key_t min = std::numeric_limits<ukv_key_t>::min();
    ukv_key_t max = std::numeric_limits<ukv_key_t>::max();
    std::size_t limit = std::numeric_limits<std::size_t>::max();
};

/**
 * @brief DataFrame represntation, capable of viewing joined contents
 * of multiple collections. When materialized, exports Apache Arrow objects.
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
 * @brief Proxy-object for binary `py_col_t` collections that adds:
 * > serialization & deserialization of Python objects.
 * > field-level lookups.
 * > patching & merging: `.patch(...)` & `.merge(...)`.
 * > DataFrame exports (out of this single collection).
 */
struct py_docs_col_t {
    py_col_t binary;
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
 *      * get_column(keys) ~ Will extract/receive binary values as Apache Arrow collections
 *      * get_matrix(keys, max_length: int, padding: byte)
 *
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
 * Unlike C++ `graph_ref_t` this may include as many as 4 collections
 * seen as one heavily attributed relational index.
 * Is similar in it's purpose to a pure-Python project - NetworkXum:
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
 * Beyond that, source and target vertices can belong to different collections.
 * To sum up, we differntiate following graph types:
 * > U: Undirected
 * > D: Directed
 * > J: Joining
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
 *
 * TODO:
 * > Implement basic algorithms: PageRank, Louvain, WCC and Force-based Layout
 * > Implement subgraph selection
 */
void wrap_networkx(py::module&);

/**
 * @brief Python bindings for a Document Store, that mimics Pandas.
 * Mostly intended for usage with NumPy and Arrow buffers.
 */
void wrap_pandas(py::module&);

} // namespace unum::ukv