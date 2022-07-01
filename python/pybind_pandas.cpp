/**
 * @brief Python bindings for a Document Store, that mimics Pandas.
 * Mostly intended for usage with NumPy and Arrow buffers.
 *
 * @section Supported Graph Types
 * We support all the NetworkX graph kinds and more:
 * https://pandas.org/documentation/stable/reference/classes/index.html#which-graph-class-should-i-use
 *
 *      | Class          | Type         | Self-loops | Parallel edges |
 *      | Graph          | undirected   | Yes        | No             |
 *      | DiGraph        | directed     | Yes        | No             |
 *      | MultiGraph     | undirected   | Yes        | Yes            |
 *      | MultiDiGraph   | directed     | Yes        | Yes            |
 *
 * Aside frim those, you can instantiate the most generic `ukv.Network`,
 * controlling wheather graph should be directed, allow loops, or have
 * attributes in source/target nodes or edges.
 *
 * @section Interface
 * Primary single element methods:
 *      * add_edge(first, second, key?, attrs?)
 *      * remove_edge(first, second, key?, attrs?)
 * Additional batch methods:
 *      * add_edges_from(firsts, seconds, keys?, attrs?)
 *      * remove_edges_from(firsts, seconds, keys?, attrs?)
 * Intentionally not implemented:
 *      * __len__() ~ It's hard to consistently estimate the collection.
 */

#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include "ukv_docs.h"
#include "ukv.hpp"

namespace py = pybind11;
using namespace unum::ukv;
using namespace unum;

/**
 * @brief A generalization of the graph supported by NetworkX.
 *
 */
struct dataframe_t : public std::enable_shared_from_this<dataframe_t> {

    ukv_t db = NULL;
    ukv_collection_t col = NULL;

    std::vector<std::string> fields_subset;

    dataframe_t() = default;
    dataframe_t(dataframe_t&&) = delete;
    dataframe_t(dataframe_t const&) = delete;
};

PYBIND11_MODULE(ukv.pandas, m) {
    m.doc() =
        "Python library for Tabular data processing workloads.\n"
        "Similar to NetworkX, but implemented in C/C++ and \n"
        "with support for persistent storage and ACID operations.\n"
        "---------------------------------------------\n";

    auto df = py::class_<dataframe_t, std::shared_ptr<dataframe_t>>(m, "DataFrame");
    df.def(py::init([](std::vector<std::string> fields, ) { return std::make_shared<dataframe_t>(); }));

    // Batch Access
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.loc
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.iloc
    df.def("loc", [](dataframe_t& df, py::handle const ids, ukv_format_t) {});
    df.def("iloc", [](dataframe_t& df, py::handle const ids, ukv_format_t) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html
    df.def("sample", [](dataframe_t& df, std::size_t count, bool replace) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.replace.html
    df.def("replace", [](dataframe_t& df, std::string to_replace, std::string value) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.append.html
    df.def("append", [](dataframe_t& df, py::bytes const& data, ukv_format_t) {});
}
