/**
 * @brief Python bindings for a Document Store, that mimics Pandas.
 * Mostly intended for usage with NumPy and Arrow buffers.
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

#include "pybind.hpp"
#include "ukv_docs.h"

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

void ukv::wrap_dataframe(py::module& m) {

    auto df = py::class_<dataframe_t, std::shared_ptr<dataframe_t>>(m, "DataFrame", py::module_local());
    df.def(py::init([](std::vector<std::string> fields) { return std::make_shared<dataframe_t>(); }));

    // Batch Access
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.loc
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.iloc
    df.def("loc", [](dataframe_t& df, py::handle const ids, ukv_format_t) {});
    df.def("iloc", [](dataframe_t& df, py::handle const ids, ukv_format_t) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html
    df.def("sample", [](dataframe_t& df, std::size_t count, bool replace) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.append.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.append.html
    df.def("append", [](dataframe_t& df, py::bytes const& data, ukv_format_t) {});
    df.def("assign", [](dataframe_t& df, py::bytes const& data, ukv_format_t) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_numpy.html
    df.def("to_json", [](dataframe_t& df, py::object const& path_or_buf) {});
    df.def("to_parquet", [](dataframe_t& df, py::object const& path_or_buf) {});
    df.def("to_csv", [](dataframe_t& df, py::object const& path_or_buf) {});
    df.def("to_numpy", [](dataframe_t& df, py::handle const& mat) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.replace.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.join.html
    df.def("replace", [](dataframe_t& df) {});
    df.def("merge", [](dataframe_t& df) {});
    df.def("join", [](dataframe_t& df) {});
}
