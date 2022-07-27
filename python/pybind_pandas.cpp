/**
 * @brief Python bindings for a Document Store, that mimics Pandas.
 * Mostly intended for usage with NumPy and Arrow buffers.
 */

#include "pybind.hpp"
#include "ukv/docs.h"

using namespace unum::ukv;
using namespace unum;

/**
 *
 */
struct frame_t : public std::enable_shared_from_this<frame_t> {

    ukv_t db = NULL;
    ukv_collection_t col = NULL;

    std::vector<std::string> fields_subset;

    frame_t() = default;
    frame_t(frame_t&&) = delete;
    frame_t(frame_t const&) = delete;
};

void ukv::wrap_dataframe(py::module& m) {

    // Once packed, our DataFrames output Apache Arrow Tables / RecordBatches:
    // https://stackoverflow.com/a/57907044/2766161
    // https://arrow.apache.org/docs/python/integration/extending.html
    auto df = py::class_<frame_t, std::shared_ptr<frame_t>>(m, "DataFrame", py::module_local());
    df.def(py::init([](std::vector<std::string> fields) { return std::make_shared<frame_t>(); }));

    // Batch Access
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.loc
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.iloc
    df.def("loc", [](frame_t& df, py::handle const ids, ukv_doc_format_t) {});
    df.def("iloc", [](frame_t& df, py::handle const ids, ukv_doc_format_t) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html
    df.def("sample", [](frame_t& df, std::size_t count, bool replace) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.append.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.append.html
    df.def("append", [](frame_t& df, py::bytes const& data, ukv_doc_format_t) {});
    df.def("assign", [](frame_t& df, py::bytes const& data, ukv_doc_format_t) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_numpy.html
    df.def("to_json", [](frame_t& df, py::object const& path_or_buf) {});
    df.def("to_parquet", [](frame_t& df, py::object const& path_or_buf) {});
    df.def("to_csv", [](frame_t& df, py::object const& path_or_buf) {});
    df.def("to_numpy", [](frame_t& df, py::handle const& mat) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.replace.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.join.html
    df.def("replace", [](frame_t& df) {});
    df.def("merge", [](frame_t& df) {});
    df.def("join", [](frame_t& df) {});
}
