#include "pybind.hpp"
#include "crud.hpp"
#include "cast.hpp"

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

void ukv::wrap_pandas(py::module& m) {

    // Once packed, our DataFrames output Apache Arrow Tables / RecordBatches:
    // https://stackoverflow.com/a/57907044/2766161
    // https://arrow.apache.org/docs/python/integration/extending.html
    //
    // Expected usage:
    //
    // > Take first 5 rows starting with ID #100:
    //   db.main.docs.astype('int32').loc[100:].head(5).df
    //   Note that contrary to usual python slices, both the start and the stop are included
    // > Take rows with IDs #100, #101:
    //   db.main.docs.loc[[100, 101]].astype('float').df
    // > Take specific columns from a rows range:
    //   db.main.docs.loc[100:101].astype({'age':'float', 'name':'str'}).df
    //
    auto df = py::class_<py_frame_t, std::shared_ptr<py_frame_t>>(m, "DataFrame", py::module_local());
    df.def(py::init([](py::handle dtype) {
        // `dtype` can be a `dict` or a `list[tuple[str, str]]`, where every pair of
        // strings contains a column name and Python type descriptor
        return std::make_shared<py_frame_t>();
    }));

    df.def("astype", [](py_frame_t& df, py::handle dtype) {
        // `dtype` can be one string, one enum, a `dict` or a `list[tuple[str, str]]`,
        // where every pair of strings contains a column name and Python type descriptor.
        return df.shared_from_this();
    });

    // Primary batch export functions, that output Arrow Tables.
    // Addresses may be: specific IDs or a slice.
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.loc.html#pandas.DataFrame.loc
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.iloc.html#pandas.DataFrame.iloc
    df.def_readonly_property("df", [](py_frame_t& df) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.index.html#pandas.DataFrame.index
    df.def("index", [](py_frame_t& df) {});
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.columns.html#pandas.DataFrame.columns
    df.def("columns", [](py_frame_t& df) {});
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dtypes.html#pandas.DataFrame.dtypes
    df.def("dtypes", [](py_frame_t& df) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html
    df.def("sample", [](py_frame_t& df, std::size_t count, bool replace) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.concat.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.assign.html
    df.def("concat", [](py_frame_t const& df, py_frame_t const& df_other) {});
    df.def("assign", [](py_frame_t& df, py_frame_t const& df_other) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_numpy.html
    df.def("to_json", [](py_frame_t& df, py::object const& path_or_buf) {});
    df.def("to_parquet", [](py_frame_t& df, py::object const& path_or_buf) {});
    df.def("to_csv", [](py_frame_t& df, py::object const& path_or_buf) {});
    df.def("to_numpy", [](py_frame_t& df, py::handle mat) {});
    df.def("to_arrow", [](py_frame_t& df, py::handle mat) {});

    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.replace.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    // https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.join.html
    df.def("replace", [](py_frame_t& df) {});
    df.def("merge", [](py_frame_t& df) {});
    df.def("join", [](py_frame_t& df) {});

    df.def("__getitem__", [](py_frame_t& df, py::handle ids) {});
    df.def("loc", [](py_frame_t& df, py::handle ids, ukv_format_t) {});
    df.def("iloc", [](py_frame_t& df, py::handle ids, ukv_format_t) {});
}
