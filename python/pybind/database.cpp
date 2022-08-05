#include "pybind.hpp"
#include "crud.hpp"
#include "cast.hpp"

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

static void commit_txn(py_txn_t& py_txn) {

    [[maybe_unused]] py::gil_scoped_release release;
    py_txn.native.commit().throw_unhandled();
}

static py::object punned_collection( //
    py_db_t* py_db_ptr,
    py_txn_t* py_txn_ptr,
    std::string const& collection) {

    db_t& db = py_db_ptr->native;
    txn_t& txn = py_txn_ptr->native;

    auto maybe_col = !py_txn_ptr ? db.collection(collection.c_str()) : txn.collection(collection.c_str());
    maybe_col.throw_unhandled();

    auto py_col = std::make_shared<py_col_t>();
    py_col->name = collection;
    py_col->db_ptr = py_db_ptr->shared_from_this();
    py_col->txn_ptr = py_txn_ptr ? py_txn_ptr->shared_from_this() : nullptr;
    py_col->native = *std::move(maybe_col);
    return py::cast(py_col);
}

template <typename py_wrap_at>
py::array_t<ukv_key_t> py_scan( //
    py_wrap_at& wrap,
    ukv_key_t min_key,
    ukv_size_t scan_length) {

    py_task_ctx_t ctx = wrap;
    ukv_key_t* found_keys = nullptr;
    ukv_val_len_t* found_lengths = nullptr;
    status_t status;

    ukv_scan( //
        ctx.db,
        ctx.txn,
        1,
        ctx.col,
        0,
        &min_key,
        0,
        &scan_length,
        0,
        ctx.options,
        &found_keys,
        &found_lengths,
        ctx.arena,
        status.member_ptr());

    status.throw_unhandled();
    return py::array_t<ukv_key_t>(scan_length, found_keys);
}

template <typename range_at>
auto since(range_at& range, ukv_key_t key) {
    range.min_key = key;
    return std::move(range);
};

template <typename range_at>
auto until(range_at& range, ukv_key_t key) {
    range.max_key = key;
    return std::move(range);
};

template <typename range_at, typename stream_at>
auto iterate(range_at& range) {
    stream_at stream = range.native.begin();
    stream.seek(range.min_key);
    return std::make_shared<py_stream_gt<stream_at>>(std::move(stream), range.max_key);
};

void ukv::wrap_database(py::module& m) {
    // Define our primary classes: `DataBase`, `Collection`, `Transaction`
    auto py_db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase", py::module_local());
    auto py_col = py::class_<py_col_t, std::shared_ptr<py_col_t>>(m, "Collection", py::module_local());
    auto py_txn = py::class_<py_txn_t, std::shared_ptr<py_txn_t>>(m, "Transaction", py::module_local());
    auto py_keys_range =
        py::class_<py_range_gt<keys_range_t>, std::shared_ptr<py_range_gt<keys_range_t>>>(m,
                                                                                          "Keys_Range",
                                                                                          py::module_local());
    auto py_kvrange =
        py::class_<py_range_gt<keys_vals_range_t>, std::shared_ptr<py_range_gt<keys_vals_range_t>>>(m,
                                                                                                    "Items_Range",
                                                                                                    py::module_local());
    auto py_kstream =
        py::class_<py_stream_gt<keys_stream_t>, std::shared_ptr<py_stream_gt<keys_stream_t>>>(m,
                                                                                              "Keys_Stream",
                                                                                              py::module_local());
    auto py_kvstream = py::class_<py_stream_gt<keys_vals_stream_t>, std::shared_ptr<py_stream_gt<keys_vals_stream_t>>>(
        m,
        "Items_Stream",
        py::module_local());

    // Define keys_range
    py_keys_range.def("__iter__", &iterate<py_range_gt<keys_range_t>, keys_stream_t>);
    py_keys_range.def("since", &since<py_range_gt<keys_range_t>>);
    py_keys_range.def("until", &until<py_range_gt<keys_range_t>>);

    py_keys_range.def("__getitem__", [](py_range_gt<keys_range_t>& keys_range, py::slice slice) {
        Py_ssize_t start, stop, step;
        if (PySlice_Unpack(slice.ptr(), &start, &stop, &step) || step != 1 || start >= stop)
            throw std::invalid_argument("Invalid Slice");
        keys_stream_t stream = keys_range.native.begin(stop);
        auto keys = stream.keys_batch();
        return py::array(std::min(stop - start, Py_ssize_t(keys.size()) - start), keys.begin() + start);
    });

    // Define keys_vals_range
    py_kvrange.def("__iter__", &iterate<py_range_gt<keys_vals_range_t>, keys_vals_stream_t>);
    py_kvrange.def("since", &since<py_range_gt<keys_vals_range_t>>);
    py_kvrange.def("until", &until<py_range_gt<keys_vals_range_t>>);

    // Define keys_stream
    py_kstream.def("__next__", [](py_stream_gt<keys_stream_t>& keys_stream) {
        ukv_key_t key = keys_stream.native.key();
        if (keys_stream.native.is_end() || keys_stream.last)
            throw py::stop_iteration();
        if (key == keys_stream.stop_point)
            keys_stream.last = true;
        ++keys_stream.native;
        return key;
    });

    // Define keys_vals_stream
    py_kvstream.def("__next__", [](py_stream_gt<keys_vals_stream_t>& keys_vals_stream) {
        ukv_key_t key = keys_vals_stream.native.key();
        if (keys_vals_stream.native.is_end() || keys_vals_stream.last)
            throw py::stop_iteration();
        if (key == keys_vals_stream.stop_point)
            keys_vals_stream.last = true;
        value_view_t value_view = keys_vals_stream.native.value();
        PyObject* value_ptr = PyBytes_FromStringAndSize(value_view.c_str(), value_view.size());
        ++keys_vals_stream.native;
        return py::make_tuple(key, py::reinterpret_borrow<py::object>(value_ptr));
    });

    py_col.def_property_readonly("keys", [](py_col_t& py_col) {
        keys_range_t range(py_col.db_ptr->native, nullptr, py_col.native);
        return py::cast(std::make_shared<py_range_gt<keys_range_t>>(std::move(range)));
    });
    py_col.def_property_readonly("items", [](py_col_t& py_col) {
        keys_vals_range_t range(py_col.db_ptr->native, nullptr, py_col.native);
        return py::cast(std::make_shared<py_range_gt<keys_vals_range_t>>(std::move(range)));
    });

    py::enum_<ukv_format_t>(m, "Format", py::module_local())
        .value("Binary", ukv_format_binary_k)
        .value("Graph", ukv_format_graph_k)
        .value("MsgPack", ukv_format_msgpack_k)
        .value("JSON", ukv_format_json_k)
        .value("BSON", ukv_format_bson_k)
        .value("CBOR", ukv_format_cbor_k)
        .value("UBJSON", ukv_format_ubjson_k);

    // Define `DataBase`
    py_db.def( //
        py::init([](std::string const& config, bool open) {
            db_t db;
            if (open)
                db.open(config).throw_unhandled();
            auto py_db_ptr = std::make_shared<py_db_t>(std::move(db), config);
            return py_db_ptr;
        }),
        py::arg("config") = "",
        py::arg("open") = true);

    // Define `Collection`s member method, without defining any external constructors
    py_col.def("set", &write_binary<py_col_t>);
    py_col.def("pop", &remove_binary<py_col_t>);  // Unlike Python, won't return the result
    py_col.def("has_key", &has_binary<py_col_t>); // Similar to Python 2
    py_col.def("get", &read_binary<py_col_t>);
    py_col.def("update", &update_binary<py_col_t>);
    py_col.def("scan", &py_scan<py_col_t>);

    // Cleanup
    py_db.def("clear", [](py_db_t& py_db) { py_db.native.clear().throw_unhandled(); });
    py_col.def("clear", [](py_col_t& py_col) {
        db_t& db = py_col.db_ptr->native;
        db.remove(py_col.name.c_str()).throw_unhandled();
        auto maybe_col = db.collection(py_col.name.c_str());
        maybe_col.throw_unhandled();
        py_col.native = *std::move(maybe_col);
    });

    // `Transaction`:
    py_txn.def( //
        py::init([](py_db_t& py_db, bool begin, bool track_reads, bool flush_writes, bool snapshot) {
            auto db_ptr = py_db.shared_from_this();
            auto maybe_txn = py_db.native.transact(snapshot);
            maybe_txn.throw_unhandled();
            auto py_txn_ptr = std::make_shared<py_txn_t>(std::move(db_ptr), *std::move(maybe_txn));
            py_txn_ptr->track_reads = track_reads;
            py_txn_ptr->flush_writes = flush_writes;
            return py_txn_ptr;
        }),
        py::arg("db"),
        py::arg("begin") = true,
        py::arg("track_reads") = false,
        py::arg("flush_writes") = false,
        py::arg("snapshot") = false);

    // Resource management
    py_txn.def("__enter__", [](py_txn_t& py_txn) {
        if (py_txn.native)
            return py_txn.shared_from_this();

        [[maybe_unused]] py::gil_scoped_release release;
        py_txn.native.reset().throw_unhandled();
        return py_txn.shared_from_this();
    });
    py_txn.def("commit", &commit_txn);

    py_db.def("__enter__", [](py_db_t& py_db) {
        if (!py_db.native)
            py_db.native.open(py_db.config).throw_unhandled();
        return py_db.shared_from_this();
    });
    py_db.def("close", [](py_db_t& py_db) { py_db.native.close(); });

    py_db.def( //
        "__exit__",
        [](py_db_t& py_db, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
            py_db.native.close();
            return false;
        });
    py_txn.def(
        "__exit__",
        [](py_txn_t& py_txn, py::object const& exc_type, py::object const& exc_value, py::object const& traceback) {
            try {
                commit_txn(py_txn);
            }
            catch (...) {
                // We must now propagate this exception upwards:
                // https://stackoverflow.com/a/35483461
                // https://gist.github.com/YannickJadoul/f1fc8db711ed980cf02610277af058e4
                // https://github.com/pybind/pybind11/commit/5a7d17ff16a01436f7228a688c62511ab8c3efde
            }
            return false;
        });

    // Operator overloads used to access collections
    py_db.def_property_readonly("main", [](py_db_t& py_db) { return punned_collection(&py_db, nullptr, ""); });
    py_txn.def_property_readonly("main",
                                 [](py_txn_t& py_txn) { return punned_collection(py_txn.db_ptr.get(), &py_txn, ""); });

    py_db.def(
        "__contains__",
        [](py_db_t& py_db, std::string const& collection) {
            auto maybe = py_db.native.contains(collection.c_str());
            maybe.throw_unhandled();
            return *maybe;
        },
        py::arg("collection"));
    py_db.def(
        "__getitem__",
        [](py_db_t& py_db, std::string const& collection) -> py::object {
            return punned_collection(&py_db, nullptr, collection);
        },
        py::arg("collection"));
    py_txn.def(
        "__getitem__",
        [](py_txn_t& py_txn, std::string const& collection) {
            return punned_collection(py_txn.db_ptr.get(), &py_txn, collection);
        },
        py::arg("collection"));
    py_db.def(
        "__delitem__",
        [](py_db_t& py_db, std::string const& collection) { //
            py_db.native.remove(collection.c_str()).throw_unhandled();
        },
        py::arg("collection"));

    // Typed collections: Graphs, Docs
    py_col.def_property_readonly( //
        "graph",
        [](py_col_t& py_col) {
            auto py_graph = std::make_shared<py_graph_t>();
            py_graph->db_ptr = py_col.db_ptr;
            py_graph->index = py_col.replicate();
            return py::cast(py_graph);
        });
    py_col.def_property_readonly( //
        "docs",
        [](py_col_t& py_col) { return 0; });
    py_col.def_property_readonly( //
        "media",
        [](py_col_t& py_col) { return 0; });

    // Additional operator overloads
    py_col.def("__setitem__", &write_binary<py_col_t>);
    py_col.def("__delitem__", &remove_binary<py_col_t>);
    py_col.def("__contains__", &has_binary<py_col_t>);
    py_col.def("__getitem__", &read_binary<py_col_t>);
}
