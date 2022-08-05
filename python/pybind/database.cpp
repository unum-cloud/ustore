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

template <typename native_at>
struct py_stream_with_ending_gt {
    native_at native;
    ukv_key_t terminal = ukv_key_unknown_k;
};

using py_kstream_t = py_stream_with_ending_gt<keys_stream_t>;
using py_kvstream_t = py_stream_with_ending_gt<pairs_stream_t>;

template <typename range_at>
range_at& since(range_at& range, ukv_key_t key) {
    range.members.since(key);
    return range;
}

template <typename range_at>
range_at& until(range_at& range, ukv_key_t key) {
    range.members.until(key);
    return range;
}

template <typename range_at>
auto iterate(range_at& range) {
    using native_t = typename range_at::iterator_type;
    using wrap_t = py_stream_with_ending_gt<native_t>;
    native_t stream = range.begin();
    wrap_t wrap {std::move(stream), range.members.max_key()};
    return std::make_shared<wrap_t>(std::move(wrap));
}

void ukv::wrap_database(py::module& m) {
    // Define our primary classes: `DataBase`, `Collection`, `Transaction`
    auto py_db = py::class_<py_db_t, std::shared_ptr<py_db_t>>(m, "DataBase", py::module_local());
    auto py_txn = py::class_<py_txn_t, std::shared_ptr<py_txn_t>>(m, "Transaction", py::module_local());

    auto py_col = py::class_<py_col_t>(m, "Collection", py::module_local());
    auto py_krange = py::class_<keys_range_t>(m, "KeysRange", py::module_local());
    auto py_kvrange = py::class_<pairs_range_t>(m, "ItemsRange", py::module_local());
    auto py_kstream = py::class_<py_kstream_t>(m, "KeysStream", py::module_local());
    auto py_kvstream = py::class_<py_kvstream_t>(m, "ItemsStream", py::module_local());

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

#pragma region CRUD Operations

    py_col.def("set", &write_binary<py_col_t>);
    py_col.def("pop", &remove_binary<py_col_t>);  // Unlike Python, won't return the result
    py_col.def("has_key", &has_binary<py_col_t>); // Similar to Python 2
    py_col.def("get", &read_binary<py_col_t>);
    py_col.def("update", &update_binary<py_col_t>);
    py_col.def("scan", &scan_binary<py_col_t>);
    py_col.def("__setitem__", &write_binary<py_col_t>);
    py_col.def("__delitem__", &remove_binary<py_col_t>);
    py_col.def("__contains__", &has_binary<py_col_t>);
    py_col.def("__getitem__", &read_binary<py_col_t>);

    py_col.def("clear", [](py_col_t& py_col) {
        db_t& db = py_col.db_ptr->native;
        db.remove(py_col.name.c_str()).throw_unhandled();
        auto maybe_col = db.collection(py_col.name.c_str());
        maybe_col.throw_unhandled();
        py_col.native = *std::move(maybe_col);
    });

#pragma region Transactions and Lifetime

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

#pragma region Managing Collections

    py_db.def("clear", [](py_db_t& py_db) { py_db.native.clear().throw_unhandled(); });
    py_db.def_property_readonly("main", [](py_db_t& py_db) { return punned_collection(&py_db, nullptr, ""); });
    py_txn.def_property_readonly("main",
                                 [](py_txn_t& py_txn) { return punned_collection(py_txn.db_ptr.get(), &py_txn, ""); });

    py_db.def(
        "__contains__",
        [](py_db_t& py_db, std::string const& name) { return py_db.native.contains(name.c_str()).throw_or_release(); },
        py::arg("collection"));
    py_db.def(
        "__getitem__",
        [](py_db_t& py_db, std::string const& name) { return punned_collection(&py_db, nullptr, name); },
        py::arg("collection"));
    py_txn.def(
        "__getitem__",
        [](py_txn_t& py_txn, std::string const& name) { return punned_collection(py_txn.db_ptr.get(), &py_txn, name); },
        py::arg("collection"));
    py_db.def(
        "__delitem__",
        [](py_db_t& py_db, std::string const& name) { py_db.native.remove(name.c_str()).throw_unhandled(); },
        py::arg("collection"));

    py_col.def_property_readonly("graph", [](py_col_t& py_col) {
        auto py_graph = std::make_shared<py_graph_t>();
        py_graph->db_ptr = py_col.db_ptr;
        py_graph->index = py_col.replicate();
        return py::cast(py_graph);
    });
    py_col.def_property_readonly("docs", [](py_col_t& py_col) { return 0; });
    py_col.def_property_readonly("media", [](py_col_t& py_col) { return 0; });

#pragma region Streams and Ranges

    py_krange.def("__iter__", &iterate<keys_range_t>);
    py_krange.def("since", &since<keys_range_t>);
    py_krange.def("until", &until<keys_range_t>);
    py_kvrange.def("__iter__", &iterate<pairs_range_t>);
    py_kvrange.def("since", &since<pairs_range_t>);
    py_kvrange.def("until", &until<pairs_range_t>);

    py_krange.def("__getitem__", [](keys_range_t& keys_range, py::slice slice) {
        Py_ssize_t start = 0, stop = 0, step = 0;
        if (PySlice_Unpack(slice.ptr(), &start, &stop, &step) || step != 1 || start >= stop)
            throw std::invalid_argument("Invalid Slice");
        keys_stream_t stream = keys_range.members.keys_begin(stop).throw_or_release();
        auto keys = stream.keys_batch();
        auto remaining = std::min<Py_ssize_t>(stop - start, keys.size() - start);
        return py::array(remaining, keys.begin() + start);
    });

    py_kstream.def("__next__", [](py_kstream_t& kstream) {
        ukv_key_t key = kstream.native.key();
        if (kstream.native.is_end() || kstream.terminal == key)
            throw py::stop_iteration();
        ++kstream.native;
        return key;
    });
    py_kvstream.def("__next__", [](py_kvstream_t& kvstream) {
        ukv_key_t key = kvstream.native.key();
        if (kvstream.native.is_end() || kvstream.terminal == key)
            throw py::stop_iteration();

        value_view_t value_view = kvstream.native.value();
        PyObject* value_ptr = PyBytes_FromStringAndSize(value_view.c_str(), value_view.size());
        ++kvstream.native;
        return py::make_tuple(key, py::reinterpret_borrow<py::object>(value_ptr));
    });

    py_col.def_property_readonly("keys", [](py_col_t& py_col) {
        members_range_t members(py_col.db_ptr->native,
                                py_col.txn_ptr ? py_col.txn_ptr->native : ukv_txn_t(nullptr),
                                py_col.native);
        keys_range_t range {members};
        return py::cast(std::make_shared<keys_range_t>(range));
    });
    py_col.def_property_readonly("items", [](py_col_t& py_col) {
        members_range_t members(py_col.db_ptr->native,
                                py_col.txn_ptr ? py_col.txn_ptr->native : ukv_txn_t(nullptr),
                                py_col.native);
        pairs_range_t range {members};
        return py::cast(std::make_shared<pairs_range_t>(range));
    });
}
