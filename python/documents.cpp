#include "pybind.hpp"
#include <cast.hpp>
#include <crud.hpp>
#include <nlohmann.hpp>

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

class docs_pairs_stream_t {

    ukv_database_t db_ = nullptr;
    ukv_collection_t collection_ = ukv_collection_main_k;
    ukv_transaction_t txn_ = nullptr;

    arena_t arena_scan_;
    arena_t arena_read_;
    ukv_length_t read_ahead_ = 0;

    ukv_key_t next_min_key_ = std::numeric_limits<ukv_key_t>::min();
    indexed_range_gt<ukv_key_t*> fetched_keys_;
    embedded_bins_t values_view;
    std::size_t fetched_offset_ = 0;

    status_t prefetch() noexcept {

        if (next_min_key_ == ukv_key_unknown_k)
            return {};

        ukv_key_t* found_keys = nullptr;
        ukv_length_t* found_offsets = nullptr;
        ukv_length_t* found_counts = nullptr;
        ukv_length_t* found_lengths = nullptr;
        ukv_bytes_ptr_t found_values = nullptr;
        ukv_str_view_t fields = nullptr;
        status_t status;

        ukv_scan( //
            db_,
            txn_,
            1,
            &collection_,
            0,
            &next_min_key_,
            0,
            nullptr,
            0,
            &read_ahead_,
            0,
            ukv_options_default_k,
            &found_offsets,
            &found_counts,
            &found_keys,
            arena_scan_.member_ptr(),
            status.member_ptr());
        if (!status)
            return status;

        fetched_keys_ = indexed_range_gt<ukv_key_t*> {found_keys, found_keys + *found_counts};
        fetched_offset_ = 0;
        auto count = static_cast<ukv_size_t>(fetched_keys_.size());

        ukv_docs_read( //
            db_,
            txn_,
            count,
            &collection_,
            0,
            found_keys,
            sizeof(ukv_key_t),
            &fields,
            0,
            ukv_doc_field_json_k,
            ukv_options_default_k,
            nullptr,
            &found_offsets,
            &found_lengths,
            &found_values,
            arena_read_.member_ptr(),
            status.member_ptr());
        if (!status)
            return status;

        values_view = embedded_bins_t {count, found_offsets, found_lengths, found_values};
        next_min_key_ = count <= read_ahead_ ? ukv_key_unknown_k : fetched_keys_[count - 1] + 1;
        return {};
    }

  public:
    static constexpr std::size_t default_read_ahead_k = 256;

    docs_pairs_stream_t(ukv_database_t db,
                        ukv_collection_t collection = ukv_collection_main_k,
                        std::size_t read_ahead = docs_pairs_stream_t::default_read_ahead_k,
                        ukv_transaction_t txn = nullptr)
        : db_(db), collection_(collection), txn_(txn), arena_scan_(db_), arena_read_(db_),
          read_ahead_(static_cast<ukv_size_t>(read_ahead)) {}

    status_t seek(ukv_key_t key) noexcept {
        fetched_keys_ = {};
        fetched_offset_ = 0;
        next_min_key_ = key;
        return prefetch();
    }

    status_t advance() noexcept {
        if (fetched_offset_ >= fetched_keys_.size())
            return prefetch();
        ++fetched_offset_;
        return {};
    }

    docs_pairs_stream_t& operator++() noexcept {
        status_t status = advance();
        if (status)
            return *this;

        fetched_keys_ = {};
        fetched_offset_ = 0;
        next_min_key_ = ukv_key_unknown_k;
        return *this;
    }

    ukv_key_t key() const noexcept { return fetched_keys_[fetched_offset_]; }
    value_view_t value() const noexcept {
        auto it = values_view.begin();
        for (size_t i = 0; i != fetched_offset_; ++i)
            ++it;
        return *it;
    }

    bool is_end() const noexcept {
        return next_min_key_ == ukv_key_unknown_k && fetched_offset_ >= fetched_keys_.size();
    }
};

class py_docs_kvrange_t {

    ukv_database_t db_;
    ukv_transaction_t txn_;
    ukv_collection_t collection_;
    ukv_key_t min_key_;
    ukv_key_t max_key_;

  public:
    py_docs_kvrange_t(ukv_database_t db,
                      ukv_transaction_t txn = nullptr,
                      ukv_collection_t collection = ukv_collection_main_k,
                      ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                      ukv_key_t max_key = ukv_key_unknown_k) noexcept
        : db_(db), txn_(txn), collection_(collection), min_key_(min_key), max_key_(max_key) {}

    py_docs_kvrange_t& since(ukv_key_t min_key) noexcept {
        min_key_ = min_key;
        return *this;
    }
    py_docs_kvrange_t& until(ukv_key_t max_key) noexcept {
        max_key_ = max_key;
        return *this;
    }

    ukv_key_t max_key() noexcept { return max_key_; }
    docs_pairs_stream_t begin() noexcept(false) {
        docs_pairs_stream_t stream {db_, collection_, docs_pairs_stream_t::default_read_ahead_k, txn_};
        status_t status = stream.seek(min_key_);
        return stream;
    }
};

static void write_one_doc(py_docs_collection_t& collection,
                          PyObject* key_py,
                          PyObject* val_py /*, ukv_doc_field_type_t format*/) {
    json_t json = to_json(val_py);
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    collection.binary.native[key] = json.dump().c_str();
}

static void write_many_docs(py_docs_collection_t& collection,
                            PyObject* keys_py,
                            PyObject* vals_py /*,
                            ukv_doc_field_type_t format */) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    std::vector<json_t> vals;
    py_transform_n(vals_py, &to_json, std::back_inserter(vals));
    if (keys.size() != vals.size())
        throw std::invalid_argument("Keys count must match values count");
    // TODO: Fix: This must be a single batch read operation!
    for (size_t i = 0; i < keys.size(); ++i)
        collection.binary.native[keys[i]] = vals[i].dump().c_str();
}

static void write_same_doc(py_docs_collection_t& collection,
                           PyObject* keys_py,
                           PyObject* val_py /*, ukv_doc_field_type_t format*/) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    auto json = to_json(val_py).dump();
    // TODO: Fix: This must be a single batch write operation!
    for (size_t i = 0; i < keys.size(); ++i)
        collection.binary.native[keys[i]] = json.c_str();
}

static void write_doc(py_docs_collection_t& collection,
                      py::object key_py,
                      py::object val_py /*, ukv_doc_field_type_t format*/) {
    auto is_single_key = PyLong_Check(key_py.ptr());
    auto func = !is_single_key ? &write_many_docs : &write_one_doc;
    return func(collection, key_py.ptr(), val_py.ptr());
}

static void broadcast_doc(py_docs_collection_t& collection,
                          py::object key_py,
                          py::object val_py /*, ukv_doc_field_type_t format*/) {
    return write_same_doc(collection, key_py.ptr(), val_py.ptr());
}

static py::object read_one_doc(py_docs_collection_t& collection, PyObject* key_py /*, ukv_doc_field_type_t format*/) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    auto json = json_t::parse(collection.binary.native[key].value()->c_str());
    return from_json(json);
}

static py::object read_many_docs(py_docs_collection_t& collection,
                                 PyObject* keys_py /*, ukv_doc_field_type_t format*/) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    py::list values(keys.size());
    // TODO: Fix: This must be a single batch read operation!
    for (size_t i = 0; i < keys.size(); ++i)
        values[i] = from_json(json_t::parse(collection.binary.native[keys[i]].value()->c_str()));
    return values;
}

static py::object read_doc(py_docs_collection_t& collection, py::object key_py /*, ukv_doc_field_type_t format*/) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &read_one_doc : &read_many_docs;
    return func(collection, key_py.ptr());
}

static void remove_doc(py_docs_collection_t& collection, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &write_one_binary : &write_many_binaries;
    return func(collection.binary, key_py.ptr(), Py_None);
}

static py::object has_doc(py_docs_collection_t& collection, py::object key_py) {
    return has_binary(collection.binary, key_py);
}

static py::object scan_doc(py_docs_collection_t& collection, ukv_key_t min_key, ukv_size_t scan_limit) {
    return scan_binary(collection.binary, min_key, scan_limit);
}

static void merge_patch(py_docs_collection_t& collection,
                        py::object key_py,
                        py::object val_py,
                        ukv_doc_modification_t format) {
    // collection.binary.native.as(format);
    write_one_doc(collection, key_py.ptr(), val_py.ptr());
    // collection.binary.native.as(ukv_doc_field_default_k);
}

void ukv::wrap_document(py::module& m) {
    using py_docs_kvstream_t = py_stream_with_ending_gt<docs_pairs_stream_t>;

    auto py_docs_collection = py::class_<py_docs_collection_t>(m, "DocsCollection", py::module_local());
    auto py_docs_kvrange = py::class_<py_docs_kvrange_t>(m, "DocsKVRange", py::module_local());
    auto py_docs_kvstream = py::class_<py_docs_kvstream_t>(m, "DocsKVStream", py::module_local());

    py_docs_collection.def("set", &write_doc);
    py_docs_collection.def("get", &read_doc);
    py_docs_collection.def("remove", &remove_doc);
    py_docs_collection.def("has_key", &has_doc);
    py_docs_collection.def("scan", &scan_doc);
    py_docs_collection.def("broadcast", &broadcast_doc);

    py_docs_collection.def("__setitem__", &write_doc);
    py_docs_collection.def("__delitem__", &remove_doc);
    py_docs_collection.def("__getitem__", &read_doc);
    py_docs_collection.def("__contains__", &has_doc);

    py_docs_collection.def("clear", [](py_docs_collection_t& collection) {
        py_db_t& py_db = *collection.binary.py_db_ptr.lock().get();
        database_t& db = py_db.native;
        db.drop(collection.binary.name.c_str()).throw_unhandled();
        collection.binary.native = db.collection(collection.binary.name.c_str()).throw_or_release();
    });

    py_docs_collection.def("patch", [](py_docs_collection_t& collection, py::object key_py, py::object val_py) {
        merge_patch(collection, key_py, val_py, ukv_doc_modify_patch_k);
    });

    py_docs_collection.def("merge", [](py_docs_collection_t& collection, py::object key_py, py::object val_py) {
        merge_patch(collection, key_py, val_py, ukv_doc_modify_merge_k);
    });

    py_docs_collection.def_property_readonly("keys", [](py_docs_collection_t& collection) {
        bins_range_t members(collection.binary.db(), collection.binary.txn(), *collection.binary.member_collection());
        keys_range_t range {members};
        return py::cast(std::make_unique<keys_range_t>(range));
    });

    py_docs_collection.def_property_readonly("items", [](py_docs_collection_t& py_collection) {
        py_docs_kvrange_t range(py_collection.binary.db(),
                                py_collection.binary.txn(),
                                *py_collection.binary.member_collection());
        return py::cast(std::make_unique<py_docs_kvrange_t>(range));
    });

    py_docs_kvrange.def("__iter__", [](py_docs_kvrange_t& range) {
        docs_pairs_stream_t stream = range.begin();
        py_stream_with_ending_gt<docs_pairs_stream_t> wrap {std::move(stream), range.max_key()};
        return std::make_unique<py_stream_with_ending_gt<docs_pairs_stream_t>>(std::move(wrap));
    });

    py_docs_kvrange.def("since", [](py_docs_kvrange_t& range, ukv_key_t key) { return range.since(key); });
    py_docs_kvrange.def("until", [](py_docs_kvrange_t& range, ukv_key_t key) { return range.until(key); });

    py_docs_kvstream.def("__next__", [](py_docs_kvstream_t& kvstream) {
        ukv_key_t key = kvstream.native.key();
        if (kvstream.native.is_end() || kvstream.stop)
            throw py::stop_iteration();
        kvstream.stop = kvstream.terminal == key;
        value_view_t value_view = kvstream.native.value();
        auto json = json_t::parse(value_view.c_str());
        ++kvstream.native;
        return py::make_tuple(key, from_json(json));
    });
}