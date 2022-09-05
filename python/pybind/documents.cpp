#include "pybind.hpp"
#include <cast.hpp>
#include <crud.hpp>
#include <nlohmann.hpp>

using namespace unum::ukv::pyb;
using namespace unum::ukv;
using namespace unum;

class docs_pairs_stream_t {

    ukv_t db_ = nullptr;
    ukv_col_t col_ = ukv_col_main_k;
    ukv_txn_t txn_ = nullptr;

    arena_t arena_scan_;
    arena_t arena_read_;
    ukv_val_len_t read_ahead_ = 0;

    ukv_key_t next_min_key_ = std::numeric_limits<ukv_key_t>::min();
    indexed_range_gt<ukv_key_t*> fetched_keys_;
    embedded_bins_t values_view;
    std::size_t fetched_offset_ = 0;

    status_t prefetch() noexcept {

        if (next_min_key_ == ukv_key_unknown_k)
            return {};

        ukv_key_t* found_keys = nullptr;
        ukv_val_len_t* found_offs = nullptr;
        ukv_val_len_t* found_lens = nullptr;
        ukv_val_ptr_t found_vals = nullptr;
        ukv_str_view_t fields = nullptr;
        status_t status;

        ukv_scan( //
            db_,
            txn_,
            1,
            &col_,
            0,
            &next_min_key_,
            0,
            &read_ahead_,
            0,
            ukv_options_default_k,
            nullptr,
            &found_lens,
            &found_keys,
            arena_scan_.member_ptr(),
            status.member_ptr());
        if (!status)
            return status;

        auto present_end = std::find(found_keys, found_keys + read_ahead_, ukv_key_unknown_k);
        fetched_keys_ = indexed_range_gt<ukv_key_t*> {found_keys, present_end};
        fetched_offset_ = 0;
        auto count = static_cast<ukv_size_t>(fetched_keys_.size());

        ukv_docs_read( //
            db_,
            txn_,
            count,
            &col_,
            0,
            found_keys,
            sizeof(ukv_key_t),
            &fields,
            0,
            ukv_options_default_k,
            ukv_format_json_k,
            ukv_type_any_k,
            &found_vals,
            &found_offs,
            &found_lens,
            nullptr,
            arena_read_.member_ptr(),
            status.member_ptr());
        if (!status)
            return status;

        values_view = embedded_bins_t {found_vals, found_offs, found_lens, count};
        next_min_key_ = count <= read_ahead_ ? ukv_key_unknown_k : fetched_keys_[count - 1] + 1;
        return {};
    }

  public:
    static constexpr std::size_t default_read_ahead_k = 256;

    docs_pairs_stream_t(ukv_t db,
                        ukv_col_t col = ukv_col_main_k,
                        std::size_t read_ahead = docs_pairs_stream_t::default_read_ahead_k,
                        ukv_txn_t txn = nullptr)
        : db_(db), col_(col), txn_(txn), arena_scan_(db_), arena_read_(db_),
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

    ukv_t db_;
    ukv_txn_t txn_;
    ukv_col_t col_;
    ukv_key_t min_key_;
    ukv_key_t max_key_;

  public:
    py_docs_kvrange_t(ukv_t db,
                      ukv_txn_t txn = nullptr,
                      ukv_col_t col = ukv_col_main_k,
                      ukv_key_t min_key = std::numeric_limits<ukv_key_t>::min(),
                      ukv_key_t max_key = ukv_key_unknown_k) noexcept
        : db_(db), txn_(txn), col_(col), min_key_(min_key), max_key_(max_key) {}

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
        docs_pairs_stream_t stream {db_, col_, docs_pairs_stream_t::default_read_ahead_k, txn_};
        status_t status = stream.seek(min_key_);
        return stream;
    }
};

static void write_one_doc(py_docs_col_t& col, PyObject* key_py, PyObject* val_py) {
    json_t json = to_json(val_py);
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    col.binary.native[key] = json.dump().c_str();
}

static void write_many_docs(py_docs_col_t& col, PyObject* keys_py, PyObject* vals_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    std::vector<json_t> vals;
    py_transform_n(vals_py, &to_json, std::back_inserter(vals));
    if (keys.size() != vals.size())
        throw std::invalid_argument("Keys count must match values count");
    // TODO: Fix: This must be a single batch read operation!
    for (size_t i = 0; i < keys.size(); ++i)
        col.binary.native[keys[i]] = vals[i].dump().c_str();
}

static void write_same_doc(py_docs_col_t& col, PyObject* keys_py, PyObject* val_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    auto json = to_json(val_py).dump();
    // TODO: Fix: This must be a single batch write operation!
    for (size_t i = 0; i < keys.size(); ++i)
        col.binary.native[keys[i]] = json.c_str();
}

static void write_doc(py_docs_col_t& col, py::object key_py, py::object val_py) {
    auto is_single_key = PyLong_Check(key_py.ptr());
    auto func = !is_single_key ? &write_many_docs : &write_one_doc;
    return func(col, key_py.ptr(), val_py.ptr());
}

static void broadcast_doc(py_docs_col_t& col, py::object key_py, py::object val_py) {
    return write_same_doc(col, key_py.ptr(), val_py.ptr());
}

static py::object read_one_doc(py_docs_col_t& col, PyObject* key_py) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    auto json = json_t::parse(col.binary.native[key].value()->c_str());
    return from_json(json);
}

static py::object read_many_docs(py_docs_col_t& col, PyObject* keys_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    py::list values(keys.size());
    // TODO: Fix: This must be a single batch read operation!
    for (size_t i = 0; i < keys.size(); ++i)
        values[i] = from_json(json_t::parse(col.binary.native[keys[i]].value()->c_str()));
    return values;
}

static py::object read_doc(py_docs_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &read_one_doc : &read_many_docs;
    return func(col, key_py.ptr());
}

static void remove_one_doc(py_docs_col_t& col, PyObject* key_py) {
    ukv_key_t key = py_to_scalar<ukv_key_t>(key_py);
    col.binary.native[key] = nullptr;
}

static void remove_many_docs(py_docs_col_t& col, PyObject* keys_py) {
    std::vector<ukv_key_t> keys;
    py_transform_n(keys_py, &py_to_scalar<ukv_key_t>, std::back_inserter(keys));
    for (auto key : keys)
        col.binary.native[key] = nullptr;
}

static void remove_doc(py_docs_col_t& col, py::object key_py) {
    auto is_single = PyLong_Check(key_py.ptr());
    auto func = is_single ? &remove_one_doc : &remove_many_docs;
    return func(col, key_py.ptr());
}

static py::object has_doc(py_docs_col_t& col, py::object key_py) {
    return has_binary(col.binary, key_py);
}

static py::object scan_doc(py_docs_col_t& col, ukv_key_t min_key, ukv_size_t scan_length) {
    return scan_binary(col.binary, min_key, scan_length);
}

static void merge_patch(py_docs_col_t& col, py::object key_py, py::object val_py, ukv_format_t format) {
    col.binary.native.as(format);
    write_one_doc(col, key_py.ptr(), val_py.ptr());
    col.binary.native.as(ukv_format_json_k);
}

void ukv::wrap_document(py::module& m) {
    using py_docs_kvstream_t = py_stream_with_ending_gt<docs_pairs_stream_t>;

    auto py_docs_col = py::class_<py_docs_col_t>(m, "DocsCollection", py::module_local());
    auto py_docs_kvrange = py::class_<py_docs_kvrange_t>(m, "DocsKVRange", py::module_local());
    auto py_docs_kvstream = py::class_<py_docs_kvstream_t>(m, "DocsKVStream", py::module_local());

    py_docs_col.def("set", &write_doc);
    py_docs_col.def("get", &read_doc);
    py_docs_col.def("remove", &remove_doc);
    py_docs_col.def("has_key", &has_doc);
    py_docs_col.def("scan", &scan_doc);
    py_docs_col.def("broadcast", &broadcast_doc);

    py_docs_col.def("__setitem__", &write_doc);
    py_docs_col.def("__delitem__", &remove_doc);
    py_docs_col.def("__getitem__", &read_doc);
    py_docs_col.def("__contains__", &has_doc);

    py_docs_col.def("clear", [](py_docs_col_t& col) {
        py_db_t& py_db = *col.binary.py_db_ptr.lock().get();
        db_t& db = py_db.native;
        db.remove(col.binary.name.c_str()).throw_unhandled();
        col.binary.native = db.collection(col.binary.name.c_str()).throw_or_release();
        col.binary.native.as(ukv_format_json_k);
    });

    py_docs_col.def("patch", [](py_docs_col_t& col, py::object key_py, py::object val_py) {
        merge_patch(col, key_py, val_py, ukv_format_json_patch_k);
    });

    py_docs_col.def("merge", [](py_docs_col_t& col, py::object key_py, py::object val_py) {
        merge_patch(col, key_py, val_py, ukv_format_json_merge_patch_k);
    });

    py_docs_col.def_property_readonly("keys", [](py_docs_col_t& col) {
        members_range_t members(col.binary.db(), col.binary.txn(), *col.binary.member_col());
        keys_range_t range {members};
        return py::cast(std::make_unique<keys_range_t>(range));
    });

    py_docs_col.def_property_readonly("items", [](py_docs_col_t& py_col) {
        py_docs_kvrange_t range(py_col.binary.db(), py_col.binary.txn(), *py_col.binary.member_col());
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