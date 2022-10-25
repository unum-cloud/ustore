/**
 * @brief Unlike @see "pybind11/nlohmann.hpp" provides a more fine-grained
 * control over how the conversion is implemented.
 *
 * TODO: Revert to this: https://github.com/ashvardanian/UKV/commit/6d6df13188efb0ad12d67c96ce2904fda6c838d0
 */
#pragma once
#include <fmt/core.h>
#include <pybind11/pybind11.h>
#include <nlohmann/json.hpp>

namespace unum::ukv::pyb {

namespace py = pybind11;
using json_t = nlohmann::json;

PyObject* from_json(nlohmann::json const& js) {
    if (js.is_null())
        return Py_None;
    else if (js.is_boolean())
        return PyBool_FromLong(js.get<long>());
    else if (js.is_number_integer())
        return PyLong_FromLong(js.get<nlohmann::json::number_integer_t>());
    else if (js.is_number_float())
        return PyFloat_FromDouble(js.get<double>());
    else if (js.is_string()) {
        auto str = js.get<nlohmann::json::string_t>();
        return PyUnicode_FromStringAndSize(str.c_str(), str.size());
    }
    else if (js.is_array()) {
        PyObject* obj = PyTuple_New(js.size());
        for (size_t i = 0; i < js.size(); i++)
            PyTuple_SetItem(obj, i, from_json(js[i]));
        return obj;
    }
    else // Object
    {
        PyObject* obj = PyDict_New();
        for (nlohmann::json::const_iterator it = js.cbegin(); it != js.cend(); ++it)
            PyDict_SetItem(obj, PyUnicode_FromStringAndSize(it.key().c_str(), it.key().size()), from_json(it.value()));
        return obj;
    }
}

inline void to_string(PyObject* obj, std::string& output) {
    if (PyLong_Check(obj))
        fmt::format_to(std::back_inserter(output), "{}", PyLong_AsLong(obj));
    else if (PyFloat_Check(obj))
        fmt::format_to(std::back_inserter(output), "{}", PyFloat_AsDouble(obj));
    else if (PyBytes_Check(obj)) {
        output.reserve(output.size() + PyBytes_Size(obj) + 2);
        output += "\"";
        output += PyBytes_AsString(obj);
        output += "\"";
    }
    else if (PyUnicode_Check(obj)) {
        output.reserve(output.size() + PyUnicode_GET_LENGTH(obj) + 2);
        output += "\"";
        output += PyBytes_AsString(PyUnicode_AsASCIIString(obj));
        output += "\"";
    }
    else if (PySequence_Check(obj)) {
        output += "[";
        for (Py_ssize_t i = 0; i < PySequence_Length(obj); i++) {
            to_string(PySequence_GetItem(obj, i), output);
            output += ",";
        }
        output[output.size() - 1] = ']';
    }
    else if (PyDict_Check(obj)) {
        output += "{";
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(obj, &pos, &key, &value)) {
            to_string(key, output);
            output += ":";
            to_string(value, output);
            output += ",";
        }
        output[output.size() - 1] = '}';
    }
    else
        throw std::runtime_error("invalid type for conversion.");
}

} // namespace unum::ukv::pyb