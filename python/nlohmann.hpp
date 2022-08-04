#include <nlohmann/json.hpp>

PyObject* from_json(nlohmann::json const& js) {
    if (js.is_null())
        return Py_None;
    else if (js.is_boolean())
        return PyBool_FromLong(js.get<long>());
    else if (js.is_number_integer())
        return PyLong_FromLong(js.get<nlohmann::json::number_integer_t>());
    else if (js.is_number_float())
        return PyFloat_FromDouble(js.get<double>());
    else if (js.is_string())
        return PyBytes_FromString(js.get<nlohmann::json::string_t>().c_str());
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
            PyDict_SetItem(obj, PyBytes_FromString(it.key().c_str()), from_json(it.value()));
        return obj;
    }
}

void to_json(nlohmann::json& js, PyObject* obj) {
    if (obj == Py_None)
        js = nullptr;
    else if (PyBool_Check(obj))
        js = static_cast<bool>(PyLong_AsLong(obj));
    else if (PyLong_Check(obj))
        js = PyLong_AsLong(obj);
    else if (PyFloat_Check(obj))
        js = PyFloat_AsDouble(obj);
    else if (PyBytes_Check(obj))
        js = nlohmann::json::string_t(PyBytes_AsString(obj));
    else if (PyUnicode_Check(obj))
        js = nlohmann::json::string_t(PyBytes_AsString(PyUnicode_AsASCIIString(obj)));
    else if (PySequence_Check(obj)) {
        js = nlohmann::json::array();
        for (Py_ssize_t i = 0; i < PySequence_Length(obj); i++)
            js.emplace_back(PySequence_GetItem(obj, i));
    }
    else if (PyMapping_Check(obj)) {
        js = nlohmann::json::object();
        PyObject* keyvals = PyMapping_Items(obj);
        for (Py_ssize_t i = 0; i < PyMapping_Length(keyvals); i++) {
            PyObject* kv = PyList_GetItem(keyvals, i);
            PyObject* k = PyTuple_GetItem(kv, 0);
            PyObject* v = PyTuple_GetItem(kv, 1);
            nlohmann::json key(k);
            js[key.get<nlohmann::json::string_t>()] = nlohmann::json(v);
        }
    }
    else
        throw std::runtime_error("invalid type for conversion.");
}
