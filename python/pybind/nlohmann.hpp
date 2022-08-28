/**
 * @brief Unlike @see "pybind11/nlohmann.hpp" provides a more fine-grained
 * control over how the conversion is implemented.
 *
 * TODO: Revert to this: https://github.com/ashvardanian/UKV/commit/6d6df13188efb0ad12d67c96ce2904fda6c838d0
 */
#pragma once
#include <pybind11/pybind11.h>
#include <nlohmann/json.hpp>

namespace unum::ukv::pyb {

namespace py = pybind11;
using json_t = nlohmann::json;

inline py::object from_json(const json_t& j) {
    if (j.is_null())
        return py::none();
    else if (j.is_boolean())
        return py::bool_(j.get<bool>());
    else if (j.is_number_integer())
        return py::int_(j.get<json_t::number_integer_t>());
    else if (j.is_number_unsigned())
        return py::int_(j.get<json_t::number_unsigned_t>());
    else if (j.is_number_float())
        return py::float_(j.get<double>());
    else if (j.is_string())
        return py::str(j.get<std::string>());
    else if (j.is_array()) {
        py::list obj(j.size());
        for (std::size_t i = 0; i < j.size(); i++)
            obj[i] = from_json(j[i]);
        return std::move(obj);
    }
    else { // Object
        py::dict obj;
        for (json_t::const_iterator it = j.cbegin(); it != j.cend(); ++it)
            obj[py::str(it.key())] = from_json(it.value());
        return std::move(obj);
    }
}

inline json_t to_json(const py::handle& obj) {
    if (obj.ptr() == nullptr || obj.is_none())
        return nullptr;
    if (py::isinstance<py::bool_>(obj))
        return obj.cast<bool>();
    if (py::isinstance<py::int_>(obj)) {
        json_t::number_integer_t s = obj.cast<json_t::number_integer_t>();
        if (py::int_(s).equal(obj))
            return s;
        json_t::number_unsigned_t u = obj.cast<json_t::number_unsigned_t>();
        if (py::int_(u).equal(obj))
            return u;
        throw std::runtime_error(
            "to_json received an integer out of range for both json_t::number_integer_t and "
            "json_t::number_unsigned_t type: " +
            py::repr(obj).cast<std::string>());
    }
    if (py::isinstance<py::float_>(obj))
        return obj.cast<double>();
    if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        return base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>();
    }
    if (py::isinstance<py::str>(obj))
        return obj.cast<std::string>();
    if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
        auto out = json_t::array();
        for (const py::handle value : obj)
            out.push_back(to_json(value));
        return out;
    }
    if (py::isinstance<py::dict>(obj)) {
        auto out = json_t::object();
        for (const py::handle key : obj)
            out[py::str(key).cast<std::string>()] = to_json(obj[key]);
        return out;
    }
    throw std::runtime_error("Invalid type: " + py::repr(obj).cast<std::string>());
}

} // namespace unum::ukv::pyb