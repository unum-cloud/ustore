/**
 * @file docs.hpp
 * @author Ashot Vardanian
 * @date 4 Jul 2022
 * @brief C++ bindings for @see "ukv/docs.h".
 *
 * Most field-level operations are still accessible through normal @c `value_refs_t`.
 * This interface mostly helps with tabular and SoA <-> AoS operations involving:
 * > ukv_docs_gist(...)
 * > ukv_docs_gather_scalars(...)
 * > ukv_docs_gather_strings(...)
 */

#pragma once
#include "ukv/docs.h"
#include "ukv/ukv.hpp"

namespace unum::ukv {} // namespace unum::ukv
