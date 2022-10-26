/**
 * @file ukv.h
 * @author Ashot Vardanian
 * @date 12 Jun 2022
 * @addtogroup C
 *
 * @brief Binary Interface Standard for Multi-Modal Databases.
 *
 * Provides:
 * - @b ABI stability for the essential CRUD operations,
 * - @b Interoperability with higher-level languages.
 * - @b Flexibility in choosing the underlying engine.
 * - @b ACID transactions and snapshots support.
 * - @b Batch operations support.
 *
 * ## Hourglass Design
 *
 * Most of todays advanced software is written in C++, both the programs
 * and the libraries. Still, maintaining shared ABI-stable C++ interface
 * is essentially impossible in todays reality.
 *
 * That is why the "hourglass" pattern is used. The implementation is in
 * C++, the heavily templated convinient wrappers are in C++, but the
 * intermediate layer is C99 to maintain ABI stability and allow rolling
 * minor updates without recompilation of the user code.
 *
 * ## Interface Conventions
 *
 * - Choosing more arguments over more functions.
 *   Aiming for flexibility, we have functions have 4 allowed outputs or more.
 *   To cover all the combinations requests we will need 4!=24 functions just
 *   to replace that one.
 *
 * - Strides! Higher level systems may pack groups of arguments into AoS
 *   instead of SoA. To minimize the need of copies and data re-layout,
 *   we use @b byte-length strides arguments, similar to BLAS libraries.
 *   Passing Zero as a "stride" means repeating the same value.
 *
 * - Wrapping function arguments into structs.
 *   Some functions have over 20 arguments, accounting for all the options
 *   and strides. The majority of those are optional. This makes it impossible
 *   to remember the exact order. Using `struct`, the order becomes irrelevant.
 *   All arguments get names and default values can be skipped.
 *
 * ### Why not use LevelDB or RocksDB interface?
 *
 * In no particular order:
 * - Dynamic polymorphism and multiple inheritance is a mess.
 * - Dependance on Standard Templates Library containers, can't bring your strings or trees.
 * - No support for **custom allocators**, inclusing statefull allocators and arenas.
 * - Almost every function call can through exceptions.
 * - All keys are strings.
 * These and other problems mean that interface can't be portable, ABI-safe or performant.
 *
 * ### Why not adapt SQL, MQL or Cypher?
 *
 * Those interfaces imply a lot of higher-level logic, that might not need to
 * be concern of the Key-Value Store. Furthermore, using text-based protocols
 * is error-prone and highly inefficient from serialization and parsing standpoint.
 * It might be fine for OLAP requsts being called once a second, but if the function
 * is called every microsecond, the interface must be binary.
 *
 * For those few places where such functionality can be implemented efficiently we follow
 * standardized community-drived RFCs, rather than proprietary languages. As suc, for
 * sub-document level gathers and updates we use:
 * - JSON Pointer: RFC 6901
 * - JSON Patch: RFC 6902
 * - JSON MergePatch: RFC 7386
 */

#pragma once
#include "ukv/db.h"
#include "ukv/blobs.h"
#include "ukv/paths.h"
#include "ukv/docs.h"
#include "ukv/graph.h"