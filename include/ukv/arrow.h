/**
 * Implements bindings for the Apache Arrow.
 * Internally replicates the bare-minimum defintions required
 * for Arrow to be ABI-compatiable.
 *
 * https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
 * https://arrow.apache.org/docs/format/CDataInterface.html#example-use-case
 *
 * After the data is exported into Arrow RecordBatches or Tables,
 * we can stream it with standardized messages:
 * https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
 */
#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h> // `int64_t`
#include <stdlib.h>   // `malloc`

#include "ukv/docs.h"

// if __has_include ("arrow/c/abi.h")
// #define ARROW_C_DATA_INTERFACE 1
// #define ARROW_C_STREAM_INTERFACE 1
// #endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif // ARROW_C_STREAM_INTERFACE

static void release_malloced_schema(struct ArrowSchema* schema) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
        struct ArrowSchema* child = schema->children[i];
        if (child->release != NULL)
            child->release(child);
    }
    free(schema->children);
    schema->release = NULL;
}

static void release_malloced_array(struct ArrowArray* array) {
    // Free children
    for (int64_t i = 0; i < array->n_children; ++i) {
        struct ArrowArray* child = array->children[i];
        if (child->release != NULL)
            child->release(child);
    }
    free(array->children);
    // Freeing buffers can be avoided, UKV still owns those regions
    // while the connection is alive and hasn't be reused for any other
    // requests.
    // for (int64_t i = 0; i < array->n_buffers; ++i)
    //     free((void*)array->buffers[i]);
    free(array->buffers);
    array->release = NULL;
}

static void ukv_to_arrow_schema( //
    ukv_size_t const docs_count,
    ukv_size_t const fields_count,

    struct ArrowSchema* schema,
    struct ArrowArray* array,
    ukv_error_t* error) {

    // Schema
    schema->format = "+s";
    schema->name = "";
    schema->metadata = NULL;
    schema->flags = 0;
    schema->n_children = static_cast<int64_t>(fields_count);
    schema->dictionary = NULL;
    schema->release = &release_malloced_schema;
    schema->children = malloc(sizeof(struct ArrowSchema*) * schema->n_children);

    // Data
    array->length = static_cast<int64_t>(docs_count);
    array->offset = 0;
    array->null_count = 0;
    array->n_buffers = 1;
    array->n_children = 2;
    array->dictionary = NULL;
    array->release = &release_malloced_array;
    array->buffers = malloc(sizeof(void*) * array->n_buffers);
    array->buffers[0] = NULL; // no nulls, so bitmap can be omitted
    array->children = malloc(sizeof(struct ArrowArray*) * array->n_children);

    // Allocate sub-schemas and sub-arrays
    // TODO: Don't malloc every child schema/array separately,
    // use `private_data` member for that.
    for (ukv_size_t field_idx = 0; field_idx != fields_count; ++field_idx)
        schema->children[field_idx] = malloc(sizeof(struct ArrowSchema));
    for (ukv_size_t field_idx = 0; field_idx != fields_count; ++field_idx)
        array->children[field_idx] = malloc(sizeof(struct ArrowArray));
}

static void ukv_to_arrow_column( //
    ukv_size_t const docs_count,
    ukv_str_view_t const field_name,
    ukv_type_t const field_type,

    ukv_1x8_t const* column_validities,
    ukv_val_len_t const column_offsets,
    ukv_val_ptr_t const column_contents,

    struct ArrowSchema* schema,
    struct ArrowArray* array,

    ukv_error_t* error) {

    // Export the right format string and number of buffers to be managed by Arrow.
    // For scalar arrays we need: bitmap and data.
    // For variable length arrays we need: bitmap, @b offsets and data.
    // Important note, both 32-bit and 64-bit offsets are supported.
    // https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
    // https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
    switch (field_type) {
    case ukv_type_null_k: schema->format = "n"; break;
    case ukv_type_bool_k: schema->format = "b"; break;
    // TODO: UUID logical type may be natively supported in Arrow vocabulary:
    // https://arrow.apache.org/docs/format/Columnar.html#extension-types
    case ukv_type_uuid_k: schema->format = "w:16"; break;
    case ukv_type_i8_k: schema->format = "c"; break;
    case ukv_type_i16_k: schema->format = "s"; break;
    case ukv_type_i32_k: schema->format = "i"; break;
    case ukv_type_i64_k: schema->format = "l"; break;
    case ukv_type_u8_k: schema->format = "C"; break;
    case ukv_type_u16_k: schema->format = "S"; break;
    case ukv_type_u32_k: schema->format = "I"; break;
    case ukv_type_u64_k: schema->format = "U"; break;
    case ukv_type_f16_k: schema->format = "e"; break;
    case ukv_type_f32_k: schema->format = "f"; break;
    case ukv_type_f64_k: schema->format = "g"; break;
    case ukv_type_bin_k: schema->format = "z"; break;
    case ukv_type_str_k: schema->format = "u"; break;
    }
    schema->name = field_name;
    schema->metadata = NULL;
    schema->flags = ARROW_FLAG_NULLABLE;
    schema->n_children = 0;
    schema->dictionary = NULL;
    schema->children = NULL;
    schema->release = &release_malloced_schema;

    // Export the data
    switch (field_type) {
    case ukv_type_null_k: array->n_buffers = 0; break;
    case ukv_type_bool_k:
    case ukv_type_uuid_k:
    case ukv_type_i8_k:
    case ukv_type_i16_k:
    case ukv_type_i32_k:
    case ukv_type_i64_k:
    case ukv_type_u8_k:
    case ukv_type_u16_k:
    case ukv_type_u32_k:
    case ukv_type_u64_k:
    case ukv_type_f16_k:
    case ukv_type_f32_k:
    case ukv_type_f64_k: array->n_buffers = 2; break;
    case ukv_type_bin_k:
    case ukv_type_str_k: array->n_buffers = 3; break;
    }
    array->length = docs_count;
    array->offset = 0;
    array->null_count = -1;
    array->n_children = 0;
    array->dictionary = NULL;
    array->children = NULL;
    array->release = &release_malloced_array;

    // Link our buffers
    array->buffers = malloc(sizeof(void*) * array->n_buffers);
    if (array->n_buffers == 2) {
        array->buffers[0] = (void*)column_validities;
        array->buffers[1] = (void*)column_contents;
    }
    else {
        array->buffers[0] = (void*)column_validities;
        array->buffers[1] = (void*)column_offsets;
        array->buffers[2] = (void*)column_contents;
    }
}

/**
 *
 * @param[in] collections   Can have 0, 1 or `fields_count` elements.
 *                          It allows joining the data from different collections
 *                          stored in documents under the same key.
 */
static void ukv_to_arrow_stream( //
    ukv_t const db,
    ukv_txn_t const txn,
    ukv_size_t const fields_count,

    ukv_size_t const docs_per_batch,
    ukv_key_t const min_key,
    ukv_key_t const max_key,

    ukv_col_t const* collections,
    ukv_size_t const collections_stride,

    ukv_str_view_t const* fields,
    ukv_size_t const fields_stride,

    ukv_type_t const* types,
    ukv_size_t const types_stride,

    struct ArrowArrayStream* stream,
    ukv_arena_t* arena) {
}

#ifdef __cplusplus
} /* end extern "C" */
#endif
