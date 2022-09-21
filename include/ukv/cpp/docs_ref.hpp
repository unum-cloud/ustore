/**
 * @file bins_ref.hpp
 * @author Ashot Vardanian
 * @date 26 Jun 2022
 * @brief C++ bindings for @see "ukv/db.h".
 */

#pragma once
#include "ukv/ukv.h"
#include "ukv/cpp/types.hpp"      // `arena_t`
#include "ukv/cpp/status.hpp"     // `status_t`
#include "ukv/cpp/sfinae.hpp"     // `location_store_gt`
#include "ukv/cpp/docs_table.hpp" // `docs_table_t`

namespace unum::ukv {

template <typename locations_store_t>
class docs_ref_gt;

/**
 * @brief A proxy object, that allows both lookups and writes
 * with `[]` and assignment operators for a batch of keys
 * and @b sub-keys/fields across different documents.
 * Following assignment combinations are possible:
 * > one value to many keys
 * > many values to many keys
 * > one value to one key
 * The only impossible combination is assigning many values to one key.
 *
 * @tparam locations_at Type describing the address of a value in DBMS.
 * > (ukv_collection_t?, ukv_key_t, ukv_field_t?): Single KV-pair location.
 * > (ukv_collection_t*, ukv_key_t*, ukv_field_t*): Externally owned range of keys.
 * > (ukv_collection_t[x], ukv_key_t[x], ukv_field_t[x]): On-stack array of addresses.
 *
 * @section Memory Management
 * Every "container" that overloads the @b [] operator has an internal "arena",
 * that is shared between all the `docs_ref_gt`s produced from it. That will
 * work great, unless:
 * > multiple threads are working with same collection handle or transaction.
 * > reading responses interleaves with new requests, which gobbles temporary memory.
 * For those cases, you can create a separate `arena_t` and pass it to `.on(...)`
 * member function. In such HPC environments we would recommend to @b reuse one such
 * are on every thread.
 *
 * @section Class Specs
 * > Copyable: Yes.
 * > Exceptions: Never.
 */
template <typename locations_at>
class docs_ref_gt {
  public:
    static_assert(!std::is_rvalue_reference_v<locations_at>, //
                  "The internal object can't be an R-value Reference");

    using locations_store_t = location_store_gt<locations_at>;
    using locations_plain_t = typename locations_store_t::plain_t;
    using keys_extractor_t = places_arg_extractor_gt<locations_plain_t>;
    static constexpr bool is_one_k = keys_extractor_t::is_one_k;

    using value_t = std::conditional_t<is_one_k, value_view_t, embedded_bins_t>;
    using present_t = std::conditional_t<is_one_k, bool, strided_iterator_gt<ukv_octet_t>>;
    using length_t = std::conditional_t<is_one_k, ukv_length_t, indexed_range_gt<ukv_length_t*>>;

  protected:
    ukv_database_t db_ = nullptr;
    ukv_transaction_t txn_ = nullptr;
    ukv_arena_t* arena_ = nullptr;
    locations_store_t locations_;
    ukv_doc_field_type_t type_ = ukv_doc_field_default_k;

    template <typename contents_arg_at>
    status_t any_write(contents_arg_at&&, ukv_doc_modification_t, ukv_doc_field_type_t, ukv_options_t) noexcept;

    template <typename expected_at = value_t>
    expected_gt<expected_at> any_get(ukv_doc_field_type_t, ukv_options_t) noexcept;

    template <typename expected_at, typename layout_at>
    expected_gt<expected_at> any_gather(layout_at&&, ukv_options_t) noexcept;

  public:
    docs_ref_gt(ukv_database_t db,
                ukv_transaction_t txn,
                locations_store_t locations,
                ukv_arena_t* arena,
                ukv_doc_field_type_t type = ukv_doc_field_default_k) noexcept
        : db_(db), txn_(txn), arena_(arena), locations_(locations), type_(type) {}

    docs_ref_gt(docs_ref_gt&&) = default;
    docs_ref_gt& operator=(docs_ref_gt&&) = default;
    docs_ref_gt(docs_ref_gt const&) = default;
    docs_ref_gt& operator=(docs_ref_gt const&) = default;

    docs_ref_gt& on(arena_t& arena) noexcept {
        arena_ = arena.member_ptr();
        return *this;
    }

    docs_ref_gt& as(ukv_doc_field_type_t type) noexcept {
        type_ = type;
        return *this;
    }

    expected_gt<value_t> value(bool track = false) noexcept {
        return any_get<value_t>(type_, track ? ukv_option_read_track_k : ukv_options_default_k);
    }

    expected_gt<value_t> value(ukv_doc_field_type_t type, bool track = false) noexcept {
        return any_get<value_t>(type, track ? ukv_option_read_track_k : ukv_options_default_k);
    }

    operator expected_gt<value_t>() noexcept { return value(); }

    expected_gt<length_t> length(bool track = false) noexcept {
        return any_get<length_t>(type_, track ? ukv_option_read_track_k : ukv_options_default_k);
    }

    /**
     * @brief Checks if requested keys are present in the store.
     * ! Related values may be empty strings.
     */
    expected_gt<present_t> present(bool track = false) noexcept {
        return any_get<present_t>(type_, track ? ukv_option_read_track_k : ukv_options_default_k);
    }

    /**
     * @brief Pair-wise assigns values to keys located in this proxy objects.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    template <typename contents_arg_at>
    status_t assign(contents_arg_at&& vals, bool flush = false) noexcept {
        return any_write(std::forward<contents_arg_at>(vals),
                         ukv_doc_modify_upsert_k,
                         type_,
                         flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    template <typename contents_arg_at>
    status_t assign(contents_arg_at&& vals, ukv_doc_field_type_t type, bool flush = false) noexcept {
        return any_write(std::forward<contents_arg_at>(vals),
                         ukv_doc_modify_upsert_k,
                         type,
                         flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Removes both the keys and the associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t erase(bool flush = false) noexcept { //
        return assign(nullptr, flush);
    }

    /**
     * @brief Keeps the keys, but clears the contents of associated values.
     * @param flush Pass true, if you need the data to be persisted before returning.
     * @return status_t Non-NULL if only an error had occurred.
     */
    status_t clear(bool flush = false) noexcept {
        ukv_bytes_ptr_t any = reinterpret_cast<ukv_bytes_ptr_t>(this);
        ukv_length_t len = 0;
        contents_arg_t arg {
            .offsets_begin = {},
            .lengths_begin = {&len},
            .contents_begin = {&any},
            .count = 1,
        };
        return assign(arg, flush);
    }

    template <typename contents_arg_at>
    docs_ref_gt& operator=(contents_arg_at&& vals) noexcept(false) {
        auto status = assign(std::forward<contents_arg_at>(vals));
        status.throw_unhandled();
        return *this;
    }

    docs_ref_gt& operator=(std::nullptr_t) noexcept(false) {
        auto status = erase();
        status.throw_unhandled();
        return *this;
    }

    locations_plain_t& locations() noexcept { return locations_.ref(); }
    locations_plain_t& locations() const noexcept { return locations_.ref(); }

    /**
     * @brief Patches hierarchical documents with RFC 6902 JSON Patches.
     */
    template <typename contents_arg_at>
    status_t patch(contents_arg_at&& vals, bool flush = false) noexcept {
        return any_write(std::forward<contents_arg_at>(vals),
                         ukv_doc_modify_patch_k,
                         type_,
                         flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Patches hierarchical documents with RFC 7386 JSON Merge Patches.
     */
    template <typename contents_arg_at>
    status_t merge(contents_arg_at&& vals, bool flush = false) noexcept {
        return any_write(std::forward<contents_arg_at>(vals),
                         ukv_doc_modify_merge_k,
                         type_,
                         flush ? ukv_option_write_flush_k : ukv_options_default_k);
    }

    /**
     * @brief Find the names of all unique fields in requested documents.
     */
    expected_gt<joined_strs_t> gist(bool track = false) noexcept;

    /**
     * @brief For N documents and M fields gather (N * M) responses.
     * You put in a `table_layout_view_gt` and you receive a `docs_table_gt`.
     * Any column type annotation is optional.
     */
    expected_gt<docs_table_t> gather(table_header_t const& header, bool track = false) noexcept {
        auto options = track ? ukv_option_read_track_k : ukv_options_default_k;
        return any_gather<docs_table_t, table_header_t const&>(header, options);
    }

    expected_gt<docs_table_t> gather(table_header_view_t const& header, bool track = false) noexcept {
        auto options = track ? ukv_option_read_track_k : ukv_options_default_k;
        return any_gather<docs_table_t, table_header_view_t const&>(header, options);
    }

    template <typename... column_types_at>
    expected_gt<docs_table_gt<column_types_at...>> gather( //
        table_header_gt<column_types_at...> const& header,
        bool track = false) noexcept {
        auto options = track ? ukv_option_read_track_k : ukv_options_default_k;
        using input_t = table_header_gt<column_types_at...>;
        using output_t = docs_table_gt<column_types_at...>;
        return any_gather<output_t, input_t const&>(header, options);
    }
};

static_assert(docs_ref_gt<ukv_key_t>::is_one_k);
static_assert(std::is_same_v<docs_ref_gt<ukv_key_t>::value_t, value_view_t>);
static_assert(docs_ref_gt<ukv_key_t>::is_one_k);
static_assert(!docs_ref_gt<places_arg_t>::is_one_k);

template <typename locations_at>
template <typename expected_at>
expected_gt<expected_at> docs_ref_gt<locations_at>::any_get(ukv_doc_field_type_t type, ukv_options_t options) noexcept {

    status_t status;
    ukv_length_t* found_offsets = nullptr;
    ukv_length_t* found_lengths = nullptr;
    ukv_bytes_ptr_t found_values = nullptr;
    ukv_octet_t* found_presences = nullptr;
    constexpr bool wants_value = std::is_same_v<value_t, expected_at>;
    constexpr bool wants_length = std::is_same_v<length_t, expected_at>;
    constexpr bool wants_present = std::is_same_v<present_t, expected_at>;

    decltype(auto) locs = locations_.ref();
    auto count = keys_extractor_t {}.count(locs);
    auto keys = keys_extractor_t {}.keys(locs);
    auto collections = keys_extractor_t {}.collections(locs);
    auto fields = keys_extractor_t {}.fields(locs);
    auto has_fields = fields && (!fields.repeats() || *fields);

    ukv_docs_read( //
        db_,
        txn_,
        count,
        collections.get(),
        collections.stride(),
        keys.get(),
        keys.stride(),
        fields.get(),
        fields.stride(),
        type,
        options,
        wants_present ? &found_presences : nullptr,
        wants_value ? &found_offsets : nullptr,
        wants_value || wants_length ? &found_lengths : nullptr,
        wants_value ? &found_values : nullptr,
        arena_,
        status.member_ptr());

    if (!status)
        return std::move(status);

    if constexpr (wants_length) {
        indexed_range_gt<ukv_length_t*> many {found_lengths, found_lengths + count};
        if constexpr (is_one_k)
            return many[0];
        else
            return many;
    }
    else if constexpr (wants_present) {
        strided_iterator_gt<ukv_octet_t> many {found_presences};
        if constexpr (is_one_k)
            return bool(many[0]);
        else
            return many;
    }
    else {
        embedded_bins_t many {count, found_offsets, found_lengths, found_values};
        if constexpr (is_one_k)
            return many[0];
        else
            return many;
    }
}

template <typename locations_at>
template <typename contents_arg_at>
status_t docs_ref_gt<locations_at>::any_write(contents_arg_at&& vals_ref,
                                              ukv_doc_modification_t modification,
                                              ukv_doc_field_type_t type,
                                              ukv_options_t options) noexcept {
    status_t status;
    using value_extractor_t = contents_arg_extractor_gt<std::remove_reference_t<contents_arg_at>>;

    decltype(auto) locs = locations_.ref();
    auto count = keys_extractor_t {}.count(locs);
    auto keys = keys_extractor_t {}.keys(locs);
    auto collections = keys_extractor_t {}.collections(locs);
    auto fields = keys_extractor_t {}.fields(locs);

    auto vals = vals_ref;
    auto contents = value_extractor_t {}.contents(vals);
    auto offsets = value_extractor_t {}.offsets(vals);
    auto lengths = value_extractor_t {}.lengths(vals);

    ukv_docs_write( //
        db_,
        txn_,
        count,
        collections.get(),
        collections.stride(),
        keys.get(),
        keys.stride(),
        fields.get(),
        fields.stride(),
        nullptr,
        offsets.get(),
        offsets.stride(),
        lengths.get(),
        lengths.stride(),
        contents.get(),
        contents.stride(),
        modification,
        type,
        options,
        arena_,
        status.member_ptr());

    return status;
}

template <typename locations_at>
expected_gt<joined_strs_t> docs_ref_gt<locations_at>::gist(bool track) noexcept {

    status_t status;
    ukv_size_t found_count = 0;
    ukv_length_t* found_offsets = nullptr;
    ukv_str_span_t found_strings = nullptr;

    auto options = track ? ukv_option_read_track_k : ukv_options_default_k;
    decltype(auto) locs = locations_.ref();
    auto count = keys_extractor_t {}.count(locs);
    auto keys = keys_extractor_t {}.keys(locs);
    auto collections = keys_extractor_t {}.collections(locs);

    ukv_docs_gist( //
        db_,
        txn_,
        count,
        collections.get(),
        collections.stride(),
        keys.get(),
        keys.stride(),
        options,
        &found_count,
        &found_offsets,
        &found_strings,
        arena_,
        status.member_ptr());

    joined_strs_t view {found_count, found_offsets, found_strings};
    return {std::move(status), std::move(view)};
}

template <typename locations_at>
template <typename expected_at, typename layout_at>
expected_gt<expected_at> docs_ref_gt<locations_at>::any_gather(layout_at&& layout, ukv_options_t options) noexcept {

    decltype(auto) locs = locations_.ref();
    auto count = keys_extractor_t {}.count(locs);
    auto keys = keys_extractor_t {}.keys(locs);
    auto collections = keys_extractor_t {}.collections(locs);

    status_t status;
    expected_at view {
        count,
        layout.fields().size(),
        collections,
        keys,
        layout.fields().begin().get(),
        layout.types().begin().get(),
    };

    ukv_docs_gather( // Inputs:
        db_,
        txn_,
        count,
        layout.fields().size(),
        collections.get(),
        collections.stride(),
        keys.get(),
        keys.stride(),
        layout.fields().begin().get(),
        layout.fields().stride(),
        layout.types().begin().get(),
        layout.types().stride(),
        options,

        // Outputs:
        view.member_validities(),
        view.member_conversions(),
        view.member_collisions(),
        view.member_scalars(),
        view.member_offsets(),
        view.member_lengths(),
        view.member_tape(),

        // Meta
        arena_,
        status.member_ptr());

    return {std::move(status), std::move(view)};
}

} // namespace unum::ukv
