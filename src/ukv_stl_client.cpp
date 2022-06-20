#include <vector>
#include <string>
#include <string_view>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>
#include <cstring> // `std::memcpy`

#include "ukv.h"

/*********************************************************/
/*****************	 C++ Implementation	  ****************/
/*********************************************************/

namespace {

struct remote_db_t {
    int socket;
    int io_context;
};

} // namespace

/*********************************************************/
/*****************	 Primary Functions	  ****************/
/*********************************************************/

void ukv_open(
    // Inputs:
    [[maybe_unused]] char const* config,
    // Outputs:
    ukv_t* db,
    [[maybe_unused]] ukv_error_t* c_error) {

    *db = new remote_db_t {};
}

void ukv_write(
    // Inputs:
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_write_t const c_options,
    //
    ukv_val_ptr_t const* c_values,
    ukv_val_len_t const* c_values_lengths,
    // Outputs:
    ukv_error_t* c_error) {

    remote_db_t& db = *reinterpret_cast<remote_db_t*>(c_db);

}

void ukv_read(
    // Inputs:
    ukv_t const c_db,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_read_t const c_options,

    // In-outs:
    void** c_arena,
    size_t* c_arena_length,

    // Outputs:
    ukv_val_ptr_t* c_values,
    ukv_val_len_t* c_values_lengths,
    ukv_error_t* c_error) {

    remote_db_t& db = *reinterpret_cast<remote_db_t*>(c_db);

}

/*********************************************************/
/*****************	Columns Management	  ****************/
/*********************************************************/

void ukv_column_upsert(
    // Inputs:
    ukv_t const c_db,
    char const* c_column_name,
    // Outputs:
    ukv_column_t* c_column,
    ukv_error_t* c_error) {

    remote_db_t& db = *reinterpret_cast<remote_db_t*>(c_db);

}

void ukv_column_remove(
    // Inputs:
    ukv_t const c_db,
    char const* c_column_name,
    // Outputs:
    [[maybe_unused]] ukv_error_t* c_error) {

    remote_db_t& db = *reinterpret_cast<remote_db_t*>(c_db);

}

/*********************************************************/
/*****************		Transactions	  ****************/
/*********************************************************/

void ukv_txn_begin(
    // Inputs:
    ukv_t const c_db,
    size_t const c_sequence_number,
    // Outputs:
    ukv_txn_t* c_txn,
    ukv_error_t* c_error) {

    remote_db_t& db = *reinterpret_cast<remote_db_t*>(c_db);

}

void ukv_txn_write(
    // Inputs:
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    //
    ukv_val_ptr_t const* c_values,
    ukv_val_len_t const* c_values_lengths,
    // Outputs:
    ukv_error_t* c_error) {

    // We need a `shared_lock` here just to avoid any changes to
    // the underlying addresses of columns.
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    remote_db_t& db = *txn.db_ptr;

}

void ukv_txn_read(
    // Inputs:
    ukv_txn_t const c_txn,
    ukv_key_t const* c_keys,
    size_t const c_keys_count,
    ukv_column_t const* c_columns,
    size_t const c_columns_count,
    [[maybe_unused]] ukv_options_read_t const options,

    // In-outs:
    void** c_arena,
    size_t* c_arena_length,

    // Outputs:
    ukv_val_ptr_t* c_values,
    ukv_val_len_t* c_values_lengths,
    ukv_error_t* c_error) {

    // This read can fail, if the values to be read have already
    // changed since the beginning of the transaction!
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    remote_db_t& db = *txn.db_ptr;

}

void ukv_txn_commit(
    // Inputs:
    ukv_txn_t const c_txn,
    [[maybe_unused]] ukv_options_write_t const options,
    // Outputs:
    ukv_error_t* c_error) {

    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    remote_db_t& db = *txn.db_ptr;

}

/*********************************************************/
/*****************		  Iterators	      ****************/
/*********************************************************/

void ukv_iter_make(ukv_column_t const, ukv_iter_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_seek(ukv_iter_t const, ukv_key_t, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_advance(ukv_iter_t const, size_t const, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_read_key(ukv_iter_t const, ukv_key_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_read_value_size(ukv_iter_t const, size_t*, size_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

void ukv_iter_read_value(ukv_iter_t const, void**, size_t*, ukv_val_ptr_t*, ukv_val_len_t*, ukv_error_t* error) {
    *error = "Iterators aren't supported by std::unordered_map";
}

/*********************************************************/
/*****************	  Memory Management   ****************/
/*********************************************************/

void ukv_arena_free(ukv_t const, void* c_ptr, size_t c_len) {
    allocator_t {}.deallocate(reinterpret_cast<byte_t*>(c_ptr), c_len);
}

void ukv_txn_free(ukv_t const, ukv_txn_t const c_txn) {
    txn_t& txn = *reinterpret_cast<txn_t*>(c_txn);
    delete &txn;
}

void ukv_free(ukv_t c_db) {
    remote_db_t& db = *reinterpret_cast<remote_db_t*>(c_db);
    delete &db;
}

void ukv_column_free(ukv_t const, ukv_column_t const) {
    // In this in-memory freeing the column handle does nothing.
    // The DB destructor will automatically cleanup the memory.
}

void ukv_iter_free(ukv_t const, ukv_iter_t const) {
}

void ukv_error_free(ukv_error_t) {
}
