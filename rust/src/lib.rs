mod error;
mod ukv;

use std::ffi::CString;

include!("../src/ukv/bindings.rs");

type UkvDatabaseInitType = ukv_database_init_t;

pub struct Database {
	db: UkvDatabaseInitType,
}

impl Database {
	/// Open a new database using ukv_database_init()
	pub fn new() -> Result<Self, error::DataStoreError> {
		let c_str = CString::default();
		let config: *const std::ffi::c_char = c_str.as_ptr();
		let error: *mut *const std::ffi::c_char = &mut c_str.as_ptr();
		let mut void_fn = &mut () as *mut _ as *mut std::ffi::c_void;
		let db_ptr = &mut void_fn;
		let mut database = UkvDatabaseInitType {
			config,
			db: db_ptr,
			error,
		};
		// Calling C++ function generated from bindgen
		unsafe {
			ukv_database_init(&mut database);
		};

		Ok(Self {
			db: database,
		})
	}

	pub fn contains_key() {}

	// Close a database using ukv_database_free()
	pub fn close() -> Result<(), error::DataStoreError> {
		let void_fn = &mut () as *mut _ as *mut std::ffi::c_void;
		unsafe {
			ukv_database_free(void_fn);
		};
		Ok(())
	}
}
