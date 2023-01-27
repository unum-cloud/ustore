#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

mod error;

pub mod bindings {
	include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

type UkvDatabaseInitType = bindings::ukv_database_init_t;

pub struct Database {
	pub db: UkvDatabaseInitType,
}

impl Default for Database {
	fn default() -> Self {
		let config: *const _ = std::ffi::CString::default().as_ptr();
		let error: *mut _ = &mut std::ffi::CString::default().as_ptr();
		let void_fn = &mut () as *mut _ as *mut std::ffi::c_void;
		let mut db = UkvDatabaseInitType {
			error,
			config,
			db: void_fn as _,
		};

		unsafe { bindings::ukv_database_init(&mut db) };
		Self {
			db,
		}
	}
}

impl Database {
	/// Open a new database using ukv_database_init()
	pub fn new() -> Self {
		let config: *const _ = std::ffi::CString::default().as_ptr();
		let error: *mut _ = &mut std::ffi::CString::default().as_ptr();
		let void_fn = &mut () as *mut _ as *mut std::ffi::c_void;
		let mut db = UkvDatabaseInitType {
			error,
			config,
			db: void_fn as _,
		};

		unsafe { bindings::ukv_database_init(&mut db) };
		Self {
			db,
		}
	}

	pub fn contains_key() {}

	// Close a database using ukv_database_free()
	pub fn close() -> Result<(), error::DataStoreError> {
		let void_fn = &mut () as *mut _ as *mut std::ffi::c_void;
		unsafe { bindings::ukv_database_free(void_fn) };

		Ok(())
	}
}
