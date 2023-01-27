extern crate bindgen;

use std::env::var;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Tell cargo to invalidate the built crate whenever the wrapper changes
	println!("cargo:rerun-if-changed=wrapper.h");
	// Tell cargo to look for shared libraries in the specified directory
	println!("cargo:rustc-link-search=../include/ukv");

	let bindings = bindgen::Builder::default()
		// .header("../include/ukv/db.h")
		// .header("../include/ukv/blobs.h")
		.header("./wrapper.h")
		.detect_include_paths(true)
		.parse_callbacks(Box::new(bindgen::CargoCallbacks))
		.generate()?;

	// Write the bindings to the $OUT_DIR/bindings.rs file.
	let out_path = PathBuf::from(var("OUT_DIR")?);
	bindings.write_to_file(out_path.join("bindings.rs"))?;
	Ok(())
}
