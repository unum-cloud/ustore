extern crate bindgen;

use std::env::var;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("cargo:rerun-if-changed=wrapper.h");
	// println!("cargo:rustc-link-search=../include");

	let bindings = bindgen::Builder::default()
		.header("./wrapper.h")
		.clang_args(&["-I../include", "-I../include/ukv"])
		.detect_include_paths(true)
		.parse_callbacks(Box::new(bindgen::CargoCallbacks))
		.generate()?;

	let out_path = PathBuf::from(var("OUT_DIR")?);
	bindings.write_to_file(out_path.join("bindings.rs"))?;
	Ok(())
}
