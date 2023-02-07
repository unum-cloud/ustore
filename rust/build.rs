extern crate bindgen;
extern crate cmake;
extern crate num_cpus;

use std::path::PathBuf;

/// Gets the absolute path of the parent directory. This will be used for
/// specifying the cmake directory and linking the library files from there.
fn get_parent_dir() -> std::io::Result<String> {
	let path = std::env::current_dir().unwrap();
	let path = format!("{}/../", path.as_os_str().to_string_lossy());
	let path = PathBuf::from(path);
	let path = path.canonicalize().unwrap();
	let path = path.as_os_str();
	Ok(format!("{}", path.to_string_lossy()))
}

const ENABLED: &'static str = "1";
const DISABLED: &'static str = "0";

fn main() {
	let makeflags = format!("-j{}", num_cpus::get());
	std::env::set_var("CARGO_MAKEFLAGS", &makeflags);
	std::env::set_var("MAKEFLAGS", makeflags);

	let parent_dir = get_parent_dir().unwrap();
	let dst = cmake::Config::new(&parent_dir)
		.define(
			"UKV_BUILD_ENGINE_UMEM",
			if cfg!(feature = "umem") {
				ENABLED
			} else {
				DISABLED
			},
		)
		.define(
			"UKV_BUILD_ENGINE_LEVELDB",
			if cfg!(feature = "leveldb") {
				ENABLED
			} else {
				DISABLED
			},
		)
		.define(
			"UKV_BUILD_ENGINE_ROCKSDB",
			if cfg!(feature = "rocksdb") {
				ENABLED
			} else {
				DISABLED
			},
		)
		.define(
			"UKV_BUILD_API_FLIGHT_CLIENT",
			if cfg!(feature = "flight-client") {
				ENABLED
			} else {
				DISABLED
			},
		)
		.define(
			"UKV_BUILD_API_FLIGHT_SERVER",
			if cfg!(feature = "flight-server") {
				ENABLED
			} else {
				DISABLED
			},
		)
		.define(
			"CMAKE_BUILD_TYPE",
			if std::env::var("PROFILE").unwrap() == "release" {
				"Release"
			} else {
				"Debug"
			},
		)
		.define("UKV_BUILD_TESTS", DISABLED)
		.define("UKV_BUILD_BENCHMARKS", DISABLED)
		.build();

	println!("cargo:rustc-link-search=native={}", dst.display());

	#[cfg(feature = "umem")]
	println!("cargo:rustc-link-lib=ukv_embedded_umem");

	#[cfg(feature = "rocksdb")]
	println!("cargo:rustc-link-lib=ukv_embedded_rocksdb");

	#[cfg(feature = "leveldb")]
	println!("cargo:rustc-link-lib=ukv_embedded_leveldb");

	#[cfg(feature = "flight-client")]
	println!("cargo:rustc-link-lib=ukv_flight_client");

	#[cfg(feature = "flight-server")]
	println!("cargo:rustc-link-lib=ukv_flight_server");

	let output = PathBuf::from(std::env::var("OUT_DIR").unwrap());
	fs_extra::copy_items(
		&[format!("{}/include", parent_dir).as_str()],
		output.display().to_string().as_str(),
		&fs_extra::dir::CopyOptions {
			depth: 0, // unlimited
			overwrite: true,
			skip_exist: true,
			copy_inside: false,
			content_only: false,
			buffer_size: 64_000,
		},
	)
	.unwrap();
	drop(parent_dir);

	let bindings = bindgen::Builder::default()
		.header("wrapper.h")
		.size_t_is_usize(true)
		.detect_include_paths(true)
		.parse_callbacks(Box::new(bindgen::CargoCallbacks))
		.clang_arg(format!("-I{}/include/", dst.display()))
		.clang_arg(format!("-I{}/include/ukv", dst.display()))
		.generate()
		.unwrap();
	bindings.write_to_file(output.join("bindings.rs")).unwrap();
	eprintln!("{}", output.display()); // printing in the end for debugging purposes
}
