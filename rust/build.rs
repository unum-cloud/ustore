extern crate bindgen;
extern crate num_cpus;

use std::io::Write;
use std::path::PathBuf;

/// Gets the absolute path of the parent directory. This will be used for
/// specifying the cmake directory and linking the library files from there.
fn get_parent_dir() -> std::io::Result<String> {
    let path = std::env::current_dir()?;
    let path = format!("{}/../", path.as_os_str().to_string_lossy());
    let path = PathBuf::from(path);
    let path = path.canonicalize()?;
    let path = path.as_os_str();
    Ok(format!("{}", path.to_string_lossy()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let src = get_parent_dir()?;
    let cmd = std::process::Command::new("cmake")
        .arg("-DCMAKE_C_COMPILER=gcc") // this works, however, changing to clang will fail
        .arg("-DCMAKE_CXX_COMPILER=g++") // this works, however, changing to clang++ will fail
        .arg("-DUKV_BUILD_TESTS=0")
        .arg("-DUKV_BUILD_BENCHMARKS=0")
        .arg("-DUKV_BUILD_BUNDLES=1")
        .arg(format!(
            "-DUKV_BUILD_ENGINE_UMEM={}",
            if cfg!(feature = "umem") { 1 } else { 0 }
        ))
        .arg(format!(
            "-DUKV_BUILD_ENGINE_LEVELDB={}",
            if cfg!(feature = "leveldb") { 1 } else { 0 }
        ))
        .arg(format!(
            "-DUKV_BUILD_ENGINE_ROCKSDB={}",
            if cfg!(feature = "rocksdb") { 1 } else { 0 }
        ))
        .arg("-DUKV_BUILD_API_FLIGHT_CLIENT=0")
        .arg("-DUKV_BUILD_API_FLIGHT_SERVER=0")
        .arg("-B ./build_release")
        .current_dir(&src)
        .output()
        .expect("Could not spawn a `cmake` process");

    println!("CMake: {}", cmd.status);
    std::io::stdout().write_all(&cmd.stdout)?;
    std::io::stderr().write_all(&cmd.stderr)?;

    let cmd = std::process::Command::new("make")
        .arg(format!("-j{}", num_cpus::get()))
        .arg("-C")
        .arg("./build_release")
        .current_dir(&src)
        .output()
        .expect("Could not spawn a `make` process");

    println!("Make: {}", cmd.status);
    std::io::stdout().write_all(&cmd.stdout)?;
    std::io::stderr().write_all(&cmd.stderr)?;

    let cmd = std::process::Command::new("gcc")
        .arg("-E")
        .arg("-I")
        .arg(format!("{}/include", &src))
        .arg("-I")
        .arg(format!("{}/include/ukv", &src))
        .arg("wrapper.h")
        .arg("-o")
        .arg("wrapper.expanded.h")
        .output()
        .expect("Could not spawn a `gcc` expansion process");

    println!("GCC: {}", cmd.status);
    std::io::stdout().write_all(&cmd.stdout)?;
    std::io::stderr().write_all(&cmd.stderr)?;

    println!(
        "cargo:rustc-link-search=native={}/build_release/build/lib",
        &src
    );
    #[cfg(feature = "umem")]
    println!("cargo:rustc-link-lib=static=umem_bundle");
    #[cfg(feature = "rocksdb")]
    println!("cargo:rustc-link-lib=static=rocksdb_bundle");
    #[cfg(feature = "leveldb")]
    println!("cargo:rustc-link-lib=static=leveldb_bundle");

    let output = PathBuf::from(std::env::var("OUT_DIR")?);
    let bindings = bindgen::Builder::default()
        .header("wrapper.expanded.h")
        .size_t_is_usize(true)
        .enable_cxx_namespaces()
        .detect_include_paths(true)
        .respect_cxx_access_specs(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .clang_arg("-I")
        .clang_arg(format!("{}/include", &src))
        .clang_arg("-I")
        .clang_arg(format!("{}/include/ukv", &src))
        .clang_arg("--verbose")
        .generate()?;

    bindings.write_to_file(output.join("bindings.rs"))?;
    println!("cargo:rerun-if-changed=wrapper.h");
    Ok(())
}
