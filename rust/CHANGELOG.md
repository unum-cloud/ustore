# CHANGELOG

## 26-01-2023

### Contributors

-   chungquantin

### Added

-   Database structure

```rs
struct Database {
 db: UkvDatabaseInitType
}
```

-   Build bindings to `cpp` header file in `include/ukv`. To generate bindings code stored, use `cargo build`. `build.rs` includes code to build bindings using `bindgen`
-   Support two APIs: `ukv_database_init` and `ukv_database_free`
-   Add clippy, rust-fmt and editor config file for formatting and linting

### Issues

-   Can't generate bindings on file with linked source header. For example, generate on file `blobs.h` which includes `ukv/db.h` would throw error `No header file found`.
