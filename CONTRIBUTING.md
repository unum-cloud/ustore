# Contribution Guide

Thank you for even opening this page!
It's always nice to have third-party contributors!

---

To keep the quality of the code high, we have a set of guidelines.

- Whats the procedure?
- How to organize branches?
- How to organize issues?
- How to style commits?

## Implementing a new Language Binding

When you start enumerating the dozens of functions across `/include/ukv` header files, you may not immediately want to implement all of them for a new language binding.
We feel the same way, and here is the order in which we approach implementing new language bindings.

1. To open and close DB: `ukv_database_init()`, `ukv_database_free()`.
2. To access binary values in the main collection: `ukv_write()`, `ukv_read()`.
3. Supporting multiple named collections: `ukv_collection_list()`, `ukv_collection_create()`, `ukv_collection_drop()`.
4. Supporting transactions: `ukv_transaction_init()`, `ukv_transaction_stage()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
5. Supporting scans: `ukv_scan()`.

Once you wrap those functions, we consider the binding usable.
What have we forgotten?

* Machine Learning: `ukv_sample()`.
* Metadata: `ukv_database_control()`, `ukv_measure()`.
* [Graphs](https://unum.cloud/ukv/c/#graphs).
* [Documents](https://unum.cloud/ukv/c/#documents).
* [Paths](https://unum.cloud/ukv/c/#paths).
* [Vectors](https://unum.cloud/ukv/c/#vectors).

All of those are optional and can be implemented in any order, if at all.
Bindings for top-tier languages with compatible build-systems can be integrated into the primary repository.

## Implementing a new Engine

Implementing a new engine is similar to [implementing a new binding](#implementing-a-new-language-binding).
Implementations can be of 2 kinds:

* Lite: native implementation of BLOB modality.
* Complete: native implementation of all modalities.

All existing Open Source implementations are Lite.
This means that the Graph modality logic that comes with RocksDB-based builds is identical to the Graph modality logic that comes with LevelDB-based builds.
So the engine only implements the functions from "db.h" and "blobs.h", and the rest of the logic comes from a generic "modality_graph.cpp", that transforms and redirects calls into BLOB layer.

With this in mind, if you were to implement an engine based on ETCD, FoundationDB, Redis, DragonFly, LMDB, or WiredTiger, you would start with the following functions:

1. To open and close DB: `ukv_database_init()`, `ukv_database_free()`.
2. To access binary values in the main collection: `ukv_write()`, `ukv_read()`.
3. Supporting multiple named collections: `ukv_collection_list()`, `ukv_collection_create()`, `ukv_collection_drop()`.
4. Supporting transactions: `ukv_transaction_init()`, `ukv_transaction_stage()`, `ukv_transaction_commit()`, `ukv_transaction_free()`.
5. Supporting scans: `ukv_scan()`.
6. Machine Learning: `ukv_sample()`. Rarely supported, generally faked via reservoir sampling of bulk scans.
7. Metadata: `ukv_database_control()`, `ukv_measure()`. Can be simply silenced.
8. Memory management: `ukv_arena_free()`, `ukv_error_free()`.

Additionally, you have to configure a few `extern` constants, depending on the range of supported functionality of the underlying engine:

* `bool ukv_supports_transactions_k`.
* `bool ukv_supports_named_collections_k`.
* `bool ukv_supports_snapshots_k`.

For RocksDB, all of those are `true`.
For LevelDB, only the latter.
You should also define the following:

* `ukv_collection_t ukv_collection_main_k`. `0`, by default.
* `ukv_length_t ukv_length_missing_k`. `UINT_MAX`, by default.
* `ukv_key_t ukv_key_unknown_k`. `LONG_MAX`, by default.

> If you are implementing a Lite engine on top of a standalone DBMS, you must acknowledge that logic will split between multiple machines. As such, Graph updates generally translate into binary Read-Modify-Write operations. If those are forwarded to a remote location, you will pay double the networking costs for two round trips.
