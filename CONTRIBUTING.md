# Contributing to UKV

When contributing to the development of UKV, please first discuss the change you wish to make via issue, email, or any other method with the maintainers before making a change.

## Starting with Issues

Start contributing by adding new issue or continuing an [existing](https://github.com/unum-cloud/ukv/issues) one.
To create an effective and high quality ticket, try to add the following information:

* Steps to reproduce the issue, or a minimal test case.
* Hardware and Operating System environment specs.

Here is a template:

```txt
Title, like "UKV crashes when I do X"

The X consists of multiple steps...
1.
2.
3.

## Environment

* Compiler: GCC 10.
* OS: Ubuntu 20.04.
* ISA: x86.
* CPU: Intel Tiger Lake.

## Config

I am using the default config.
```

## Commits and Commit Messages

1. Subject ~~top~~ line is up to 50 characters.
   1. It shouldn't end with a period.
   2. It must start with verb.
   3. It can contain the programming language abbreaviation.
2. Description lines are optional and limited to 72 characters.
   1. Use the body to explain what and why vs. how. Code already answers the latter.
   2. If relevant, reference the issue at the end, using the hashtag notation.

A nice commit message can be:

```txt
Add: Support for shared memory exports

This feature minimizes the amount of data we need to transmit
through sockets. If the server and clients run on the same machine,
clients could access exported data in shared memory only knowing
its address (pointer).

See: #XXX
```

### Verbs

We agree on a short list of leading active verbs for the subject line:

* Add = Create a capability e.g. feature, test, dependency.
* Cut = Remove a capability e.g. feature, test, dependency.
* Fix = Fix an issue e.g. bug, typo, accident, misstatement.
* Make = Change the build process, dependencies, versions, or tooling.
* Refactor = A code change that MUST be just a refactoring.
* Form = Refactor of formatting, e.g. omit whitespace.
* Perf = Refactor of performance, e.g. speed up code.
* Docs = Refactor of documentation or spelling, e.g. help files.

Which is a well known and widely adopted set.

## Pull Request Process

1. Ensure your code compiles. Run `cmake . && make` before creating the pull request.
2. Feel free to open PR Drafts for larger changes, if you want a more active participation from maintainers and the community.
3. Merge into `dev` branch. The `main` branch is protected.


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

All existing Open Source implementations are Lite. This means that the Graph modality logic that comes with RocksDB-based builds is identical to the Graph modality logic that comes with LevelDB-based builds. So the engine only implements the functions from "db.h" and "blobs.h", and the rest of the logic comes from a generic "modality_graph.cpp", that transforms and redirects calls into BLOB layer.

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
