<h1 align="center">UKV</h1>
<h3 align="center">Universal Binary Interface</h3>
<h3 align="center">For The Fastest DBMS Ever Built</h3>
<h5 align="center">RocksDB • LevelDB • UDisk • UMem</h5>
<h5 align="center">Blobs • Documents • Graphs • Vectors</h5>
<h5 align="center">C • C++ • Python • Java • GoLang</h5>
<br/>

<p align="center">
<a href="https://twitter.com/unum_cloud"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?color=purple&style=flat-square"></a>
&nbsp;&nbsp;
<a href="https://www.linkedin.com/company/unum-cloud/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?color=purple&style=flat-square"></a>
&nbsp;&nbsp;
<a href="https://www.github.com/unum-cloud/"><img src="https://img.shields.io/github/issues-closed-raw/unum-cloud/ukv?color=purple&style=flat-square"></a>
&nbsp;&nbsp;
<a href="https://www.github.com/unum-cloud/"><img src="https://img.shields.io/github/stars/unum-cloud/ukv?color=purple&style=flat-square"></a>
</p>

## What is UKV?

UKV is an open C-layer binary standard for "Create, Read, Update, Delete" operations, or CRUD for short.
Many databases exist today, providing similar functionality and performance under different interfaces.
We approach the problem from a different angle.
If databases do similar things, let's standardize the interface and compete for the best implementation.
That way migrations are easier and the system can be modularized into different parts, to give users the absolute flexibility in choosing between different [Data Forms ~ Modalities](#modalities) and [Key-Value Store ~ Engines](#engines) implementations, as well as different Distribution forms and [Client SDKs](#frontends).

## The [BLAS][blas] of [CRUD][crud]

Such generic standard exists in computing since 1979.
It is called [BLAS][blas] and was the locomotive of Numerical Methods across all disciplines in the past 50 years.
Every deep-learning you use relies of BLAS.
What is the standard that your DBMS can rely on?

![Universal Key Values by Unum](assets/UKV.png)

We haven't passed the test of time, like BLAS, but we can go beyond them in modularity and a great reference implementation.
Today, Intel, Nvidia, AMD, GraphCore, Cerebras and many others ship optimized implementations of BLAS for their hardware, but people rarely use the open-source reference design.

---

## Modularity

Our reference implementation aims to be faster and more scalable, than established DBs, like MongoDB, Neo4J, Redis, ETCD and Postgres.
You can compose your own "X" DBMS killer from any combination of components.

| Engine  | Modality  |          Distribution           |            Frontend             |
| :-----: | :-------: | :-----------------------------: | :-----------------------------: |
|         |           |                                 |                                 |
| LevelDB |   Blobs   |            Embedded             |            C and C++            |
| RocksDB |  Graphs   |        Client-Server RPC        |             Python              |
|  UMem   | Documents |     Arrow Flight RPC Server     | GoLang <sup>*in-progress*</sup> |
|  UDisk  |  Vectors  | Distributed <sup>*coming*</sup> |  Java <sup>*in-progress*</sup>  |

Run `cloc $(git ls-files)` and you will see, that UKV fits into just around 20'000 Lines fo Code.
Compare this to bulky 1.5 Million LOC in Postgres and 4.5 Million LOC for MySQL ([*source*][dbms-cloc]).
They target just the tabular workloads.
After standardizing the API, we can make the system a lot reacher in features!

<h6 align="center">
ACID transactions across many collections • Snapshots  • Operation-level WATCHes • 
BSON, JSON, MessagePack documents support • JSON Patches & Merrge-Patches • JSON Pointers Addressing • 
Native Apache Arrow format support in all APIs • Arrow Flight RPC Server • Bulk Scans • Random Samping • 
Pandas Tabular API • NetworkX Graph API • PyTorch & TensorFlow Data-Loaders
</h6>

---

## Usecases

Let's start with the simplest and work our way up.

1. Getting a Python, GoLang, Java wrapper for vanilla RocksDB or LevelDB.
2. Serving them over a network via Apache Arrow Flight RPC.
3. Embedded Document and GraphDB, that will avoid networking overheads.
4. Semlessly Tiering Multi-Modal DBMS between UMem and persistent backends. 

Even with just a single node, in a 2U chassis today we can easily get 24x 16 TB of NVMe storage connected to 2x CPU sockets, totalling at 384 TB of space, capable of yielding ~120 GB/s of read throughput, out of which, ~70 GB/s our in-house engine can already sustain.
Combining it with numerous features above and GPU acceleration, once can get an all-one Data-Lake with the feel of Pandas, speed of Rapids, scale of [Hadoop][hadoop] and consistency of Postgres.

![UKV Data-lake](assets/UKV_Combo.png)

<h3 align="left"><s>One ring to rule them all.</s></h3>
<h3 align="left">One lake to server them all.</h3>

---

## The Simplicity of Consistency

It is normal to have a separate Postgres for your transactional data, a MongoDB for your large flexible-schema document collections, a Neo4J instance for your graphs, and an [S3][s3] storage bucket for your media data, all serving the different data needs of a single business.

> Example: a social network, storing passwords in Postgres, posts in MongoDB, user relations in Neo4J and post attachments in S3.

So when the data is updated, you have to apply changes across all those instances, manually rolling them back if one of the parts failed.
Needless to say, every system has a different API, different guarantees, and runtime constraints.

---

## Binary = Performance

Over the years we broke speed limits on CPUs and GPUs using SIMD, branch-less computing and innovation in parallel algorithm design.
We deliberately minimize the dynamic allocations in all modality implementations.
Our engine implementations - "UMem" and "UDisk" follow the same tradition, but in "LevelDB" and "RocksDB" we are forced to re-allocate, as the library authors didn't design a proper binary interface.
The one they provide heavily depends on C++ Standard Templates Library and the use fo standard allocators.

Our interface is very fast, has no dynamic polymorphism and throws no exceptions, to match the speed and the quality of fast underlying engines and work for months uninterrupted!
We have numerous detailed blog posts on performance:

## Engines

Backends differ in their functionality and purposes.
The underlying embedded key value stores include:

| Name    |  Speed   |       OS        | Transact | Collections | Persistent | [Snapshots][snap] | [Watches][watch] |
| :------ | :------: | :-------------: | :------: | :---------: | :--------: | :---------------: | :--------------: |
| LevelDB |   0.5x   | POSIX + Windows |    ❌     |      ❌      |     ✅      |         ❌         |        ❌         |
| RocksDB |    1x    | POSIX + Windows |    ✅     |      ✅      |     ✅      |         ✅         |        ✅         |
| UMem    | **10x**  | POSIX + Windows |    ✅     |      ✅      |     ❌      |         ❌         |        ✅         |
| UDisk   | **3-5x** |      Linux      |    ✅     |      ✅      |     ✅      |         ✅         |        ✅         |


* UMem in-memory backend originally served educational purposes. Then it was superseeded by the [`consistent_set`][consistent_set] and can now be considered the fastest in-memory Key-Value Store, superior to Redis, MemCached or ETCD.
* LevelDB was originally designed at Google and extensively used across the industry, thanks to its simplicity.
* RocksDB improves over LevelDB, extending its functionality with transactions, named collections, and higher performance.
* UDisk is our **proprietary** in-house implementation with superior concurrency-control mechnisms and Linux kernel-bypass techniques, as well as, GPU acceleration. The first of its kind.

All of those backends were [benchmarked for weeks](https://unum.cloud/ucsb) using [UCSB](https://github.com/unum-cloud/ucsb), so you can choose the best stack for you specific use case.

![UCSB 10 TB Results](https://unum.cloud/assets/post/2022-09-13-ucsb-10tb/ucsb-10tb-duration.png)

We have published the results for BLOB-layer abstractions for [10 TB][ucsb-10], and, previously, [1 TB][ucsb-1] collections.
Above binary layer, in logic, those numbers are further multiplied.
Where MongoDB does 2'000 operations/s, our Community Edition does 10'000 ops/s and the Pro Version yields 50'000 ops/s.

## Frontends

Currently, at Proof-of-Concept stage, we support only the essential functionality in select programming languages.

| Name        | Transact | Collections | Batches | Docs  | Graphs | Copies |
| :---------- | :------: | :---------: | :-----: | :---: | :----: | :----: |
| C ³         |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |   0    |
| C++ ³       |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |   0    |
| Python ¹ ³  |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |  0-1   |
| Arrow RPC ³ |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |  0-2   |
| GoLang      |    ✅     |      ✅      |    ✅    |   ❌   |   ❌    |   1    |
| Java        |    ✅     |      ✅      |    ❌    |   ❌   |   ❌    |   1    |
|             |          |             |         |       |        |        |
| C# ²        |    ❌     |      ❌      |    ❌    |   ❌   |   ❌    |        |
| REST API ²  |    ❌     |      ❌      |    ❌    |   ❌   |   ❌    |        |
| Wolfram ¹ ² |    ❌     |      ❌      |    ❌    |   ❌   |   ❌    |        |

* Copies: Number of re-allocations/conversions per byte.
* ¹: Support tensor lookups and media data.
* ²: Missing, to be implemented.
* ³: Supports tabular Arrow exports.

## Modalities

We came from humble beginnings.
We just wanted to standardize binary Key-Value operations.
Integer keys, variable length values.
That's it.

You can also think of such a KVS as a memory-allocator:

* The key is a 64-bit integer, just like a pointer on most modern systems.
* The value is the variable length block, addressed by it.

Once you have a good enough shared interface, it is relatively easy to build on top of it, adding support for:

* Documents,
* Graphs,
* Vectors, and
* Paths.

Is there something else you need?
Submit a feature request!

## Installation & Deployment

* For Python: `pip install ukv`
* For Conan: `conan install ukv`
* For Docker image: `docker run --rm --name test_ukv -p 38709:38709 unum/ukv`

To build from source:

```sh
cmake \
    -DUKV_BUILD_PYTHON=1 \
    -DUKV_BUILD_TESTS=1 \
    -DUKV_BUILD_BENCHMARKS=1 \
    -DUKV_BUILD_FLIGHT_RPC=1 . && \
    make -j16
```

To add to your CMake project:

```cmake
cmake_minimum_required(VERSION 3.11)
project(ukv_example)

include(FetchContent)

FetchContent_Declare(
    ukv
    GIT_REPOSITORY https://github.com/unum-cloud/UKV.git
    GIT_SHALLOW TRUE
    GIT_TAG v0.3.0
)
FetchContent_MakeAvailable(ukv)
set(ukv_INCLUDE_DIR ${ukv_SOURCE_DIR}/include)
include_directories(${ukv_INCLUDE_DIR})

add_executable(ukv_example_test main.cpp)
target_link_libraries(ukv_example_test ukv)
```

For Flight RPC, Apache Arrow must be pre-installed.

To build language bindings:

```sh
./python/run.sh
./java/run.sh
./golang/run.sh
```

Building Flight RPC Docker Image:

```sh
docker build -t ukv .
```

Building Conan package, without installing it:

```sh
conan create . ukv/testing --build=missing
```

* To see a usage examples, check the [C][c-example] API and the [C++ API](cpp-example) tests.
* To read the documentation, [check unum.cloud/ukv](https://unum.cloud/UKV).
* To contribute to the development, [check the `src/`](https://github.com/unum-cloud/UKV/blob/main/src).

[c-example]: https://github.com/unum-cloud/UKV/blob/main/tests/compilation.cpp
[cpp-example]: https://github.com/unum-cloud/UKV/blob/main/tests/compilation.cpp

## Similar Projects

* [EJDB](https://github.com/Softmotions/ejdb) is a pure C embeddable JSON database engine.
  * **Pros**:
    * C11 API.
    * Many bindings, including JS and *currently* better Java support.
    * MacOS and Windows support, that we *currently* don't prioritize.
  * **Cons**:
    * Very slow.
    * No ACID transactions.
    * No way to swap the backend "engine".
    * No support for non-document modalities, like Graphs.
    * No support for batch operations.
    * Bindings are just string exchange interfaces.
* [SurrealDB](https://github.com/surrealdb/surrealdb) is a scalable, distributed, collaborative, document-graph database, for the realtime web.
  * **Pros**:
    * Many bindings, including JS.
    * MacOS and Windows support, that we *currently* don't prioritize.
    * User permissions management functionality.
  * **Cons**:
    * Very slow.
    * No way to swap the backend "engine".
    * Custom string-based query language.
    * Bindings are just string exchange interfaces.
    * No C API [yet](https://surrealdb.com/docs/integration/libraries/c).

## Presets, Limitations and FAQ

* Keys are 64-bit integers. Use "paths" modality for string keys. [Why?](ukv_key_t)
* Values are binary strings under 4 GB long. [Why?](ukv_length_t)
* Transactions are ACI(D) by-default. [What does it mean?](ukv_transaction_t)
* Why not use LevelDB or RocksDB interface? [](ukv.h)
* Why not use SQL, MQL or Cypher? [](ukv.h)


[blas]: https://en.wikipedia.org/wiki/Basic_Linear_Algebra_Subprograms
[crud]: https://en.wikipedia.org/wiki/Create,_read,_update_and_delete
[acid]: https://en.wikipedia.org/wiki/ACID
[arrow]: https://arrow.apache.org/
[patch]: https://datatracker.ietf.org/doc/html/rfc6902
[mpack]: https://msgpack.org/index.html
[flight]: https://arrow.apache.org/docs/format/Flight.html
[pointer]: https://datatracker.ietf.org/doc/html/rfc6901
[bson]: https://www.mongodb.com/json-and-bson
[pytorch]: https://pytorch.org/
[tensorflow]: https://tensorflow.org
[rocksdb]: https://rocksdb.org
[leveldb]: https://github.com/google/leveldb
[hadoop]: https://hadoop.apache.org
[networkx]: https://networkx.org
[gunrock]: https://gunrock.github.io
[s3]: https://aws.amazon.com/s3
[dbms-cloc]: https://news.ycombinator.com/item?id=24813239
[ucsb-10]: https://unum.cloud/post/2022-03-22-ucsb/
[ucsb-1]: https://unum.cloud/post/2021-11-25-ycsb/
[watch]: https://redis.io/commands/watch/
[snap]: https://github.com/facebook/rocksdb/wiki/Snapshot
[acid]: https://en.wikipedia.org/wiki/ACID
[consistent_set]: https://github.com/ashvardanian/consistent_set