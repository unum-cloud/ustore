<h1 align="center">UKV</h1>
<h3 align="center">
Universal Binary Interface<br/>
For The Fastest DBMS Ever Built
</h3>

<h5 align="center">
RocksDB • LevelDB • UDisk • UMem<br/>
Blobs • Documents • Graphs • Texts • Vectors<br/>
C • C++ • Python • Java • GoLang • Apache Arrow
</h5>
<br/>

<p align="center">
<a href="https://twitter.com/unum_cloud"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?color=purple&style=flat-square"/></a>
&nbsp;&nbsp;
<a href="https://www.linkedin.com/company/unum-cloud/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?color=purple&style=flat-square"/></a>
&nbsp;&nbsp;
<a href="https://www.github.com/unum-cloud/"><img src="https://img.shields.io/github/issues-closed-raw/unum-cloud/ukv?color=purple&style=flat-square"/></a>
&nbsp;&nbsp;
<a href="https://www.github.com/unum-cloud/"><img src="https://img.shields.io/github/stars/unum-cloud/ukv?color=purple&style=flat-square"/></a>
&nbsp;&nbsp;
<a href="#"><img src="https://img.shields.io/github/workflow/status/unum-cloud/ukv/Build?color=purple&style=flat-square"/></a>
</p>

## What is UKV?

UKV is an **open** C-layer binary standard for "Create, Read, Update, Delete" operations, or CRUD for short.
Many databases exist today, providing similar functionality and performance under different interfaces.
It is a problem for DBMS users, introducing vendor locks and additional friction for adopting newer and better technologies.

If databases do similar things, let's standardize the interface and compete for the best implementation.
That way, migrations are more manageable, and the system can be modularized into parts, giving users absolute flexibility in choosing between different:

* Forms of data, or [Modalities](#modalities),
* Key-Value Store implementations, or [Engines](#engines),
* Distribution forms, or Packaging,
* Client SDKs, or [Frontends](#frontends) for higher-level languages.

---

## The [BLAS][blas] of [CRUD][crud]

Such generic standards have existed in computing since 1979.
BLAS was the locomotive of Numerical Methods across all disciplines in the past 50 years. 
Every deep-learning you use relies on BLAS.
What is the standard that your DBMS can be built around?

![UKV: Small Map](assets/charts/Intro.png)

We have yet to pass the test of time, like BLAS, but we can surpass them in modularity and provide a better reference implementation.
Today, Intel, Nvidia, AMD, GraphCore, Cerebras, and many others ship optimized implementations of BLAS for their hardware.
Similarly, we ship proprietary [heavily-tested]() and [extensively-benchmarked]() implementations of UKV to our customers, but even the provided FOSS reference design should be better than whatever OLTP DBMS you are using today.

> Why not use LevelDB or RocksDB interface directly? [link]()

---

## Modularity

The C Standard is just [a few header files][c-standard].
The rest of the project implements it using some of the best FOSS solutions, resulting in this vast map of possible combinations.

![UKV: Full Map](assets/charts/Modularity.png)

<p align="center">
ACID transactions across many collections • Snapshots • Operation-level WATCHes • BSON, JSON, MessagePack documents support • RFC JSON Patches & Merge-Patches • JSON Pointers Addressing • Native Apache Arrow format support in all APIs • Apache Arrow Flight RPC Server • Bulk Scans • Random Samping • Pandas Tabular API • NetworkX Graph API • PyTorch & TensorFlow Data-Loaders
</p>

---

## One Data Lake to Serve Them All

Before going into the specifics of every Frontend, Backend, Modality, or Distribution, let's imagine a few immediate use cases.

1. Getting a Python, GoLang, or Java wrapper for vanilla RocksDB or LevelDB.
2. Serving them over a network via Apache Arrow Flight RPC to Spark, Kafka, or PyTorch.
3. Embedded Document and GraphDB that will avoid networking overheads.
4. Seamlessly Tiering Multi-Modal DBMS between UMem and persistent backends.

Even with just a single node, in a 2U chassis in 2022, we can quickly get 24x 16 TB of NVMe storage connected to 2x CPU sockets, totaling 384 TB of space, capable of yielding ~120 GB/s of read throughput, out of which, ~70 GB/s our in-house engine can already sustain.
With NVMe-oF, this can scale horizontally to Petabytes of low-latency storage served through 200 GBit/s Mellanox Infiniband fibers!
Combining it with the numerous features above and GPU acceleration, one can get an all-one Data Lake with the feel of Pandas, Rapids speed, Hadoop scale, and Postgres consistency.

---

## OLAP + OLTP = HTAP: 🐦🐦 + 🪨 = ☠️☠️

It is normal these days to have hundreds of Databases for one project.
At least one for every kind of workload.

1. A Postgres for transactional data.
2. A MongoDB for collections of flexible-schema documents.
3. A Neo4J for graphs.
4. An S3 storage bucket for BLOBs.

If you were to build a Social Network, you would put the passwords in first, the user-generated content in second, relevant interactions in third, and media attachments in fourth.

So when the data is updated, you have to apply changes across all those instances, manually rolling them back if one of the parts fails.
Every system has a different API, different guarantees, and runtime constraints.
Already sounds like too many wasted engineering hours.

![UKV: HTAP](assets/charts/HTAP.png)

As it is not one store, different teams work on different versions of data.
Some of those datasets can quickly become irrelevant, especially in:

* Fraud Detection, where parties constantly adapt,
* Recommender Systems, if new products and content appear every day,
* Real-Time Pricing, where market conditions dictate the cost of services.

By the time row-major OLTP data is exported into the column-major OLAP store, it might be too late.
Every data scientist knows - "Garbage In, Means Garbage Out".
Outdated input will give you an obsolete result, and the business will lose money.

But if you have just 1 Hybrid Store, the pain is gone.
And the engineering teams can spend time doing something productive rather than packaging and versioning endless Parquet files around your system.

---

## Backend = Modalities + Engine + Distribution

A backend is a composition of just 2-3 parts.
An Engine, being a key-value store for the serialized representation.
An implementation of Modalities, being various serialization and indexing approaches for structured data.
And a Distribution form, such as the implementation of some web-protocol for communication with the outside world.

![UKV: Backend](assets/charts/Backend.png)

### Engines

Following engines can be used almost interchangeably.

|                   | LevelDB | RocksDB  |  UDisk  |  UMem   |
| :---------------- | :-----: | :------: | :-----: | :-----: |
| **Speed**         |   1x    |    2x    | **10x** | **30x** |
| **Persistent**    |    ✅    |    ✅     |    ✅    |    ❌    |
| **Transactional** |    ❌    |    ✅     |    ✅    |    ✅    |
| [Watches][watch]  |    ❌    |    ✅     |    ✅    |    ✅    |
| [Snapshots][snap] |    ✅    |    ✅     |    ✅    |    ❌    |
| Named Collections |    ❌    |    ✅     |    ✅    |    ✅    |
| Random Sampling   |    ❌    |    ❌     |    ✅    |    ✅    |
| Bulk Enumeration  |    ❌    |    ❌     |    ✅    |    ✅    |
| Encryption        |    ❌    |    ❌     |    ✅    |    ❌    |
| Open-Source       |    ✅    |    ✅     |    ❌    |    ✅    |
| Compatibility     |   Any   |   Any    |  Linux  |   Any   |
| Maintainer        | Google  | Facebook |  Unum   |  Unum   |

Historically, LevelDB was the first one.
RocksDB then improved on functionality and performance.
Now it serves as the foundation for half of the DBMS startups.

UMem and UDisk are both designed and maintained by Unum from scratch.
Both are feature-complete, but the most crucial feature our infrastructure provides is performance.
Being fast in memory is easy.
The core logic of UMem can be found in the templated header-only [`consistent_set`][consistent_set] library.

Designing UDisk was much more challenging and required partial kernel bypass with `io_uring`, complete bypass with `SPDK`, and GPU acceleration. 
**UDisk is the first engine to be designed with parallel architectures in mind**.

![UCSB 10 TB Results](https://unum.cloud/assets/post/2022-09-13-ucsb-10tb/ucsb-10tb-duration.png)

All of those backends were [benchmarked for weeks](https://unum.cloud/ucsb) using [UCSB](https://github.com/unum-cloud/ucsb), so you can choose the best stack for you specific use case.
We have published the results for BLOB-layer abstractions for [10 TB][ucsb-10], and, previously, [1 TB][ucsb-1] collections.
Above binary layer, in logic, those numbers are further multiplied.
Where MongoDB does 2'000 operations/s, our Community Edition does 10'000 ops/s and the Pro Version yields 50'000 ops/s.

> Read more about benchmarks [here](#benchmarks).

### Modalities

The same DBMS can contain multiple collections.
Each collection can store BLOBs or any modality of structured data.
Data of different modalities can't be stored in the same collection.
ACID transactions across modalities are supported.

|                           |                     Documents                      |                 Graphs                 |                       Vectors                        |
| :------------------------ | :------------------------------------------------: | :------------------------------------: | :--------------------------------------------------: |
| Values                    |           JSON-like Hierarchical Objects           |       Labeled Directed Relations       |             High-Dimensional Embeddings              |
| Specialized Functionality | JSON ⇔ BSON ⇔ MessagePack, Sub-Document Operations | Gather Neighbors, Count Vertex Degrees | Quantization, K-Approximate Nearest-Neighbors Search |
| Examples                  |                      MongoDB                       |                 Neo4J                  |                       Pinecone                       |

One of our core objectives was to select the minimal core set of functions for each modality.
In that case, implementing them can be easy for any passionate developer.
If the low-level interfaces are flexible, making the high-level interfaces rich is easy.

## Frontend = SDK ∨ API

UKV for Python and for C++ look very different.
Our Python SDK mimics other Python libraries - Pandas and NetworkX.
Similarly, C++ library provides the interface C++ developers expect.

![UKV: Frontends](assets/charts/Frontend.png)

As we know people use different languages for different purposes.
Some C-level functionality isn't implemented for some languages.
Either because there was no demand for it, or as we haven't gottent to it yet.


| Name      | Transact | Collections | Batches | Docs  | Graphs | Copies |
| :-------- | :------: | :---------: | :-----: | :---: | :----: | :----: |
| C         |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |   0    |
| C++       |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |   0    |
| Python    |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |  0-1   |
| GoLang    |    ✅     |      ✅      |    ✅    |   ❌   |   ❌    |   1    |
| Java      |    ✅     |      ✅      |    ❌    |   ❌   |   ❌    |   1    |
| Arrow RPC |    ✅     |      ✅      |    ✅    |   ✅   |   ✅    |  1-2   |

Some APIs here by themself are a gem and give you essentially unlimited compatibility with all kinds of tools and languages.

![UKV: Frontends](assets/charts/Arrow.png)

Arrow, for instance, brings an entire ecosystem with support for  C, C++, C#, Go, Java, JavaScript, Julia, MATLAB, Python, R, Ruby and Rust.

---

## Documentation

For guidance on installation, development, deployment, and administration, see our [documentation](https://unum.cloud/UKV).

## Installation

The entire DBMS fits into a sub 100 MB Docker image.
Run the following script to pull and run the container, exposing Apache Arrow Flight RPC server on the port `38709`.
Client SDKs will also communicate through that same port.

```sh
docker run --rm --name TestUKV -p 38709:38709 unum/ukv
```

For C/C++ clients and for the embedded distribution of UKV, CMake is the default form of installation.
It may require installing Arrow separately.

```cmake
FetchContent_Declare(
    ukv
    GIT_REPOSITORY https://github.com/unum-cloud/UKV.git
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(ukv)
set(ukv_INCLUDE_DIR ${ukv_SOURCE_DIR}/include)
include_directories(${ukv_INCLUDE_DIR})
```

For Conan users a shorter alternative for C/C++ is available:

```sh
conan install ukv
```

For Python users, it is the classical:

```sh
pip install ukv
```

## Getting Started

* Using C SDK
* Using C++ SDK
* Using Python SDK
* Using Java SDK
* Using GoLang SDK
* Using Arrow Flight RPC API

## Testing

We split tests into 4 categories:

1. Compilation: Validate meta-programming.
2. API: Prevent passing incompatible function arguments.
3. Unit: Short and cover most of the functionality.
4. **Stress**: Very long and multithreaded.

The latter can run for days and simulate millions of concurrent transactions, ensuring the data remains intact.

All unit tests are packed into a single executable to simplify running it during development.
Every backend produces one such executable.
The in-memory embedded variant is generally used for debugging any non-engine level logic.

We have a [separate Documentation page here](htts://unum.cloud/UKV/tests/) covering the implemented tests.
Any additions, especially to the stress tests, will be highly welcomed!

## Benchmarks

It is always best to implement an application-specific benchmark, as every use case is different.
Still, for the binary layer logic, we have built a dedicated project to evaluate persistent data structures - [UCSB][ucsb].
It doesn't depend on UKV and uses native interfaces of all the engines to put everyone in the same shoes.

For more advanced modality-specific workloads, we have the following benchmarks provided in this repo:

* **Twitter**. It takes the `.ndjson` dump of their [`GET statuses/sample` API][twitter-samples] and imports it into the Documents collection. We then measure random-gathers' speed at document-level, field-level, and multi-field tabular exports. We also construct a graph from the same data in a separate collection. And evaluate Graph construction time and traversals from random starting points.
* **Tabular**. Similar to the previous benchmark, but generalizes it to arbitrary datasets with some additional context. It supports Parquet and CSV input files.
* **Vector**. Given a memory-mapped file with a big matrix, builds an Approximate Nearest Neighbors Search index from the rows of that matrix. Evaluates both construction and query time.

We are working hard to prepare a comprehensive overview of different parts of UKV compared to industry-standard tools.
On both our hardware and most common instances across public clouds.
All of them are forming a [separate Documentation page here](htts://unum.cloud/UKV/benchmarks/).

## Tooling

We are preparing additional tools to simplify the DBMS management:

* Bulk dataset imports and exports.
* Backups and replication.
* Visualization tools and dashboards.

The development of those tools will be covered on a [separate Documentation page here](htts://unum.cloud/UKV/tools/).

## Development & Licensing


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
[c-standard]: https://github.com/unum-cloud/UKV/tree/main/include/ukv
[twitter-samples]: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/sample-realtime/overview