<h1 align="center">Universal Keys & Values</h1>
<h3 align="center">
Binary Interface Standard for Database Management<br/>
Backed by Open-Source and Proprietary Implementations<br/>
</h3>

<div align="center">
<b>built on</b>:
<a href="https://github.com/facebook/rocksdb">RocksDB</a>
•
<a href="https://github.com/google/leveldb">LevelDB</a>
•
UDisk
•
<a href="https://github.com/ashvardanian/consistent_set">UMem</a>
<br/>
<b>to store</b>:
Blobs
•
Documents
•
Graphs
•
Vectors
•
Texts
<br/>
<b>accessible from</b>:
C
•
C++
•
Python
•
Java
•
GoLang
•
<a href="https://arrow.apache.org/">Apache Arrow</a>
<br/>
<b>available on</b>:
<a href="#installation">CMake</a>
•
<a href="https://pypi.org/project/ukv/">PyPI</a>
•
<a href="https://hub.docker.com/repository/docker/unum/ukv">Docker Hub</a>
</div>

---

**UKV is an open standard** for a NoSQL binary database interface, focusing on "Create, Read, Update, Delete" operations, or CRUD for short.
It is defined as a [few isolated C headers][ukv-c-headers], and comes with numerous backend [**engines**](#engines), supporting various data [**modalities**](#modalities), and an ecosystem of [**frontends**](#frontends) to access the data.

![UKV: Small Map](assets/charts/Intro.png)

It can easily be expanded to support alternative underlying Key-Value Stores (KVS), embedded, standalone, or sharded, such as [Redis][redis].
In the same way, you can add SDKs for other high-level languages or support for data types specific to your application that we haven't considered.
This gives you the flexibility to iterate on different parts of this modular data lake without changing the business logic of your application, decoupling it from the underlying storage technology.

![UCSB 10 TB Results](assets/charts/Performance.png)

**Flexibility is essential, but so is performance**.
Even our open-source baseline implementations of Document and Graph modalities often outperform classical commercial DBMS products.
The proprietary version obliterates them.
Modern persistent IO on high-end servers can exceed 120 GB/s per socket when built on user-space drivers like [SPDK][spdk].
To harness that power, unlike LevelDB and RocksDB, we had to design an interface that avoids dynamic polymorphism and has no constraints on memory allocation strategies and used containers.
That is how UKV began.

Half of the modern data storage systems are built on RocksDB.
If they were built on UKV, they could have become 5x faster by changing a single config file.

---

- [Basic Use Cases](#basic-use-cases)
- [Features](#features)
- [Available Backends](#available-backends)
  - [Engines](#engines)
  - [Modalities](#modalities)
- [Available Frontends](#available-frontends)
- [Documentation](#documentation)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Tooling](#tooling)
- [Development \& Licensing](#development--licensing)
- [Presets, Limitations and FAQ](#presets-limitations-and-faq)

---

<p align="center">
<a href="https://twitter.com/unum_cloud"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?"/></a>
&nbsp;&nbsp;
<a href="https://www.linkedin.com/company/unum-cloud/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?"/></a>
&nbsp;&nbsp;
<a href="https://www.github.com/unum-cloud/"><img src="https://img.shields.io/github/issues-closed-raw/unum-cloud/ukv?"/></a>
&nbsp;&nbsp;
<a href="https://www.github.com/unum-cloud/"><img src="https://img.shields.io/github/stars/unum-cloud/ukv?"/></a>
&nbsp;&nbsp;
<a href="#"><img src="https://img.shields.io/github/workflow/status/unum-cloud/ukv/Build"/></a>
</p>

## Basic Use Cases

1. Getting a Python, GoLang, or Java wrapper for vanilla RocksDB or LevelDB.
2. Serving them via Apache Arrow Flight RPC to Spark, Kafka, or PyTorch.
3. Embedded Document and Graph DB that will avoid networking overheads.
4. Tiering DBMS between in-memory and persistent backends under one API.

Already excited?
Give it a try.
It is as easy as using `dict` in Python.

```python
$ pip install ukv
$ python

>>> import ukv.umem as embedded
>>> db = embedded.DataBase()
>>> db.main[42] = 'Hi'
```

## Features

A Key Value Store is generally an associative container with sub-linear search time.
Every DBMS uses such abstractions for primary keys in each collection and indexes.
But UKV has more to offer.
Especially to Machine Learning practitioners.

<table>
<td style="text-align: center">

<ul>
<li> <b>ACID Transactions</b> across collections  </li>
<li> Persistent <b>Snapshots</b> </li>
<li> Operation-level <b>WATCH</b>-es  </li>
<li> <b>BSON, JSON, MessagePack</b> support  </li>
<li> <b>JSON Patches</b> & Merge-Patches  </li>
<li> <b>JSON Pointers</b> Addressing  </li>
</ul>

</td>
<td style="text-align: center">

<ul>
<li> Native Apache <b>Arrow</b> format support </li>
<li> <b>Arrow Flight RPC</b> server implementation </li>
<li> <b>Bulk Scans</b>, Random <b>Sampling</b> </li>
<li> <b>Pandas</b> Tabular interface </li>
<li> <b>NetworkX</b> Graph interface </li>
<li> <b>PyTorch</b> & <b>TensorFlow</b> Data-Loaders </li>
</ul>
</td>
</table>

## Available Backends

Any backend can be defined by three parameters.
First, the Engine, being a Key-Value Store for the serialized representations.
Second, implementations of Modalities, being various serialization and indexing approaches for structured data.
Third, a Distribution form, such as the implementation of some web-protocol for communication with the outside world.

![UKV: Backend](assets/charts/Backend.png)

### Engines

Following engines can be used almost interchangeably.
Historically, LevelDB was the first one.
RocksDB then improved on functionality and performance.
Now it serves as the foundation for half of the DBMS startups.

|                          | LevelDB | RocksDB  |  UDisk  |  UMem   |
| :----------------------- | :-----: | :------: | :-----: | :-----: |
| **Speed**                |   1x    |    2x    | **10x** | **30x** |
| **Persistent**           |    ✓    |    ✓     |    ✓    |    ✗    |
| **Transactional**        |    ✗    |    ✓     |    ✓    |    ✓    |
| **Block Device Support** |    ✗    |    ✗     |    ✓    |    ✗    |
| Encryption               |    ✗    |    ✗     |    ✓    |    ✗    |
| [Watches][watch]         |    ✗    |    ✓     |    ✓    |    ✓    |
| [Snapshots][snap]        |    ✓    |    ✓     |    ✓    |    ✗    |
| Random Sampling          |    ✗    |    ✗     |    ✓    |    ✓    |
| Bulk Enumeration         |    ✗    |    ✗     |    ✓    |    ✓    |
| Named Collections        |    ✗    |    ✓     |    ✓    |    ✓    |
| Open-Source              |    ✓    |    ✓     |    ✗    |    ✓    |
| Compatibility            |   Any   |   Any    |  Linux  |   Any   |
| Maintainer               | Google  | Facebook |  Unum   |  Unum   |

UMem and UDisk are both designed and maintained by Unum.
Both are feature-complete, but the most crucial feature our alternatives provide is performance.
Being fast in memory is easy.
The core logic of UMem can be found in the templated header-only <code class="docutils literal notranslate"><a href="https://github.com/ashvardanian/consistent_set" class="pre">consistent_set</a></code> library.

Designing UDisk was a much more challenging 7-year long endeavour.
It included inventing new tree-like structures, implementing partial kernel bypass with `io_uring`, complete bypass with `SPDK`, GPU acceleration, and even a custom internal filesystem.
**UDisk is the first engine to be designed from scratch with parallel architectures and kernel-bypass in mind**.

> [Jump to Benchmarks](#benchmarks).

### Modalities

The same DBMS can contain multiple collections.
Each collection can store BLOBs or any modality of structured data.
Data of different modalities can't be stored in the same collection.
ACID transactions across modalities are supported.

|                           |                     Documents                      |                 Graphs                 |                       Vectors                        |
| :------------------------ | :------------------------------------------------: | :------------------------------------: | :--------------------------------------------------: |
| Values                    |           JSON-like Hierarchical Objects           |       Labeled Directed Relations       |             High-Dimensional Embeddings              |
| Specialized Functionality | JSON ⇔ BSON ⇔ MessagePack, Sub-Document Operations | Gather Neighbors, Count Vertex Degrees | Quantization, K-Approximate Nearest-Neighbors Search |
| Examples                  |              MongoDB, Postgres, MySQL              |           Neo4J, TigerGraph            |               Elastic Search, Pinecone               |

One of our core objectives was to select the minimal core set of functions for each modality.
In that case, implementing them can be easy for any passionate developer.
If the low-level interfaces are flexible, making the high-level interfaces rich is easy.

## Available Frontends

UKV for Python and for C++ look very different.
Our Python SDK mimics other Python libraries - [Pandas][pandas] and [NetworkX][networkx].
Similarly, C++ library provides the interface C++ developers expect.

![UKV: Frontends](assets/charts/Frontend.png)

As we know, people use different languages for different purposes.
Some C-level functionality isn't implemented for some languages.
Either because there was no demand for it, or as we haven't gotten to it yet.

| Name             | Transact | Collections | Batches | Docs  | Graphs | Copies |
| :--------------- | :------: | :---------: | :-----: | :---: | :----: | :----: |
| C Standard       |    ✓     |      ✓      |    ✓    |   ✓   |   ✓    |   0    |
|                  |          |             |         |       |        |        |
| C++ SDK          |    ✓     |      ✓      |    ✓    |   ✓   |   ✓    |   0    |
| Python SDK       |    ✓     |      ✓      |    ✓    |   ✓   |   ✓    |  0-1   |
| GoLang SDK       |    ✓     |      ✓      |    ✓    |   ✗   |   ✗    |   1    |
| Java SDK         |    ✓     |      ✓      |    ✗    |   ✗   |   ✗    |   1    |
|                  |          |             |         |       |        |        |
| Arrow Flight API |    ✓     |      ✓      |    ✓    |   ✓   |   ✓    |  0-2   |

Some frontends here have entire ecosystems around them!
[Apache Arrow Flight][flight] API, for instance, has its own bindings for  C, C++, C#, Go, Java, JavaScript, Julia, MATLAB, Python, R, Ruby and Rust.

![UKV: Frontends](assets/charts/Arrow.png)

## Documentation

For guidance on installation, development, deployment, and administration, see our [documentation](https://unum.cloud/UKV).

## Installation

The entire DBMS fits into a sub 100 MB Docker image.
Run the following script to pull and run the container, exposing [Apache Arrow Flight][flight] server on the port `38709`.
Client SDKs will also communicate through that same port, by default.

```sh
docker run --rm --name TestUKV -p 38709:38709 unum/ukv
```

For C/C++ clients and for the embedded distribution of UKV, CMake is the default form of installation.
It may require installing Arrow separately, if you want client-server communication.

```cmake
FetchContent_Declare(
    ukv
    GIT_REPOSITORY https://github.com/unum-cloud/UKV.git
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(ukv)
include_directories(${ukv_SOURCE_DIR}/include)
```

After that, you only need to choose linking target, such as `ukv_embedded_rocksdb`, `ukv_embedded_umem`, `ukv_flight_client`, or something else.

For Python users, it is the classical:

```sh
pip install ukv
```

Which will bring all the libraries packed into a single wheel: `ukv.umem`, `ukv.rocksdb`, `ukv.leveldb`, `ukv.flight_client`.

> [Read full installation guide in our docs here][ukv-install].

## Getting Started

- [Using the C Standard Directly][ukv-c-tutor]
  - Most Flexible!
  - Most Performant!
  - Comparatively verbose.
- [Using C++ SDK][ukv-cpp-tutor]
- [Using Python SDK][ukv-python-tutor]
- [Using Java SDK][ukv-java-tutor]
- [Using GoLang SDK][ukv-golang-tutor]
- [Using Arrow Flight API][ukv-flight-tutor]

## Testing

We split tests into 4 categories:

1. Compilation: Validate meta-programming.
2. API: Prevent passing incompatible function arguments.
3. Unit: Short and cover most of the functionality.
4. **Stress**: Very long and multithreaded.

All unit tests are packed into a single executable to simplify running it during development.
Every backend produces one such executable.
The in-memory embedded variant is generally used for debugging any non-engine level logic.

The stress tests, on the other hand, can run for days and simulate millions of concurrent transactions, ensuring the data remains intact.
Any additions, especially to the stress tests, will be highly welcomed!

> [Read full testing guide in our docs here][ukv-tests].

## Benchmarks

It is always best to implement an application-specific benchmark, as every use case is different.
Still, for the binary layer logic, we have built a dedicated project to evaluate persistent data structures - [UCSB][ucsb].
It doesn't depend on UKV and uses native interfaces of all the engines to put everyone in the same shoes.

All engines were benchmarked for weeks using [UCSB][ucsb].
We have already published the results for BLOB-layer abstractions for [10 TB][ucsb-10], and, previously, [1 TB][ucsb-1] collections.

For more advanced modality-specific workloads, we have the following benchmarks provided in this repo:

- **Twitter**. It takes the `.ndjson` dump of their <code class="docutils literal notranslate"><a href="https://developer.twitter.com/en/docs/twitter-api/v1/tweets/sample-realtime/overview" class="pre">GET statuses/sample</a></code> API and imports it into the Documents collection. We then measure random-gathers' speed at document-level, field-level, and multi-field tabular exports. We also construct a graph from the same data in a separate collection. And evaluate Graph construction time and traversals from random starting points.
- **Tabular**. Similar to the previous benchmark, but generalizes it to arbitrary datasets with some additional context. It supports Parquet and CSV input files. 🔜
- **Vector**. Given a memory-mapped file with a big matrix, builds an Approximate Nearest Neighbors Search index from the rows of that matrix. Evaluates both construction and query time. 🔜

We are working hard to prepare a comprehensive overview of different parts of UKV compared to industry-standard tools.
On both our hardware and most common instances across public clouds.

> [Read full benchmarking guide in our docs here][ukv-benchmarks].

## Tooling

Tools are built on top of the UKV interface and aren't familiar with the underlying backend implementation.
They are meant to simplify DevOps and DBMS management.
Following tools are currently in the works.

- Bulk dataset imports and exports for industry-standard Parquet, NDJSON and CSV files. 🔜
- Rolling backups and replication. 🔜
- Visualization tools and dashboards. 🔜

> [Read full tooling guide in our docs here][ukv-tools].

## Development & Licensing

This is an umbrella project for a number of FOSS libraries.
Sometimes a couple of libraries can be used together for the same task to complement each other functionality.
For example, `simdjson` is used for document validation and sampling, while `yyjson` is used for updates.

A full list of dependencies is available at the link below, together with detailed descriptions of reference implementations.
Licensing depends on which parts you are using.

> [Read full development and contribution guide in our docs here][ukv-details].

## Presets, Limitations and FAQ

- Keys are 64-bit integers, by default. [Why?][ukv-keys-size]
- Values are binary strings under 4 GB long. [Why?][ukv-values-size]
- Transactions are ACI(D) by-default. [What does it mean?][ukv-acid]
- Why not use LevelDB or RocksDB interface? [Answered][ukv-vs-rocks]
- Why not use SQL, MQL or Cypher? [Answered][ukv-vs-sql]

[ukv-c-tutor]: https://unum.cloud/UKV/c
[ukv-cpp-tutor]: https://unum.cloud/UKV/cpp
[ukv-python-tutor]: https://unum.cloud/UKV/python
[ukv-java-tutor]: https://unum.cloud/UKV/java
[ukv-golang-tutor]: https://unum.cloud/UKV/golang
[ukv-flight-tutor]: https://unum.cloud/UKV/flight
[ukv-tests]: https://unum.cloud/UKV/tests
[ukv-benchmarks]: https://unum.cloud/UKV/benchmarks
[ukv-tools]: https://unum.cloud/UKV/tools
[ukv-install]: https://unum.cloud/UKV/install
[ukv-details]: https://unum.cloud/UKV/details
[ukv-keys-size]: https://unum.cloud/UKV/c#integer-keys
[ukv-values-size]: https://unum.cloud/UKV/c#smallish-values
[ukv-acid]: https://unum.cloud/UKV/c#acid-transactions
[ukv-vs-rocks]: https://unum.cloud/UKV/related#leveldb-rocksdb
[ukv-vs-sql]: https://unum.cloud/UKV/related#sql-mql-cypher
[ukv-c-headers]: https://github.com/unum-cloud/UKV/tree/main/include/ukv

[ucsb-10]: https://unum.cloud/post/2022-03-22-ucsb
[ucsb-1]: https://unum.cloud/post/2021-11-25-ycsb
[ucsb]: https://github.com/unum-cloud/ucsb

[acid]: https://en.wikipedia.org/wiki/ACID
[flight]: https://arrow.apache.org/docs/format/Flight.html
[networkx]: https://networkx.org
[pandas]: https://pandas.pydata.org
[watch]: https://redis.io/commands/watch/
[snap]: https://github.com/facebook/rocksdb/wiki/Snapshot
[spdk]: https://spdk.io
[redis]: https://redis.com
