# Universal Keys & Values

⚠️ Under active development! Not all APIs are stable!

## The BLAS of CRUD

![Universal Key Values by Unum](assets/UKV.png)

Imagine having a standardized cross-lingual interface for all your things "Data":

* Storing binary blobs
* Building up graphs & indexes
* Querying structured documents
* [ACID](https://en.wikipedia.org/wiki/ACID) transactions across tables, docs & graphs via one API!
* [Apache Arrow](https://arrow.apache.org/) interop and [Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
* Familiar high-level [drivers](#frontends) for tabular & graph analytics
* Handling JSON, [BSON](https://www.mongodb.com/json-and-bson), [MsgPack](https://msgpack.org/index.html)
* [JSON-Pointers](https://datatracker.ietf.org/doc/html/rfc6901) & [Field-level Patches](https://datatracker.ietf.org/doc/html/rfc6902), no custom Query Languages
* Packing Tensors for [PyTorch](https://pytorch.org/) and [TensorFlow](tensorflow.org)

UKV does just that, abstracting away the implementation from the user.
In under 20K LOC you get a reference implementation in C++, support for any classical backend, and bindings for [Python](#python), [GoLang](#golang), [Java](#java).
You can combine every [engine](#engines) with every modality, [frontend](#frontends) and distribution form:

| Engine  | Modality | Distribution                    | Frontend                        |
| :------ | :------- | :------------------------------ | :------------------------------ |
|         |          |                                 |                                 |
| RAM     | Blobs    | Embedded                        | C and C++                       |
| LevelDB | Docs     | Standalone                      | Python                          |
| RocksDB | Graphs   | Distributed <sup>*coming*</sup> | GoLang <sup>*in-progress*</sup> |
| UnumKV  |          |                                 | Java <sup>*in-progress*</sup>   |

This would produce hundreds of binaries for all kinds of use cases, like:

* Python, GoLang, Java and other high-level bindings for [RocksDB](rocksdb.org) and [LevelDB](https://github.com/google/leveldb).
* Performant embedded store in the foundation of your in-house storage solution.
* Document store, that is simpler and faster than putting JSONs in MongoDB or Postgres.
* Graph database, with the feel of [NetworkX](https://networkx.org), ~~soon~~ speed of [GunRock](http://gunrock.github.io) and scale of [Hadoop](https://hadoop.apache.org).
* Low-latency media storage for games, CDNs and ML/BI pipelines.

But more importantly, if you choose backends that support transactions and collections, you can get an all-in one solution:

![UKV Monolithic Data-lake](assets/UKV_Combo.png)

It is normal to have a separate Postgres for your transactional data, a MongoDB for your large flexible-schema document collections, a Neo4J instance for your graphs, and an S3 storage bucket for your media data, all serving the different data needs of a single business.

> Example: a social network, storing passwords in Postgres, posts in MongoDB, user relations in Neo4J and post attachments in S3.

So when the data is updated, you have to apply changes across all those instances, manually rolling them back if one of the parts failed.
Needless to say, every system has a different API, different guarantees, and runtime constraints.
UKV provides something far more uniform, simple, and performant *with the right backend*.
When picking the UnumKV backend, we bring our entire IO stack, bypassing the Linux kernel for storage and networking operations.
This yields speedups not just for small-ish OLTP and mid-size OLAP, but even streaming-out Gigabyte-sized videos.
**One ~~ring~~ data-lake to rule them all.**

## Engines

Backends differ in their functionality and purposes.
The underlying embedded key value stores include:

| Name    |  Speed   |       OS        | Transact | Collections | Persistent | [Snapshots][2] | [Watches][1] |
| :------ | :------: | :-------------: | :------: | :---------: | :--------: | :------------: | :----------: |
| RAM     | **10x**  | POSIX + Windows |    ✅     |      ✅      |     ❌      |       ❌        |      ✅       |
| LevelDB |   0.5x   | POSIX + Windows |    ❌     |      ❌      |     ✅      |       ❌        |      ❌       |
| RocksDB |    1x    | POSIX + Windows |    ✅     |      ✅      |     ✅      |       ✅        |      ✅       |
| UnumKV  | **3-5x** |      Linux      |    ✅     |      ✅      |     ✅      |       ✅        |      ✅       |


* RAM in-memory backend was originally served educational purposes. Then it was superseeded by the [`consistent_set`][consistent_set] and can now be considered the fastest in-memory Key-Value Store, superior to Redis, MemCached or ETCD.
* LevelDB was originally designed at Google and extensively used across the industry, thanks to its simplicity.
* RocksDB improves over LevelDB, extending its functionality with transactions, named collections, and higher performance.
* UnumKV is our proprietary in-house implementation with superior concurrency and kernel-bypass techniques, as well as, GPU acceleration.

All of those backends were [benchmarked for weeks](https://unum.cloud/ucsb) using [UCSB](https://github.com/unum-cloud/ucsb), so you can choose the best stack for you specific use case.

![UCSB 10 TB Results](https://unum.cloud/assets/post/2022-09-13-ucsb-10tb/ucsb-10tb-duration.png)

[1]: https://redis.io/commands/watch/
[2]: https://github.com/facebook/rocksdb/wiki/Snapshot
[acid]: https://en.wikipedia.org/wiki/ACID
[consistent_set]: https://github.com/ashvardanian/consistent_set

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

But why even bother mixing all of it in one DBMS?

There are too extremes these days: consistency and scalability, especially when working with heavily linked flexible schema data.
The consistent camp would take a tabular/relational DBMS and add a JSON column and additional columns for every relationship they want to maintain.
The others would take 2 different DBMS solutions - one for large collections of entries and one for the links between them, often - MongoDB and Neo4J.
In that case, every DBMS will have a custom modality-specific scaling, sharding, and replication strategy, but synchronizing them would be impossible in mutable conditions.
This makes it hard for the developers to choose a future-proof solution for their projects.
By putting different modality collections in one DBMS, we allow operation-level consistency controls giving the users all the flexibility one can get.

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
