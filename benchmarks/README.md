# UKV Benchmarks

## UCSB

For low-level binary workloads, we have a separate project called UCSB.
It covers LMDB, LevelDB, RocksDB, WiredTiger, Redis, MongoDB, and our Key-Value Stores.

## Twitter

Twitter benchmark operated on real-world sample of Tweets obtained via Twitter Stream API.
It is frequently used across the industry, but the current "User Agreement" bans publicly sharing such datasets.

We took over 1 TB of tweets packed consecutively into `.ndjson` files and simulated constructing collections of Documents, Graphs, and Paths.
You can obtain results for your hardware and your sample of Tweets using the following command.

```sh
cmake -DCMAKE_BUILD_TYPE=Release . && make ukv_umem_twitter_benchmark && ./build/bin/ukv_umem_twitter_benchmark
```

We manually repeated this same benchmark for a few other DBMS brands.
We tried to keep the RAM usage identical and limited the number of clients to 16 processes on the same machine on which the server was running.
The following results should be taken with the grain of salt and aren't as accurate as [UCSB](#ucsb) but they might be relevant for general perception.

For document-like workloads:

|          | Insertion   |  Retrieval  |
| -------- | ----------- | :---------: |
| MongoDB  | 18 K ops/s  | 210 K ops/s |
| Yugabyte | 2 K ops/s   | 82 K ops/s  |
|          |             |             |
| UMem     | 157 K ops/s |  2 M ops/s  |
| RocksDB  | 15 K ops/s  | 140 K ops/s |
| UDisk    | 42 K ops/s  |  1 M ops/s  |

For graphs:

|       | Insertion  | Two-hop Retrieval |
| ----- | ---------- | :---------------: |
| Neo4J | 2 K ops/s  |    1.5 K ops/s    |
|       |            |                   |
| UDisk | 37 K ops/s |    52 K ops/s     |

For MongoDB, we also tried the official <code class="docutils literal notranslate"><a href="https://www.mongodb.com/docs/database-tools/mongoimport/" class="pre">mongoimport</a></code> tool, which supports both `.csv` and `.ndjson`.
The results are mixed compared to a multi-process setup.
For Neo4J, significantly better results are possible if you are doing initialization with a pre-processed `.csv` file, but the same applies to our solutions.

## Tables and Graphs

Generalizing the Twitter benchmark, we wrote a Python and a C++ benchmark for tabular datasets.
Unlike the previous benchmark, the schema must be fixed, but you can input `.json`, `.csv`, and `.parquet` files.

```sh
cmake -DCMAKE_BUILD_TYPE=Release . && make ukv_umem_tabular_graph_benchmark && ./build/bin/ukv_umem_tabular_graph_benchmark
```

For Python:

```sh
python benchmarks/tabular_graph_benchmark.py
```

We manually repeated this same benchmark for several other DBMS brands and multiple publicly available datasets.
Below are the results.

### Reconstructing the Bitcoin Graph


### Friendster Adjacency List

We take a real-world graph dataset, distributed in `.csv` form - the "Friendster" social network.
It contains:

* 8'254'696 vertices.
* 1'847'117'371 edges.
* ~225 edges per vertex.

With Neo4J, imports took 3h 45m, averaging at:

* 136'449 edges/s.
* 5.3 MB/s.