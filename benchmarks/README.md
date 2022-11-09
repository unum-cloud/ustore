# UKV Benchmarks

## Twitter

Operates on collections of `.ndjson` files gathered from Twitter Stream API.
Those were conducted on 1.2 TB collected of Tweets augmented to form a 10 TB dataset.

```sh
cmake -DCMAKE_CXX_COMPILER=g++ -DCMAKE_C_COMPILER=gcc -DCMAKE_BUILD_TYPE=Release . && make ukv_umem_twitter_benchmark && ./build/bin/ukv_umem_twitter_benchmark
```

Our baseline will be MongoDB.
The <code class="docutils literal notranslate"><a href="https://www.mongodb.com/docs/database-tools/mongoimport/" class="pre">mongoimport</a></code> official tool supports both `.csv` and `.ndjson` imports and typical performance will be as follows:

|         | Tweets    | Imports                     |        Retrieval        | Sampling |
| ------- | --------- | --------------------------- | :---------------------: | :------: |
| MongoDB | 1'048'576 | 9 K docs/s ~ **32 MB/s**    |                         |          |
|         |           |                             |                         |          |
| UMem    | 1'048'576 | 157 K docs/s ~ **850 MB/s** |                         |          |
| LevelDB | 1'048'576 |                             |                         |          |
| RocksDB | 1'048'576 | 15 K docs/s ~ **80 MB/s**   | 140 K docs/s ~ 750 MB/s |          |
| UnumDB  | 1'048'576 |                             |                         |          |

Even with provided tooling it generally performs around 10'000 insertions per second and won't surpass 100 MB/s.


## Adjacency List

Here we start with Neo4J, which also has an official tool for `.csv` import.
We take a real-world graph dataset, distributed in `.csv` form - the "Friendster" social network.
It contains:

* 8'254'696 vertices.
* 1'847'117'371 edges.
* ~225 edges per vertex.

The import too 3h 45m, averaging at:

* 136'449 edges/s.
* 5.3 MB/s.

Comparing that with 

## Bitcoin Graph


### Benchmarking on Twitter JSONs

Ingestion speed:

* MongoDB: 2'000 tweets/s.
* MongoDB with `mongoimport`: 10'000 tweets/s.
* UKV: FOSS RocksDB + FOSS JSON modality: 11'000 tweets/s.
* UKV: proprietary UnumKV + FOSS JSON modality: 42'000 tweets/s.
* UKV: proprietary UnumKV + proprietary JSON modality: 60'000 tweets/s.

All of UKV interfaces look same, but work different.
It is a modular system you can assemble the way you like!

Gathering Speeds:

* MongoDB: 2'000 tweets/s.
* MongoDB with `mongoimport`: 10'000 tweets/s.
* UKV: FOSS RocksDB + FOSS JSON modality: 11'000 tweets/s.
* UKV: proprietary UnumKV + FOSS JSON modality: 42'000 tweets/s.
* UKV: proprietary UnumKV + proprietary JSON modality: 60'000 tweets/s.

Aside from lookups and gathers, we support random sampling for Machine Learning applications.

### Benchmarking on Bitcoin Graph

Ingestion speed:

* Neo4J:
* ArangoDB:
* TigerGraph:
* UKV: FOSS RocksDB + FOSS Graph modality: 
* UKV: proprietary UnumKV + FOSS Graph modality: 
* UKV: proprietary UnumKV + proprietary Graph modality: 

Gathering Speeds:

* Neo4J:
* ArangoDB:
* TigerGraph:
* UKV: FOSS RocksDB + FOSS Graph modality: 
* UKV: proprietary UnumKV + FOSS Graph modality: 
* UKV: proprietary UnumKV + proprietary Graph modality: 