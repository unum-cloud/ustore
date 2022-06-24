# Universal Key-Values

Universal Key-Value store interface for both in-memory and persistent ACID transactional collections written in C/C++ and Assembly with bindings for [Python](#python), [GoLang](#golang), [Java](#java), JavaScript, C#.

```mermaid
flowchart LR
  
  ukv(((UKV)))

  id1[In-Memory Store using STL] --> ukv;
  id3[Persistent Store using RocksDB] --> ukv;
  id2[In-Memory Store by Unum] --> ukv;
  id4[Persistent Store by Unum] --> ukv;
  %% id5[In-Memory Distributed Store by Unum] --> ukv;
  %% id6[Persistent Distributed Store by Unum] --> ukv;
  
  %% id2 -.-> id5 
  %% id4 -.-> id6 
  
  ukv --> Python;
  ukv --> GoLang;
  ukv --> Java;
  ukv --> REST;
  
  %% ukv --> SQL;
  %% ukv ---> Redis;
  %% ukv ---> Lucene;
  %% ukv ---> MongoDB;
  
  style id2 stroke:#743cce,stroke-width:2px;
  style id4 stroke:#743cce,stroke-width:2px;
```

Assumptions and limitations:

* Keys are constant length native integer types. High-performance solutions are impossible with variable size keys. 64-bit unsigned integers are currently chosen as the smallest native numeric type, that can address modern datasets.
* Values are serialized into variable-length byte strings.
* Iterators and enumerators often come with certain relevance, consistency or performance tradeoffs or aren't supported at all. Check the specs of exact backend.
* Transactions are ACI(D) by-default, meaning that:
  * Atomicity is guaranteed,
  * Consistency is implemented in the strongest form - tracking all key and metadata lookups by default,
  * Isolation is guaranteed, but may be implemented differently, depending on backend - in-memory systems generally prefer "locking" over "multi-versioning".
  * Durability doesn't apply to in-memory systems, but even in persistent stores its often disabled to be implemented in higher layers of infrastructure.

## Development

To build and test any set of bindings:

1. Build (`cmake . && make`) or download the prebuilt `libukv.a`,
2. Call `./language/run.sh` in your terminal.

### Python

Current impleentation relies on [PyBind11](https://github.com/pybind/pybind11).
It's feature-rich, but not very performant, supporting:

* Named Collections
* Transactions
* Single & *Batch* Operations
* *Tensor Exports*

Using it can be as easy as:

```python
import ukv

db = ukv.DataBase()
db[42] = 'purpose of life'.encode()
db['sub-collection'][0] = db[42]
del db[42]
assert len(db['sub-collection'][0]) == 15
```

All familiar Pythonic stuff!

### Java

These bindings are impemented via [Java Native Interface](https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/jniTOC.html).
This interface is more performant than Python, but is feature complete yet.
It mimics native `HashMap` and `Ditionary` classes, but has no support for batch operations yet.

```java
DataBase db = new DataBase("");
db.put(42, "purpose of life".getBytes());
assert db.get(42) == "purpose of life".getBytes() : "Big surprise";
db.close();
```

All `get` requests cause memory allocations in Java Runtime and export data into native Java types.
Most `set` requests will simply cast and forward values without additional copies.
Aside from opening and closing this class is **thread-safe** for higher interop with other Java-based tools.

Implementation follows the ["best practices" defined by IBM](https://developer.ibm.com/articles/j-jni/).

### GoLang

GoLang bindings are implemented using [cGo](https://pkg.go.dev/cmd/cgo).
The language lacks operator and function overloads, so we can't mimic native collections.
Instead we mimic the interfaces of most commonly used ORMs.

```go
db := DataBase{}
db.Reconnect("")
db.Set(42, &[]byte{4, 2})
db.Get(42)
```

Implementation-wise, GoLang variant performs `memcpy`s on essentially every call.
As GoLang has no exceptions in the classical OOP sense, most functions return multiple values, error being the last one in each pack.
Batch lookup operations are imlemented via channels sending slices, to avoid reallocations.

## TODOs

* [ ] gRPC server and client in C++
* [ ] GoLang memory pinning Channel Batch Reads of [slices](https://stackoverflow.com/a/36546929)
* [ ] Swift Bindings
* [ ] Rust Bindings
* [ ] Scala Bindings
* [ ] Wolfram Language Bindings
* [ ] Read/Write Apache Arrow Tables via C API
* [ ] Java Apache Arrow support
