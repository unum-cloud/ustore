# About Related Projects

## Why not adapt SQL, MQL or Cypher?

Those interfaces imply a lot of higher-level logic, that might not need to
be concern of the Key-Value Store. Furthermore, using text-based protocols
is error-prone and highly inefficient from serialization and parsing standpoint.
It might be fine for OLAP requsts being called once a second, but if the function
is called every microsecond, the interface must be binary.

For those few places where such functionality can be implemented efficiently we follow
standardized community-drived RFCs, rather than proprietary languages.
As such, for sub-document level gathers and updates we use:

* JSON Pointer: RFC 6901
* JSON Patch: RFC 6902
* JSON MergePatch: RFC 7386

## Why not use LevelDB or RocksDB interface?

* Dynamic polymorphism and multiple inheritance is a mess.
* Dependance on Standard Templates Library containers, can't bring your strings or trees.
* No support for **custom allocators**, inclusing statefull allocators and arenas.
* Almost every function call can through exceptions.
* All keys are strings. [Why is it bad?][]

These and other problems mean that interface can't be portable, ABI-safe or performant.

## How is this different from X?

|                       | [EJDB][ejdb] | [SurrealDB][surreal] | [ArangoDB][arango] |  UKV  | Explanation                                                                                                        |
| :-------------------- | :----------: | :------------------: | :----------------: | :---: | :----------------------------------------------------------------------------------------------------------------- |
| Multi-Modal           |      ?       |          ?           |         ?          |   ✅   | Capable of storing Docs, Graphs and more in a single DBMS                                                          |
| Has a C Layer         |      ?       |          ?           |         ?          |   ✅   | Essential for compatibility                                                                                        |
| Is Fast               |      ?       |          ?           |         ?          |   ✅   |
| High-level bindings   |      ?       |          ?           |         ?          |   ✅   | Can you call it from Python, JS, Java, GoLang or some other high-level language?                                   |
| Structured bindings   |      ?       |          ?           |         ?          |   ✅   | Are bindings capable of anything beyond passing strings to the server?                                             |
| Modular backends      |      ?       |          ?           |         ?          |   ✅   | Can you replace the DBMS without rewriting the application code?                                                   |
| Supports Transactions |      ?       |          ?           |         ?          |   ✅   | Can you compose complex consistent logic, like Read-Modify Writes or are you bound to the provided function calls? |
| Batch Operations      |      ?       |          ?           |         ?          |   ✅   | Can you read or write many entries at once? Together with Range-Scans and Sampling it is the                       |

[ejdb]: https://github.com/Softmotions/ejdb
[surreal]: https://github.com/surrealdb/surrealdb
[arango]: https://github.com/arango/arangodb

