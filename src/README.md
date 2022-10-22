\page cpp_readme
# UKV: Open Source Reference Implementation

UKV is an open standard.
Anyone can contribute to the interface, or suggest an implementation.
Unum implements implementations:

* Fully Open Source Reference Implementation
* Proprietary Hardware-Accelerated Implementation

Even the reference design is expected to be faster than the DBMS you are using today.
Below are somewhat more detailed descriptions of the public backens we maintain.


## Modalities


## Embedded Implementations


## Standalone Servers

### Arrow Flight RPC Server & Client

We currently use Apache Arrow Flight RPC as the primary client-server communication protocol due to its extensive support across the compute ecosystem.
This makes it easy for the external frameworks to send and gather info from UKV and underlying databases even without explicitly implementing UKV function calls.

### RESTful API

We implement a REST server using `Boost.Beast` and the underlying `Boost.Asio`, as the go-to Web-Dev libraries in C++.
To test the REST API, `./src/run_rest.sh` and then cURL into it:

```sh
curl -X PUT \
  -H "Accept: Application/json" \
  -H "Content-Type: application/octet-stream" \
  0.0.0.0/8080/one/42?col=sub \
  -d 'purpose of life'

curl -i \
  -H "Accept: application/octet-stream" \
  0.0.0.0/8080/one/42?col=sub
```

The [`OneAPI` specification](/openapi.yaml) documentation is in-development.

## Shared Code


### Deduplicate, Gather, Join, Scatter

In most modalities we may receive batches of requests, where distinct queries map into the same "key-value pair" entries.
Examples:

* In Docs: For the same document update 2 distinct internal fields.
* 

To be efficient on reads and consistent on writes, we must deduplicate the keys to only query them once.

In that case, the trivial

* "gather+scatter" operation gets two more stages: deduplication and join.

