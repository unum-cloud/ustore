# Prebuilt Open-Source UKV Implementations

## Development

### Deduplicate, Gather, Join, Scatter

In most modalities we may receive batches of requests, where distinct queries map into the same "key-value pair" entries.
Examples:

* In Docs: For the same document update 2 distinct internal fields.
* 

To be efficient on reads and consistent on writes, we must deduplicate the keys to only query them once.

In that case, the trivial

* "gather+scatter" operation gets two more stages: deduplication and join.

