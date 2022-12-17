# Testintg UKV

## Unit Tests

Unit tests are small and generally challenge just one functionality.
Unlike [Stress Tests](#stress-tests), they should be nimble and take only a few seconds to complete, as they are triggered on every single commit.
If you are using VSCode, as we do, the `.vscode` top-level directory already comes with preconfigured launchers for debugging.

Primary unit tests are in one file - [`unit.cpp`](https://github.com/unum-cloud/UKV/blob/main/tests/unit.cpp).
Those same tests are used for both embedded and standalone DBMS across all Engines.
You can find a complete list of unit tests [on our website here](https://unum.cloud/UKV/tests/unti.html), and you are welcome to contribute.

Here are a few suggestions for implementing unit-tests:

* **Minimalism** is key!
* Remember that `char` easily upcasts to `ukv_key_t`, so prefer single-character keys.
* No test should affect other tests! So if you are testing a persistent DBMS, `.clear()` it in the end.
* If C++ is limiting, remember that you can always use the C standard directly.

## Stress Tests

Stress tests are designed to run infinitely, mostly on RAM-disks, doing thousands of transactions concurrently and checking if any corner cases or concurrency and consistency bugs pop up.

## Integration Tests

Integration tests are mostly implemented in Python.