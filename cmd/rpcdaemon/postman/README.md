# Postman testing

There are three files here:

- RPC_Testing.json
- Trace_Testing.json
- Overlay_Testing.json

You can import them into Postman using these
instructions: https://github.com/erigontech/erigon/wiki/Using-Postman-to-Test-TurboGeth-RPC

The first one is used to generate help text and other documentation as well as running a sanity check against a new
release. There is basically one test for each of the 81 RPC endpoints.

The second file contains 31 test cases specifically for the nine trace routines (five tests for five of the routines,
three for another, one each for the other three).

The third file contains 12 test cases for the overlay API for CREATE and CREATE2.

Another collection of related tests can be found
here: https://github.com/TrueBlocks/trueblocks-core/tree/1dd55cf1028837f5611b5b389a3272f776567d5d/src/other/trace_tests
