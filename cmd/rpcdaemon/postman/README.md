# Postman testing

There are three files here:

- RPC_Testing.json
- Trace_Testing.json
- Overlay_Testing.json

You can import them into Postman using these
instructions: https://github.com/ledgerwatch/erigon/wiki/Using-Postman-to-Test-TurboGeth-RPC

The first one is used to generate help text and other documentation as well as running a sanity check against a new
release. There is basically one test for each of the 81 RPC endpoints.

The second file contains 31 test cases specifically for the nine trace routines (five tests for five of the routines,
three for another, one each for the other three).

The third file contains 12 test cases for the overlay API for CREATE and CREATE2.

Another collection of related tests can be found
here: https://github.com/Great-Hill-Corporation/trueblocks-core/tree/develop/src/other/trace_tests
