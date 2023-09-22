# erigon-lib

Parts of Erigon codebase, written from scratch and licensed under Apache 2.0.

## License requirements

erigon-lib dependencies use various open source licenses compatible with Apache 2.0. This is checked on CI using `make lint-licenses`.

In order to keep license purity it is not allowed to refer to the code in the erigon root module from erigon-lib. This is ensured by the `go.mod` separation.

It is not allowed to copy or move code from erigon to erigon-lib unless all original authors agree to relief the code license from GPL to Apache 2.0.

## Code migration policy

It is encouraged to write new erigon code inside erigon-lib.

It is encouraged to move and relicense parts of the code from erigon to erigon-lib
that are safe and easy to move. For example, code written from scratch by erigon core contributors that has no significant external contributions could be refactored and moved.
