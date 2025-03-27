# BLS

Just a simple library that wraps github.com/supranational/blst for a much more comprehensible UX and UI, compatible with Ethereum 2.0

Current Functionalities:

* `Verify`: [specs](https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#bls-signatures)
* `VerifyAggregate`: [specs](https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#bls-signatures)
* `Sign`: [specs](https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#bls-signatures)
* `Multiple Aggregate`

## Benchmarks

This library has an internal cache which may alleviate key decompression. it takes more memory though.

```
goos: darwin
goarch: arm64
pkg: github.com/erigontech/erigon/cl/utils/bls
BenchmarkAggregateSigCached-12       	    1592	    692514 ns/op	   11450 B/op	      96 allocs/op
BenchmarkAggregateSigNonCached-12    	     924	   1233480 ns/op	   12550 B/op	     108 allocs/op
PASS
```
