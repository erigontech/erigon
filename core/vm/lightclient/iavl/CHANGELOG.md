# Changelog

## 0.12.0 (November 26, 2018)

BREAKING CHANGES

- Uses new Tendermint ReverseIterator API.  See https://github.com/tendermint/tendermint/pull/2913

## 0.11.1 (October 29, 2018)

IMPROVEMENTS

- Uses GoAmino v0.14

## 0.11.0 (September 7, 2018)

BREAKING CHANGES

- Changed internal database key format to store int64 key components in a full 8-byte fixed width ([#107])
- Removed some architecture dependent methods (e.g., use `Get` instead of `Get64` etc) ([#96])  

IMPROVEMENTS

- Database key format avoids use of fmt.Sprintf fmt.Sscanf leading to ~10% speedup in benchmark BenchmarkTreeLoadAndDelete ([#107], thanks to [@silasdavis])

[#107]: https://github.com/tendermint/iavl/pull/107
[@silasdavis]: https://github.com/silasdavis
[#96]: https://github.com/tendermint/iavl/pull/96

## 0.10.0

BREAKING CHANGES

- refactored API for clean separation of [mutable][1] and [immutable][2] tree (#92, #88);
with possibility to:
  - load read-only snapshots at previous versions on demand
  - load mutable trees at the most recently saved tree

[1]: https://github.com/tendermint/iavl/blob/9e62436856efa94c1223043be36ebda01ae0b6fc/mutable_tree.go#L14-L21
[2]: https://github.com/tendermint/iavl/blob/9e62436856efa94c1223043be36ebda01ae0b6fc/immutable_tree.go#L10-L17

BUG FIXES

- remove memory leaks (#92)

IMPROVEMENTS

- Change tendermint dep to ^v0.22.0 (#91)

## 0.10.0 (July 11, 2018)

BREAKING CHANGES

- getRangeProof and Get\[Versioned\]\[Range\]WithProof return nil proof/error if tree is empty.

## 0.9.2 (July 3, 2018)

IMPROVEMENTS

- some minor changes: mainly lints, updated parts of documentation, unexported some helpers (#80)

## 0.9.1 (July 1, 2018)

IMPROVEMENTS

- RangeProof.ComputeRootHash() to compute root rather than provide as in Verify(hash)
- RangeProof.Verify\*() first require .Verify(root), which memoizes

## 0.9.0 (July 1, 2018)

BREAKING CHANGES

- RangeProof.VerifyItem doesn't require an index.
- Only return values in range when getting proof.
- Return keys as well.

BUG FIXES

- traversal bugs in traverseRange.

## 0.8.2

* Swap `tmlibs` for `tendermint/libs`
* Remove `sha256truncated` in favour of `tendermint/crypto/tmhash` - same hash
  function but technically a breaking change to the API, though unlikely to effect anyone.

NOTE this means IAVL is now dependent on Tendermint Core for the libs (since it
makes heavy use of the `db` package). Ideally, that dependency would be
abstracted away, and/or this repo will be merged into the Cosmos-SDK, which is
currently is primary consumer. Once it achieves greater stability, we could
consider breaking it out into it's own repo again.

## 0.8.1

*July 1st, 2018*

BUG FIXES

- fix bug in iterator going outside its range

## 0.8.0 (June 24, 2018)

BREAKING CHANGES

- Nodes are encoded using proto3/amino style integers and byte slices (ie. varints and
  varint prefixed byte slices)
- Unified RangeProof
- Proofs are encoded using Amino
- Hash function changed from RIPEMD160 to the first 20 bytes of SHA256 output

## 0.7.0 (March 21, 2018)

BREAKING CHANGES

- LoadVersion and Load return the loaded version number
    - NOTE: this behaviour was lost previously and we failed to document in changelog,
        but now it's back :)

## 0.6.1 (March 2, 2018)

IMPROVEMENT

- Remove spurious print statement from LoadVersion

## 0.6.0 (March 2, 2018)

BREAKING CHANGES

- NewTree order of arguments swapped
- int -> int64, uint64 -> int64
- NewNode takes a version
- Node serialization format changed so version is written right after size
- SaveVersion takes no args (auto increments)
- tree.Get -> tree.Get64
- nodeDB.SaveBranch does not take a callback
- orphaningTree.SaveVersion -> SaveAs
- proofInnerNode includes Version
- ReadKeyXxxProof consolidated into ReadKeyProof
- KeyAbsentProof doesn't include Version
- KeyRangeProof.Version -> Versions

FEATURES

- Implement chunking algorithm to serialize entire tree

## 0.5.0 (October 27, 2017)

First versioned release!
(Originally accidentally released as v0.2.0)

