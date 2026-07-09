// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package state

import (
	"fmt"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// codeSizeFromStateObject is the per-stateObject code-size fetch used by
// readCodeSize: cached so.code first, else a single code read that populates
// the cache.
func codeSizeFromStateObject(sdb *IntraBlockState, so *stateObject, addr accounts.Address) (int, error) {
	if so == nil || so.deleted {
		return 0, nil
	}
	if so.code.Bytes != nil {
		sdb.callCodeAccessHook(addr, so.code.Bytes)
		return so.code.Len(), nil
	}
	if so.data.CodeHash.IsEmpty() {
		return 0, nil
	}
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
		sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	// Size-only read for Stateless-witness correctness (see GetCodeSize): a
	// witness node has the size but not the bytes, so ReadAccountCode returns
	// nil there and reports EXTCODESIZE 0.
	size, err := sdb.stateReader.ReadAccountCodeSize(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.codeReadDuration += time.Since(readStart)
		sdb.codeReadCount++
	}
	sdb.stateReader.SetTrace(false, "")
	return size, err
}

// committedStorageDirect reads a storage slot's committed value straight from
// the state reader, with no stateObject. Used on the parallel path for a cold
// slot: no versionMap cell and no stateObject materialized this tx. When this
// tx created the contract (own CreateContract cell) its storage is fresh, so a
// cold slot reads zero rather than a prior incarnation's committed value —
// under noMaterialize there is no fresh stateObject to short-circuit that.
func (sdb *IntraBlockState) committedStorageDirect(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	if cc, ok := sdb.versionedWriteCreateContract(addr); ok && cc {
		return uint256.Int{}, nil
	}
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
		sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	res, ok, err := sdb.stateReader.ReadAccountStorage(addr, key)
	if dbg.KVReadLevelledMetrics {
		sdb.storageReadDuration += time.Since(readStart)
	}
	sdb.storageReadCount++
	sdb.stateReader.SetTrace(false, "")
	if err != nil {
		return uint256.Int{}, err
	}
	if !ok {
		res.Clear()
	}
	return res, nil
}

// committedCodeDirect reads an account's committed code bytes straight from the
// state reader, with no stateObject. Reached only for a cold CodePath read (the
// versionMap CodePath cell already missed upstream). A contract this tx created
// (own CreateContract cell) has no code until SetCode runs, so it reads empty
// rather than a prior incarnation's bytes.
func (sdb *IntraBlockState) committedCodeDirect(addr accounts.Address) ([]byte, error) {
	if cc, ok := sdb.versionedWriteCreateContract(addr); ok && cc {
		return nil, nil
	}
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
		sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	code, err := sdb.stateReader.ReadAccountCode(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.codeReadDuration += time.Since(readStart)
		sdb.codeReadCount++
	}
	sdb.stateReader.SetTrace(false, "")
	return code, err
}

// codeSeed returns the code this tx currently sees at addr — its own Code write
// cell if it wrote one, else the committed value — without recording an OCC
// read. On the noMaterialize path the transient stateObject is rebuilt from the
// tx-start account, so its code reflects the committed value; seeding it with
// this lets stateObject.SetCode compare against the current code (matching the
// cached path) instead of the stale tx-start value.
func (sdb *IntraBlockState) codeSeed(addr accounts.Address, currentHash accounts.CodeHash) (accounts.Code, error) {
	if _, isDirty := sdb.journal.dirties[addr]; isDirty {
		if vw, ok := sdb.versionedWrites.GetCode(addr); ok {
			return vw.Val, nil
		}
	}
	if currentHash == accounts.EmptyCodeHash {
		return accounts.Code{Hash: accounts.EmptyCodeHash}, nil
	}
	bytes, err := sdb.committedCodeDirect(addr)
	if err != nil {
		return accounts.Code{}, err
	}
	return accounts.Code{Hash: currentHash, Bytes: bytes}, nil
}

// committedCodeHash returns the tx-start code hash from the committed reader
// (normalised to EmptyCodeHash for an absent or code-less account), without
// recording an OCC read. Used on the noMaterialize path where the rebuilt
// transient's original reflects this tx's own code cell rather than tx start.
func (sdb *IntraBlockState) committedCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	acc, err := sdb.stateReader.ReadAccountData(addr)
	if err != nil {
		return accounts.EmptyCodeHash, err
	}
	if acc == nil || acc.CodeHash.IsEmpty() {
		return accounts.EmptyCodeHash, nil
	}
	return acc.CodeHash, nil
}

// committedCodeSizeDirect reads an account's committed code size straight from
// the state reader, with no stateObject. Size-only for stateless-witness
// correctness (a witness node carries the size but not the bytes). A contract
// this tx created has zero code size until SetCode runs.
func (sdb *IntraBlockState) committedCodeSizeDirect(addr accounts.Address) (int, error) {
	if cc, ok := sdb.versionedWriteCreateContract(addr); ok && cc {
		return 0, nil
	}
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
		sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	size, err := sdb.stateReader.ReadAccountCodeSize(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.codeReadDuration += time.Since(readStart)
		sdb.codeReadCount++
	}
	sdb.stateReader.SetTrace(false, "")
	return size, err
}

// versionedReadCore runs the type-independent part of a versionMap-aware read —
// the writeSet/versionMap/readSet tier probes plus destruct/revival logic — and
// returns a readPathResult telling the typed wrapper which source to read the
// path-typed value from. Panics ErrDependency on an intra-tx version conflict.
type readPathOutcome uint8

const (
	outcomeUnset readPathOutcome = iota

	outcomeLegacyStorage // versionMap == nil: typed wrapper does direct storage read on r.so
	outcomeWriteSetHit   // r.vw is set; typed wrapper returns its Val*
	outcomeMapDone       // versionMap hit; the path's typed map*Val field carries the value
	outcomeReadSetHit    // a prior read matched; typed wrapper re-fetches it via GetX
	outcomeStorageRead   // r.so resolved; wrapper does typed storage read + records r.hdr
	outcomeReturnZero    // typed wrapper returns the path-typed zero value
	outcomeReturnDefault // typed wrapper returns its caller-supplied defaultV
)

// readPathResult communicates the outcome of versionedReadCore to a
// typed wrapper.  Exactly one source field is populated for the
// tier-hit outcomes; the wrapper performs the typed extraction and
// records the read via the typed ReadSet.SetX path when r.recordVR is true.
type readPathResult struct {
	outcome readPathOutcome

	// Source records for typed extraction.  For outcomeWriteSetHit the
	// path-typed pointer corresponding to the read path is set; the
	// others are nil.  Per-typed fields avoid the any-boxing that a
	// single AnyVersionedWrite field would force on the hot path.
	vwAddress        *VersionedWrite[*accounts.Account]
	vwBalance        *VersionedWrite[uint256.Int]
	vwNonce          *VersionedWrite[uint64]
	vwIncarnation    *VersionedWrite[uint64]
	vwSelfDestruct   *VersionedWrite[bool]
	vwCreateContract *VersionedWrite[bool]
	vwCode           *VersionedWrite[accounts.Code]
	vwCodeHash       *VersionedWrite[accounts.CodeHash]
	vwCodeSize       *VersionedWrite[int]
	vwStorage        *VersionedWrite[uint256.Int]

	so *stateObject // outcomeStorageRead / outcomeLegacyStorage

	// Typed map-read values: the wrapper reads its path's field directly,
	// avoiding the any-box (a heap alloc per non-storage read) the generic
	// ReadResult.value would impose. Value, not *WriteCell, so reads are
	// race-free against a concurrent FlushVersionedWrites mutating cell.Value.
	mapAddressVal        *accounts.Account
	mapBalanceVal        uint256.Int
	mapNonceVal          uint64
	mapIncarnationVal    uint64
	mapSelfDestructVal   bool
	mapCreateContractVal bool
	mapCodeVal           []byte
	mapCodeHashVal       accounts.CodeHash
	mapCodeSizeVal       int
	mapStorageVal        uint256.Int

	// hdr is the skeleton header for the wrapper to record (with its typed
	// value) via the typed recordX path when recordVR is true.
	hdr      ReadHeader
	recordVR bool

	source  ReadSource
	version Version

	err error
}

// versionedReadCore is the non-generic body that drives the read.
// Typed wrappers (readBalance, readNonce, readState, …) consume the
// result.  See readPathOutcome for the outcome enumeration.
//
// skipStorage=true: do not attempt a storage-read fallback when the
// in-memory tiers miss — the caller resolves the value itself (refresh*
// wrappers return their defaultV; AddressPath reads avoid recursing back
// through getStateObject).
//
// versionedReadCore writes its discriminated result into *r (caller
// allocates on the stack).  This avoids a ~256-byte return-value copy
// per call — the readPathResult struct is large because it bundles
// every outcome's source field; pointer-passing keeps the struct in
// the caller's stack frame and the core mutates it in place.
func versionedReadCore(s *IntraBlockState, addr accounts.Address, path AccountPath, key accounts.StorageKey, commited bool, skipStorage bool, r *readPathResult) {
	*r = readPathResult{}

	if s.versionMap == nil {
		so, err := s.getStateObject(addr, true)
		if err != nil {
			r.err = err
			r.source = StorageRead
			r.version = UnknownVersion
			return
		}
		r.outcome = outcomeLegacyStorage
		r.so = so
		r.source = StorageRead
		r.version = UnknownVersion
		return
	}

	if so, ok := s.stateObjects[addr]; ok && so.deleted {
		// When the in-memory deletion reflects a prior tx's selfdestruct, surface
		// the SD version rather than UnknownVersion, so synthetic CreateAccount read
		// records match later SD-zero-path reads and don't force a version conflict.
		if destructed, sdRes, ok := s.readSelfDestructMemo(addr); ok && sdRes.Status() == MVReadResultDone && destructed {
			sdVer := Version{TxIndex: sdRes.DepIdx(), Incarnation: sdRes.Incarnation()}
			if !commited {
				s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: sdVer},
					Val:        true,
				})
			}
			r.outcome = outcomeReturnZero
			r.source = MapRead
			r.version = sdVer
			return
		}
		r.outcome = outcomeReturnDefault
		r.source = StorageRead
		r.version = UnknownVersion
		return
	}

	var destructedVersion Version
	if destructed, sdRes, ok := s.readSelfDestructMemo(addr); ok && sdRes.Status() == MVReadResultDone && destructed {
		destructTxIndex := sdRes.DepIdx()
		// A tx's own same-tx write to this path is returned directly: it always
		// observes its own write, even after a prior tx's self-destruct.
		if !commited {
			if hasWrite := s.versionedWriteHit(addr, path, key, r); hasWrite {
				r.outcome = outcomeWriteSetHit
				r.source = WriteSetRead
				r.version = Version{TxIndex: s.txIndex, Incarnation: s.version}
				return
			}
		}
		// This field is revived only if the version map holds a write to THIS path
		// at a strictly higher TxIndex; per-path (not account-wide) so a field with
		// no post-self-destruct write correctly reads as the fresh account's zero.
		revived := false
		if pathRevival := s.versionMap.ReadStatus(addr, path, key, s.txIndex); pathRevival.DepIdx() > destructTxIndex &&
			(pathRevival.Status() == MVReadResultDone || pathRevival.Status() == MVReadResultDependency) {
			revived = true
		}
		if !revived && path != CodePath {
			sdVersion := Version{TxIndex: destructTxIndex, Incarnation: sdRes.Incarnation()}
			if commited {
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = sdVersion
				return
			}
			// Match main's `versionedWrite(addr, SelfDestructPath, key)`: the
			// own-write lookup is keyed by `key`, so for a StoragePath read
			// (key=slot) it never matches the per-address SelfDestructPath write
			// (stored under NilKey) and the slot reads as the post-SD zero — a
			// fresh contract's slots are empty. Only an account-field read
			// (key=NilKey) consults the SelfDestructPath own-write; a same-tx
			// SelfDestructPath=false there means the account was revived, so we
			// fall through.
			sd, sdOK := false, false
			if key == accounts.NilKey {
				sd, sdOK = s.versionedWriteSelfDestruct(addr)
			}
			if !sdOK || sd {
				s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: sdVersion},
					Val:        true,
				})
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = sdVersion
				return
			}
			// SelfDestructPath write exists with Val==false: fall through with
			// destructedVersion recorded so the stale-readSet dependency check
			// can use it.
			destructedVersion = Version{TxIndex: destructTxIndex}
		}
	}

	// Dispatch to the path's typed ReadX: returns T directly plus a ReadResult
	// (Status/DepIdx/Incarnation), never an any-boxed value — the box the
	// generic Read does is a heap alloc per non-storage read. On a miss ReadX
	// returns a pre-seeded (UnknownDep, -1) result, so res is always valid.
	var res ReadResult
	switch path {
	case AddressPath:
		r.mapAddressVal, res, _ = s.versionMap.ReadAddress(addr, s.txIndex)
	case BalancePath:
		r.mapBalanceVal, res, _ = s.versionMap.ReadBalance(addr, s.txIndex)
	case NoncePath:
		r.mapNonceVal, res, _ = s.versionMap.ReadNonce(addr, s.txIndex)
	case IncarnationPath:
		r.mapIncarnationVal, res, _ = s.versionMap.ReadIncarnation(addr, s.txIndex)
	case CodePath:
		var mc accounts.Code
		mc, res, _ = s.versionMap.ReadCode(addr, s.txIndex)
		r.mapCodeVal = mc.Bytes
	case CodeHashPath:
		r.mapCodeHashVal, res, _ = s.versionMap.ReadCodeHash(addr, s.txIndex)
	case CodeSizePath:
		r.mapCodeSizeVal, res, _ = s.versionMap.ReadCodeSize(addr, s.txIndex)
	case SelfDestructPath:
		r.mapSelfDestructVal, res, _ = s.versionMap.ReadSelfDestruct(addr, s.txIndex)
	case CreateContractPath:
		r.mapCreateContractVal, res, _ = s.versionMap.ReadCreateContract(addr, s.txIndex)
	case StoragePath:
		r.mapStorageVal, res, _ = s.versionMap.ReadStorage(addr, key, s.txIndex)
	default:
		panic(fmt.Errorf("readPaths: unknown path %v", path))
	}

	hdr := ReadHeader{Version: Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}}

	if !commited {
		if hasWrite := s.versionedWriteHit(addr, path, key, r); hasWrite {
			if res.Status() == MVReadResultDone {
				if prHeader, prOK := s.versionedReads.getHeader(addr, path, key); prOK {
					if hdr.Version.TxIndex > destructedVersion.TxIndex && hdr.Version != prHeader.Version {
						if hdr.Version.TxIndex > s.dep {
							s.dep = hdr.Version.TxIndex
						}
						if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
							fmt.Printf("%d (%d.%d) WR DEP (%d.%d)!=(%d.%d) %x %s\n",
								s.blockNum, s.txIndex, s.version,
								prHeader.Version.TxIndex, prHeader.Version.Incarnation,
								hdr.Version.TxIndex, hdr.Version.Incarnation,
								addr, AccountKey{path, key})
						}
						s.versionedReads.SetHeader(addr, path, key, hdr)
						panic(ErrDependency)
					}
				}
			}
			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD (%s) %x %s\n",
					s.blockNum, s.txIndex, s.version, WriteSetRead,
					addr, AccountKey{path, key})
			}
			r.outcome = outcomeWriteSetHit
			r.source = WriteSetRead
			r.version = Version{TxIndex: s.txIndex, Incarnation: s.version}
			return
		}
	}

	switch res.Status() {
	case MVReadResultDone:
		hdr.Source = MapRead
		if prHeader, ok := s.versionedReads.getHeader(addr, path, key); ok {
			if prHeader.Version == hdr.Version {
				if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) RD (%s:%s) %x %s\n",
						s.blockNum, s.txIndex, s.version, MapRead, res.DepString(),
						addr, AccountKey{path, key})
				}
				r.outcome = outcomeReadSetHit
				r.source = MapRead
				r.version = hdr.Version
				return
			}
			if hdr.Version.TxIndex > s.dep {
				s.dep = hdr.Version.TxIndex
			}
			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD DEP (%d.%d)!=(%d.%d) %x %s\n",
					s.blockNum, s.txIndex, s.version,
					prHeader.Version.TxIndex, prHeader.Version.Incarnation,
					hdr.Version.TxIndex, hdr.Version.Incarnation,
					addr, AccountKey{path, key})
			}
			s.versionedReads.SetHeader(addr, path, key, hdr)
			panic(ErrDependency)
		}
		// CodePath trumped by SelfDestruct at >= DepIdx
		if path == CodePath {
			if destructed, sdres, ok := s.versionMap.ReadSelfDestruct(addr, s.txIndex); ok && sdres.Status() == MVReadResultDone && destructed && sdres.DepIdx() >= res.DepIdx() {
				r.outcome = outcomeReturnDefault
				r.source = MapRead
				r.version = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
				return
			}
		}
		r.outcome = outcomeMapDone
		r.hdr = hdr
		r.recordVR = true
		r.source = MapRead
		r.version = hdr.Version
		return

	case MVReadResultDependency:
		if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) MP DEP (%d.%d) %x %s\n",
				s.blockNum, s.txIndex, s.version,
				res.DepIdx(), res.Incarnation(),
				addr, AccountKey{path, key})
		}
		if res.DepIdx() > s.dep {
			s.dep = res.DepIdx()
		}
		hdr.Source = MapRead
		s.versionedReads.SetHeader(addr, path, key, hdr)
		panic(ErrDependency)

	case MVReadResultNone:
		if !commited {
			if prHeader, ok := s.versionedReads.getHeader(addr, path, key); ok {
				if prHeader.Version == hdr.Version {
					if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d (%d.%d) RD (%s) %x %s\n",
							s.blockNum, s.txIndex, s.version, ReadSetRead,
							addr, AccountKey{path, key})
					}
					r.outcome = outcomeReadSetHit
					r.source = ReadSetRead
					r.version = prHeader.Version
					return
				}
				if prHeader.Source == MapRead {
					if path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath {
						_, accSource, accVersion, _ := readAccountInternal(s, addr)
						if accSource == prHeader.Source && accVersion == prHeader.Version {
							r.outcome = outcomeReadSetHit
							r.source = ReadSetRead
							r.version = prHeader.Version
							return
						}
					}
					if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d (%d.%d) RM DEP FALLTHROUGH (%d.%d)!=(%d.%d) %x %s\n",
							s.blockNum, s.txIndex, s.version,
							prHeader.Version.TxIndex, prHeader.Version.Incarnation,
							hdr.Version.TxIndex, hdr.Version.Incarnation,
							addr, AccountKey{path, key})
					}
					// Fall through to storage read.
				}
			}
		}

		// A self-destructed account's per-account field with no versionMap cell
		// reads as the post-SD zero, recorded as a dependency on the SelfDestructPath
		// entry; a bare StorageRead/UnknownVersion would be rejected by the validator
		// (path==AddressPath cross-check) and loop on validator-invalid retries.
		if path == BalancePath || path == NoncePath || path == IncarnationPath ||
			path == CodeHashPath || path == CodePath || path == CodeSizePath {
			if destructed, sd, ok := s.versionMap.ReadSelfDestruct(addr, s.txIndex); ok && sd.Status() == MVReadResultDone && destructed {
				sdVer := Version{TxIndex: sd.DepIdx(), Incarnation: sd.Incarnation()}
				if !commited {
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: sdVer},
						Val:        true,
					})
				}
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = sdVer
				return
			}
		}

		// StoragePath: zero out unwritten slots when prior tx wrote Incarnation.
		if path == StoragePath {
			if inc, incRes, incOK := s.versionMap.ReadIncarnation(addr, s.txIndex); incOK && incRes.Status() == MVReadResultDone {
				hdr.Source = StorageRead
				s.versionedReads.SetHeader(addr, path, key, hdr)
				incVersion := Version{TxIndex: incRes.DepIdx(), Incarnation: incRes.Incarnation()}
				s.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{
					ReadHeader: ReadHeader{Source: MapRead, Version: incVersion},
					Val:        inc,
				})
				if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) RD (%s) %x %s: zero (IncarnationPath written by tx %d)\n",
						s.blockNum, s.txIndex, s.version, StorageRead,
						addr, AccountKey{path, key}, incRes.DepIdx())
				}
				r.outcome = outcomeReturnZero
				r.source = StorageRead
				r.version = UnknownVersion
				return
			}
		}

		// skipStorage: callers that don't want a storage fallback (the
		// refresh* wrappers + AddressPath internal callers).  Signal the
		// wrapper to record a header-only read for ValidateVersion.
		if skipStorage {
			r.outcome = outcomeReturnDefault
			r.source = UnknownSource
			r.version = UnknownVersion
			if !commited && path != CodePath {
				hdr.Source = StorageRead
				r.hdr = hdr
				r.recordVR = true
			}
			return
		}

		// Resolve stateObject (via AddressPath account for the four
		// account-field paths, else direct getStateObject fallback).
		var so *stateObject
		if path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath {
			readAccount, accSource, accVersion, err := readAccountInternal(s, addr)
			if err != nil {
				r.err = err
				r.outcome = outcomeReturnDefault
				r.source = accSource
				r.version = UnknownVersion
				return
			}
			if readAccount != nil {
				hdr.Source = accSource
				hdr.Version = accVersion
				so = newObject(s, addr, readAccount, readAccount)
			}
		}
		// Cold committed storage read: resolve directly from the state reader
		// without materializing a stateObject. A stateObject exists here only
		// when a write this tx materialized one (created contract / fakeStorage /
		// dirty slots live on that object), so reuse it when present.
		if path == StoragePath && so == nil {
			hdr.Source = StorageRead
			if cached, ok := s.stateObjects[addr]; ok {
				so = cached
			} else {
				// A cold slot depends only on its own StoragePath cell — record
				// no AddressPath dependency (that would be a false dep). The value
				// is the committed slot straight from the state reader.
				val, err := s.committedStorageDirect(addr, key)
				if err != nil {
					r.err = err
					r.outcome = outcomeReturnDefault
					r.source = StorageRead
					r.version = UnknownVersion
					return
				}
				r.mapStorageVal = val
				r.outcome = outcomeStorageRead
				r.hdr = hdr
				r.recordVR = true
				r.source = hdr.Source
				r.version = hdr.Version
				return
			}
		}
		// Cold code / code-size read: resolve directly from the state reader
		// without materializing a stateObject. A cached object (write this tx)
		// carries dirtyCode / the loaded bytes, so reuse it when present. Code
		// paths record only their own dependency (no false AddressPath dep) and
		// their recorded value is not compared in validation (noValueRead).
		if (path == CodePath || path == CodeSizePath) && so == nil {
			hdr.Source = StorageRead
			if cached, ok := s.stateObjects[addr]; ok {
				so = cached
			} else {
				if path == CodePath {
					code, err := s.committedCodeDirect(addr)
					if err != nil {
						r.err = err
						r.outcome = outcomeReturnDefault
						r.source = StorageRead
						r.version = UnknownVersion
						return
					}
					r.mapCodeVal = code
				} else {
					size, err := s.committedCodeSizeDirect(addr)
					if err != nil {
						r.err = err
						r.outcome = outcomeReturnDefault
						r.source = StorageRead
						r.version = UnknownVersion
						return
					}
					r.mapCodeSizeVal = size
				}
				r.outcome = outcomeStorageRead
				r.hdr = hdr
				r.recordVR = true
				r.source = hdr.Source
				r.version = hdr.Version
				return
			}
		}
		if so == nil {
			hdr.Source = StorageRead
			obj, err := s.getStateObject(addr, true)
			if err != nil {
				r.err = err
				r.outcome = outcomeReturnDefault
				r.source = StorageRead
				r.version = UnknownVersion
				return
			}
			so = obj
		}
		r.outcome = outcomeStorageRead
		r.so = so
		r.hdr = hdr
		r.recordVR = true
		r.source = hdr.Source
		r.version = hdr.Version
		return
	}

	r.outcome = outcomeReturnDefault
	r.source = UnknownSource
	r.version = UnknownVersion
}

// readAccountInternal performs an AddressPath versionedReadCore + typed
// extraction of *accounts.Account.  Used internally by versionedReadCore
// to resolve sibling-account reads without taking a typed callback.
func readAccountInternal(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, AddressPath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwAddress.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetAddress(addr)
		if tr.Val != nil {
			return tr.Val.Account(), r.source, r.version, nil
		}
		return nil, r.source, r.version, nil
	case outcomeMapDone:
		acc := r.mapAddressVal
		if r.recordVR {
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{r.hdr, NewAccountView(acc)})
		}
		return acc, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnDefault:
		// outcomeReturnDefault from the skipStorage branch may carry
		// recordVR=true.  The AddressPath defaultV is nil.
		if r.recordVR {
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: r.hdr})
		}
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readAccountInternal: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readBalance returns the address's balance using the version-aware
// read pipeline.  Inlines the storage-read fallback.
func readBalance(s *IntraBlockState, addr accounts.Address) (uint256.Int, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, BalancePath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwBalance.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetBalance(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapBalanceVal
		if r.recordVR {
			s.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint256.Int
		if r.so != nil && !r.so.deleted {
			v = r.so.Balance()
		}
		if r.recordVR {
			s.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, nil
		}
		return r.so.Balance(), StorageRead, UnknownVersion, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return uint256.Int{}, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readBalance: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshBalance is the in-memory-only variant: returns currentBalance on
// miss and does not perform a storage fallback.  When the core signals
// recordVR, records the read with currentBalance as the typed default.
func refreshBalance(s *IntraBlockState, addr accounts.Address, currentBalance uint256.Int) (uint256.Int, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, BalancePath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return currentBalance, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwBalance.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetBalance(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapBalanceVal, r.source, r.version, nil
	case outcomeReturnZero:
		// Account was self-destructed (or not present): return the zero value,
		// NOT the caller's stale pre-destruct balance. outcomeReturnDefault
		// keeps the current value; only this branch must zero it.
		return uint256.Int{}, r.source, r.version, nil
	case outcomeReturnDefault:
		if r.recordVR {
			s.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{r.hdr, currentBalance})
		}
		return currentBalance, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshBalance: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readNonce returns the nonce using the version-aware read pipeline.
func readNonce(s *IntraBlockState, addr accounts.Address) (uint64, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, NoncePath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		return 0, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwNonce.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetNonce(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapNonceVal
		if r.recordVR {
			s.versionedReads.SetNonce(addr, VersionedRead[uint64]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint64
		if r.so != nil && !r.so.deleted {
			v = r.so.Nonce()
		}
		if r.recordVR {
			s.versionedReads.SetNonce(addr, VersionedRead[uint64]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return 0, StorageRead, UnknownVersion, nil
		}
		return r.so.Nonce(), StorageRead, UnknownVersion, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return 0, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readNonce: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func refreshNonce(s *IntraBlockState, addr accounts.Address, currentNonce uint64) (uint64, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, NoncePath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return currentNonce, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwNonce.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetNonce(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapNonceVal, r.source, r.version, nil
	case outcomeReturnZero:
		return 0, r.source, r.version, nil
	case outcomeReturnDefault:
		if r.recordVR {
			s.versionedReads.SetNonce(addr, VersionedRead[uint64]{r.hdr, currentNonce})
		}
		return currentNonce, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshNonce: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readIncarnation returns the incarnation counter.
func readIncarnation(s *IntraBlockState, addr accounts.Address) (uint64, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, IncarnationPath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		return 0, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwIncarnation.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetIncarnation(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapIncarnationVal
		if r.recordVR {
			s.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint64
		if r.so != nil && !r.so.deleted {
			v = r.so.data.Incarnation
		}
		if r.recordVR {
			s.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return 0, StorageRead, UnknownVersion, nil
		}
		return r.so.data.Incarnation, StorageRead, UnknownVersion, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return 0, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readIncarnation: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func refreshIncarnation(s *IntraBlockState, addr accounts.Address, currentIncarnation uint64) (uint64, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, IncarnationPath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return currentIncarnation, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwIncarnation.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetIncarnation(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapIncarnationVal, r.source, r.version, nil
	case outcomeReturnZero:
		return 0, r.source, r.version, nil
	case outcomeReturnDefault:
		if r.recordVR {
			s.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{r.hdr, currentIncarnation})
		}
		return currentIncarnation, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshIncarnation: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readCode returns the contract code. The commited flag selects whether
// the version-aware lookup honours the committed-only contract.
func readCode(s *IntraBlockState, addr accounts.Address, commited bool) ([]byte, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, CodePath, accounts.NilKey, commited, false, &r)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwCode.Val.Bytes, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetCode(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapCodeVal
		if r.recordVR {
			s.versionedReads.SetCode(addr, VersionedRead[[]byte]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v []byte
		if r.so != nil {
			if !r.so.deleted {
				code, err := r.so.Code()
				if err != nil {
					return nil, StorageRead, UnknownVersion, err
				}
				v = code
			}
		} else {
			v = r.mapCodeVal
		}
		if r.recordVR {
			s.versionedReads.SetCode(addr, VersionedRead[[]byte]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return nil, StorageRead, UnknownVersion, nil
		}
		code, err := r.so.Code()
		return code, StorageRead, UnknownVersion, err
	case outcomeReturnZero, outcomeReturnDefault:
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCode: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshCode is the in-memory-only variant for CodePath.
// CodePath is never recorded via the skipStorage branch in legacy
// (the `path != CodePath` guard), so no recording on the default case.
func refreshCode(s *IntraBlockState, addr accounts.Address) ([]byte, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, CodePath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwCode.Val.Bytes, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetCode(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapCodeVal, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshCode: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readCodeSize returns the contract code size.
func readCodeSize(s *IntraBlockState, addr accounts.Address) (int, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, CodeSizePath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		return 0, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwCodeSize.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetCodeSize(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapCodeSizeVal
		if r.recordVR {
			s.versionedReads.SetCodeSize(addr, VersionedRead[int]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v int
		if r.so != nil {
			// CodeSizePath delegates to the per-stateObject code-size pattern:
			// prefer cached so.code length, else load full code once (geth-style)
			// and populate so.code.
			sz, err := codeSizeFromStateObject(s, r.so, addr)
			if err != nil {
				return 0, r.source, r.version, err
			}
			v = sz
		} else {
			v = r.mapCodeSizeVal
		}
		if r.recordVR {
			s.versionedReads.SetCodeSize(addr, VersionedRead[int]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		v, err := codeSizeFromStateObject(s, r.so, addr)
		if err != nil {
			return 0, StorageRead, UnknownVersion, err
		}
		return v, StorageRead, UnknownVersion, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return 0, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCodeSize: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readCodeHash returns the contract code hash.
func readCodeHash(s *IntraBlockState, addr accounts.Address) (accounts.CodeHash, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, CodeHashPath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		return accounts.NilCodeHash, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwCodeHash.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetCodeHash(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapCodeHashVal
		if r.recordVR {
			s.versionedReads.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v accounts.CodeHash
		if r.so != nil && !r.so.deleted {
			v = r.so.data.CodeHash
		} else {
			v = accounts.NilCodeHash
		}
		if r.recordVR {
			s.versionedReads.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return accounts.NilCodeHash, StorageRead, UnknownVersion, nil
		}
		return r.so.data.CodeHash, StorageRead, UnknownVersion, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return accounts.NilCodeHash, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCodeHash: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshCodeHash is the in-memory-only variant for CodeHashPath.
func refreshCodeHash(s *IntraBlockState, addr accounts.Address, currentHash accounts.CodeHash) (accounts.CodeHash, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, CodeHashPath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return currentHash, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwCodeHash.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetCodeHash(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapCodeHashVal, r.source, r.version, nil
	case outcomeReturnZero:
		return accounts.NilCodeHash, r.source, r.version, nil
	case outcomeReturnDefault:
		if r.recordVR {
			s.versionedReads.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{r.hdr, currentHash})
		}
		return currentHash, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshCodeHash: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readState reads a storage slot; it is readStateForSet without the
// SetState-only "clean" bool.
func readState(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, error) {
	v, source, version, _, err := readStateForSet(s, addr, key)
	return v, source, version, err
}

// readStateForSet is the SetState-specific variant.  Returns the
// additional "clean" bool (the second return of stateObject.GetState),
// which SetState uses to decide between deleting vs. updating the
// versioned write on revert.
func readStateForSet(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, bool, error) {
	var r readPathResult
	versionedReadCore(s, addr, StoragePath, key, false, false, &r)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, false, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwStorage.Val, r.source, r.version, false, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetStorage(addr, key)
		return tr.Val, r.source, r.version, false, nil
	case outcomeMapDone:
		v := r.mapStorageVal
		if r.recordVR {
			s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, false, nil
	case outcomeStorageRead:
		var v uint256.Int
		var clean bool
		if r.so != nil {
			if !r.so.deleted {
				v, clean = r.so.GetState(key)
			}
		} else {
			// Cold committed read resolved by committedStorageDirect: no dirty
			// value exists on the parallel path, so it is always clean.
			v, clean = r.mapStorageVal, true
		}
		if r.recordVR {
			s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, clean, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, false, nil
		}
		v, clean := r.so.GetState(key)
		return v, StorageRead, UnknownVersion, clean, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return uint256.Int{}, r.source, r.version, false, nil
	default:
		panic(fmt.Sprintf("readStateForSet: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readCommittedState reads a storage slot with committed-view semantics.
func readCommittedState(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, StoragePath, key, true, false, &r)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwStorage.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetStorage(addr, key)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapStorageVal
		if r.recordVR {
			s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint256.Int
		if r.so != nil {
			if !r.so.deleted {
				cv, err := r.so.GetCommittedState(key)
				if err != nil {
					return uint256.Int{}, StorageRead, UnknownVersion, err
				}
				v = cv
			}
		} else {
			v = r.mapStorageVal
		}
		if r.recordVR {
			s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, nil
		}
		v, err := r.so.GetCommittedState(key)
		return v, StorageRead, UnknownVersion, err
	case outcomeReturnZero, outcomeReturnDefault:
		return uint256.Int{}, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCommittedState: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readSelfDestruct returns whether the account is selfdestructed.
func readSelfDestruct(s *IntraBlockState, addr accounts.Address) (bool, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, SelfDestructPath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		return false, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwSelfDestruct.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetSelfDestruct(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		v := r.mapSelfDestructVal
		if r.recordVR {
			s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v bool
		if r.so != nil {
			if r.so.deleted {
				v = false
			} else if r.so.createdContract {
				v = false
			} else {
				v = r.so.selfdestructed
			}
		}
		if r.recordVR {
			s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{r.hdr, v})
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil {
			return false, StorageRead, UnknownVersion, nil
		}
		if r.so.deleted || r.so.createdContract {
			return false, StorageRead, UnknownVersion, nil
		}
		return r.so.selfdestructed, StorageRead, UnknownVersion, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return false, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readSelfDestruct: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshSelfDestruct is the in-memory-only variant.
func refreshSelfDestruct(s *IntraBlockState, addr accounts.Address) (bool, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, SelfDestructPath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return false, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwSelfDestruct.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetSelfDestruct(addr)
		return tr.Val, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapSelfDestructVal, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnDefault:
		if r.recordVR {
			// SelfDestructPath defaultV is false — the zero value.
			s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{ReadHeader: r.hdr})
		}
		return false, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshSelfDestruct: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readAccount returns the *accounts.Account for an address.
func readAccount(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	return readAccountInternal(s, addr)
}

// refreshAccount is the in-memory-only variant for AddressPath.
func refreshAccount(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, AddressPath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vwAddress.Val, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetAddress(addr)
		if tr.Val != nil {
			return tr.Val.Account(), r.source, r.version, nil
		}
		return nil, r.source, r.version, nil
	case outcomeMapDone:
		return r.mapAddressVal, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnDefault:
		if r.recordVR {
			// AddressPath defaultV is nil.
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: r.hdr})
		}
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshAccount: unexpected outcome %d for %x", r.outcome, addr))
	}
}
