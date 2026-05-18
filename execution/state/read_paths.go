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
// readCodeSize.  Mirrors the geth-style pattern: cached so.code first,
// else a single code read with cache populate.  KV-read levelled metrics
// are recorded so the legacy versionedRead's instrumentation is
// preserved.
func codeSizeFromStateObject(sdb *IntraBlockState, so *stateObject, addr accounts.Address) int {
	if so == nil || so.deleted {
		return 0
	}
	if so.code != nil {
		return len(so.code)
	}
	if so.data.CodeHash.IsEmpty() {
		return 0
	}
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
		sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	code, codeErr := sdb.stateReader.ReadAccountCode(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.codeReadDuration += time.Since(readStart)
		sdb.codeReadCount++
	}
	sdb.stateReader.SetTrace(false, "")
	l := len(code)
	if codeErr == nil && code != nil {
		so.code = code
	}
	return l
}

// versionedReadCore performs the type-independent orchestration of a
// versionMap-aware read: in-memory tier probes (writeSet, versionMap,
// readSet) plus the destruct/revival logic.  Returns a discriminated
// readPathResult that tells the typed wrapper which source record to
// extract the path-typed value from (and where to record it back into
// the readSet).
//
// Behaviour preserved from the legacy versionedRead[T]:
//   - panics ErrDependency on intra-tx read/write version conflicts
//     (D.1, E.2, F branches); the parallel executor recovers.
//   - the selfdestruct revival check probes Balance/Nonce/CodeHash
//     siblings before short-circuiting to zero on a non-CodePath read.
//   - CodePath is exempt from the SD short-circuit (C.4) and is
//     separately trumped by a Done SelfDestruct at >= the code's DepIdx
//     (E.3a).
//   - StoragePath reads return zero when IncarnationPath was rewritten
//     by a prior tx in this block (G.6), with the IncarnationPath dep
//     recorded.
//   - For paths in {Balance, Nonce, Incarnation, CodeHash}, an
//     unwritten slot may resolve via the AddressPath account (G.7).
//
// The wrapper completes the read by:
//   - filling vrSkel's typed Val* and calling versionedReads.Set
//     when r.recordVR is true;
//   - performing any typed storage read against r.so when
//     r.outcome == outcomeStorageRead or outcomeLegacyStorage;
//   - returning the path-typed zero (or the caller's defaultV) for the
//     return-default outcomes.
type readPathOutcome uint8

const (
	outcomeUnset readPathOutcome = iota

	outcomeLegacyStorage // versionMap == nil: typed wrapper does direct storage read on r.so
	outcomeWriteSetHit   // r.vw is set; typed wrapper returns its Val*
	outcomeMapDone       // r.mapRes.Value() carries the typed value; wrapper type-asserts
	outcomeReadSetHit    // r.pr is set; typed wrapper returns its Val*
	outcomeStorageRead   // r.so resolved; wrapper does typed storage read + records vrSkel
	outcomeReturnZero    // typed wrapper returns the path-typed zero value
	outcomeReturnDefault // typed wrapper returns its caller-supplied defaultV
)

// readPathResult communicates the outcome of versionedReadCore to a
// typed wrapper.  Exactly one source field is populated for the
// tier-hit outcomes; the wrapper performs the typed extraction and
// records the read into versionedReads when r.recordVR is true.
type readPathResult struct {
	outcome readPathOutcome

	// Source records for typed extraction:
	vw     *VersionedWrite // outcomeWriteSetHit
	pr     *VersionedRead  // outcomeReadSetHit
	mapRes ReadResult      // outcomeMapDone
	so     *stateObject    // outcomeStorageRead / outcomeLegacyStorage

	// Skeleton VR for the wrapper to complete with the typed Val* and
	// record via versionedReads.Set when recordVR is true.
	vrSkel   VersionedRead
	recordVR bool

	source  ReadSource
	version Version

	// destructedVersion: set when the SD short-circuit was bypassed by
	// the writeSet's SelfDestructPath=false case (C.7) so that the
	// later D.1 dependency check can compare against it.  Not consumed
	// by the wrapper.
	destructedVersion Version //nolint:unused // documents the bridge; consumed inside core only

	err error
}

// versionedReadCore is the non-generic body that drives the read.
// Typed wrappers (readBalance, readNonce, readState, …) consume the
// result.  See readPathOutcome for the outcome enumeration.
//
// skipStorage=true: do not attempt a storage-read fallback when the
// in-memory tiers miss.  This is the analogue of the legacy
// `readStorage==nil` branch and is used by:
//   - refresh* wrappers, which want the caller's defaultV on miss
//   - readAccount* (AddressPath), which never resolves via getStateObject
//     because getStateObject itself recurses back into versionedReadCore
//     via getVersionedAccount → readAccount.
func versionedReadCore(s *IntraBlockState, addr accounts.Address, path AccountPath, key accounts.StorageKey, commited bool, skipStorage bool) readPathResult {
	r := readPathResult{}

	if s.versionMap == nil {
		so, err := s.getStateObject(addr, true)
		if err != nil {
			r.err = err
			r.source = StorageRead
			r.version = UnknownVersion
			return r
		}
		r.outcome = outcomeLegacyStorage
		r.so = so
		r.source = StorageRead
		r.version = UnknownVersion
		return r
	}

	if so, ok := s.stateObjects[addr]; ok && so.deleted {
		r.outcome = outcomeReturnDefault
		r.source = StorageRead
		r.version = UnknownVersion
		return r
	}

	var destructedVersion Version
	if destructed, sdRes, ok := s.versionMap.ReadSelfDestruct(addr, s.txIndex); ok && sdRes.Status() == MVReadResultDone && destructed {
		destructTxIndex := sdRes.DepIdx()
		revived := false
		if hi, ok := s.versionMap.LatestTxIndex(addr, BalancePath, accounts.NilKey, s.txIndex); ok && hi > destructTxIndex {
			revived = true
		}
		if !revived {
			if hi, ok := s.versionMap.LatestTxIndex(addr, NoncePath, accounts.NilKey, s.txIndex); ok && hi > destructTxIndex {
				revived = true
			}
		}
		if !revived {
			if hi, ok := s.versionMap.LatestTxIndex(addr, CodeHashPath, accounts.NilKey, s.txIndex); ok && hi > destructTxIndex {
				revived = true
			}
		}
		if !revived && path != CodePath {
			sdVersion := Version{TxIndex: destructTxIndex, Incarnation: sdRes.Incarnation()}
			if commited {
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = sdVersion
				return r
			}
			if vw, ok := s.versionedWrite(addr, SelfDestructPath, key); !ok || vw.ValBool {
				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(VersionedRead{
					Address: addr,
					Path:    SelfDestructPath,
					Key:     accounts.NilKey,
					Source:  MapRead,
					Version: sdVersion,
					ValBool: true,
				})
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = sdVersion
				return r
			}
			// vw exists and vw.ValBool == false: fall through with
			// destructedVersion recorded so the D.1 dep check can use it.
			destructedVersion = Version{TxIndex: destructTxIndex}
		}
	}

	res := s.versionMap.Read(addr, path, key, s.txIndex)

	vr := VersionedRead{
		Address: addr,
		Path:    path,
		Key:     key,
		Version: Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()},
	}

	if !commited {
		if vw, ok := s.versionedWrite(addr, path, key); ok {
			if res.Status() == MVReadResultDone {
				if pr, prOK := s.versionedReads[addr][AccountKey{Path: path, Key: key}]; prOK {
					if vr.Version.TxIndex > destructedVersion.TxIndex && vr.Version != pr.Version {
						if vr.Version.TxIndex > s.dep {
							s.dep = vr.Version.TxIndex
						}
						if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
							fmt.Printf("%d (%d.%d) WR DEP (%d.%d)!=(%d.%d) %x %s: %s\n",
								s.blockNum, s.txIndex, s.version,
								pr.Version.TxIndex, pr.Version.Incarnation,
								vr.Version.TxIndex, vr.Version.Incarnation,
								addr, AccountKey{path, key}, valueStringFromVR(&pr))
						}
						if s.versionedReads == nil {
							s.versionedReads = ReadSet{}
						}
						s.versionedReads.Set(vr)
						panic(ErrDependency)
					}
				}
			}
			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD (%s) %x %s: %s\n",
					s.blockNum, s.txIndex, s.version, WriteSetRead,
					addr, AccountKey{path, key}, valueStringFromVW(&vw))
			}
			r.outcome = outcomeWriteSetHit
			r.vw = &vw
			r.source = WriteSetRead
			r.version = Version{TxIndex: s.txIndex, Incarnation: s.version}
			return r
		}
	}

	switch res.Status() {
	case MVReadResultDone:
		vr.Source = MapRead
		if pr, ok := s.versionedReads[addr][AccountKey{Path: path, Key: key}]; ok {
			if pr.Version == vr.Version {
				if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) RD (%s:%s) %x %s: %s\n",
						s.blockNum, s.txIndex, s.version, MapRead, res.DepString(),
						addr, AccountKey{path, key}, valueStringFromVR(&pr))
				}
				r.outcome = outcomeReadSetHit
				r.pr = &pr
				r.source = MapRead
				r.version = vr.Version
				return r
			}
			if vr.Version.TxIndex > s.dep {
				s.dep = vr.Version.TxIndex
			}
			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD DEP (%d.%d)!=(%d.%d) %x %s\n",
					s.blockNum, s.txIndex, s.version,
					pr.Version.TxIndex, pr.Version.Incarnation,
					vr.Version.TxIndex, vr.Version.Incarnation,
					addr, AccountKey{path, key})
			}
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(vr)
			panic(ErrDependency)
		}
		// CodePath trumped by SelfDestruct at >= DepIdx (E.3a)
		if path == CodePath {
			if destructed, sdres, ok := s.versionMap.ReadSelfDestruct(addr, s.txIndex); ok && sdres.Status() == MVReadResultDone && destructed && sdres.DepIdx() >= res.DepIdx() {
				r.outcome = outcomeReturnDefault
				r.source = MapRead
				r.version = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
				return r
			}
		}
		r.outcome = outcomeMapDone
		r.mapRes = res
		r.vrSkel = vr
		r.recordVR = true
		r.source = MapRead
		r.version = vr.Version
		return r

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
		vr.Source = MapRead
		if s.versionedReads == nil {
			s.versionedReads = ReadSet{}
		}
		s.versionedReads.Set(vr)
		panic(ErrDependency)

	case MVReadResultNone:
		if vreads := s.versionedReads; !commited && vreads != nil {
			if pr, ok := vreads[addr][AccountKey{Path: path, Key: key}]; ok {
				if pr.Version == vr.Version {
					if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d (%d.%d) RD (%s) %x %s: %s\n",
							s.blockNum, s.txIndex, s.version, ReadSetRead,
							addr, AccountKey{path, key}, valueStringFromVR(&pr))
					}
					r.outcome = outcomeReadSetHit
					r.pr = &pr
					r.source = ReadSetRead
					r.version = pr.Version
					return r
				}
				if pr.Source == MapRead {
					if path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath {
						_, accSource, accVersion, _ := readAccountInternal(s, addr)
						if accSource == pr.Source && accVersion == pr.Version {
							r.outcome = outcomeReadSetHit
							r.pr = &pr
							r.source = ReadSetRead
							r.version = pr.Version
							return r
						}
					}
					if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d (%d.%d) RM DEP FALLTHROUGH (%d.%d)!=(%d.%d) %x %s\n",
							s.blockNum, s.txIndex, s.version,
							pr.Version.TxIndex, pr.Version.Incarnation,
							vr.Version.TxIndex, vr.Version.Incarnation,
							addr, AccountKey{path, key})
					}
					// Fall through to storage read.
				}
			}
		}

		// StoragePath: zero out unwritten slots when prior tx wrote Incarnation.
		if path == StoragePath {
			if inc, incRes, incOK := s.versionMap.ReadIncarnation(addr, s.txIndex); incOK && incRes.Status() == MVReadResultDone {
				vr.Source = StorageRead
				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(vr)
				incVersion := Version{TxIndex: incRes.DepIdx(), Incarnation: incRes.Incarnation()}
				s.versionedReads.Set(VersionedRead{
					Address: addr,
					Path:    IncarnationPath,
					Key:     accounts.NilKey,
					Source:  MapRead,
					Version: incVersion,
					ValU64:  inc,
				})
				if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) RD (%s) %x %s: zero (IncarnationPath written by tx %d)\n",
						s.blockNum, s.txIndex, s.version, StorageRead,
						addr, AccountKey{path, key}, incRes.DepIdx())
				}
				r.outcome = outcomeReturnZero
				r.source = StorageRead
				r.version = UnknownVersion
				return r
			}
		}

		// skipStorage: callers that don't want a storage fallback (the
		// refresh* wrappers + AddressPath internal callers).  Record the
		// VR for ValidateVersion and return.  Preserves the legacy
		// readStorage==nil semantics (versionedio.go:1280-1302 in the
		// generic versionedRead).
		if skipStorage {
			if !commited && path != CodePath {
				vr.Source = StorageRead
				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(vr)
			}
			r.outcome = outcomeReturnDefault
			r.source = UnknownSource
			r.version = UnknownVersion
			return r
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
				return r
			}
			if readAccount != nil {
				vr.Source = accSource
				vr.Version = accVersion
				so = newObject(s, addr, readAccount, readAccount)
			}
		}
		if so == nil {
			vr.Source = StorageRead
			obj, err := s.getStateObject(addr, true)
			if err != nil {
				r.err = err
				r.outcome = outcomeReturnDefault
				r.source = StorageRead
				r.version = UnknownVersion
				return r
			}
			so = obj
		}
		r.outcome = outcomeStorageRead
		r.so = so
		r.vrSkel = vr
		r.recordVR = true
		r.source = vr.Source
		r.version = vr.Version
		return r
	}

	r.outcome = outcomeReturnDefault
	r.source = UnknownSource
	r.version = UnknownVersion
	return r
}

// readAccountInternal performs an AddressPath versionedReadCore + typed
// extraction of *accounts.Account.  Used internally by versionedReadCore
// to resolve sibling-account reads (the G.2 and G.7 branches) without
// taking a typed callback.
func readAccountInternal(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, AddressPath, accounts.NilKey, false, true)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValAcc, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValAcc, r.source, r.version, nil
	case outcomeMapDone:
		acc, ok := r.mapRes.Value().(*accounts.Account)
		if !ok {
			return nil, UnknownSource, r.version, fmt.Errorf("versionedRead AddressPath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValAcc = acc
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return acc, r.source, r.version, nil
	default:
		// outcomeReturnDefault / outcomeReturnZero / outcomeStorageRead /
		// outcomeLegacyStorage: no in-memory tier hit.  AddressPath
		// callers (sibling-revival check) treat as nil.
		return nil, r.source, r.version, nil
	}
}

// readBalance returns the address's balance using the version-aware
// read pipeline.  Inlines the storage-read fallback.
func readBalance(s *IntraBlockState, addr accounts.Address) (uint256.Int, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, BalancePath, accounts.NilKey, false, false)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU256, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU256, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint256.Int)
		if !ok {
			return uint256.Int{}, UnknownSource, r.version, fmt.Errorf("versionedRead BalancePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint256.Int
		if r.so != nil && !r.so.deleted {
			v = r.so.Balance()
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, nil
		}
		return r.so.Balance(), StorageRead, UnknownVersion, nil
	}
	// outcomeReturnZero / outcomeReturnDefault → zero
	return uint256.Int{}, r.source, r.version, nil
}

// refreshBalance is the in-memory-only variant used by
// refreshVersionedAccount.  Returns currentBalance on miss; does not
// record the read or perform a storage fallback.
func refreshBalance(s *IntraBlockState, addr accounts.Address, currentBalance uint256.Int) (uint256.Int, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, BalancePath, accounts.NilKey, false, true)
	if r.err != nil {
		return currentBalance, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU256, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU256, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint256.Int)
		if !ok {
			return currentBalance, UnknownSource, r.version, fmt.Errorf("versionedRead BalancePath: unexpected value type %T", r.mapRes.Value())
		}
		return v, r.source, r.version, nil
	}
	return currentBalance, r.source, r.version, nil
}

// readNonce returns the nonce using the version-aware read pipeline.
func readNonce(s *IntraBlockState, addr accounts.Address) (uint64, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, NoncePath, accounts.NilKey, false, false)
	if r.err != nil {
		return 0, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU64, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU64, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint64)
		if !ok {
			return 0, UnknownSource, r.version, fmt.Errorf("versionedRead NoncePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValU64 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint64
		if r.so != nil && !r.so.deleted {
			v = r.so.Nonce()
		}
		if r.recordVR {
			r.vrSkel.ValU64 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return 0, StorageRead, UnknownVersion, nil
		}
		return r.so.Nonce(), StorageRead, UnknownVersion, nil
	}
	return 0, r.source, r.version, nil
}

func refreshNonce(s *IntraBlockState, addr accounts.Address, currentNonce uint64) (uint64, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, NoncePath, accounts.NilKey, false, true)
	if r.err != nil {
		return currentNonce, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU64, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU64, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint64)
		if !ok {
			return currentNonce, UnknownSource, r.version, fmt.Errorf("versionedRead NoncePath: unexpected value type %T", r.mapRes.Value())
		}
		return v, r.source, r.version, nil
	}
	return currentNonce, r.source, r.version, nil
}

// readIncarnation returns the incarnation counter.
func readIncarnation(s *IntraBlockState, addr accounts.Address) (uint64, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, IncarnationPath, accounts.NilKey, false, false)
	if r.err != nil {
		return 0, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU64, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU64, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint64)
		if !ok {
			return 0, UnknownSource, r.version, fmt.Errorf("versionedRead IncarnationPath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValU64 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint64
		if r.so != nil && !r.so.deleted {
			v = r.so.data.Incarnation
		}
		if r.recordVR {
			r.vrSkel.ValU64 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return 0, StorageRead, UnknownVersion, nil
		}
		return r.so.data.Incarnation, StorageRead, UnknownVersion, nil
	}
	return 0, r.source, r.version, nil
}

func refreshIncarnation(s *IntraBlockState, addr accounts.Address, currentIncarnation uint64) (uint64, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, IncarnationPath, accounts.NilKey, false, true)
	if r.err != nil {
		return currentIncarnation, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU64, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU64, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint64)
		if !ok {
			return currentIncarnation, UnknownSource, r.version, fmt.Errorf("versionedRead IncarnationPath: unexpected value type %T", r.mapRes.Value())
		}
		return v, r.source, r.version, nil
	}
	return currentIncarnation, r.source, r.version, nil
}

// readCode returns the contract code. The commited flag selects whether
// the version-aware lookup honours the committed-only contract.
func readCode(s *IntraBlockState, addr accounts.Address, commited bool) ([]byte, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, CodePath, accounts.NilKey, commited, false)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValBytes, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValBytes, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().([]byte)
		if !ok {
			return nil, UnknownSource, r.version, fmt.Errorf("versionedRead CodePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValBytes = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v []byte
		if r.so != nil && !r.so.deleted {
			code, err := r.so.Code()
			if err != nil {
				return nil, StorageRead, UnknownVersion, err
			}
			v = code
		}
		// CodePath never records via setVRVal in the legacy path
		// (see the readStorage==nil guard in versionedRead) — the
		// recorder check below mirrors that.  For non-nil readStorage
		// the original recorded via copyV; we preserve that.
		if r.recordVR {
			r.vrSkel.ValBytes = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return nil, StorageRead, UnknownVersion, nil
		}
		code, err := r.so.Code()
		return code, StorageRead, UnknownVersion, err
	}
	return nil, r.source, r.version, nil
}

// refreshCode is the in-memory-only variant for CodePath.
func refreshCode(s *IntraBlockState, addr accounts.Address) ([]byte, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, CodePath, accounts.NilKey, false, true)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValBytes, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValBytes, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().([]byte)
		if !ok {
			return nil, UnknownSource, r.version, fmt.Errorf("versionedRead CodePath: unexpected value type %T", r.mapRes.Value())
		}
		return v, r.source, r.version, nil
	}
	return nil, r.source, r.version, nil
}

// readCodeSize returns the contract code size.
func readCodeSize(s *IntraBlockState, addr accounts.Address) (int, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, CodeSizePath, accounts.NilKey, false, false)
	if r.err != nil {
		return 0, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValInt, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValInt, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(int)
		if !ok {
			return 0, UnknownSource, r.version, fmt.Errorf("versionedRead CodeSizePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValInt = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		// CodeSizePath delegates to the per-stateObject code-size
		// pattern: prefer cached so.code length, else load full code
		// once (geth-style) and populate so.code.
		v := codeSizeFromStateObject(s, r.so, addr)
		if r.recordVR {
			r.vrSkel.ValInt = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		v := codeSizeFromStateObject(s, r.so, addr)
		return v, StorageRead, UnknownVersion, nil
	}
	return 0, r.source, r.version, nil
}

// readCodeHash returns the contract code hash.
func readCodeHash(s *IntraBlockState, addr accounts.Address) (accounts.CodeHash, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, CodeHashPath, accounts.NilKey, false, false)
	if r.err != nil {
		return accounts.NilCodeHash, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValHash, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValHash, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(accounts.CodeHash)
		if !ok {
			return accounts.NilCodeHash, UnknownSource, r.version, fmt.Errorf("versionedRead CodeHashPath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValHash = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
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
			r.vrSkel.ValHash = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return accounts.NilCodeHash, StorageRead, UnknownVersion, nil
		}
		return r.so.data.CodeHash, StorageRead, UnknownVersion, nil
	}
	return accounts.NilCodeHash, r.source, r.version, nil
}

// refreshCodeHash is the in-memory-only variant for CodeHashPath.
func refreshCodeHash(s *IntraBlockState, addr accounts.Address, currentHash accounts.CodeHash) (accounts.CodeHash, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, CodeHashPath, accounts.NilKey, false, true)
	if r.err != nil {
		return currentHash, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValHash, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValHash, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(accounts.CodeHash)
		if !ok {
			return currentHash, UnknownSource, r.version, fmt.Errorf("versionedRead CodeHashPath: unexpected value type %T", r.mapRes.Value())
		}
		return v, r.source, r.version, nil
	}
	return currentHash, r.source, r.version, nil
}

// readState reads a storage slot.
func readState(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, StoragePath, key, false, false)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU256, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU256, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint256.Int)
		if !ok {
			return uint256.Int{}, UnknownSource, r.version, fmt.Errorf("versionedRead StoragePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint256.Int
		if r.so != nil && !r.so.deleted {
			v, _ = r.so.GetState(key)
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, nil
		}
		v, _ := r.so.GetState(key)
		return v, StorageRead, UnknownVersion, nil
	}
	return uint256.Int{}, r.source, r.version, nil
}

// readStateForSet is the SetState-specific variant.  Returns the
// additional "clean" bool (the second return of stateObject.GetState),
// which SetState uses to decide between deleting vs. updating the
// versioned write on revert.
func readStateForSet(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, bool, error) {
	r := versionedReadCore(s, addr, StoragePath, key, false, false)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, false, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU256, r.source, r.version, false, nil
	case outcomeReadSetHit:
		return r.pr.ValU256, r.source, r.version, false, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint256.Int)
		if !ok {
			return uint256.Int{}, UnknownSource, r.version, false, fmt.Errorf("versionedRead StoragePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, false, nil
	case outcomeStorageRead:
		var v uint256.Int
		var clean bool
		if r.so != nil && !r.so.deleted {
			v, clean = r.so.GetState(key)
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, clean, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, false, nil
		}
		v, clean := r.so.GetState(key)
		return v, StorageRead, UnknownVersion, clean, nil
	}
	return uint256.Int{}, r.source, r.version, false, nil
}

// readCommittedState reads a storage slot with committed-view semantics.
func readCommittedState(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, StoragePath, key, true, false)
	if r.err != nil {
		return uint256.Int{}, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValU256, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValU256, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(uint256.Int)
		if !ok {
			return uint256.Int{}, UnknownSource, r.version, fmt.Errorf("versionedRead StoragePath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeStorageRead:
		var v uint256.Int
		if r.so != nil && !r.so.deleted {
			cv, err := r.so.GetCommittedState(key)
			if err != nil {
				return uint256.Int{}, StorageRead, UnknownVersion, err
			}
			v = cv
		}
		if r.recordVR {
			r.vrSkel.ValU256 = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
		}
		return v, r.source, r.version, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, nil
		}
		v, err := r.so.GetCommittedState(key)
		return v, StorageRead, UnknownVersion, err
	}
	return uint256.Int{}, r.source, r.version, nil
}

// readSelfDestruct returns whether the account is selfdestructed.
func readSelfDestruct(s *IntraBlockState, addr accounts.Address) (bool, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, SelfDestructPath, accounts.NilKey, false, false)
	if r.err != nil {
		return false, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValBool, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValBool, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(bool)
		if !ok {
			return false, UnknownSource, r.version, fmt.Errorf("versionedRead SelfDestructPath: unexpected value type %T", r.mapRes.Value())
		}
		if r.recordVR {
			r.vrSkel.ValBool = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
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
			r.vrSkel.ValBool = v
			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}
			s.versionedReads.Set(r.vrSkel)
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
	}
	return false, r.source, r.version, nil
}

// refreshSelfDestruct is the in-memory-only variant.
func refreshSelfDestruct(s *IntraBlockState, addr accounts.Address) (bool, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, SelfDestructPath, accounts.NilKey, false, true)
	if r.err != nil {
		return false, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValBool, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValBool, r.source, r.version, nil
	case outcomeMapDone:
		v, ok := r.mapRes.Value().(bool)
		if !ok {
			return false, UnknownSource, r.version, fmt.Errorf("versionedRead SelfDestructPath: unexpected value type %T", r.mapRes.Value())
		}
		return v, r.source, r.version, nil
	}
	return false, r.source, r.version, nil
}

// readAccount returns the *accounts.Account for an address.
func readAccount(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	return readAccountInternal(s, addr)
}

// refreshAccount is the in-memory-only variant for AddressPath.
func refreshAccount(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	r := versionedReadCore(s, addr, AddressPath, accounts.NilKey, false, true)
	if r.err != nil {
		return nil, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return r.vw.ValAcc, r.source, r.version, nil
	case outcomeReadSetHit:
		return r.pr.ValAcc, r.source, r.version, nil
	case outcomeMapDone:
		acc, ok := r.mapRes.Value().(*accounts.Account)
		if !ok {
			return nil, UnknownSource, r.version, fmt.Errorf("versionedRead AddressPath: unexpected value type %T", r.mapRes.Value())
		}
		return acc, r.source, r.version, nil
	}
	return nil, r.source, r.version, nil
}
