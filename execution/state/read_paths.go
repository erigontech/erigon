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

// originIndex is the versionMap slot for the pre-block committed base ("origin" =
// all account fields at once). It sits below every real task index (the -1 block-init
// task reads at floor -2) and above UnknownDep (-3), so readFloor never confuses it
// with a missing cell and lifecycle checks treat it as an inert baseline.
const originIndex = -2

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
	// Size-only read for stateless-witness correctness: a witness node has the
	// size but not the bytes, so ReadAccountCode would report EXTCODESIZE 0.
	size, err := sdb.stateReader.ReadAccountCodeSize(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.codeReadDuration += time.Since(readStart)
		sdb.codeReadCount++
	}
	sdb.stateReader.SetTrace(false, "")
	return size, err
}

// committedStorageDirect reads a storage slot's committed value straight from the
// state reader, no stateObject. A contract this tx created (own CreateContract cell)
// has fresh storage, so a cold slot reads zero rather than a prior incarnation's value.
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
// state reader, no stateObject. A contract this tx created has no code until SetCode
// runs, so it reads empty rather than a prior incarnation's bytes.
func (sdb *IntraBlockState) committedCodeDirect(addr accounts.Address) ([]byte, error) {
	if cc, ok := sdb.versionedWriteCreateContract(addr); ok && cc {
		return nil, nil
	}
	codeHash, err := sdb.committedCodeHash(addr)
	if err != nil {
		return nil, err
	}
	if codeHash.IsEmpty() {
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

// codeSeed returns the code this tx currently sees at addr — its own Code write cell
// if any, else the committed value — without recording an OCC read. Lets SetCode on the
// rebuilt noMaterialize transient compare against current code, not the stale tx-start value.
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
// recording an OCC read.
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

// committedCodeSizeDirect reads an account's committed code size straight from the
// state reader, no stateObject. Size-only for stateless-witness correctness (a witness
// node carries the size but not the bytes).
func (sdb *IntraBlockState) committedCodeSizeDirect(addr accounts.Address) (int, error) {
	if cc, ok := sdb.versionedWriteCreateContract(addr); ok && cc {
		return 0, nil
	}
	codeHash, err := sdb.committedCodeHash(addr)
	if err != nil {
		return 0, err
	}
	if codeHash.IsEmpty() {
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

// readPathOutcome tells a typed wrapper which source to read the path-typed value from.
type readPathOutcome uint8

const (
	_ readPathOutcome = iota // unset (zero value)

	outcomeLegacyStorage // versionMap == nil: typed wrapper does direct storage read on r.so
	outcomeWriteSetHit   // r.vw is set; typed wrapper returns its Val*
	outcomeMapDone       // versionMap hit; the path's typed map*Val field carries the value
	outcomeReadSetHit    // a prior read matched; typed wrapper re-fetches it via GetX
	outcomeStorageRead   // r.so resolved; wrapper does typed storage read + records r.hdr
	outcomeReturnZero    // typed wrapper returns the path-typed zero value (account absent)
	outcomeReturnEmpty   // typed wrapper returns the path-typed empty value (account exists, field wiped): EmptyCodeHash for CodeHashPath, otherwise the zero value
	outcomeReturnDefault // typed wrapper returns its caller-supplied defaultV
)

// readPathResult communicates the outcome of versionedReadCore to a typed wrapper.
// Exactly one source field is populated for the tier-hit outcomes; the wrapper does
// the typed extraction and records the read via ReadSet.SetX when r.recordVR is true.
type readPathResult struct {
	outcome readPathOutcome

	// Per-typed write pointers (only the read path's is set on outcomeWriteSetHit)
	// avoid the any-boxing a single AnyVersionedWrite field would force on the hot path.
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

	// account carries the composed base account for the four account-field paths
	// (Balance/Nonce/Incarnation/CodeHash) when it resolved from the versionMap, so
	// the wrapper extracts the field with no stateObject alloc. Set only for a live
	// account; the wrapper normalizes CodeHash (empty→EmptyCodeHash) to match newObject.
	account *accounts.Account

	// Typed map-read values let the wrapper read its path's field directly, avoiding the
	// any-box a generic value would impose. Value, not *WriteCell, so reads are race-free
	// against a concurrent FlushVersionedWrites mutating cell.Value.
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

	// hdr is the skeleton header the wrapper records (with its typed value) when recordVR is true.
	hdr      ReadHeader
	recordVR bool

	source  ReadSource
	version Version

	err error
}

// wipedFieldOutcome distinguishes how a field wiped by a self-destruct reads back:
// outcomeReturnEmpty when the account was revived (a field with no post-destruct write
// reads as its empty value — EmptyCodeHash for the code hash), else outcomeReturnZero
// when the account is absent (reads as the zero value).
func wipedFieldOutcome(s *IntraBlockState, addr accounts.Address) readPathOutcome {
	if state, _, _ := s.versionMap.AccountLifecycleAt(addr, s.txIndex); state == LifecycleRevived {
		return outcomeReturnEmpty
	}
	return outcomeReturnZero
}

// versionedReadCore drives the type-independent part of a versionMap-aware read
// (writeSet/versionMap/readSet tier probes plus destruct/revival logic); typed
// wrappers consume the result. skipStorage suppresses the storage-read fallback on a
// miss, for callers that resolve the value themselves. Result is written into *r
// (caller stack, passed pre-zeroed) to avoid a return-value copy per read.
func versionedReadCore(s *IntraBlockState, addr accounts.Address, path AccountPath, key accounts.StorageKey, commited bool, skipStorage bool, r *readPathResult) {
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
		// When the deletion reflects a prior tx's selfdestruct, surface the SD version
		// rather than UnknownVersion so records match later SD-zero-path reads and
		// don't force a version conflict.
		if destructed, sdRes, ok := s.readSelfDestructMemo(addr); ok && sdRes.resolved() && destructed {
			sdVer := Version{TxIndex: sdRes.DepIdx(), Incarnation: sdRes.Incarnation()}
			if !commited {
				s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: sdVer},
					Val:        true,
				})
			}
			r.outcome = wipedFieldOutcome(s, addr)
			r.source = MapRead
			r.version = sdVer
			return
		}
		r.outcome = outcomeReturnDefault
		r.source = StorageRead
		r.version = UnknownVersion
		return
	}

	// Block-STM read-once: a value already recorded for (addr, path, key) this attempt
	// must be returned unchanged. Re-resolving and re-recording would let a later-diverged
	// writer overwrite the recorded read, so seal-time re-validation would compare the
	// re-resolved version against itself and miss the divergence. Per-cell, not per-address:
	// only the tx's own write to this exact cell takes precedence (via versionedWriteHit below).
	if !commited {
		if !s.versionedWrites.Has(WriteHeader{Address: addr, Path: path, Key: key}) {
			if prHeader, ok := s.versionedReads.getHeader(addr, path, key); ok &&
				(prHeader.Source == MapRead || prHeader.Source == StorageRead) {
				r.outcome = outcomeReadSetHit
				r.source = prHeader.Source
				r.version = prHeader.Version
				return
			}
		}
	}

	var destructedVersion Version
	if destructed, sdRes, ok := s.readSelfDestructMemo(addr); ok && sdRes.resolved() && destructed {
		destructTxIndex := sdRes.DepIdx()
		// A tx always observes its own same-tx write, even after a prior tx's self-destruct.
		if !commited {
			if hasWrite := s.versionedWriteHit(addr, path, key, r); hasWrite {
				r.outcome = outcomeWriteSetHit
				r.source = WriteSetRead
				r.version = Version{TxIndex: s.txIndex, Incarnation: s.version}
				return
			}
		}
		// Per-path revival: the field is revived only if a write to THIS path exists at a
		// strictly higher TxIndex, so a field with no post-SD write reads as the fresh zero.
		revived := false
		pathRead := s.versionMap.ReadStatus(addr, path, key, s.txIndex)
		if pathRead.DepIdx() > destructTxIndex &&
			(pathRead.resolved() || pathRead.Status() == MVReadResultDependency) {
			revived = true
		}
		if !revived && path != CodePath {
			sdVersion := Version{TxIndex: destructTxIndex, Incarnation: sdRes.Incarnation()}
			if s.eip8246 && path == BalancePath {
				// EIP-8246 preserves only the balance across SELFDESTRUCT; code, nonce and
				// incarnation clear as in a normal destruct (they take the wiped path below).
				s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: sdVersion},
					Val:        true,
				})
			} else {
				if commited {
					r.outcome = wipedFieldOutcome(s, addr)
					r.source = MapRead
					r.version = sdVersion
					return
				}
				// The own-write lookup is keyed by `key`: a StoragePath read (key=slot)
				// never matches the per-address SelfDestructPath write (NilKey), so the slot
				// reads post-SD zero. Only an account-field read (key=NilKey) consults the SD
				// own-write; a same-tx SelfDestructPath=false there means revived.
				sd, sdOK := false, false
				if key == accounts.NilKey {
					sd, sdOK = s.versionedWriteSelfDestruct(addr)
				}
				if !sdOK || sd {
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: sdVersion},
						Val:        true,
					})
					// Record the wiped-slot zero anchored on the destruct so validation
					// checks the destruct dependency, not the stale pre-destruct floor.
					if path == StoragePath {
						s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{
							ReadHeader: ReadHeader{Source: MapRead, Version: sdVersion},
							Val:        uint256.Int{},
						})
					}
					r.outcome = wipedFieldOutcome(s, addr)
					r.source = MapRead
					r.version = sdVersion
					return
				}
				// Revived (SelfDestructPath=false): fall through, recording
				// destructedVersion for the stale-readSet dependency check.
				destructedVersion = Version{TxIndex: destructTxIndex}
			}
		}
	}

	// Typed ReadX returns T directly plus a ReadResult, never an any-boxed value (a
	// heap alloc per non-storage read). On a miss it returns a pre-seeded (UnknownDep,
	// -1) result, so res is always valid.
	var res ReadResult
reread:
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
						if s.waitCommit != nil && s.waitCommit(hdr.Version.TxIndex) {
							goto reread
						}
						// Shutdown / no pause hook: fall through to the tx's own write.
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
	case MVReadResultDone, MVReadResultValidated:
		hdr.Source = MapRead
		if prHeader, ok := s.versionedReads.getHeader(addr, path, key); ok {
			// Base-state read-once PIN: a slot already read this execution returns the
			// SAME pinned dependency even if the floor has since moved. Re-resolving would
			// rewrite the recorded dep to the surviving version and hide a consumed-but-
			// abandoned value from seal validation; pinning lets validation catch the
			// version change and re-execute.
			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD (%s:%s) %x %s\n",
					s.blockNum, s.txIndex, s.version, MapRead, res.DepString(),
					addr, AccountKey{path, key})
			}
			r.outcome = outcomeReadSetHit
			r.source = MapRead
			r.version = prHeader.Version
			return
		}
		// Code/code-size written before an in-block SELFDESTRUCT is wiped; a revival
		// that rewrote no code must not resurrect it. Use the destruct history, not
		// the latest SelfDestruct cell (which a revival sets false), and anchor on
		// canonicalVer like the storage path so a revival above the wipe does not
		// livelock validation.
		if path == CodePath || path == CodeSizePath {
			if state, canonicalVer, destroyedAt := s.versionMap.AccountLifecycleAt(addr, s.txIndex); state != LifecycleLive && hdr.Version.TxIndex <= destroyedAt {
				if !commited {
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: canonicalVer},
						Val:        true,
					})
				}
				r.outcome = outcomeReturnDefault
				r.source = MapRead
				r.version = canonicalVer
				return
			}
		}
		// A Done storage cell written before an in-block SELFDESTRUCT is stale: the
		// destruct wipes the slot and a recreate leaves it unwritten, so a later read
		// must see zero. Anchor the dependency on canonicalVer (the latest SD cell the
		// validator resolves), never the wipe — a revival sitting above the wipe would
		// otherwise make validation disagree forever and livelock.
		if path == StoragePath {
			if state, canonicalVer, destroyedAt := s.versionMap.AccountLifecycleAt(addr, s.txIndex); state != LifecycleLive && hdr.Version.TxIndex <= destroyedAt {
				if !commited {
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: canonicalVer},
						Val:        true,
					})
					// Not recorded as a storage read: for a revived account canonicalVer
					// is the revival cell, whose incarnation shifts as that tx re-executes,
					// so anchoring here never settles (self-loop). The genuinely-absent
					// case anchors on the stable destruct and does record it.
				}
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = canonicalVer
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
		// The predecessor write is in flight (Estimate): wait for it to commit, then
		// re-read the now-committed cell rather than aborting. waitCommit returns false
		// only on shutdown; callers with no pause hook (serial/historical) never observe
		// an Estimate and fall back to the in-flight value.
		if s.waitCommit != nil && s.waitCommit(res.DepIdx()) {
			goto reread
		}
		r.outcome = outcomeMapDone
		r.hdr = hdr
		r.recordVR = true
		r.source = MapRead
		r.version = hdr.Version
		return

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
					// Torn read: a prior version-map read of this key at prHeader.Version
					// no longer resolves (the writer's cell was superseded mid-execution),
					// so this tx saw two states of the writer, not a settled snapshot. Depend
					// on the vanished writer to force re-execution rather than committing a
					// torn result the version-only validator can't catch. Only a real (>=0)
					// writer forces the re-exec.
					if prHeader.Version.TxIndex > s.dep {
						s.dep = prHeader.Version.TxIndex
					}
					// Fall through to storage read.
				}
			}
		}

		// A self-destructed account's per-account field with no versionMap cell reads
		// post-SD zero, anchored on the SelfDestructPath entry; a bare StorageRead/
		// UnknownVersion would be rejected by the validator cross-check and loop.
		if path == BalancePath || path == NoncePath || path == IncarnationPath ||
			path == CodeHashPath || path == CodePath || path == CodeSizePath {
			if destructed, sd, ok := s.versionMap.ReadSelfDestruct(addr, s.txIndex); ok && sd.resolved() && destructed {
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

		// A prior tx that bumped Incarnation cleared the old incarnation's storage and
		// code; read empty when no newer cell exists (a revival writes its own cell,
		// returned by the version-map read above before reaching here).
		if path == StoragePath || path == CodePath || path == CodeSizePath {
			if inc, incRes, incOK := s.versionMap.ReadIncarnation(addr, s.txIndex); incOK && incRes.resolved() {
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

		// skipStorage: signal the wrapper to record a header-only read for ValidateVersion.
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
				// readAccountInternal returns non-nil only for a live account, so the
				// wrapper reads the field directly with no stateObject alloc or deleted check.
				r.account = readAccount
			}
		}
		// Cold committed storage read: resolve directly from the state reader without
		// materializing a stateObject, reusing one only if a write this tx already made it.
		if path == StoragePath {
			hdr.Source = StorageRead
			if cached, ok := s.stateObjects[addr]; ok {
				so = cached
			} else {
				// A cold slot depends only on its own StoragePath cell — recording an
				// AddressPath dependency would be a false dep.
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
		// Cold code / code-size read: resolve directly from the state reader without
		// materializing a stateObject, reusing a cached one when present. Code paths
		// record only their own dependency (no false AddressPath dep).
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
		if so == nil && r.account == nil {
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
		// A field resolved from this tx's own AddressPath write (WriteSetRead) carries no
		// cross-tx dependency; recording it would make the validator (floored below the
		// tx's own writes) return None and wrongly invalidate the tx.
		r.recordVR = hdr.Source != WriteSetRead
		r.source = hdr.Source
		r.version = hdr.Version
		return
	}

	r.outcome = outcomeReturnDefault
	r.source = UnknownSource
	r.version = UnknownVersion
}

// readAccountInternal performs an AddressPath versionedReadCore + typed extraction
// of *accounts.Account, for sibling-account reads that take no typed callback.
func readAccountInternal(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetAddress(addr); ok && warmSource(tr.Source) {
			if tr.Val != nil {
				return tr.Val.Account(), tr.Source, tr.Version, nil
			}
			return nil, tr.Source, tr.Version, nil
		}
	}
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
		// A hit on the seeded origin is the committed base; apply the in-block destruct
		// gate. A destructed origin reads absent and is NOT recorded as an AddressPath read
		// (its SD dependency travels on the field reads); recording it would trip the origin
		// cross-check against the destruct's Incarnation cell and invalidate the tx forever.
		if r.version.TxIndex == originIndex && gateOriginAccount(s, addr, acc) == nil {
			return nil, r.source, r.version, nil
		}
		if r.recordVR {
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{r.hdr, NewAccountView(acc)})
		}
		return acc, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnEmpty:
		// Absent because a prior tx self-destructed it; the SD dependency is already
		// recorded. Return absent WITHOUT re-seeding the committed origin — a stale origin
		// read would be invalidated forever by the same-tx create+destruct cross-check
		// (same hazard the outcomeMapDone gate above avoids).
		return nil, r.source, r.version, nil
	case outcomeReturnDefault:
		// versionMap miss: the single point where the committed whole-account origin
		// enters the execution store.
		if acc, src, ver, seeded, err := seedOrigin(s, addr); seeded {
			return acc, src, ver, err
		}
		// The skipStorage branch may carry recordVR=true; AddressPath defaultV is nil.
		if r.recordVR {
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: r.hdr})
		}
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readAccountInternal: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// SeedOrigin publishes acc as addr's committed origin (at originIndex) for the one
// write path that obtains a committed account outside the read path: calcFees credits
// the fee recipients' Balance via a versionedStateReader with no seeding read, so
// without this their whole-account base is absent and the apply compose wipes it.
func SeedOrigin(vm *VersionMap, addr accounts.Address, acc *accounts.Account) {
	if vm == nil || acc == nil {
		return
	}
	origin := *acc
	vm.WriteOriginAddressOnce(addr, &origin)
}

// seedStorageOrigin records a cold slot's committed value as its versionMap origin
// (originIndex), mirroring seedOrigin: later reads and apply resolve the slot base from
// this one shared cell instead of each re-resolving it from the execution store, which
// left bases inconsistent across readers and unvalidatable (a bare UnknownVersion
// StorageRead is version-only-validated). Only a genuine cold committed read seeds; a
// versionMap or write-set hit is untouched. Mutates r so the seeded cell and read agree.
func seedStorageOrigin(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey, val uint256.Int, r *readPathResult) {
	if s.versionMap == nil || r.source != StorageRead || r.version != UnknownVersion {
		return
	}
	s.versionMap.WriteStorage(addr, key, Version{TxIndex: originIndex}, val, true)
	r.hdr.Version = Version{TxIndex: originIndex}
	r.version = Version{TxIndex: originIndex}
}

// gateOriginAccount applies the in-block lifecycle gate to a committed-origin account:
// if a prior tx destroyed it with no revival, it reads as absent. In-block-created
// accounts are not gated here — their lifecycle is carried by their own cells.
func gateOriginAccount(s *IntraBlockState, addr accounts.Address, acc *accounts.Account) *accounts.Account {
	if acc == nil {
		return nil
	}
	if s.versionMap.IsNetAbsent(addr, s.txIndex) {
		return nil
	}
	return acc
}

// seedOrigin handles an AddressPath versionMap miss: it reads the committed pre-block
// account once, seeds it at originIndex so later reads and apply resolve from the same
// base, and records the read at originIndex so the tx's own re-reads agree. seeded=false
// means the account does not exist committed, so the caller falls back to the miss record.
func seedOrigin(s *IntraBlockState, addr accounts.Address) (acc *accounts.Account, src ReadSource, ver Version, seeded bool, err error) {
	if s.versionMap == nil {
		return nil, UnknownSource, UnknownVersion, false, nil
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	committed, err := s.stateReader.ReadAccountData(addr)
	if dbg.KVReadLevelledMetrics {
		s.accountReadDuration += time.Since(readStart)
		s.accountReadCount++
	}
	if err != nil {
		return nil, StorageRead, UnknownVersion, true, err
	}
	if committed == nil {
		return nil, UnknownSource, UnknownVersion, false, nil
	}
	origin := *committed
	s.versionMap.WriteAddress(addr, Version{TxIndex: originIndex}, &origin, true)
	ver = Version{TxIndex: originIndex}
	// A destructed origin reads absent and is NOT recorded as an AddressPath read (its
	// SD dependency travels on the field reads). Only an alive origin is recorded, so its
	// cross-check catches a later lower-tx destruct.
	if gateOriginAccount(s, addr, &origin) == nil {
		return nil, MapRead, ver, true, nil
	}
	s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader{Source: MapRead, Version: ver}, NewAccountView(&origin)})
	return &origin, MapRead, ver, true, nil
}

// warmSource reports whether a recorded read source is a plain committed/map
// read that the wrapper-level read-once fast path can return directly.
func warmSource(src ReadSource) bool { return src == MapRead || src == StorageRead }

// warmReadable reports whether addr has no own write this tx, so a recorded read
// of it is a stable snapshot the read-once fast path can serve (own writes take
// precedence and must go through the full path). Same gate as versionedWriteHit.
func (s *IntraBlockState) warmReadable(addr accounts.Address) bool {
	_, dirty := s.journal.dirties[addr]
	return !dirty
}

// readBalance returns the address's balance using the version-aware
// read pipeline.  Inlines the storage-read fallback.
func readBalance(s *IntraBlockState, addr accounts.Address) (uint256.Int, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetBalance(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
	var r readPathResult
	versionedReadCore(s, addr, BalancePath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		s.recordStateReadError(r.err)
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
		if r.account != nil {
			v = r.account.Balance
		} else if r.so != nil && !r.so.deleted {
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		return uint256.Int{}, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readBalance: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshBalance is the in-memory-only variant: returns currentBalance on
// miss and does not perform a storage fallback.  When the core signals
// recordVR, records the read with currentBalance as the typed default.
func refreshBalance(s *IntraBlockState, addr accounts.Address, currentBalance uint256.Int) (uint256.Int, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetBalance(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
		// Record so a repeat (e.g. the next Empty()) hits the read-once fast path.
		if r.recordVR {
			s.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{r.hdr, r.mapBalanceVal})
		}
		return r.mapBalanceVal, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnEmpty:
		// Self-destructed or absent: zero, not the caller's stale pre-destruct balance
		// (outcomeReturnDefault keeps the current value; only this branch must zero it).
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
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetNonce(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
	var r readPathResult
	versionedReadCore(s, addr, NoncePath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		s.recordStateReadError(r.err)
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
		if r.account != nil {
			v = r.account.Nonce
		} else if r.so != nil && !r.so.deleted {
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		return 0, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readNonce: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func refreshNonce(s *IntraBlockState, addr accounts.Address, currentNonce uint64) (uint64, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetNonce(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
		if r.recordVR {
			s.versionedReads.SetNonce(addr, VersionedRead[uint64]{r.hdr, r.mapNonceVal})
		}
		return r.mapNonceVal, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnEmpty:
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
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetIncarnation(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
		if r.account != nil {
			v = r.account.Incarnation
		} else if r.so != nil && !r.so.deleted {
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
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
	case outcomeReturnZero, outcomeReturnEmpty:
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
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCode(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
	var r readPathResult
	versionedReadCore(s, addr, CodePath, accounts.NilKey, commited, false, &r)
	if r.err != nil {
		s.recordStateReadError(r.err)
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCode: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshCode is the in-memory-only variant for CodePath. CodePath is never recorded
// via the skipStorage branch (the `path != CodePath` guard), so the default case records nothing.
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshCode: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readCodeSize returns the contract code size.
func readCodeSize(s *IntraBlockState, addr accounts.Address) (int, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCodeSize(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
	var r readPathResult
	versionedReadCore(s, addr, CodeSizePath, accounts.NilKey, false, false, &r)
	if r.err != nil {
		s.recordStateReadError(r.err)
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		return 0, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCodeSize: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readCodeHash returns the contract code hash.
func readCodeHash(s *IntraBlockState, addr accounts.Address, commited bool) (accounts.CodeHash, ReadSource, Version, error) {
	if !commited && s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCodeHash(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
	var r readPathResult
	versionedReadCore(s, addr, CodeHashPath, accounts.NilKey, commited, false, &r)
	if r.err != nil {
		s.recordStateReadError(r.err)
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
		switch {
		case r.account != nil:
			// Match newObject: an empty CodeHash normalizes to EmptyCodeHash.
			v = r.account.CodeHash
			if v.IsEmpty() {
				v = accounts.EmptyCodeHash
			}
		case r.so != nil && !r.so.deleted:
			v = r.so.data.CodeHash
		default:
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
	case outcomeReturnEmpty:
		return accounts.EmptyCodeHash, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCodeHash: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// refreshCodeHash is the in-memory-only variant for CodeHashPath.
func refreshCodeHash(s *IntraBlockState, addr accounts.Address, currentHash accounts.CodeHash) (accounts.CodeHash, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCodeHash(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
		if r.recordVR {
			s.versionedReads.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{r.hdr, r.mapCodeHashVal})
		}
		return r.mapCodeHashVal, r.source, r.version, nil
	case outcomeReturnZero:
		return accounts.NilCodeHash, r.source, r.version, nil
	case outcomeReturnEmpty:
		return accounts.EmptyCodeHash, r.source, r.version, nil
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
				var err error
				if v, clean, err = r.so.GetState(key); err != nil {
					return uint256.Int{}, r.source, r.version, false, err
				}
			}
		} else {
			// Cold committed read resolved by committedStorageDirect: no dirty
			// value exists on the parallel path, so it is always clean.
			v, clean = r.mapStorageVal, true
		}
		if clean {
			seedStorageOrigin(s, addr, key, v, &r)
		}
		if r.recordVR {
			s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{r.hdr, v})
		}
		return v, r.source, r.version, clean, nil
	case outcomeLegacyStorage:
		if r.so == nil || r.so.deleted {
			return uint256.Int{}, StorageRead, UnknownVersion, false, nil
		}
		v, clean, err := r.so.GetState(key)
		if err != nil {
			return uint256.Int{}, StorageRead, UnknownVersion, false, err
		}
		return v, StorageRead, UnknownVersion, clean, nil
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
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
		seedStorageOrigin(s, addr, key, v, &r)
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		return uint256.Int{}, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readCommittedState: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// readSelfDestruct returns whether the account is selfdestructed.
func readSelfDestruct(s *IntraBlockState, addr accounts.Address) (bool, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetSelfDestruct(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
			switch {
			case r.so.deleted:
				v = false
			case r.so.createdContract:
				v = false
			default:
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
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
	case outcomeReturnZero, outcomeReturnEmpty, outcomeReturnDefault:
		if r.recordVR {
			// AddressPath defaultV is nil.
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: r.hdr})
		}
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshAccount: unexpected outcome %d for %x", r.outcome, addr))
	}
}
