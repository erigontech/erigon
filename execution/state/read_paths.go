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

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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
	// A witness node holds the size but not the code bytes, so read size directly.
	size, err := sdb.stateReader.ReadAccountCodeSize(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.codeReadDuration += time.Since(readStart)
		sdb.codeReadCount++
	}
	sdb.stateReader.SetTrace(false, "")
	return size, err
}

// committedStorageDirect reads a cold slot straight from the state reader; a
// contract this tx created reads zero here, never a prior incarnation's value.
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

// codeSeed returns the code this tx currently sees (own write, else committed)
// without recording an OCC read, so SetCode compares against current code.
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

// committedCodeHash normalizes an absent or code-less account to EmptyCodeHash.
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

type readPathOutcome uint8

const (
	outcomeLegacyStorage readPathOutcome = iota
	outcomeWriteSetHit
	outcomeMapDone
	outcomeReadSetHit
	outcomeStorageRead
	outcomeReturnZero    // erased by self-destruct, or genuinely absent: the path-typed zero
	outcomeReturnDefault // no information either way: the caller's current in-memory value
)

// readPathResult is versionedReadCore's result: outcome selects which field below holds the value.
type readPathResult struct {
	outcome readPathOutcome

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

	so *stateObject

	// Values, not *WriteCell: copying out keeps reads race-free against a concurrent FlushVersionedWrites mutating cell.Value.
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

	hashOfMapCodeVal accounts.CodeHash

	hdr      ReadHeader
	recordVR bool

	source  ReadSource
	version Version

	err error
}

// readValueUnchanged reports whether a version churn actually changed the value,
// so a spurious churn need not abort the tx. Unhandled paths default to "changed".
func (s *IntraBlockState) readValueUnchanged(addr accounts.Address, path AccountPath, key accounts.StorageKey, r *readPathResult) bool {
	switch path {
	case AddressPath:
		pr, ok := s.versionedReads.GetAddress(addr)
		if !ok {
			return false
		}
		var prAcc *accounts.Account
		if pr.Val != nil {
			prAcc = pr.Val.Account()
		}
		if EIP161EmptyRemoval(s.eip161, s.isAura, addr) && prAcc.Empty() && r.mapAddressVal.Empty() {
			return !s.versionMap.accountLiveAt(addr, s.txIndex)
		}
		return prAcc != nil && r.mapAddressVal != nil
	case BalancePath:
		pr, ok := s.versionedReads.GetBalance(addr)
		return ok && pr.Val.Eq(&r.mapBalanceVal)
	case NoncePath:
		pr, ok := s.versionedReads.GetNonce(addr)
		return ok && pr.Val == r.mapNonceVal
	case IncarnationPath:
		pr, ok := s.versionedReads.GetIncarnation(addr)
		return ok && pr.Val == r.mapIncarnationVal
	case CodeHashPath:
		pr, ok := s.versionedReads.GetCodeHash(addr)
		return ok && pr.Val == r.mapCodeHashVal
	case StoragePath:
		pr, ok := s.versionedReads.GetStorage(addr, key)
		return ok && pr.Val.Eq(&r.mapStorageVal)
	default:
		return false
	}
}

// versionedReadCore drives the read into the caller-allocated *r. skipStorage=true
// skips the storage-read fallback: the caller resolves the value itself.
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
		// Surface the prior tx's self-destruct version so this synthetic read matches later SD-zero reads.
		destructed, sdRes, sdOK := s.readSelfDestructMemo(addr)
		switch {
		case sdOK && sdRes.Status() == MVReadResultDone && destructed:
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
		case sdOK && sdRes.Status() == MVReadResultDone && !destructed:
			// A later tx revived the account: fall through so map cells serve the revival.
		default:
			r.outcome = outcomeReturnDefault
			r.source = StorageRead
			r.version = UnknownVersion
			return
		}
	}

	// Read-once fast path: a Done value resolved this attempt is safe to reuse without re-probing.
	if !commited {
		if _, dirty := s.journal.dirties[addr]; !dirty {
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
	if destructed, sdRes, ok := s.readSelfDestructMemo(addr); ok && sdRes.Status() == MVReadResultDone && destructed {
		destructTxIndex := sdRes.DepIdx()
		sdVer := Version{TxIndex: sdRes.DepIdx(), Incarnation: sdRes.Incarnation()}
		if !commited {
			if hasWrite := s.versionedWriteHit(addr, path, key, r); hasWrite {
				r.outcome = outcomeWriteSetHit
				r.source = WriteSetRead
				r.version = Version{TxIndex: s.txIndex, Incarnation: s.version}
				return
			}
		}
		// Revival is per-path, not account-wide: an unwritten field stays the fresh account's zero.
		revived := false
		if pathRevival := s.versionMap.ReadStatus(addr, path, key, s.txIndex); pathRevival.DepIdx() > destructTxIndex &&
			(pathRevival.Status() == MVReadResultDone || pathRevival.Status() == MVReadResultDependency) {
			revived = true
		}
		if !revived && path != CodePath {
			sdVersion := Version{TxIndex: destructTxIndex, Incarnation: sdVer.Incarnation}
			if s.eip8246 && (path == BalancePath || path == CodeHashPath || path == IncarnationPath) {
				// EIP-8246: no burn, so record the dependency and fall through to the live value.
				s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: sdVersion},
					Val:        true,
				})
			} else {
				if commited {
					r.outcome = outcomeReturnZero
					r.source = MapRead
					r.version = sdVersion
					return
				}
				// Keyed by `key`: a StoragePath read (key=slot) never matches the per-address
				// SelfDestructPath write (key=NilKey), so slots read as post-SD zero.
				sd, sdOK := false, false
				if key == accounts.NilKey {
					sd, sdOK = s.versionedWriteSelfDestruct(addr)
				}
				if !sdOK || sd {
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: sdVersion},
						Val:        true,
					})
					// A balance-only revival writes no CodeHash entry, yet a live account's
					// wiped code hash is keccak(''), not the nil hash of a nonexistent one.
					if path == CodeHashPath && !s.versionMap.destroyedAndUnrevived(addr, s.txIndex) {
						r.outcome = outcomeMapDone
						r.mapCodeHashVal = accounts.EmptyCodeHash
						r.hdr = ReadHeader{Source: MapRead, Version: sdVersion}
						r.recordVR = !commited
						r.source = MapRead
						r.version = sdVersion
						return
					}
					r.outcome = outcomeReturnZero
					r.source = MapRead
					r.version = sdVersion
					return
				}
				// Revived (SelfDestruct=false): keep destructedVersion for the stale-read check below.
				destructedVersion = Version{TxIndex: destructTxIndex}
			}
		}
	}

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
		r.mapCodeVal, r.hashOfMapCodeVal = mc.Bytes, mc.Hash
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
					if hdr.Version.TxIndex > destructedVersion.TxIndex && hdr.Version != prHeader.Version && !s.readValueUnchanged(addr, path, key, r) {
						if hdr.Version.TxIndex > s.dep {
							s.dep = hdr.Version.TxIndex
						}
						if dbg.TraceReexec {
							fmt.Printf(
								"DEP-WR blk=%d tx=%d inc=%d %x %s pr=(%d.%d) cur=(%d.%d)\n",
								s.blockNum,
								s.txIndex,
								s.version,
								addr,
								AccountKey{path, key},
								prHeader.Version.TxIndex,
								prHeader.Version.Incarnation,
								hdr.Version.TxIndex,
								hdr.Version.Incarnation,
							)
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
		// A provisional prior read is this load's own nil probe; skip it and adopt the fresh header below.
		if prHeader, ok := s.versionedReads.getHeader(addr, path, key); ok && prHeader.Source != ProvisionalRead {
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
			if s.readValueUnchanged(addr, path, key, r) {
				// Version churned but the value didn't: a spurious dependency, not a real one.
				r.outcome = outcomeReadSetHit
				r.source = MapRead
				r.version = prHeader.Version
				return
			}
			if hdr.Version.TxIndex > s.dep {
				s.dep = hdr.Version.TxIndex
			}
			if dbg.TraceReexec {
				fmt.Printf(
					"DEP-RD blk=%d tx=%d inc=%d %x %s pr=(%d.%d,src=%s) cur=(%d.%d)\n",
					s.blockNum,
					s.txIndex,
					s.version,
					addr,
					AccountKey{path, key},
					prHeader.Version.TxIndex,
					prHeader.Version.Incarnation,
					prHeader.Source,
					hdr.Version.TxIndex,
					hdr.Version.Incarnation,
				)
				if path == AddressPath {
					s.traceDepReadContext(addr, r)
				}
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
		// A cached value may predate a later SELFDESTRUCT that erased it; checking
		// only the latest SelfDestruct entry misses this, since a revival writes
		// SelfDestruct=false on top. Scan the range for a destruction (bounds differ
		// per path: EIP-8246 keeps balance/code-hash entries written by the
		// destroyer, so their scan starts above it) and record both the wipe and a
		// SelfDestruct=true dependency, so a re-executed destruction is re-checked.
		if path == StoragePath || path == CodePath || path == CodeSizePath || path == NoncePath ||
			path == CodeHashPath || path == BalancePath {
			lo := hdr.Version.TxIndex
			if path == CodeHashPath || path == BalancePath {
				lo++
			}
			if sdVer, ok := s.versionMap.FindDoneSelfDestructInRange(addr, lo, s.txIndex, true); ok {
				if !commited {
					s.recordWipedRead(addr, path, key, hdr.Version)
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: sdVer},
						Val:        true,
					})
				}
				if path == CodeHashPath && !s.versionMap.destroyedAndUnrevived(addr, s.txIndex) {
					r.outcome = outcomeMapDone
					r.mapCodeHashVal = accounts.EmptyCodeHash
					r.hdr = ReadHeader{Source: MapRead, Version: hdr.Version}
					r.recordVR = false
					r.source = MapRead
					r.version = hdr.Version
					return
				}
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = hdr.Version
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
		if dbg.TraceReexec {
			fmt.Printf(
				"DEP-MP blk=%d tx=%d inc=%d %x %s dep=(%d.%d)\n",
				s.blockNum,
				s.txIndex,
				s.version,
				addr,
				AccountKey{path, key},
				res.DepIdx(),
				res.Incarnation(),
			)
		}
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
				}
			}
		}

		// A bare StorageRead/UnknownVersion here would fail the validator's cross-check
		// and livelock; record the wipe pinned to the destruct's version instead.
		if path == BalancePath || path == NoncePath || path == IncarnationPath ||
			path == CodeHashPath || path == CodePath || path == CodeSizePath {
			if sdVer, ok := s.versionMap.FindDoneSelfDestructInRange(addr, 0, s.txIndex, true); ok {
				if !commited {
					s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
						ReadHeader: ReadHeader{Source: MapRead, Version: sdVer},
						Val:        true,
					})
					s.recordWipedRead(addr, path, key, sdVer)
				}
				if path == CodeHashPath && !s.versionMap.destroyedAndUnrevived(addr, s.txIndex) {
					r.outcome = outcomeMapDone
					r.mapCodeHashVal = accounts.EmptyCodeHash
					r.hdr = ReadHeader{Source: MapRead, Version: sdVer}
					r.recordVR = !commited
					r.source = MapRead
					r.version = sdVer
					return
				}
				r.outcome = outcomeReturnZero
				r.source = MapRead
				r.version = sdVer
				return
			}
		}

		// A prior tx's incarnation bump cleared the old storage/code; a revival's own
		// cell would have matched the version-map read above, so this is genuinely empty.
		if path == StoragePath || path == CodePath || path == CodeSizePath {
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
				// Carry the entry's underlying source: validation rejects ReadSetRead at MVReadResultNone.
				if accSource == ReadSetRead {
					if pr, ok := s.versionedReads.GetAddress(addr); ok {
						accSource = pr.Source
						accVersion = pr.Version
					} else {
						accSource = StorageRead
						accVersion = UnknownVersion
					}
				}
				hdr.Source = accSource
				hdr.Version = accVersion
				so = newObject(s, addr, readAccount, readAccount)
			}
		}
		if path == StoragePath && so == nil {
			hdr.Source = StorageRead
			if cached, ok := s.stateObjects[addr]; ok {
				so = cached
			} else {
				// A cold slot depends only on its own cell; an AddressPath dependency here would be false.
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
					r.mapCodeVal, r.hashOfMapCodeVal = code, accounts.NilCodeHash
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
		// An account resolved from this tx's own write carries no cross-tx dependency to record.
		r.recordVR = hdr.Source != WriteSetRead
		r.source = hdr.Source
		r.version = hdr.Version
		return
	}

	r.outcome = outcomeReturnDefault
	r.source = UnknownSource
	r.version = UnknownVersion
}

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
		if r.recordVR {
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{r.hdr, NewAccountView(acc)})
		}
		return acc, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnDefault:
		if r.recordVR {
			hdr := r.hdr
			hdr.Source = ProvisionalRead
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: hdr})
		}
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readAccountInternal: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func (s *IntraBlockState) traceDepReadContext(addr accounts.Address, r *readPathResult) {
	balV, balRes, balOK := s.versionMap.ReadBalance(addr, s.txIndex)
	nonV, nonRes, nonOK := s.versionMap.ReadNonce(addr, s.txIndex)
	sdV, sdRes, sdOK := s.versionMap.ReadSelfDestruct(addr, s.txIndex)
	pr, prOK := s.versionedReads.GetAddress(addr)
	prNil := !prOK || pr.Val == nil || pr.Val.Account() == nil
	mapValEmpty := true
	if r.mapAddressVal != nil {
		mapValEmpty = r.mapAddressVal.Empty()
	}
	fmt.Printf(
		"DEP-RD-CTX %x prNil=%v mapValNil=%v mapValEmpty=%v bal=(ok=%v,v=%v,idx=%d,st=%d) nonce=(ok=%v,v=%d,idx=%d,st=%d) sd=(ok=%v,v=%v,idx=%d,st=%d)\n",
		addr,
		prNil,
		r.mapAddressVal == nil,
		mapValEmpty,
		balOK,
		&balV,
		balRes.DepIdx(),
		balRes.Status(),
		nonOK,
		nonV,
		nonRes.DepIdx(),
		nonRes.Status(),
		sdOK,
		sdV,
		sdRes.DepIdx(),
		sdRes.Status(),
	)
}

func (s *IntraBlockState) recordWipedRead(addr accounts.Address, path AccountPath, key accounts.StorageKey, ver Version) {
	hdr := ReadHeader{Source: MapRead, Version: ver}
	switch path {
	case StoragePath:
		s.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{ReadHeader: hdr})
	case CodePath:
		s.versionedReads.SetCode(addr, VersionedRead[[]byte]{ReadHeader: hdr})
	case CodeSizePath:
		s.versionedReads.SetCodeSize(addr, VersionedRead[int]{ReadHeader: hdr})
	case NoncePath:
		s.versionedReads.SetNonce(addr, VersionedRead[uint64]{ReadHeader: hdr})
	case CodeHashPath:
		val := accounts.NilCodeHash
		if !s.versionMap.destroyedAndUnrevived(addr, s.txIndex) {
			val = accounts.EmptyCodeHash
		}
		s.versionedReads.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{ReadHeader: hdr, Val: val})
	case BalancePath:
		s.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader: hdr})
	}
}

func warmSource(src ReadSource) bool { return src == MapRead || src == StorageRead }

func (s *IntraBlockState) warmReadable(addr accounts.Address) bool {
	_, dirty := s.journal.dirties[addr]
	return !dirty
}

func readBalance(s *IntraBlockState, addr accounts.Address) (uint256.Int, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetBalance(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
		// recordVR: cache the read so a same-tx repeat hits the read-once fast
		// path instead of re-probing the version map.
		if r.recordVR {
			s.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{r.hdr, r.mapBalanceVal})
		}
		return r.mapBalanceVal, r.source, r.version, nil
	case outcomeReturnZero:
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

func readNonce(s *IntraBlockState, addr accounts.Address) (uint64, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetNonce(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
		// Not recorded: the final incarnation is resolved from the map at write normalization.
		return currentIncarnation, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshIncarnation: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func readCode(s *IntraBlockState, addr accounts.Address, commited bool) ([]byte, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCode(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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

// refreshedCode is refreshCode's result. Unlike accounts.Code it promises no
// Hash == Keccak256(Bytes): KnownHash is Nil when the source stored bytes only.
type refreshedCode struct {
	Bytes     []byte
	KnownHash accounts.CodeHash
}

// refreshCode never records on a miss: CodePath is excluded from the skipStorage default-recording branch.
func refreshCode(s *IntraBlockState, addr accounts.Address) (refreshedCode, ReadSource, Version, error) {
	var r readPathResult
	versionedReadCore(s, addr, CodePath, accounts.NilKey, false, true, &r)
	if r.err != nil {
		return refreshedCode{}, r.source, r.version, r.err
	}
	switch r.outcome {
	case outcomeWriteSetHit:
		return refreshedCode{r.vwCode.Val.Bytes, r.vwCode.Val.Hash}, r.source, r.version, nil
	case outcomeReadSetHit:
		tr, _ := s.versionedReads.GetCode(addr)
		return refreshedCode{tr.Val, r.hashOfMapCodeVal}, r.source, r.version, nil
	case outcomeMapDone:
		return refreshedCode{r.mapCodeVal, r.hashOfMapCodeVal}, r.source, r.version, nil
	case outcomeReturnZero, outcomeReturnDefault:
		return refreshedCode{}, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshCode: unexpected outcome %d for %x", r.outcome, addr))
	}
}

// codeHash avoids re-hashing when the source knew the hash. For committed bytes
// the account record is authoritative; a prior tx's write can outrun it.
func (c refreshedCode) codeHash(source ReadSource, accountHash accounts.CodeHash) accounts.CodeHash {
	if c.KnownHash != accounts.NilCodeHash {
		return c.KnownHash
	}
	if source == StorageRead {
		return accountHash
	}
	return accounts.InternCodeHash(crypto.Keccak256Hash(c.Bytes))
}

func readCodeSize(s *IntraBlockState, addr accounts.Address) (int, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCodeSize(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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

func readCodeHash(s *IntraBlockState, addr accounts.Address) (accounts.CodeHash, ReadSource, Version, error) {
	if s.warmReadable(addr) {
		if tr, ok := s.versionedReads.GetCodeHash(addr); ok && warmSource(tr.Source) {
			return tr.Val, tr.Source, tr.Version, nil
		}
	}
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
	case outcomeReturnDefault:
		if r.recordVR {
			s.versionedReads.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{r.hdr, currentHash})
		}
		return currentHash, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshCodeHash: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func readState(s *IntraBlockState, addr accounts.Address, key accounts.StorageKey) (uint256.Int, ReadSource, Version, error) {
	v, source, version, _, err := readStateForSet(s, addr, key)
	return v, source, version, err
}

// readStateForSet also returns stateObject.GetState's "clean" bool, which
// SetState uses to choose delete vs. update on revert.
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
			// Cold committed read: no dirty value exists on the parallel path, so it's always clean.
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
	case outcomeReturnZero, outcomeReturnDefault:
		return false, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("readSelfDestruct: unexpected outcome %d for %x", r.outcome, addr))
	}
}

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
			s.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{ReadHeader: r.hdr})
		}
		return false, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshSelfDestruct: unexpected outcome %d for %x", r.outcome, addr))
	}
}

func readAccount(s *IntraBlockState, addr accounts.Address) (*accounts.Account, ReadSource, Version, error) {
	return readAccountInternal(s, addr)
}

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
			hdr := r.hdr
			hdr.Source = ProvisionalRead
			s.versionedReads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: hdr})
		}
		return nil, r.source, r.version, nil
	default:
		panic(fmt.Sprintf("refreshAccount: unexpected outcome %d for %x", r.outcome, addr))
	}
}
