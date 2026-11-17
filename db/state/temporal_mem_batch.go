// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"maps"
	"slices"
	"sync"
	"unsafe"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

type dataWithTxNum struct {
	data  []byte
	txNum uint64
}

const latestShards = 32

var latestShardSeed = maphash.MakeSeed()

type latestShard struct {
	sync.RWMutex
	m map[string][]dataWithTxNum
	_ [128 - (unsafe.Sizeof(sync.RWMutex{})+unsafe.Sizeof(map[string][]dataWithTxNum(nil)))%128]byte
}

func latestShardIndex(key string) uint64 {
	return maphash.String(latestShardSeed, key) & (latestShards - 1)
}

// TemporalMemBatch - temporal read-write interface - which storing updates in RAM. Don't forget to call `.Flush()`
type TemporalMemBatch struct {
	stepSize uint64

	// inMemHistoryReads: accumulate all writes with txNums so GetAsOf can answer time-travel
	// queries from in-flight state (needed for RPC reads during live chain-tip execution).
	// Distinct from DomainBufferedWriter which also holds history in mem but only for writing.
	// Disable for offline commands (stage_exec etc.) — history reads come from disk anyway.
	inMemHistoryReads bool

	latest  [kv.DomainLen][latestShards]latestShard
	storage *btree2.Map[string, []dataWithTxNum] // TODO: replace hardcoded domain name to per-config configuration of available Guarantees/AccessMethods (range vs get)

	domainWriters [kv.DomainLen]*DomainBufferedWriter
	iiWriters     []*InvertedIndexBufferedWriter

	pastDomainWriters [kv.DomainLen][]*DomainBufferedWriter
	pastIIWriters     []*InvertedIndexBufferedWriter

	preUnwindDomainWriters [kv.DomainLen][]*DomainBufferedWriter
	preUnwindIIWriters     []*InvertedIndexBufferedWriter

	currentChangesAccumulator *changeset.StateChangeSet
	// pastChangesAccumulator is read by the parallel commitment calculator
	// goroutine (via SharedDomains.GetChangesetByBlockNum) while the exec
	// loop writes to it (via SavePastChangesetAccumulator). pastChangesLock
	// serializes those accesses so map-iteration during GetChangesetByBlockNum
	// doesn't race with map-write during SavePastChangesetAccumulator.
	pastChangesLock        sync.RWMutex
	pastChangesAccumulator map[string]*changeset.StateChangeSet

	unwindToTxNum uint64
	// unwindChangeset is keyed by the pre-step portion of each entry's Key
	// (`Key[:len(Key)-8]`) and is consulted only by the getLatest fallback —
	// it's intentionally collapsed to one entry per real key so a pre-unwind
	// value can be found by the same key the overlay uses.
	unwindChangeset *[kv.DomainLen]map[string]kv.DomainEntryDiff
	// unwindChangesetRaw preserves every diff entry (distinct by Key+step) so
	// Flush can replay a complete unwind against MDBX, including multiple
	// step entries for the same real key (e.g. a commitment branch that was
	// written in both step N and N+1 during forward execution). Collapsing
	// these — as unwindChangeset does — loses every step except the one that
	// happened to be iterated last, leaving orphan domain entries at steps
	// above the unwind target.
	unwindChangesetRaw *[kv.DomainLen][]kv.DomainEntryDiff

	metrics *kvmetrics.DomainMetrics
}

func NewTemporalMemBatch(tx kv.TemporalTx, db kv.RoDB, ioMetrics any) *TemporalMemBatch {
	sd := &TemporalMemBatch{
		storage:           btree2.NewMap[string, []dataWithTxNum](128),
		metrics:           ioMetrics.(*kvmetrics.DomainMetrics),
		inMemHistoryReads: true,
	}
	aggTx := AggTx(tx)
	sd.stepSize = aggTx.StepSize()

	sd.iiWriters = make([]*InvertedIndexBufferedWriter, len(aggTx.iis))

	for id, ii := range aggTx.iis {
		sd.iiWriters[id] = ii.NewWriter(db)
	}

	for id, d := range aggTx.d {
		sd.domainWriters[id] = d.NewWriter(db)
	}

	return sd
}

func (sd *TemporalMemBatch) SetInMemHistoryReads(v bool) { sd.inMemHistoryReads = v }
func (sd *TemporalMemBatch) InMemHistoryReads() bool     { return sd.inMemHistoryReads }

// shard returns the key's lock and map; the storage btree is guarded by shard 0 of its domain.
func (sd *TemporalMemBatch) shard(domain kv.Domain, key string) *latestShard {
	if domain == kv.StorageDomain {
		return &sd.latest[domain][0]
	}
	return &sd.latest[domain][latestShardIndex(key)]
}

func (sd *TemporalMemBatch) shards(domain kv.Domain) []latestShard {
	if domain == kv.StorageDomain {
		return sd.latest[domain][:1]
	}
	return sd.latest[domain][:]
}

func (sd *TemporalMemBatch) rlockDomain(domain kv.Domain) {
	shards := sd.shards(domain)
	for i := range shards {
		shards[i].RLock()
	}
}

func (sd *TemporalMemBatch) runlockDomain(domain kv.Domain) {
	shards := sd.shards(domain)
	for i := range shards {
		shards[i].RUnlock()
	}
}

func (sd *TemporalMemBatch) lockDomain(domain kv.Domain) {
	shards := sd.shards(domain)
	for i := range shards {
		shards[i].Lock()
	}
}

func (sd *TemporalMemBatch) unlockDomain(domain kv.Domain) {
	shards := sd.shards(domain)
	for i := range shards {
		shards[i].Unlock()
	}
}

func (sd *TemporalMemBatch) lockAllDomains() {
	for d := range sd.latest {
		sd.lockDomain(kv.Domain(d))
	}
}

func (sd *TemporalMemBatch) unlockAllDomains() {
	for d := range sd.latest {
		sd.unlockDomain(kv.Domain(d))
	}
}

func (sd *TemporalMemBatch) rlockAllDomains() {
	for d := range sd.latest {
		sd.rlockDomain(kv.Domain(d))
	}
}

func (sd *TemporalMemBatch) runlockAllDomains() {
	for d := range sd.latest {
		sd.runlockDomain(kv.Domain(d))
	}
}

func (sd *TemporalMemBatch) DomainPut(domain kv.Domain, k string, v []byte, txNum uint64, preval []byte) error {
	sameTxNumUpdate := sd.putLatest(domain, k, v, txNum)
	return sd.putHistory(domain, common.ToBytesZeroCopy(k), v, txNum, preval, sameTxNumUpdate)
}

func (sd *TemporalMemBatch) DomainDel(domain kv.Domain, k string, txNum uint64, preval []byte) error {
	sameTxNumUpdate := sd.putLatest(domain, k, nil, txNum)
	return sd.putHistory(domain, common.ToBytesZeroCopy(k), nil, txNum, preval, sameTxNumUpdate)
}

// PutCommitmentBranchDiff writes a kv.CommitmentDomain entry with an explicit
// diff target instead of domainWriters[kv.CommitmentDomain]'s own diff field.
// The commitment domain has exactly one writer (the parallel commitment
// calculator, one block at a time), so routing its diff this way needs no
// lock against SetChangesetAccumulator, unlike DomainPut/DomainDel.
func (sd *TemporalMemBatch) PutCommitmentBranchDiff(k string, v []byte, txNum uint64, preval []byte, diff *kv.DomainDiff) error {
	sameTxNumUpdate := sd.putLatest(kv.CommitmentDomain, k, v, txNum)
	kb := common.ToBytesZeroCopy(k)
	if sameTxNumUpdate {
		return sd.domainWriters[kv.CommitmentDomain].addValue(kb, v, kv.Step(txNum/sd.stepSize))
	}
	if len(v) == 0 {
		return sd.domainWriters[kv.CommitmentDomain].DeleteWithPrevDiff(kb, txNum, preval, diff)
	}
	return sd.domainWriters[kv.CommitmentDomain].PutWithPrevDiff(kb, v, txNum, preval, diff)
}

func (sd *TemporalMemBatch) putHistory(domain kv.Domain, k, v []byte, txNum uint64, preval []byte, sameTxNumUpdate bool) error {
	// A same-txNum update only rewrites the value: history and the unwind diff
	// record the value as of BEFORE txNum, which the first write already did —
	// this write's preval is the intra-txNum intermediate, not a real prev.
	if sameTxNumUpdate {
		return sd.domainWriters[domain].addValue(k, v, kv.Step(txNum/sd.stepSize))
	}
	if len(v) == 0 {
		return sd.domainWriters[domain].DeleteWithPrev(k, txNum, preval)
	}
	return sd.domainWriters[domain].PutWithPrev(k, v, txNum, preval)
}

// putLatest reports whether this write is a same-txNum update of the key,
// replacing the key's last entry in place instead of appending a version.
func (sd *TemporalMemBatch) putLatest(domain kv.Domain, key string, val []byte, txNum uint64) (sameTxNumUpdate bool) {
	// Own the bytes now: val may alias a .kv mmap (the foreground exec tx's file generation)
	// that a background merge can munmap while a concurrent commitment worker reads sd.mem.
	valWithStep := dataWithTxNum{data: bytes.Clone(val), txNum: txNum}
	sameTxNumUpdate, putKeySize, putValueSize := sd.putLatestLocked(domain, key, valWithStep)
	sd.updatePutMetrics(domain, putKeySize, putValueSize)
	return sameTxNumUpdate
}

func (sd *TemporalMemBatch) putLatestLocked(domain kv.Domain, key string, valWithStep dataWithTxNum) (sameTxNumUpdate bool, putKeySize, putValueSize int) {
	val, txNum := valWithStep.data, valWithStep.txNum
	sh := sd.shard(domain, key)
	sh.Lock()
	defer sh.Unlock()
	if domain == kv.StorageDomain {
		if old, ok := sd.storage.Get(key); ok {
			switch {
			case old[len(old)-1].txNum == txNum:
				sameTxNumUpdate = true
				putValueSize += len(val) - len(old[len(old)-1].data)
				old[len(old)-1] = valWithStep
				sd.storage.Set(key, old)
			case sd.inMemHistoryReads:
				sd.storage.Set(key, append(old, valWithStep))
				putValueSize += len(val)
			default:
				putValueSize += len(val) - len(old[len(old)-1].data)
				old[0] = valWithStep
				sd.storage.Set(key, old[:1])
			}
		} else {
			sd.storage.Set(key, []dataWithTxNum{valWithStep})
			putKeySize += len(key)
			putValueSize += len(val)
		}

		return sameTxNumUpdate, putKeySize, putValueSize
	}

	if sh.m == nil {
		sh.m = map[string][]dataWithTxNum{}
	}
	if old, ok := sh.m[key]; ok {
		switch {
		case old[len(old)-1].txNum == txNum:
			sameTxNumUpdate = true
			putValueSize += len(val) - len(old[len(old)-1].data)
			old[len(old)-1] = valWithStep
			sh.m[key] = old
		case sd.inMemHistoryReads:
			sh.m[key] = append(old, valWithStep)
			putValueSize += len(val)
		default:
			putValueSize += len(val) - len(old[len(old)-1].data)
			old[0] = valWithStep
			sh.m[key] = old[:1]
		}
	} else {
		sh.m[key] = []dataWithTxNum{valWithStep}
		putKeySize += len(key)
		putValueSize += len(val)
	}

	return sameTxNumUpdate, putKeySize, putValueSize
}

func (sd *TemporalMemBatch) updatePutMetrics(domain kv.Domain, putKeySize int, putValueSize int) {
	sd.metrics.Lock()
	defer sd.metrics.Unlock()
	sd.metrics.CachePutCount++
	sd.metrics.CachePutSize += putKeySize + putValueSize
	sd.metrics.CachePutKeySize += putKeySize
	sd.metrics.CachePutValueSize += putValueSize
	if dm, ok := sd.metrics.Domains[domain]; ok {
		dm.CachePutCount++
		dm.CachePutSize += putKeySize + putValueSize
		dm.CachePutKeySize += putKeySize
		dm.CachePutValueSize += putValueSize
	} else {
		sd.metrics.Domains[domain] = &kvmetrics.DomainIOMetrics{
			CachePutCount:     1,
			CachePutSize:      putKeySize + putValueSize,
			CachePutKeySize:   putKeySize,
			CachePutValueSize: putValueSize,
		}
	}
}

func (sd *TemporalMemBatch) GetLatest(domain kv.Domain, key []byte) (v []byte, step kv.Step, ok bool) {
	sh := sd.shard(domain, common.ToStringZeroCopy(key))
	sh.RLock()
	defer sh.RUnlock()
	return sd.getLatest(sh, domain, key)
}

// getLatest is the lock-free implementation of GetLatest.
// The caller must already hold sh's lock (either RLock or Lock).
func (sd *TemporalMemBatch) getLatest(sh *latestShard, domain kv.Domain, key []byte) (v []byte, step kv.Step, ok bool) {
	unwoundLatest := func(domain kv.Domain, key string) (v []byte, step kv.Step, ok bool) {
		if sd.unwindChangeset != nil {
			if values := sd.unwindChangeset[domain]; values != nil {
				if value, ok := values[key]; ok {
					keyStep := kv.Step(^binary.BigEndian.Uint64([]byte(value.Key[len(value.Key)-8:])))

					if value.Value == nil {
						// Different step: the entry at this step was deleted, key doesn't exist here
						return nil, keyStep, false
					}
					// Same step: restore this value
					return value.Value, keyStep, true
				}
			}
		}

		// kv.NoStepBound distinguishes "no in-flight unwind bounds this key"
		// from a bound at step 0 (a delete-shape entry above whose keyStep is
		// 0), so young chains — whose whole state lives in step 0 — still get
		// the per-key signal.
		return nil, kv.NoStepBound, false
	}

	if latest, ok := sd.overlayLatest(sh, domain, key); ok {
		return latest.data, kv.Step(latest.txNum / sd.stepSize), true
	}
	return unwoundLatest(domain, common.ToStringZeroCopy(key))
}

func (sd *TemporalMemBatch) GetAsOf(domain kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	if !sd.inMemHistoryReads && domain != kv.ReceiptDomain {
		return nil, false, kv.ErrInMemHistoryDisabled
	}
	sh := sd.shard(domain, common.ToStringZeroCopy(key))
	sh.RLock()
	defer sh.RUnlock()

	// unwoundLatest returns the pre-unwound-block value for a key that was
	// modified by the unwound block. Only fires when ts is at-or-after the
	// unwind point — for ts < unwindToTxNum the caller falls through to the
	// underlying tx (chain DB / files) which holds the older committed value
	// that was never unwound. Without this fallback, when the unwound block's
	// writes were already committed to chain DB before the in-memory unwind
	// the chain DB still holds the post-change value, and the commitment
	// calculator's asOfStateReader (committer.go) reads those stale values
	// for unwound keys and computes a wrong trie root.
	unwoundLatest := func(domain kv.Domain, key string) (v []byte, ok bool, err error) {
		if sd.unwindChangeset == nil || ts < sd.unwindToTxNum {
			return nil, false, nil
		}
		values := sd.unwindChangeset[domain]
		if values == nil {
			return nil, false, nil
		}
		value, found := values[key]
		if !found {
			return nil, false, nil
		}
		if len(value.Value) == 0 {
			// Pre-unwind state had no value at this key.
			return nil, true, nil
		}
		return value.Value, true, nil
	}

	keyS := common.ToStringZeroCopy(key)
	var dataWithTxNums []dataWithTxNum
	if domain == kv.StorageDomain {
		dataWithTxNums, ok = sd.storage.Get(keyS)
		if !ok {
			return unwoundLatest(domain, keyS)
		}
		for i, dataWithTxNum := range dataWithTxNums {
			if ts > dataWithTxNum.txNum && (i == len(dataWithTxNums)-1 || ts <= dataWithTxNums[i+1].txNum) {
				return dataWithTxNum.data, true, nil
			}
		}
		return unwoundLatest(domain, keyS)
	}

	dataWithTxNums, ok = sh.m[keyS]
	if !ok {
		return unwoundLatest(domain, keyS)
	}
	for i, dataWithTxNum := range dataWithTxNums {
		if ts > dataWithTxNum.txNum && (i == len(dataWithTxNums)-1 || ts <= dataWithTxNums[i+1].txNum) {
			return dataWithTxNum.data, true, nil
		}
	}
	return unwoundLatest(domain, keyS)
}

func (sd *TemporalMemBatch) HistorySeek(domain kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return sd.GetAsOf(domain, key, ts)
}

func (sd *TemporalMemBatch) SizeEstimate() uint64 {
	// CachePutSize is guarded by the metrics lock — the put path, Merge and the
	// reset all mutate it under sd.metrics.Lock(), and MergeMetrics folds in a
	// boundary worker's accumulator from another goroutine. Read under the same
	// lock (not the domain locks, which guard the domains maps, not this counter).
	sd.metrics.RLock()
	defer sd.metrics.RUnlock()
	return uint64(sd.metrics.CachePutSize)
}

func (sd *TemporalMemBatch) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte) (cont bool, err error)) error {
	sd.rlockDomain(domain)
	defer sd.runlockDomain(domain)
	var ramIter btree2.MapIter[string, []dataWithTxNum]
	if domain == kv.StorageDomain {
		ramIter = sd.storage.Iter()
	}

	// Wrap the callback to respect sd.unwindChangeset: when iterating the
	// underlying tx + ramIter, the chain DB may still hold values written by
	// blocks that were unwound in-memory but committed earlier. Without this
	// wrapper, createContract's DomainDelPrefix sees the stale value via
	// IteratePrefix and records it as the prev value in the new block's diff
	// — which on the next unwind restores that stale value, masking a
	// freshly-written same-value SSTORE as a no-op and producing a wrong
	// trie root. See GetAsOf's unwoundLatest for the equivalent point-read
	// fix.
	wrappedIt := it
	if sd.unwindChangeset != nil {
		if values := sd.unwindChangeset[domain]; len(values) > 0 {
			wrappedIt = func(k []byte, v []byte) (cont bool, err error) {
				if latest, ok := sd.overlayLatest(sd.shard(domain, common.ToStringZeroCopy(k)), domain, k); ok { // overlay wins over the changeset, as in getLatest
					return it(k, latest.data)
				}
				if entry, ok := values[common.ToStringZeroCopy(k)]; ok {
					if len(entry.Value) == 0 {
						// Pre-unwind state had no value at this key — skip it.
						return true, nil
					}
					// Restore the pre-unwind value.
					return it(k, entry.Value)
				}
				return it(k, v)
			}
		}
	}
	return AggTx(roTx).d[domain].debugIteratePrefixLatest(prefix, ramIter, wrappedIt, roTx)
}

// overlayLatest requires sh's lock to be held.
func (sd *TemporalMemBatch) overlayLatest(sh *latestShard, domain kv.Domain, k []byte) (dataWithTxNum, bool) {
	keyS := common.ToStringZeroCopy(k)
	var vals []dataWithTxNum
	var ok bool
	if domain == kv.StorageDomain {
		vals, ok = sd.storage.Get(keyS)
	} else {
		vals, ok = sh.m[keyS]
	}
	if !ok || len(vals) == 0 {
		return dataWithTxNum{}, false
	}
	return vals[len(vals)-1], true
}

func (sd *TemporalMemBatch) GetChangesetAccumulator() *changeset.StateChangeSet {
	return sd.currentChangesAccumulator
}

func (sd *TemporalMemBatch) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	sd.currentChangesAccumulator = acc
	for idx := range sd.domainWriters {
		if sd.currentChangesAccumulator == nil {
			sd.domainWriters[idx].SetDiff(nil)
		} else {
			sd.domainWriters[idx].SetDiff(&sd.currentChangesAccumulator.Diffs[idx])
		}
	}
}

// SetCommitmentDiff redirects only the commitment writer's diff, leaving every
// other domain writer pointed at the live accumulator.
func (sd *TemporalMemBatch) SetCommitmentDiff(acc *changeset.StateChangeSet) {
	if acc == nil {
		sd.domainWriters[kv.CommitmentDomain].SetDiff(nil)
		return
	}
	sd.domainWriters[kv.CommitmentDomain].SetDiff(&acc.Diffs[kv.CommitmentDomain])
}

func (sd *TemporalMemBatch) CommitmentDiff() *kv.DomainDiff {
	return sd.domainWriters[kv.CommitmentDomain].Diff()
}

func (sd *TemporalMemBatch) SetCommitmentDiffRaw(d *kv.DomainDiff) {
	sd.domainWriters[kv.CommitmentDomain].SetDiff(d)
}

func (sd *TemporalMemBatch) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet) {
	sd.pastChangesLock.Lock()
	defer sd.pastChangesLock.Unlock()
	if sd.pastChangesAccumulator == nil {
		sd.pastChangesAccumulator = make(map[string]*changeset.StateChangeSet)
	}
	key := make([]byte, 40)
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesAccumulator[common.ToStringZeroCopy(key)] = acc
}

// GetChangesetByBlockNum returns the changeset for a given block number and its block hash.
//
// WARNING: ambiguous when pastChangesAccumulator holds multiple changesets for
// the same block number (e.g. canonical + fork during a reorg-bounce test).
// The first match in non-deterministic map iteration order is returned.
// Prefer GetChangesetByHash when the caller has the block hash available.
func (sd *TemporalMemBatch) GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet) {
	sd.pastChangesLock.RLock()
	defer sd.pastChangesLock.RUnlock()
	for key, cs := range sd.pastChangesAccumulator {
		keyBytes := common.ToBytesZeroCopy(key)
		if binary.BigEndian.Uint64(keyBytes[:8]) == blockNumber {
			blockHash := common.BytesToHash(keyBytes[8:])
			return blockHash, cs
		}
	}
	return common.Hash{}, nil
}

// GetChangesetByHash returns the changeset saved under the exact (blockNumber,
// blockHash) key. Returns nil if not found. Use this in preference to
// GetChangesetByBlockNum when both pieces of information are known —
// pastChangesAccumulator can hold multiple changesets per block number after
// a fork-bounce, and number-only lookups can return the wrong one
// non-deterministically.
func (sd *TemporalMemBatch) GetChangesetByHash(blockNumber uint64, blockHash common.Hash) *changeset.StateChangeSet {
	var key [40]byte
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesLock.RLock()
	defer sd.pastChangesLock.RUnlock()
	return sd.pastChangesAccumulator[common.ToStringZeroCopy(key[:])]
}

func (sd *TemporalMemBatch) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	var key [40]byte
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesLock.RLock()
	cs, ok := sd.pastChangesAccumulator[common.ToStringZeroCopy(key[:])]
	sd.pastChangesLock.RUnlock()
	if ok {
		var diffs [kv.DomainLen][]kv.DomainEntryDiff
		for i := range cs.Diffs {
			diffs[i] = cs.Diffs[i].GetDiffSet()
		}
		return diffs, true, nil
	}
	return changeset.ReadDiffSet(tx, blockNumber, blockHash)
}

// Unwind drops [unwindToTxNum, ∞)
func (sd *TemporalMemBatch) Unwind(unwindToTxNum uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.lockAllDomains()
	defer sd.unlockAllDomains()

	sd.unwindToTxNum = unwindToTxNum
	sd.detachPreUnwindWriters()

	pruneSlice := func(entries []dataWithTxNum) []dataWithTxNum {
		kept := entries[:0]
		for _, e := range entries {
			if e.txNum >= unwindToTxNum { // drop [unwindToTxNum, ∞)
				continue
			}
			kept = append(kept, e)
		}
		return kept
	}
	for d := range sd.latest {
		for i := range sd.latest[d] {
			m := sd.latest[d][i].m
			for k, entries := range m {
				kept := pruneSlice(entries)
				if len(kept) == 0 {
					delete(m, k)
				} else {
					m[k] = kept
				}
			}
		}
	}
	// Collect first, mutate after: btree.Scan doesn't allow Set/Delete during traversal.
	type storageEdit struct {
		key  string
		kept []dataWithTxNum
	}
	var edits []storageEdit
	sd.storage.Scan(func(k string, entries []dataWithTxNum) bool {
		kept := pruneSlice(entries)
		if len(kept) != len(entries) {
			edits = append(edits, storageEdit{k, kept})
		}
		return true
	})
	for _, e := range edits {
		if len(e.kept) == 0 {
			sd.storage.Delete(e.key)
		} else {
			sd.storage.Set(e.key, e.kept)
		}
	}

	var unwindChangeset *[kv.DomainLen]map[string]kv.DomainEntryDiff
	var unwindChangesetRaw *[kv.DomainLen][]kv.DomainEntryDiff

	if changeset != nil {
		unwindChangeset = &[kv.DomainLen]map[string]kv.DomainEntryDiff{}
		unwindChangesetRaw = &[kv.DomainLen][]kv.DomainEntryDiff{}

		for domain, changes := range changeset {
			if unwindChangeset[domain] == nil {
				unwindChangeset[domain] = map[string]kv.DomainEntryDiff{}
			}

			// unwindChangesetRaw preserves every (key, step) entry — Flush needs
			// the full list to delete every orphan step entry from MDBX.
			unwindChangesetRaw[domain] = append(unwindChangesetRaw[domain][:0], changes...)

			for _, change := range changes {
				unwindChangeset[domain][change.Key[:len(change.Key)-8]] = change
			}
		}
	}

	sd.unwindChangeset = unwindChangeset
	sd.unwindChangesetRaw = unwindChangesetRaw
}

func (sd *TemporalMemBatch) detachPreUnwindWriters() {
	for d, w := range sd.domainWriters {
		sd.preUnwindDomainWriters[d] = append(sd.preUnwindDomainWriters[d], sd.pastDomainWriters[d]...)
		sd.pastDomainWriters[d] = nil
		if w != nil {
			sd.preUnwindDomainWriters[d] = append(sd.preUnwindDomainWriters[d], w.detach())
		}
	}
	sd.preUnwindIIWriters = append(sd.preUnwindIIWriters, sd.pastIIWriters...)
	sd.pastIIWriters = nil
	for _, w := range sd.iiWriters {
		if w != nil {
			sd.preUnwindIIWriters = append(sd.preUnwindIIWriters, w.detach())
		}
	}
}

func (sd *TemporalMemBatch) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	for _, writer := range sd.iiWriters {
		if writer.name == table {
			return writer.Add(key, txNum)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

// Close releases writer resources but must not clear the in-memory maps:
// readers of a published SD may still hold them.
func (sd *TemporalMemBatch) Close() {
	for _, d := range sd.domainWriters {
		if d != nil {
			d.Close()
		}
	}
	for _, ds := range sd.pastDomainWriters {
		for _, d := range ds {
			d.Close()
		}
	}
	for _, iiWriter := range sd.iiWriters {
		iiWriter.close()
	}
	for _, iiWriter := range sd.pastIIWriters {
		iiWriter.close()
	}
	for _, ds := range sd.preUnwindDomainWriters {
		for _, d := range ds {
			d.Close()
		}
	}
	for _, iiWriter := range sd.preUnwindIIWriters {
		iiWriter.close()
	}
}

func (sd *TemporalMemBatch) Merge(o kv.TemporalMemBatch) error {
	other, ok := o.(*TemporalMemBatch)
	if !ok {
		return fmt.Errorf("can't merge %T into *TemporalMemBatch", o)
	}
	if sd.currentChangesAccumulator != nil {
		return fmt.Errorf("can't merge to batch with non-nil currentChangesAccumulator")
	}
	if other.currentChangesAccumulator != nil {
		return fmt.Errorf("can't merge from batch with non-nil currentChangesAccumulator")
	}

	for d := range other.latest {
		for i := range other.latest[d] {
			otherEntries := other.latest[d][i].m
			if len(otherEntries) == 0 {
				continue
			}
			sh := &sd.latest[d][i]
			if sh.m == nil {
				sh.m = make(map[string][]dataWithTxNum, len(otherEntries))
			}
			maps.Copy(sh.m, otherEntries)
		}
	}

	other.storage.Scan(func(key string, value []dataWithTxNum) bool {
		sd.storage.Set(key, value)
		return true
	})

	unwound := func(b *TemporalMemBatch) bool {
		return slices.ContainsFunc(b.preUnwindDomainWriters[:], func(ws []*DomainBufferedWriter) bool { return len(ws) > 0 })
	}
	if unwound(other) && !unwound(sd) {
		sd.unwindToTxNum = other.unwindToTxNum
		sd.detachPreUnwindWriters()
	}
	for domain, writer := range sd.domainWriters {
		if writer != nil {
			sd.pastDomainWriters[domain] = append(sd.pastDomainWriters[domain], writer.detach())
		}
		sd.pastDomainWriters[domain] = append(sd.pastDomainWriters[domain], other.pastDomainWriters[domain]...)
		sd.pastDomainWriters[domain] = append(sd.pastDomainWriters[domain], other.domainWriters[domain])
		other.pastDomainWriters[domain] = nil
		other.domainWriters[domain] = nil
	}

	sd.pastIIWriters = append(sd.pastIIWriters, other.iiWriters...)
	other.iiWriters = nil
	sd.pastIIWriters = append(sd.pastIIWriters, other.pastIIWriters...)
	other.pastIIWriters = nil

	for domain, writers := range other.preUnwindDomainWriters {
		sd.preUnwindDomainWriters[domain] = append(sd.preUnwindDomainWriters[domain], writers...)
		other.preUnwindDomainWriters[domain] = nil
	}
	sd.preUnwindIIWriters = append(sd.preUnwindIIWriters, other.preUnwindIIWriters...)
	other.preUnwindIIWriters = nil

	// Fixed lock order (receiver write-lock, then `other` read-lock).
	// Callers must not interleave reciprocal Merge calls — i.e. never run
	// a.Merge(b) and b.Merge(a) concurrently, which would deadlock here.
	// In practice Merge runs single-threaded at stage commit / batch
	// rollup, so this is a documented assumption rather than an enforced
	// invariant.
	sd.pastChangesLock.Lock()
	other.pastChangesLock.RLock()
	for key, changeSet := range other.pastChangesAccumulator {
		if sd.pastChangesAccumulator == nil {
			sd.pastChangesAccumulator = map[string]*changeset.StateChangeSet{}
		}
		sd.pastChangesAccumulator[key] = changeSet
	}
	other.pastChangesLock.RUnlock()
	sd.pastChangesLock.Unlock()

	if other.unwindChangeset != nil {
		if sd.unwindChangeset == nil {
			sd.unwindToTxNum = other.unwindToTxNum
			sd.unwindChangeset = other.unwindChangeset
			sd.unwindChangesetRaw = other.unwindChangesetRaw
		} else {
			for domain, otherDiffs := range other.unwindChangeset {
				for key, otherDiff := range otherDiffs {
					if diff, ok := sd.unwindChangeset[domain][key]; ok {
						if sd.unwindToTxNum < other.unwindToTxNum {
							sd.unwindChangeset[domain][key] = changeset.MergeDiffSets([]kv.DomainEntryDiff{otherDiff}, []kv.DomainEntryDiff{diff})[0]
						} else {
							sd.unwindChangeset[domain][key] = changeset.MergeDiffSets([]kv.DomainEntryDiff{diff}, []kv.DomainEntryDiff{otherDiff})[0]
						}
					} else {
						sd.unwindChangeset[domain][key] = otherDiff
					}
				}
			}
			// Also merge the raw changesets — Flush walks these to ensure every
			// step entry in MDBX gets reverted, not just the collapsed one.
			//
			// Precondition: both sd.unwindChangesetRaw[domain] and otherDiffs
			// must be sorted by Key; MergeDiffSets relies on that ordering.
			// The invariant is established upstream — DomainDiff.GetDiffSet
			// sorts (db/kv/helpers.go), the serialize/deserialize pair
			// preserves order, TemporalMemBatch.Unwind copies its sorted input
			// verbatim, and MergeDiffSets itself returns sorted output.
			if other.unwindChangesetRaw != nil {
				if sd.unwindChangesetRaw == nil {
					sd.unwindChangesetRaw = other.unwindChangesetRaw
				} else {
					for domain, otherDiffs := range other.unwindChangesetRaw {
						if sd.unwindToTxNum < other.unwindToTxNum {
							sd.unwindChangesetRaw[domain] = changeset.MergeDiffSets(otherDiffs, sd.unwindChangesetRaw[domain])
						} else {
							sd.unwindChangesetRaw[domain] = changeset.MergeDiffSets(sd.unwindChangesetRaw[domain], otherDiffs)
						}
					}
				}
			}
			if other.unwindToTxNum < sd.unwindToTxNum {
				sd.unwindToTxNum = other.unwindToTxNum
			}
		}
	}

	other.Close()
	return nil
}

// Flush writes the mem-batch to tx. With kv.WithFlushCallback options, the
// registered per-domain callback is invoked for every (key, value, step, txNum)
// tuple after the MDBX write succeeds, so a downstream cache can never be left
// ahead of MDBX. Holds every domain read lock: writers must not change the batch
// before the callbacks run, while readers of a published batch keep going.
func (sd *TemporalMemBatch) Flush(ctx context.Context, tx kv.RwTx, opts ...kv.FlushOption) error {
	sd.rlockAllDomains()
	defer sd.runlockAllDomains()

	if err := flushWriterLists(ctx, tx, &sd.preUnwindDomainWriters, sd.preUnwindIIWriters, 0); err != nil {
		return err
	}
	if sd.unwindChangesetRaw != nil {
		for domain := range sd.unwindChangesetRaw {
			slices.SortFunc(sd.unwindChangesetRaw[domain], func(a, b kv.DomainEntryDiff) int {
				return cmp.Compare(a.Key, b.Key)
			})
		}
		if err := tx.(kv.TemporalRwTx).Unwind(ctx, sd.unwindToTxNum, sd.unwindChangesetRaw); err != nil {
			return err
		}
	}
	if err := sd.flushDiffSet(ctx, tx); err != nil {
		return err
	}
	if err := sd.flushWriters(ctx, tx); err != nil {
		return err
	}
	// PlainStateVersion advances here with the domain writes; metadata overlays
	// must not advance it independently.
	if _, err := rawdb.IncrementStateVersion(tx); err != nil {
		return fmt.Errorf("can't write plain state version: %w", err)
	}

	if len(opts) > 0 {
		var cfg kv.FlushConfig
		for _, opt := range opts {
			opt(&cfg)
		}
		for domain, cb := range cfg.DomainCallbacks {
			if domain == kv.StorageDomain {
				sd.storage.Scan(func(keyStr string, history []dataWithTxNum) bool {
					if len(history) == 0 {
						return true
					}
					latest := history[len(history)-1]
					cb([]byte(keyStr), latest.data, kv.Step(latest.txNum/sd.stepSize), latest.txNum)
					return true
				})
				continue
			}
			for i := range sd.latest[domain] {
				for keyStr, history := range sd.latest[domain][i].m {
					if len(history) == 0 {
						continue
					}
					latest := history[len(history)-1]
					cb([]byte(keyStr), latest.data, kv.Step(latest.txNum/sd.stepSize), latest.txNum)
				}
			}
		}
	}

	return nil
}

func (sd *TemporalMemBatch) flushDiffSet(_ context.Context, tx kv.RwTx) error {
	for key, changeSet := range sd.pastChangesAccumulator {
		blockNum := binary.BigEndian.Uint64(common.ToBytesZeroCopy(key[:8]))
		blockHash := common.BytesToHash(common.ToBytesZeroCopy(key[8:]))
		if err := changeset.WriteDiffSet(tx, blockNum, blockHash, changeSet); err != nil {
			return err
		}
	}
	return nil
}

func flushWriterLists(ctx context.Context, tx kv.RwTx, ds *[kv.DomainLen][]*DomainBufferedWriter, iis []*InvertedIndexBufferedWriter, keepHistoryBelowTxNum uint64) error {
	aggTx := AggTx(tx)
	for di, ws := range ds {
		if err := flushDomainWriters(ctx, tx, ws, keepHistoryBelowTxNum); err != nil {
			return err
		}
		if len(ws) > 0 {
			aggTx.d[di].closeValsCursor() // TODO: why?
		}
	}
	for _, w := range iis {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
		w.close()
	}
	return nil
}

func flushDomainWriters(ctx context.Context, tx kv.RwTx, ws []*DomainBufferedWriter, keepHistoryBelowTxNum uint64) error {
	keep := newKeepRetainedHistory(keepHistoryBelowTxNum)
	for _, w := range slices.Backward(ws) {
		if w.discard {
			continue
		}
		if err := w.h.flush(ctx, tx, keep); err != nil {
			return err
		}
	}
	for _, w := range ws {
		if !w.discard {
			if err := w.flushValues(ctx, tx); err != nil {
				return err
			}
		}
		w.Close()
	}
	return nil
}

func (sd *TemporalMemBatch) flushWriters(ctx context.Context, tx kv.RwTx) error {
	ds := sd.pastDomainWriters
	for di, w := range sd.domainWriters {
		if w != nil {
			ds[di] = append(slices.Clip(ds[di]), w)
		}
	}
	return flushWriterLists(ctx, tx, &ds, append(slices.Clip(sd.pastIIWriters), sd.iiWriters...), sd.unwindToTxNum)
}

func (sd *TemporalMemBatch) DiscardWrites(domain kv.Domain) {
	sd.domainWriters[domain].discard = true
	sd.domainWriters[domain].h.discard = true
	for _, w := range slices.Concat(sd.pastDomainWriters[domain], sd.preUnwindDomainWriters[domain]) {
		w.discard = true
		w.h.discard = true
	}
}

func AggTx(tx kv.Tx) *AggregatorRoTx {
	if withAggTx, ok := tx.(interface{ AggTx() any }); ok {
		return withAggTx.AggTx().(*AggregatorRoTx)
	}

	return nil
}
