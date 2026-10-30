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
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
)

type iodir int

const (
	get iodir = iota
	put
)

type dataWithTxNum struct {
	data  []byte
	txNum uint64
	dir   iodir
}

// TemporalMemBatch - temporal read-write interface - which storing updates in RAM. Don't forget to call `.Flush()`
type TemporalMemBatch struct {
	stepSize uint64

	getCacheSize int

	// Every write is kept with its txNum: GetAsOf answers time-travel reads from in-flight state by
	// walking these versions, and the values flush needs the newest write in each STEP. This used to be
	// optional (inMemHistoryReads, off for the integration and squeeze tooling) — but commit
	// correctness cannot depend on a tooling switch, so it is unconditional.

	latestStateLock sync.RWMutex
	domains         [kv.DomainLen]map[string][]dataWithTxNum
	storage         *btree2.Map[string, []dataWithTxNum] // TODO: replace hardcoded domain name to per-config configuration of available Guarantees/AccessMethods (range vs get)

	domainWriters [kv.DomainLen]*DomainBufferedWriter
	// mergedAway is set on the SOURCE of a Merge. Merge hands this batch's writers to the
	// destination and nils every one of its own slots, so a write afterwards is a use-after-free.
	mergedAway      atomic.Bool
	iiWriters       []*InvertedIndexBufferedWriter
	forkableWriters map[kv.ForkableId]kv.BufferedWriter

	pastDomainWriters   [kv.DomainLen][]*DomainBufferedWriter
	pastIIWriters       []*InvertedIndexBufferedWriter
	pastForkableWriters map[kv.ForkableId][]kv.BufferedWriter

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

	metrics *changeset.DomainMetrics
}

func NewTemporalMemBatch(tx kv.TemporalTx, ioMetrics any) *TemporalMemBatch {
	sd := &TemporalMemBatch{
		storage: btree2.NewMap[string, []dataWithTxNum](128),
		metrics: ioMetrics.(*changeset.DomainMetrics),
	}
	aggTx := AggTx(tx)
	sd.stepSize = aggTx.StepSize()

	sd.iiWriters = make([]*InvertedIndexBufferedWriter, len(aggTx.iis))

	for id, ii := range aggTx.iis {
		sd.iiWriters[id] = ii.NewWriter()
	}

	for id, d := range aggTx.d {
		sd.domains[id] = map[string][]dataWithTxNum{}
		sd.domainWriters[id] = d.NewWriter()
	}

	sd.forkableWriters = make(map[kv.ForkableId]kv.BufferedWriter)
	for _, id := range tx.Debug().AllForkableIds() {
		sd.forkableWriters[id] = tx.Unmarked(id).BufferedWriter()
	}

	return sd
}

func (sd *TemporalMemBatch) DomainPut(domain kv.Domain, k string, v []byte, txNum uint64, preval []byte) error {
	// CHECKED BEFORE ANYTHING IS MUTATED. putLatest would otherwise write the in-memory latest value
	// of a batch that is already dead, and only then fail.
	if err := sd.writableFor(domain); err != nil {
		return err
	}
	sd.putLatest(domain, k, v, txNum)
	return sd.putHistory(domain, common.ToBytesZeroCopy(k), v, txNum, preval)
}

func (sd *TemporalMemBatch) DomainDel(domain kv.Domain, k string, txNum uint64, preval []byte) error {
	if err := sd.writableFor(domain); err != nil {
		return err
	}
	sd.putLatest(domain, k, nil, txNum)
	return sd.putHistory(domain, common.ToBytesZeroCopy(k), nil, txNum, preval)
}

// writableFor refuses a write to a domain this batch has no writer for.
//
// ⚠ IT USED TO DEREFERENCE NOTHING AND KILL THE NODE, on every 9,600-order soak pass:
//
//	DomainBufferedWriter.PutWithPrev -> putHistory -> DomainPut -> SharedDomains.domainPut
//	-> flushPendingUpdates -> preExecuteLocked -> PreExecuteFlashblock -> driver.execRound
//
// Merge nils ALL SIX of the source batch's writers at once — measured: accounts, storage, code,
// commitment, receipt and rcache all warned in the same millisecond — so this is a pre-exec round
// still writing to a batch whose writers were handed to another batch.
//
// Skipping the write would silently drop real STATE, which is worse than crashing. Failing it
// stops the round, which the driver already handles, and says why.
func (sd *TemporalMemBatch) writableFor(domain kv.Domain) error {
	if sd.domainWriters[domain] != nil {
		return nil
	}
	return fmt.Errorf("temporal mem batch: no writer for domain %s (mergedAway=%v): a write reached a batch whose writers were handed to another batch by Merge",
		domain, sd.mergedAway.Load())
}

func (sd *TemporalMemBatch) putHistory(domain kv.Domain, k, v []byte, txNum uint64, preval []byte) error {
	// ⚠ FAIL THE WRITE, DO NOT DEREFERENCE NOTHING, AND DO NOT SKIP IT EITHER. A nil writer here
	// killed the node outright on every 9,600-order soak pass:
	//
	//	DomainBufferedWriter.PutWithPrev -> putHistory -> DomainPut -> SharedDomains.domainPut
	//	-> flushPendingUpdates -> preExecuteLocked -> PreExecuteFlashblock -> driver.execRound
	//
	// Merge nils ALL SIX of the source batch's writers at once (measured: accounts, storage, code,
	// commitment, receipt and rcache all warned in the same millisecond), so this is a pre-exec
	// round still writing to a batch whose writers have been handed away.
	//
	// Skipping would silently drop real STATE, which is worse than crashing. Returning an error
	// fails the round, which the driver already handles, and names why.
	w := sd.domainWriters[domain]
	if w == nil {
		return sd.writableFor(domain)
	}
	if len(v) == 0 {
		return w.DeleteWithPrev(k, txNum, preval)
	}
	return w.PutWithPrev(k, v, txNum, preval)
}

// historyStepsRetained is how many of the newest STEPS keep their full per-transaction version list.
// Older steps are aged down to a single version — their newest write.
//
// Both halves are load-bearing:
//   - the newest write IN EACH STEP must survive, for every step the batch touched: the values table
//     is keyed by (key, STEP), so dropping a step's representative deletes its entry at flush. That
//     is what broke the commitment file-conversion tests when a key was emitted only once.
//   - per-TRANSACTION versions are what GetAsOf walks to answer time-travel reads from in-flight
//     state. Only recent state is asked for that way; anything older is served from history files on
//     disk, so keeping full granularity for every step a long batch ever touched buys nothing.
//
// Without this a key written across many steps grew without bound in RAM. The maps are now the source
// for the values flush, and unlike the ETL collectors they used to share that job with, they do not
// spill — so the growth has to be bounded here.
//
// ⚠ THIS BOUND IS AN INTERIM, NOT THE DESIGN. The right model bounds the in-RAM history on BOTH axes
// — steps AND mem-batch size — while preserving values, so that it can serve transaction-level
// granularity for the steps still in the DB (not yet collated into files) and let a consistent
// history be produced from it. A fixed step count does neither: it is blind to batch size, and a
// batch spanning more steps than this loses per-transaction detail for in-DB steps.
//
// It is safe as an interim because DURABLE history does not come from here. Every write still goes
// through putHistory to the history writer and the history tables with full per-transaction
// granularity — untouched by any of this. These maps serve in-flight GetAsOf reads and, now, the
// values flush. So the bound can only cost resolution on in-RAM reads of a long batch's older
// steps, which is the catchup/tooling case.
//
// Sizing this properly is separate Erigon maintenance, not part of the torn-block fix.
const historyStepsRetained = 2

// appendVersion records one more version of a key, ageing the list out when the write crosses into a
// new step — the only moment anything can fall outside the retained window, so the scan is rare.
func (sd *TemporalMemBatch) appendVersion(versions []dataWithTxNum, v dataWithTxNum) []dataWithTxNum {
	crossedStep := sd.stepSize > 0 && len(versions) > 0 &&
		versions[len(versions)-1].txNum/sd.stepSize != v.txNum/sd.stepSize
	versions = append(versions, v)
	if crossedStep {
		return ageOutVersions(versions, sd.stepSize)
	}
	return versions
}

// ageOutVersions drops per-transaction detail for steps older than the retained window, keeping each
// such step's newest write. Versions arrive in txNum order, so one pass in place is enough.
func ageOutVersions(versions []dataWithTxNum, stepSize uint64) []dataWithTxNum {
	if stepSize == 0 || len(versions) < 2 {
		return versions
	}
	newestStep := versions[len(versions)-1].txNum / stepSize
	if newestStep < historyStepsRetained {
		return versions
	}
	cutoff := newestStep - historyStepsRetained
	kept := versions[:0]
	for i := range versions {
		step := versions[i].txNum / stepSize
		if step > cutoff {
			kept = append(kept, versions[i]) // inside the window: keep every version
			continue
		}
		if i+1 < len(versions) && versions[i+1].txNum/stepSize == step {
			continue // superseded within its own step
		}
		kept = append(kept, versions[i]) // the step's newest write, its representative
	}
	return kept
}

func (sd *TemporalMemBatch) putLatest(

	domain kv.Domain, key string, val []byte, txNum uint64) {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()

	var updateMetrics = func(domain kv.Domain, putKeySize int, putValueSize int) {
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
			sd.metrics.Domains[domain] = &changeset.DomainIOMetrics{
				CachePutCount:     1,
				CachePutSize:      putKeySize + putValueSize,
				CachePutKeySize:   putKeySize,
				CachePutValueSize: putValueSize,
			}
		}
	}

	// Own the bytes now: val may alias a .kv mmap (the foreground exec tx's file generation)
	// that a background merge can munmap while a concurrent commitment worker reads sd.mem.
	valWithStep := dataWithTxNum{data: common.Copy(val), txNum: txNum}
	putKeySize := 0
	putValueSize := 0
	if domain == kv.StorageDomain {
		if old, ok := sd.storage.Get(key); ok {
			sd.storage.Set(key, sd.appendVersion(old, valWithStep))
			putValueSize += len(val)
		} else {
			sd.storage.Set(key, []dataWithTxNum{valWithStep})
			putKeySize += len(key)
			putValueSize += len(val)
		}

		updateMetrics(domain, putKeySize, putValueSize)
		return
	}

	if old, ok := sd.domains[domain][key]; ok {
		sd.domains[domain][key] = sd.appendVersion(old, valWithStep)
		putValueSize += len(val)
	} else {
		sd.domains[domain][key] = []dataWithTxNum{valWithStep}
		putKeySize += len(key)
		putValueSize += len(val)
	}

	updateMetrics(domain, putKeySize, putValueSize)
}

func (sd *TemporalMemBatch) GetLatest(domain kv.Domain, key []byte) (v []byte, step kv.Step, ok bool) {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	return sd.getLatest(domain, key)
}

// getLatest is the lock-free implementation of GetLatest.
// The caller must already hold latestStateLock (either RLock or Lock),
// e.g. from within an IteratePrefix callback.
func (sd *TemporalMemBatch) getLatest(domain kv.Domain, key []byte) (v []byte, step kv.Step, ok bool) {
	var unwoundLatest = func(domain kv.Domain, key string) (v []byte, step kv.Step, ok bool) {
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

		return nil, 0, false
	}

	keyS := common.ToStringZeroCopy(key)
	var dataWithTxNums []dataWithTxNum
	if domain == kv.StorageDomain {
		dataWithTxNums, ok = sd.storage.Get(keyS)
		if !ok {
			return unwoundLatest(domain, keyS)
		}
		dataWithTxNum := dataWithTxNums[len(dataWithTxNums)-1]
		return dataWithTxNum.data, kv.Step(dataWithTxNum.txNum / sd.stepSize), ok

	}

	dataWithTxNums, ok = sd.domains[domain][keyS]
	if !ok {
		return unwoundLatest(domain, keyS)
	}
	dataWithTxNum := dataWithTxNums[len(dataWithTxNums)-1]
	return dataWithTxNum.data, kv.Step(dataWithTxNum.txNum / sd.stepSize), ok
}

func (sd *TemporalMemBatch) GetAsOf(domain kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()

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

	dataWithTxNums, ok = sd.domains[domain][keyS]
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

func (sd *TemporalMemBatch) SizeEstimate() uint64 {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	return uint64(sd.metrics.CachePutSize)
}

func (sd *TemporalMemBatch) ClearRam() {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	for i := range sd.domains {
		sd.domains[i] = map[string][]dataWithTxNum{}
	}

	sd.storage = btree2.NewMap[string, []dataWithTxNum](128)
	sd.unwindToTxNum = 0
	sd.unwindChangeset = nil
	sd.unwindChangesetRaw = nil

	sd.metrics.Lock()
	defer sd.metrics.Unlock()
	sd.metrics.CachePutCount = 0
	sd.metrics.CachePutSize = 0
	sd.metrics.CachePutKeySize = 0
	sd.metrics.CachePutValueSize = 0
	for _, dm := range sd.metrics.Domains {
		dm.CachePutCount = 0
		dm.CachePutSize = 0
		dm.CachePutKeySize = 0
		dm.CachePutValueSize = 0
	}
}

func (sd *TemporalMemBatch) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte) (cont bool, err error)) error {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
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

func (sd *TemporalMemBatch) HasPrefix(domain kv.Domain, prefix []byte, roTx kv.Tx) ([]byte, []byte, bool, error) {
	var firstKey, firstVal []byte
	var hasPrefix bool
	err := sd.IteratePrefix(domain, prefix, roTx, func(k []byte, v []byte) (bool, error) {
		if lv, _, ok := sd.getLatest(domain, k); ok {
			v = lv
		}
		if len(v) > 0 {
			firstKey = common.Copy(k)
			firstVal = common.Copy(v)
			hasPrefix = true
			return false, nil // do not continue, end on first occurrence
		}
		return true, nil
	})
	return firstKey, firstVal, hasPrefix, err
}

// HasPrefixInRAM reports whether the RAM batch contains any non-deleted entry
// for the given domain whose key starts with prefix.  It never touches disk or
// segment files — only the in-memory btree (StorageDomain) or the domain map.
func (sd *TemporalMemBatch) HasPrefixInRAM(domain kv.Domain, prefix []byte) bool {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()

	if domain == kv.StorageDomain {
		prefixStr := common.ToStringZeroCopy(prefix)
		iter := sd.storage.Iter()
		for ok := iter.Seek(prefixStr); ok; ok = iter.Next() {
			if !strings.HasPrefix(iter.Key(), prefixStr) {
				break
			}
			vals := iter.Value()
			if len(vals) > 0 && len(vals[len(vals)-1].data) > 0 {
				return true
			}
		}
		return false
	}

	prefixStr := common.ToStringZeroCopy(prefix)
	for k, vals := range sd.domains[domain] {
		if strings.HasPrefix(k, prefixStr) && len(vals) > 0 && len(vals[len(vals)-1].data) > 0 {
			return true
		}
	}
	return false
}

func (sd *TemporalMemBatch) GetChangesetAccumulator() *changeset.StateChangeSet {
	return sd.currentChangesAccumulator
}

// nilDomainWriterSeen counts domains this batch has no writer for, so the stopgap below cannot be
// silent. See SetChangesetAccumulator.
var nilDomainWriterSeen [kv.DomainLen]atomic.Uint64

// NilDomainWriters reports, per domain, how many times SetChangesetAccumulator found no writer.
// All zeros is the expected state; anything else names the domain the crash was about.
func NilDomainWriters() (out [kv.DomainLen]uint64) {
	for i := range nilDomainWriterSeen {
		out[i] = nilDomainWriterSeen[i].Load()
	}
	return out
}

func (sd *TemporalMemBatch) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	sd.currentChangesAccumulator = acc
	for idx := range sd.domainWriters {
		w := sd.domainWriters[idx]
		if w == nil {
			// ⚠ STOPGAP, AND DELIBERATELY NOT SILENT. SetDiff dereferences its receiver even for a nil
			// diff, so a missing writer here KILLED THE NODE:
			//
			//	panic: nil pointer dereference
			//	  DomainBufferedWriter.SetDiff -> SetChangesetAccumulator -> flushPendingUpdates
			//	  -> preExecFrontier.RetireBelow -> MergeExtendingFork -> updateForkChoice
			//
			// Measured once, on soak live435 pass 4 (9600 orders); 1200/2400/4800 ran clean. Writers are
			// built once from aggTx.d and are never nilled afterwards - Close() does not nil them, and it
			// already guards this same slice with `if d != nil` - so a nil here is a LIFECYCLE WINDOW this
			// read could not pin, not a static shape.
			//
			// Skipping the diff for one domain risks losing its changeset; crashing loses the node and the
			// run. The count is what makes this a stopgap rather than a paper-over: NilDomainWriters() names
			// the domain, which is the fact the panic did not carry, and all-zeros says it stopped happening.
			// SAY IT THE FIRST TIME, ONCE PER DOMAIN. A counter nothing reads is the same as no
			// counter: this was added so all-zeros could mean "it stopped happening", and nothing
			// ever called NilDomainWriters(). The 0->1 transition is the only one that logs, so a
			// recurring window costs one line per domain, not one per block.
			if nilDomainWriterSeen[idx].Add(1) == 1 {
				log.Warn("[state] no domain writer for a changeset diff — the diff is SKIPPED for this domain",
					"domain", kv.Domain(idx).String())
			}
			continue
		}
		if sd.currentChangesAccumulator == nil {
			w.SetDiff(nil)
		} else {
			w.SetDiff(&sd.currentChangesAccumulator.Diffs[idx])
		}
	}
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
		return [kv.DomainLen][]kv.DomainEntryDiff{
			cs.Diffs[kv.AccountsDomain].GetDiffSet(),
			cs.Diffs[kv.StorageDomain].GetDiffSet(),
			cs.Diffs[kv.CodeDomain].GetDiffSet(),
			cs.Diffs[kv.CommitmentDomain].GetDiffSet(),
		}, true, nil
	}
	return changeset.ReadDiffSet(tx, blockNumber, blockHash)
}

func (sd *TemporalMemBatch) Unwind(unwindToTxNum uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()

	sd.unwindToTxNum = unwindToTxNum

	// Drop overlay entries stamped with txNum > unwindToTxNum. Without this,
	// getLatest returns entries written inside the unwound range because it
	// picks dataWithTxNums[len-1] without consulting sd.unwindToTxNum — the
	// unwindChangeset fallback below is only reachable on an overlay miss.
	// Observed as post-Fusaka gas-used mismatches after forkchoice-driven
	// unwinds (an SSTORE on a slot first-written inside the unwound range
	// charges SSTORE_RESET instead of SSTORE_SET — a 17100 gas shortfall
	// per slot). Keys whose slice empties out are removed so the
	// unwindChangeset fallback can supply the pre-unwind answer.
	pruneSlice := func(entries []dataWithTxNum) []dataWithTxNum {
		kept := entries[:0]
		for _, e := range entries {
			if e.txNum <= unwindToTxNum {
				kept = append(kept, e)
			}
		}
		return kept
	}
	for d := range sd.domains {
		for k, entries := range sd.domains[d] {
			kept := pruneSlice(entries)
			if len(kept) == 0 {
				delete(sd.domains[d], k)
			} else {
				sd.domains[d][k] = kept
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

func (sd *TemporalMemBatch) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	for _, writer := range sd.iiWriters {
		if writer.name == table {
			return writer.Add(key, txNum)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

func (sd *TemporalMemBatch) PutForkable(id kv.ForkableId, num kv.Num, v []byte) error {
	f, ok := sd.forkableWriters[id]
	if !ok {
		return fmt.Errorf("forkable not found: %s", Registry.Name(id))
	}
	return f.Put(num, v)
}

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
	for _, fWriter := range sd.forkableWriters {
		fWriter.Close()
	}
	for _, fs := range sd.pastForkableWriters {
		for _, f := range fs {
			f.Close()
		}
	}
	sd.ClearRam()
}

// takeWritersFrom moves every buffered writer out of `other` and into sd's past-writer lists,
// leaving `other` with none.
//
// ⚠ IT EMPTIES `other`, SO IT RUNS ONLY WHEN `other` IS BEING CLOSED. This used to be inline in
// Merge and unconditional, while Merge's closeOther=false means "keep the source alive as the
// successor's read-through parent" and the comment at the foot of Merge calls the copy
// "non-destructive". It was not: the source came out alive for READING and dead for WRITING, and
// the pre-exec round that still owned it dereferenced nothing and killed the node on every
// 9,600-order soak pass — all six domains warning in the same millisecond. The pending-commitment
// transfer in Merge already skips itself when not closing; this is the same rule.
func (sd *TemporalMemBatch) takeWritersFrom(other *TemporalMemBatch) {
	for domain, writer := range other.domainWriters {
		// Skip a nil writer: an EMPTY block (heartbeat) touches no state, so its untouched domains have no
		// writer (domainWriters are created lazily on first write). Putting nil in here poisons
		// pastDomainWriters, which flushWriters/DiscardWrites deref WITHOUT the nil guard the domainWriters
		// loop has — a nil-deref crash when an empty block's SD is merged/promoted (frontier run-ahead).
		newer := other.pastDomainWriters[domain]
		if writer != nil {
			newer = append([]*DomainBufferedWriter{writer}, newer...)
		}
		if len(newer) > 0 {
			sd.pastDomainWriters[domain] = append(newer, sd.pastDomainWriters[domain]...)
		}
		other.domainWriters[domain] = nil
		other.pastDomainWriters[domain] = nil
	}
	// Anything that writes to `other` from here on is writing to a batch with no writers.
	other.mergedAway.Store(true)

	sd.pastIIWriters = append(sd.pastIIWriters, other.iiWriters...)
	other.iiWriters = nil
	sd.pastIIWriters = append(sd.pastIIWriters, other.pastIIWriters...)
	other.pastIIWriters = nil

	for id, writer := range other.forkableWriters {
		sd.pastForkableWriters[id] = append(sd.pastForkableWriters[id], writer)
	}
	other.forkableWriters = nil

	for id, writers := range other.pastForkableWriters {
		sd.pastForkableWriters[id] = append(sd.pastForkableWriters[id], writers...)
	}
	other.pastForkableWriters = nil
}

// ProbedWriteKeys returns, per domain, the keys every writer in this batch is about to flush — its
// own and each one taken in by a Merge. Probe-only (ERIGON_WRITER_KEY_PROBE); nil when off.
//
// It exists so a commit can be checked as a CHECKPOINT: whatever this batch held at commit must be
// readable from the DB immediately afterwards, on a fresh RoTx. That is a fidelity question, not a
// correctness one — it needs no model of what the values SHOULD be, only that they survive.
func (sd *TemporalMemBatch) ProbedWriteKeys() map[kv.Domain][][]byte {
	if !writerKeyProbe {
		return nil
	}
	out := make(map[kv.Domain][][]byte, kv.DomainLen)
	add := func(d kv.Domain, w *DomainBufferedWriter) {
		for k := range w.ProbedKeys() {
			out[d] = append(out[d], []byte(k))
		}
	}
	for di, w := range sd.domainWriters {
		if w != nil {
			add(kv.Domain(di), w)
		}
	}
	for di, ws := range sd.pastDomainWriters {
		for _, w := range ws {
			if w != nil {
				add(kv.Domain(di), w)
			}
		}
	}
	return out
}

func (sd *TemporalMemBatch) Merge(o kv.TemporalMemBatch, closeOther bool) error {

	other, ok := o.(*TemporalMemBatch)
	if !ok {
		return fmt.Errorf("Can't merge %T into *TemporalMemBatch", o)
	}

	// UNDER THE SAME LOCK EVERY READER TAKES. `domains` and `storage` are guarded by
	// latestStateLock — putLatest takes it for write, GetLatest/GetAsOf/IteratePrefix for read —
	// and this merge wrote both with no lock at all.
	//
	// It is reachable from any eth_call on PENDING state: such a call executes against the live
	// pre-execution SharedDomains through ReaderV3 → GetLatest, while the frontier run-ahead
	// promotes a finished generation into that same batch here. A Go map read concurrent with a
	// map write is not a data race the runtime tolerates — it is `fatal error: concurrent map read
	// and map write`, which no recover() can catch, so THE NODE DIES.
	//
	// Observed: live111 died in exactly this stack (eth_call → DoCall → ApplyMessage → EVM →
	// ReaderV3.readAccountData → GetLatest → getLatest). It was found by chasing a probe that
	// intermittently returned an empty result, which is the same race read non-fatally — a torn
	// read rather than a detected one.
	//
	// `other` is locked for read as well: it is a separate batch and nothing here may assume it is
	// private. Merge is the only site that holds two of these locks, so there is no ordering cycle
	// to deadlock on.
	func() {
		sd.latestStateLock.Lock()
		defer sd.latestStateLock.Unlock()
		other.latestStateLock.RLock()
		defer other.latestStateLock.RUnlock()

		// UNION THE VERSIONS, DO NOT REPLACE THEM. maps.Copy would set dst[k] = src[k], dropping every
		// version THIS batch holds for a key the source also wrote. That loses real state in two ways:
		//
		//   - the values table is keyed by (key, STEP), so a key this batch wrote in one step and the
		//     source wrote in the next owes an entry in BOTH; replacing leaves only the source's, and
		//     the earlier step's entry is never written.
		//   - GetAsOf answers time-travel reads by walking these versions, so after a merge it could no
		//     longer see writes this batch had made itself.
		//
		// `other` is newer than everything here by construction (Merge refuses sdTxNum > otherTxNum),
		// so appending keeps each key's versions in txNum order.
		for domain, otherEntries := range other.domains {
			entries := sd.domains[domain]
			for k, otherVersions := range otherEntries {
				if existing, ok := entries[k]; ok && len(existing) > 0 {
					entries[k] = append(existing, otherVersions...)
					continue
				}
				entries[k] = otherVersions
			}
		}

		other.storage.Scan(func(key string, value []dataWithTxNum) bool {
			if existing, ok := sd.storage.Get(key); ok && len(existing) > 0 {
				sd.storage.Set(key, append(existing, value...))
				return true
			}
			sd.storage.Set(key, value)
			return true
		})
	}()

	// pastDomainWriters is ordered NEWEST FIRST and flushWriters walks it backwards, so `other`'s
	// writers go in FRONT. That order no longer decides which write survives — values are flushed from
	// the maps, one entry per (key, step), and these writers carry HISTORY only, which is keyed by
	// txNum and cannot overwrite itself. The order is kept because it is the truthful one.
	//
	// ⚠ TAKING THE WRITERS IS DESTRUCTIVE, SO IT OBEYS closeOther LIKE EVERY OTHER DESTRUCTIVE STEP
	// HERE. closeOther=false means the source is a frontier block kept alive as its successor's
	// read-through parent, and the comment at the foot of this function says its data is "copied
	// above, non-destructively" — which was not true of the writers: they were confiscated and
	// nilled whatever the caller asked for. The batch was then alive for READING and dead for
	// WRITING, and the pre-exec round that still owned it dereferenced nothing and killed the node
	// on every 9,600-order soak pass (all six domains warned in the same millisecond).
	//
	// The pending-commitment transfer a few lines below already skips itself when not closing, for
	// exactly this reason. This is the same rule.
	// ⚠ UNCONDITIONAL FOR NOW, AND THAT IS A KNOWN HAZARD. Guarding this with closeOther stopped the
	// destination ever receiving the source's buffered writes, and nothing committed: measured on
	// live448, every block executed with "offered=23 kept=0 dropped=0 deferred=23" and the CLOB never
	// deployed. The source's writes evidently DO have to move even when the source stays alive as a
	// read-through parent, so the fix for the 9,600 crash is not here.
	//
	// What protects the node meanwhile is writableFor: a pre-exec round that reaches the emptied
	// batch now fails its round with a named error instead of dereferencing nothing.
	sd.takeWritersFrom(other)

	if sd.currentChangesAccumulator != nil {
		return fmt.Errorf("can't merge to batch with non-nil currentChangesAccumulator")
	}

	if other.currentChangesAccumulator != nil {
		return fmt.Errorf("can't merge from batch with non-nil currentChangesAccumulator")
	}

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
			if sd.unwindToTxNum < other.unwindToTxNum {
				sd.unwindToTxNum = other.unwindToTxNum
			}
		}
	}

	// closeOther=false ([[consensus_advance_untested_regression]] merge-vs-parenting): when the source is a
	// frontier block kept alive as its successor's read-through parent, do NOT close it here — its domain data
	// (copied above, non-destructively) must stay readable via GetLatest until the block is retired. Closing
	// here is what invalidated the parked SD and broke parenting past the first hop. Retirement closes it.
	if closeOther {
		other.Close()
	}
	return nil
}

// flushLocked is the body of Flush, factored so the callback path can run it
// inside latestStateLock without re-acquiring.
func (sd *TemporalMemBatch) flushLocked(ctx context.Context, tx kv.RwTx) error {
	// [flush-audit] A MERGED-AWAY BATCH FLUSHES NOTHING, SILENTLY. Merge hands this batch's six domain
	// writers to another batch and nils them here; flushWriters below then has nothing to write and
	// returns nil. READS still succeed — sd.domains keeps the values — so every check that reads the
	// SharedDomains agrees with the body while the COMMITTED state silently loses those writes.
	//
	// That is the measured shape: live475 block 1334 passed the seal audit (SD state matched the body)
	// and the committed chain is 27 transactions short, stranding the sponsor at nonce 12584 with
	// 18,000 pending. Same on live472 block 1318 and live454 block 2789.
	if sd.mergedAway.Load() {
		var present int
		for _, w := range sd.domainWriters {
			if w != nil {
				present++
			}
		}
		log.Warn("[flush-audit] flushing a batch whose writers were handed away by Merge — its writes cannot land",
			"writersPresent", present, "pastWriterSets", len(sd.pastDomainWriters), "domainsHeld", len(sd.domains))
	}
	if sd.unwindChangesetRaw != nil {
		for domain := range sd.unwindChangesetRaw {
			sort.Slice(sd.unwindChangesetRaw[domain], func(i, j int) bool {
				return sd.unwindChangesetRaw[domain][i].Key < sd.unwindChangesetRaw[domain][j].Key
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
	if _, err := rawdb.IncrementStateVersion(tx); err != nil {
		return fmt.Errorf("can't write plain state version: %w", err)
	}
	return nil
}

// Flush writes the mem-batch to tx.
func (sd *TemporalMemBatch) Flush(ctx context.Context, tx kv.RwTx) error {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	return sd.flushLocked(ctx, tx)
}

// Flushes the batch then invokes cb per commitment tuple under the lock.
func (sd *TemporalMemBatch) FlushWithCommitmentCallback(ctx context.Context, tx kv.RwTx, cb execctx.CommitmentFlushCallback) error {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()

	if err := sd.flushLocked(ctx, tx); err != nil {
		return err
	}

	if cb != nil {
		for keyStr, history := range sd.domains[kv.CommitmentDomain] {
			if len(history) == 0 {
				continue
			}
			latest := history[len(history)-1]
			cb([]byte(keyStr), latest.data, kv.Step(latest.txNum/sd.stepSize), latest.txNum)
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

func (sd *TemporalMemBatch) flushWriters(ctx context.Context, tx kv.RwTx) error {
	aggTx := AggTx(tx)
	// Writer ORDER no longer decides anything here. It used to: values were replayed from the writer
	// stack, only the latest value per (key, step) survives, and the writer applied last won — which was
	// not always the writer holding the newest write, because a batch keeps writing to its own writer
	// across a merge and its txNum range straddles the merged round's. That committed a two-round block
	// as if only its first round had run (live477 block 364: sealed 223, committed 128).
	//
	// Values now come from the maps, below, one entry per (key, step). The writers carry HISTORY only,
	// which is keyed by txNum and cannot overwrite itself, so these writers may be flushed in any order.
	// VALUES COME FROM THE MAPS, NOT FROM THE WRITER STACK.
	//
	// Only the latest value per (key, step) survives in the values table, so when several writers hold
	// the same key the one applied LAST wins. That made correctness depend on the writers being in
	// txNum order — which a merge cannot guarantee, because a batch keeps writing to its own writer
	// across one (block-start at tx3, a round merged at tx4, block-end at tx5), so its range straddles
	// the round's and no ordering of the two is right. A two-round block committed the coinbase as of
	// tx3 while the batch held tx4; a single-round block never lost anything.
	//
	// The maps have no such problem. Every write goes through DomainPut/DomainDel, which record the
	// value in `domains`/`storage` AND the history in the writer; Merge folds a child's maps in with
	// maps.Copy, newest winning. That is why reads are right — the parent chain is already in txNum
	// order — and it means that by flush time this batch's maps hold the merged FINAL value for every
	// key it wrote. So emit each key ONCE, from the maps, through one collector per domain. There is
	// no ordering left to get wrong.
	//
	// History still comes from the writers: it is keyed by txNum, so no history write can overwrite
	// another and the order they are applied in does not matter.
	for _, ws := range sd.pastDomainWriters {
		for i := range ws {
			if ws[i] == nil {
				continue
			}
			if err := ws[i].FlushHistory(ctx, tx); err != nil {
				return err
			}
		}
	}
	for _, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		if err := w.FlushHistory(ctx, tx); err != nil {
			return err
		}
	}

	for di := range sd.domainWriters {
		w := sd.domainWriters[di]
		if w == nil {
			continue
		}
		// Drop what execution collected into this writer: it is the per-writer stream the maps replace.
		w.ResetValues()
		domain := kv.Domain(di)
		var err error
		// ONE ENTRY PER (KEY, STEP), not one per key. The values table is keyed by key AND step, so a
		// batch spanning a step boundary legitimately leaves an entry in each step it touched, and
		// collapsing to the final value alone would delete the earlier steps' entries. Within a step
		// only the newest write survives anyway, so emit the last version in each step, oldest step
		// first — which is also the order the writers produced them in.
		emit := func(key string, versions []dataWithTxNum) error {
			if len(versions) == 0 {
				return nil
			}
			k := common.ToBytesZeroCopy(key)
			for i, v := range versions {
				// Keep only the last version within its step.
				if i+1 < len(versions) && versions[i+1].txNum/sd.stepSize == v.txNum/sd.stepSize {
					continue
				}
				if werr := w.AddValue(k, v.data, v.txNum); werr != nil {
					return werr
				}
			}
			return nil
		}
		if domain == kv.StorageDomain {
			sd.storage.Scan(func(key string, versions []dataWithTxNum) bool {
				err = emit(key, versions)
				return err == nil
			})
		} else {
			for key, versions := range sd.domains[di] {
				if err = emit(key, versions); err != nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if err := w.FlushValues(ctx, tx); err != nil {
			return err
		}
		aggTx.d[di].closeValsCursor()
		w.Close()
	}
	// The past writers' value collectors were never loaded — release them.
	for _, ws := range sd.pastDomainWriters {
		for i := range ws {
			ws[i].Close()
		}
	}

	for i := len(sd.pastIIWriters) - 1; i >= 0; i-- {
		if err := sd.pastIIWriters[i].Flush(ctx, tx); err != nil {
			return err
		}
		sd.pastIIWriters[i].close()
	}
	for _, w := range sd.iiWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
		w.close()
	}
	for _, ws := range sd.pastForkableWriters {
		for i := len(ws) - 1; i >= 0; i-- {
			if err := ws[i].Flush(ctx, tx); err != nil {
				return err
			}
			ws[i].Close()
		}
	}
	for _, w := range sd.forkableWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
		w.Close()
	}
	return nil
}

func (sd *TemporalMemBatch) DiscardWrites(domain kv.Domain) {
	sd.domainWriters[domain].discard = true
	sd.domainWriters[domain].h.discard = true
	if ws := sd.pastDomainWriters[domain]; len(ws) > 0 {
		for _, w := range ws {
			w.discard = true
			w.h.discard = true
		}
	}
}

func AggTx(tx kv.Tx) *AggregatorRoTx {
	if withAggTx, ok := tx.(interface{ AggTx() any }); ok {
		return withAggTx.AggTx().(*AggregatorRoTx)
	}

	return nil
}
