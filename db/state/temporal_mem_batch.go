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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/kvmetrics"
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

	// inMemHistoryReads: accumulate all writes with txNums so GetAsOf can answer time-travel
	// queries from in-flight state (needed for RPC reads during live chain-tip execution).
	// Distinct from DomainBufferedWriter which also holds history in mem but only for writing.
	// Disable for offline commands (stage_exec etc.) — history reads come from disk anyway.
	inMemHistoryReads bool

	latestStateLock sync.RWMutex
	domains         [kv.DomainLen]map[string][]dataWithTxNum
	storage         *btree2.Map[string, []dataWithTxNum] // TODO: replace hardcoded domain name to per-config configuration of available Guarantees/AccessMethods (range vs get)

	domainWriters [kv.DomainLen]*DomainBufferedWriter
	iiWriters     []*InvertedIndexBufferedWriter
	// iiMem is the queryable local copy of standalone inverted-index writes
	// (index -> key -> ascending txNums). The ii writers are write-only etl
	// buffers, so without this IndexRange can't answer in-flight queries for
	// standalone indices (traces, logs) the way it can for domain-backed ones
	// via the domain history. Populated by IndexAdd only when inMemHistoryReads
	// is set — the same gate under which the domain history above is retained.
	iiMem map[kv.InvertedIdx]map[string][]uint64

	pastDomainWriters [kv.DomainLen][]*DomainBufferedWriter
	pastIIWriters     []*InvertedIndexBufferedWriter

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

func NewTemporalMemBatch(tx kv.TemporalTx, ioMetrics any) *TemporalMemBatch {
	sd := &TemporalMemBatch{
		storage:           btree2.NewMap[string, []dataWithTxNum](128),
		metrics:           ioMetrics.(*kvmetrics.DomainMetrics),
		inMemHistoryReads: true,
		iiMem:             map[kv.InvertedIdx]map[string][]uint64{},
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

	return sd
}

func (sd *TemporalMemBatch) SetInMemHistoryReads(v bool) { sd.inMemHistoryReads = v }
func (sd *TemporalMemBatch) InMemHistoryReads() bool     { return sd.inMemHistoryReads }

func (sd *TemporalMemBatch) DomainPut(domain kv.Domain, k string, v []byte, txNum uint64, preval []byte) error {
	sameTxNumUpdate := sd.putLatest(domain, k, v, txNum)
	return sd.putHistory(domain, common.ToBytesZeroCopy(k), v, txNum, preval, sameTxNumUpdate)
}

func (sd *TemporalMemBatch) DomainDel(domain kv.Domain, k string, txNum uint64, preval []byte) error {
	sameTxNumUpdate := sd.putLatest(domain, k, nil, txNum)
	return sd.putHistory(domain, common.ToBytesZeroCopy(k), nil, txNum, preval, sameTxNumUpdate)
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
			sd.metrics.Domains[domain] = &kvmetrics.DomainIOMetrics{
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
			if old[len(old)-1].txNum == txNum {
				sameTxNumUpdate = true
				putValueSize += len(val) - len(old[len(old)-1].data)
				old[len(old)-1] = valWithStep
				sd.storage.Set(key, old)
			} else if sd.inMemHistoryReads {
				sd.storage.Set(key, append(old, valWithStep))
				putValueSize += len(val)
			} else {
				putValueSize += len(val) - len(old[len(old)-1].data)
				old[0] = valWithStep
				sd.storage.Set(key, old[:1])
			}
		} else {
			sd.storage.Set(key, []dataWithTxNum{valWithStep})
			putKeySize += len(key)
			putValueSize += len(val)
		}

		updateMetrics(domain, putKeySize, putValueSize)
		return sameTxNumUpdate
	}

	if old, ok := sd.domains[domain][key]; ok {
		if old[len(old)-1].txNum == txNum {
			sameTxNumUpdate = true
			putValueSize += len(val) - len(old[len(old)-1].data)
			old[len(old)-1] = valWithStep
			sd.domains[domain][key] = old
		} else if sd.inMemHistoryReads {
			sd.domains[domain][key] = append(old, valWithStep)
			putValueSize += len(val)
		} else {
			putValueSize += len(val) - len(old[len(old)-1].data)
			old[0] = valWithStep
			sd.domains[domain][key] = old[:1]
		}
	} else {
		sd.domains[domain][key] = []dataWithTxNum{valWithStep}
		putKeySize += len(key)
		putValueSize += len(val)
	}

	updateMetrics(domain, putKeySize, putValueSize)
	return sameTxNumUpdate
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

		// kv.NoStepBound distinguishes "no in-flight unwind bounds this key"
		// from a bound at step 0 (a delete-shape entry above whose keyStep is
		// 0), so young chains — whose whole state lives in step 0 — still get
		// the per-key signal.
		return nil, kv.NoStepBound, false
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
	if !sd.inMemHistoryReads {
		return nil, false, errors.New("GetAsOf called on TemporalMemBatch with inMemHistoryReads disabled")
	}
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

// RangeAsOf returns domain values over [fromKey, toKey) as of txNum ts, merging
// the in-memory (not-yet-committed) history with committed DB+files. In-memory
// values take precedence per key; keys deleted or not yet created as of ts are
// omitted. Only used by the RPC test harness to read the in-flight tip.
func (sd *TemporalMemBatch) RangeAsOf(ctx context.Context, domain kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int, roTx kv.Tx) (stream.KV, error) {
	committed, err := AggTx(roTx).RangeAsOf(ctx, roTx, domain, fromKey, toKey, ts, asc, limit)
	if err != nil {
		return nil, err
	}
	if !sd.inMemHistoryReads {
		return committed, nil
	}
	mem := sd.memRangeAsOf(domain, fromKey, toKey, ts, asc)
	if len(mem) == 0 {
		return committed, nil
	}
	merged := stream.UnionKV(&memKVIter{pairs: mem}, committed, -1)
	return &liveLimitKV{inner: merged, limit: limit}, nil
}

// memRangeAsOf collects the in-memory latest history for domain over
// [fromKey, toKey), resolved as of ts. Tombstones (empty value) are kept so
// they mask committed rows; keys with no in-memory entry at or before ts are
// omitted so committed state supplies them.
func (sd *TemporalMemBatch) memRangeAsOf(domain kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By) []memKVPair {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	inRange := func(k string) bool {
		if fromKey != nil && k < string(fromKey) {
			return false
		}
		if toKey != nil && k >= string(toKey) {
			return false
		}
		return true
	}
	var pairs []memKVPair
	collect := func(k string, entries []dataWithTxNum) {
		if !inRange(k) {
			return
		}
		if v, ok := asOfEntry(entries, ts); ok {
			pairs = append(pairs, memKVPair{k: []byte(k), v: v})
		}
	}
	if domain == kv.StorageDomain {
		sd.storage.Scan(func(k string, entries []dataWithTxNum) bool {
			collect(k, entries)
			return true
		})
	} else {
		for k, entries := range sd.domains[domain] {
			collect(k, entries)
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if asc == order.Asc {
			return bytes.Compare(pairs[i].k, pairs[j].k) < 0
		}
		return bytes.Compare(pairs[i].k, pairs[j].k) > 0
	})
	return pairs
}

// HistorySeek returns the in-memory historical value of key as of ts: the value
// just before the first recorded change at or after ts. ok is true when the key
// has such a change in memory; a creation event yields (non-nil empty, true).
// Returns (nil, false) when ts is past all in-memory changes so the caller can
// fall back to committed history.
func (sd *TemporalMemBatch) HistorySeek(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	if !sd.inMemHistoryReads {
		return nil, false, nil
	}
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	ks := common.ToStringZeroCopy(key)
	var entries []dataWithTxNum
	if domain == kv.StorageDomain {
		entries, _ = sd.storage.Get(ks)
	} else {
		entries = sd.domains[domain][ks]
	}
	for i := range entries {
		if entries[i].txNum >= ts {
			if i == 0 {
				return []byte{}, true, nil
			}
			return entries[i-1].data, true, nil
		}
	}
	return nil, false, nil
}

// asOfEntry returns the value in effect at ts from a txNum-ascending history
// (the entry with the greatest txNum < ts). ok is false when ts precedes the
// first entry, i.e. the key did not exist yet.
func asOfEntry(entries []dataWithTxNum, ts uint64) ([]byte, bool) {
	for i := range entries {
		if ts > entries[i].txNum && (i == len(entries)-1 || ts <= entries[i+1].txNum) {
			return entries[i].data, true
		}
	}
	return nil, false
}

// HistoryRange returns one entry per key changed in [fromTs, toTs), each with
// its value just before the range, merging in-memory history with committed.
// In-memory keys take precedence. Only used by the RPC test harness.
func (sd *TemporalMemBatch) HistoryRange(ctx context.Context, domain kv.Domain, fromTs, toTs int, asc order.By, limit int, roTx kv.Tx) (stream.KV, error) {
	committed, err := AggTx(roTx).HistoryRange(domain, fromTs, toTs, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	if !sd.inMemHistoryReads {
		return committed, nil
	}
	mem := sd.memHistoryRange(domain, fromTs, toTs, asc, roTx)
	if len(mem) == 0 {
		return committed, nil
	}
	return stream.UnionKV(&memKVIter{pairs: mem}, committed, limit), nil
}

// memHistoryRange collects keys changed in [fromTs, toTs) from the in-memory
// history, each paired with its pre-range value (in-memory value just before
// fromTs, falling back to committed state when the key predates its in-memory
// history). Empty pre-values are kept — they mean "did not exist before".
func (sd *TemporalMemBatch) memHistoryRange(domain kv.Domain, fromTs, toTs int, asc order.By, roTx kv.Tx) []memKVPair {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	from, to := uint64(fromTs), uint64(toTs)
	var pairs []memKVPair
	collect := func(k string, entries []dataWithTxNum) {
		changed := false
		for i := range entries {
			if entries[i].txNum >= from && entries[i].txNum < to {
				changed = true
				break
			}
		}
		if !changed {
			return
		}
		pre, ok := asOfEntry(entries, from)
		if !ok {
			pre, _, _ = AggTx(roTx).GetAsOf(domain, common.ToBytesZeroCopy(k), from, roTx)
		}
		pairs = append(pairs, memKVPair{k: []byte(k), v: pre})
	}
	if domain == kv.StorageDomain {
		sd.storage.Scan(func(k string, entries []dataWithTxNum) bool {
			collect(k, entries)
			return true
		})
	} else {
		for k, entries := range sd.domains[domain] {
			collect(k, entries)
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if asc == order.Asc {
			return bytes.Compare(pairs[i].k, pairs[j].k) < 0
		}
		return bytes.Compare(pairs[i].k, pairs[j].k) > 0
	})
	return pairs
}

// IndexRange returns the txNums at which key k appears in [fromTs, toTs),
// merging the in-flight in-memory index with the committed inverted index.
// Domain-backed indices derive their in-memory txNums from the domain history;
// standalone indices (logs, traces) read them from the local ii collection.
func (sd *TemporalMemBatch) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	committed, err := AggTx(roTx).IndexRange(name, k, fromTs, toTs, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	if !sd.inMemHistoryReads {
		return committed, nil
	}
	at := AggTx(roTx)
	var memTs []uint64
	found := false
	for i := range at.d {
		if at.d[i].d.HistoryIdx == name {
			memTs = sd.memIndexTxNums(kv.Domain(i), k, fromTs, toTs, asc)
			found = true
			break
		}
	}
	if !found {
		memTs = sd.memIndexTxNumsII(name, k, fromTs, toTs, asc)
	}
	if len(memTs) == 0 {
		return committed, nil
	}
	return stream.Union[uint64](stream.Array(memTs), committed, asc, limit), nil
}

// memIndexTxNumsII reads the local standalone inverted-index collection for the
// txNums of key k in [fromTs, toTs), deduplicated and ordered per asc.
func (sd *TemporalMemBatch) memIndexTxNumsII(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By) []uint64 {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	txNums := sd.iiMem[name][common.ToStringZeroCopy(k)]
	out := make([]uint64, 0, len(txNums))
	for _, tn := range txNums {
		if !idxTxNumInRange(tn, fromTs, toTs, asc) {
			continue
		}
		if len(out) > 0 && out[len(out)-1] == tn {
			continue // ascending append order makes duplicates consecutive
		}
		out = append(out, tn)
	}
	if asc == order.Desc {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
}

func (sd *TemporalMemBatch) memIndexTxNums(domain kv.Domain, k []byte, fromTs, toTs int, asc order.By) []uint64 {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	ks := common.ToStringZeroCopy(k)
	var entries []dataWithTxNum
	if domain == kv.StorageDomain {
		entries, _ = sd.storage.Get(ks)
	} else {
		entries = sd.domains[domain][ks]
	}
	out := make([]uint64, 0, len(entries))
	for i := range entries {
		if tn := entries[i].txNum; idxTxNumInRange(tn, fromTs, toTs, asc) {
			out = append(out, tn)
		}
	}
	if asc == order.Desc {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
}

// idxTxNumInRange applies the inverted-index range bounds: asc = [fromTs, toTs),
// desc = (toTs, fromTs]. A negative bound is unbounded.
func idxTxNumInRange(tn uint64, fromTs, toTs int, asc order.By) bool {
	if asc == order.Asc {
		return (fromTs < 0 || tn >= uint64(fromTs)) && (toTs < 0 || tn < uint64(toTs))
	}
	return (fromTs < 0 || tn <= uint64(fromTs)) && (toTs < 0 || tn > uint64(toTs))
}

type memKVPair struct{ k, v []byte }

type memKVIter struct {
	pairs []memKVPair
	i     int
}

func (m *memKVIter) HasNext() bool { return m.i < len(m.pairs) }
func (m *memKVIter) Close()        {}
func (m *memKVIter) Next() ([]byte, []byte, error) {
	p := m.pairs[m.i]
	m.i++
	return p.k, p.v, nil
}

// liveLimitKV drops tombstone (empty-value) rows from the merged stream and
// stops after limit live rows (limit < 0 means unlimited).
type liveLimitKV struct {
	inner stream.KV
	limit int
	seen  int
	k, v  []byte
	ready bool
	err   error
}

func (m *liveLimitKV) advance() {
	if m.ready || m.err != nil {
		return
	}
	if m.limit >= 0 && m.seen >= m.limit {
		return
	}
	for m.inner.HasNext() {
		k, v, err := m.inner.Next()
		if err != nil {
			m.err = err
			return
		}
		if len(v) == 0 {
			continue
		}
		m.k, m.v, m.ready = k, v, true
		return
	}
}

func (m *liveLimitKV) HasNext() bool { m.advance(); return m.ready || m.err != nil }
func (m *liveLimitKV) Close()        { m.inner.Close() }
func (m *liveLimitKV) Next() ([]byte, []byte, error) {
	m.advance()
	if m.err != nil {
		return nil, nil, m.err
	}
	m.ready = false
	m.seen++
	return m.k, m.v, nil
}

func (sd *TemporalMemBatch) SizeEstimate() uint64 {
	// CachePutSize is guarded by the metrics lock — the put path, Merge and the
	// reset all mutate it under sd.metrics.Lock(), and MergeMetrics folds in a
	// boundary worker's accumulator from another goroutine. Read under the same
	// lock (not latestStateLock, which guards the domains map, not this counter).
	sd.metrics.RLock()
	defer sd.metrics.RUnlock()
	return uint64(sd.metrics.CachePutSize)
}

func (sd *TemporalMemBatch) ClearRam() {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	for i := range sd.domains {
		sd.domains[i] = map[string][]dataWithTxNum{}
	}

	sd.storage = btree2.NewMap[string, []dataWithTxNum](128)
	sd.iiMem = map[kv.InvertedIdx]map[string][]uint64{}
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
	for table, byKey := range sd.iiMem {
		for k, txNums := range byKey {
			kept := txNums[:0]
			for _, tn := range txNums {
				if tn <= unwindToTxNum {
					kept = append(kept, tn)
				}
			}
			if len(kept) == 0 {
				delete(byKey, k)
			} else {
				byKey[k] = kept
			}
		}
		if len(byKey) == 0 {
			delete(sd.iiMem, table)
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
			if sd.inMemHistoryReads {
				sd.iiMemAdd(table, key, txNum)
			}
			return writer.Add(key, txNum)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

func (sd *TemporalMemBatch) iiMemAdd(table kv.InvertedIdx, key []byte, txNum uint64) {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	byKey := sd.iiMem[table]
	if byKey == nil {
		byKey = map[string][]uint64{}
		sd.iiMem[table] = byKey
	}
	ks := string(key)
	// IndexAdd is called in ascending txNum order during execution, so append
	// keeps each key's txNum slice sorted.
	byKey[ks] = append(byKey[ks], txNum)
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
	sd.ClearRam()
}

func (sd *TemporalMemBatch) Merge(o kv.TemporalMemBatch) error {
	other, ok := o.(*TemporalMemBatch)
	if !ok {
		return fmt.Errorf("Can't merge %T into *TemporalMemBatch", o)
	}

	for domain, otherEntries := range other.domains {
		entries := sd.domains[domain]
		maps.Copy(entries, otherEntries)
	}

	other.storage.Scan(func(key string, value []dataWithTxNum) bool {
		sd.storage.Set(key, value)
		return true
	})

	for domain, writer := range other.domainWriters {
		sd.pastDomainWriters[domain] = append(sd.pastDomainWriters[domain], writer)
		other.domainWriters[domain] = nil
	}

	for domain, writers := range other.pastDomainWriters {
		sd.pastDomainWriters[domain] = append(sd.pastDomainWriters[domain], writers...)
		other.pastDomainWriters[domain] = nil
	}

	sd.pastIIWriters = append(sd.pastIIWriters, other.iiWriters...)
	other.iiWriters = nil
	sd.pastIIWriters = append(sd.pastIIWriters, other.pastIIWriters...)
	other.pastIIWriters = nil

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

	other.Close()
	return nil
}

// flushLocked is the body of Flush, factored so the callback path can run it
// inside latestStateLock without re-acquiring.
func (sd *TemporalMemBatch) flushLocked(ctx context.Context, tx kv.RwTx) error {
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

// Flush writes the mem-batch to tx. With kv.WithFlushCallback options, the
// registered per-domain callback is invoked for every (key, value, step, txNum)
// tuple after the MDBX write succeeds, so a downstream cache can never be left
// ahead of MDBX. Runs under latestStateLock so the callback's snapshot matches
// flush-time state.
func (sd *TemporalMemBatch) Flush(ctx context.Context, tx kv.RwTx, opts ...kv.FlushOption) error {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()

	if err := sd.flushLocked(ctx, tx); err != nil {
		return err
	}

	if len(opts) > 0 {
		var cfg kv.FlushConfig
		for _, opt := range opts {
			opt(&cfg)
		}
		for domain, cb := range cfg.DomainCallbacks {
			// StorageDomain values live in the separate sd.storage btree, not
			// sd.domains[StorageDomain]; iterate it directly so the storage
			// cache is flush-updated and never serves stale slots.
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
			for keyStr, history := range sd.domains[domain] {
				if len(history) == 0 {
					continue
				}
				latest := history[len(history)-1]
				cb([]byte(keyStr), latest.data, kv.Step(latest.txNum/sd.stepSize), latest.txNum)
			}
		}
	}

	return nil
}

// FlushWithCommitmentCallback flushes the batch then invokes cb per
// commitment-domain tuple under the lock.
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
	for _, ws := range sd.pastDomainWriters {
		for _, w := range slices.Backward(ws) {
			if err := w.Flush(ctx, tx); err != nil {
				return err
			}
			w.Close()
		}
	}
	for di, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
		aggTx.d[di].closeValsCursor() //TODO: why?
		w.Close()
	}
	for _, writer := range slices.Backward(sd.pastIIWriters) {
		if err := writer.Flush(ctx, tx); err != nil {
			return err
		}
		writer.close()
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
