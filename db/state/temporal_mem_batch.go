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
	"maps"
	"sort"
	"sync"
	"unsafe"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
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

	latestStateLock sync.RWMutex
	domains         [kv.DomainLen]map[string][]dataWithTxNum
	storage         *btree2.Map[string, []dataWithTxNum] // TODO: replace hardcoded domain name to per-config configuration of available Guarantees/AccessMethods (range vs get)

	domainWriters   [kv.DomainLen]*DomainBufferedWriter
	iiWriters       []*InvertedIndexBufferedWriter
	forkableWriters map[kv.ForkableId]kv.BufferedWriter

	pastDomainWriters   [kv.DomainLen][]*DomainBufferedWriter
	pastIIWriters       []*InvertedIndexBufferedWriter
	pastForkableWriters map[kv.ForkableId][]kv.BufferedWriter

	currentChangesAccumulator *changeset.StateChangeSet
	pastChangesAccumulator    map[string]*changeset.StateChangeSet

	unwindToTxNum   uint64
	unwindChangeset *[kv.DomainLen]map[string]kv.DomainEntryDiff

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
	sd.putLatest(domain, k, v, txNum)
	return sd.putHistory(domain, toBytesZeroCopy(k), v, txNum, preval)
}

func (sd *TemporalMemBatch) DomainDel(domain kv.Domain, k string, txNum uint64, preval []byte) error {
	sd.putLatest(domain, k, nil, txNum)
	return sd.putHistory(domain, toBytesZeroCopy(k), nil, txNum, preval)
}

func (sd *TemporalMemBatch) putHistory(domain kv.Domain, k, v []byte, txNum uint64, preval []byte) error {
	if len(v) == 0 {
		return sd.domainWriters[domain].DeleteWithPrev(k, txNum, preval)
	}
	return sd.domainWriters[domain].PutWithPrev(k, v, txNum, preval)
}

func (sd *TemporalMemBatch) putLatest(domain kv.Domain, key string, val []byte, txNum uint64) {
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

	valWithStep := dataWithTxNum{data: val, txNum: txNum}
	putKeySize := 0
	putValueSize := 0
	if domain == kv.StorageDomain {
		if old, ok := sd.storage.Get(key); ok {
			sd.storage.Set(key, append(old, valWithStep))
			putValueSize += len(val) - len(old[len(old)-1].data)
		} else {
			sd.storage.Set(key, []dataWithTxNum{valWithStep})
			putKeySize += len(key)
			putValueSize += len(val)
		}

		updateMetrics(domain, putKeySize, putValueSize)
		return
	}

	if old, ok := sd.domains[domain][key]; ok {
		putValueSize += len(val) - len(old[len(old)-1].data)
		sd.domains[domain][key] = append(old, valWithStep)
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

	keyS := toStringZeroCopy(key)
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

	keyS := toStringZeroCopy(key)
	var dataWithTxNums []dataWithTxNum
	if domain == kv.StorageDomain {
		dataWithTxNums, ok = sd.storage.Get(keyS)
		if !ok {
			return nil, false, nil
		}
		for i, dataWithTxNum := range dataWithTxNums {
			if ts > dataWithTxNum.txNum && (i == len(dataWithTxNums)-1 || ts <= dataWithTxNums[i+1].txNum) {
				return dataWithTxNum.data, true, nil
			}
		}
		return nil, false, nil
	}

	dataWithTxNums, ok = sd.domains[domain][keyS]
	if !ok {
		return nil, false, nil
	}
	for i, dataWithTxNum := range dataWithTxNums {
		if ts > dataWithTxNum.txNum && (i == len(dataWithTxNums)-1 || ts <= dataWithTxNums[i+1].txNum) {
			return dataWithTxNum.data, true, nil
		}
	}
	return nil, false, nil
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

func (sd *TemporalMemBatch) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte, step kv.Step) (cont bool, err error)) error {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	var ramIter btree2.MapIter[string, []dataWithTxNum]
	if domain == kv.StorageDomain {
		ramIter = sd.storage.Iter()
	}

	return AggTx(roTx).d[domain].debugIteratePrefixLatest(prefix, ramIter, it, roTx)
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
	if sd.pastChangesAccumulator == nil {
		sd.pastChangesAccumulator = make(map[string]*changeset.StateChangeSet)
	}
	key := make([]byte, 40)
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesAccumulator[toStringZeroCopy(key)] = acc
}

// GetChangesetByBlockNum returns the changeset for a given block number and its block hash.
func (sd *TemporalMemBatch) GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet) {
	for key, cs := range sd.pastChangesAccumulator {
		keyBytes := toBytesZeroCopy(key)
		if binary.BigEndian.Uint64(keyBytes[:8]) == blockNumber {
			blockHash := common.BytesToHash(keyBytes[8:])
			return blockHash, cs
		}
	}
	return common.Hash{}, nil
}

func (sd *TemporalMemBatch) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	var key [40]byte
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	if changeset, ok := sd.pastChangesAccumulator[toStringZeroCopy(key[:])]; ok {
		return [kv.DomainLen][]kv.DomainEntryDiff{
			changeset.Diffs[kv.AccountsDomain].GetDiffSet(),
			changeset.Diffs[kv.StorageDomain].GetDiffSet(),
			changeset.Diffs[kv.CodeDomain].GetDiffSet(),
			changeset.Diffs[kv.CommitmentDomain].GetDiffSet(),
		}, true, nil
	}
	return changeset.ReadDiffSet(tx, blockNumber, blockHash)
}

func (sd *TemporalMemBatch) Unwind(unwindToTxNum uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.unwindToTxNum = unwindToTxNum
	var unwindChangeset *[kv.DomainLen]map[string]kv.DomainEntryDiff

	if changeset != nil {
		unwindChangeset = &[kv.DomainLen]map[string]kv.DomainEntryDiff{}

		for domain, changes := range changeset {
			if unwindChangeset[domain] == nil {
				unwindChangeset[domain] = map[string]kv.DomainEntryDiff{}
			}

			for _, change := range changes {
				unwindChangeset[domain][change.Key[:len(change.Key)-8]] = change
			}
		}
	}

	sd.unwindChangeset = unwindChangeset
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
	for _, iiWriter := range sd.iiWriters {
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

	for id, writer := range other.forkableWriters {
		sd.pastForkableWriters[id] = append(sd.pastForkableWriters[id], writer)
	}
	other.forkableWriters = nil

	for id, writers := range other.pastForkableWriters {
		sd.pastForkableWriters[id] = append(sd.pastForkableWriters[id], writers...)
	}
	other.pastForkableWriters = nil

	if sd.currentChangesAccumulator != nil {
		return fmt.Errorf("can't merge to batch with non-nil currentChangesAccumulator")
	}

	if other.currentChangesAccumulator != nil {
		return fmt.Errorf("can't merge from batch with non-nil currentChangesAccumulator")
	}

	for key, changeSet := range other.pastChangesAccumulator {
		if sd.pastChangesAccumulator == nil {
			sd.pastChangesAccumulator = map[string]*changeset.StateChangeSet{}
		}
		sd.pastChangesAccumulator[key] = changeSet
	}

	if other.unwindChangeset != nil {
		if sd.unwindChangeset == nil {
			sd.unwindToTxNum = other.unwindToTxNum
			sd.unwindChangeset = other.unwindChangeset
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
			if sd.unwindToTxNum < other.unwindToTxNum {
				sd.unwindToTxNum = other.unwindToTxNum
			}
		}
	}

	other.Close()
	return nil
}

func (sd *TemporalMemBatch) Flush(ctx context.Context, tx kv.RwTx) error {
	if sd.unwindChangeset != nil {
		var changeSet [kv.DomainLen][]kv.DomainEntryDiff
		for domain, diffEntries := range sd.unwindChangeset {
			for _, entry := range diffEntries {
				changeSet[domain] = append(changeSet[domain], entry)
			}
			sort.Slice(changeSet[domain], func(i, j int) bool {
				return changeSet[domain][i].Key < changeSet[domain][j].Key
			})
		}
		tx.(kv.TemporalRwTx).Unwind(ctx, sd.unwindToTxNum, &changeSet)
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

func (sd *TemporalMemBatch) flushDiffSet(_ context.Context, tx kv.RwTx) error {
	for key, changeSet := range sd.pastChangesAccumulator {
		blockNum := binary.BigEndian.Uint64(toBytesZeroCopy(key[:8]))
		blockHash := common.BytesToHash(toBytesZeroCopy(key[8:]))
		if err := changeset.WriteDiffSet(tx, blockNum, blockHash, changeSet); err != nil {
			return err
		}
	}
	return nil
}

func (sd *TemporalMemBatch) flushWriters(ctx context.Context, tx kv.RwTx) error {
	aggTx := AggTx(tx)
	for _, ws := range sd.pastDomainWriters {
		for i := len(ws) - 1; i >= 0; i-- {
			if err := ws[i].Flush(ctx, tx); err != nil {
				return err
			}
			ws[i].Close()
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

func toStringZeroCopy(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len(v))
}

func toBytesZeroCopy(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }
