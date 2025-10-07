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
	"sync"
	"time"
	"unsafe"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

type iodir int

const (
	get iodir = iota
	put
)

type dataWithPrevStep struct {
	data     []byte
	prevStep kv.Step
	dir      iodir
}

// TemporalMemBatch - temporal read-write interface - which storing updates in RAM. Don't forget to call `.Flush()`
type TemporalMemBatch struct {
	stepSize uint64

	getCacheSize int

	latestStateLock sync.RWMutex
	domains         [kv.DomainLen]map[string]dataWithPrevStep
	storage         *btree2.Map[string, dataWithPrevStep] // TODO: replace hardcoded domain name to per-config configuration of available Guarantees/AccessMethods (range vs get)

	domainWriters [kv.DomainLen]*DomainBufferedWriter
	iiWriters     []*InvertedIndexBufferedWriter

	currentChangesAccumulator *changeset.StateChangeSet
	pastChangesAccumulator    map[string]*changeset.StateChangeSet
	metrics                   *DomainMetrics
}

func newTemporalMemBatch(tx kv.TemporalTx, metrics *DomainMetrics) *TemporalMemBatch {
	sd := &TemporalMemBatch{
		storage: btree2.NewMap[string, dataWithPrevStep](128),
		metrics: metrics,
	}
	aggTx := AggTx(tx)
	sd.stepSize = aggTx.StepSize()

	sd.iiWriters = make([]*InvertedIndexBufferedWriter, len(aggTx.iis))

	for id, ii := range aggTx.iis {
		sd.iiWriters[id] = ii.NewWriter()
	}

	for id, d := range aggTx.d {
		sd.domains[id] = map[string]dataWithPrevStep{}
		sd.domainWriters[id] = d.NewWriter()
	}

	return sd
}

func (sd *TemporalMemBatch) DomainPut(domain kv.Domain, k string, v []byte, txNum uint64, preval []byte, prevStep kv.Step) error {
	sd.putLatest(domain, k, v, txNum)
	return sd.putHistory(domain, toBytesZeroCopy(k), v, txNum, preval, prevStep)
}

func (sd *TemporalMemBatch) DomainDel(domain kv.Domain, k string, txNum uint64, preval []byte, prevStep kv.Step) error {
	sd.putLatest(domain, k, nil, txNum)
	return sd.putHistory(domain, toBytesZeroCopy(k), nil, txNum, preval, prevStep)
}

func (sd *TemporalMemBatch) putHistory(domain kv.Domain, k, v []byte, txNum uint64, preval []byte, prevStep kv.Step) error {
	if len(v) == 0 {
		return sd.domainWriters[domain].DeleteWithPrev(k, txNum, preval, prevStep)
	}
	return sd.domainWriters[domain].PutWithPrev(k, v, txNum, preval, prevStep)
}

func (sd *TemporalMemBatch) putLatest(domain kv.Domain, key string, val []byte, txNum uint64) {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	valWithPrevStep := dataWithPrevStep{data: val, prevStep: kv.Step(txNum / sd.stepSize)}
	putSize := 0
	if domain == kv.StorageDomain {
		if old, ok := sd.storage.Set(key, valWithPrevStep); ok {
			putSize += len(val) - len(old.data)
		} else {
			putSize += len(key) + len(val)
		}

		sd.metrics.Lock()
		sd.metrics.CachePutCount++
		sd.metrics.CachePutSize += putSize
		if dm, ok := sd.metrics.Domains[domain]; ok {
			dm.CachePutCount++
			dm.CachePutSize += putSize
		} else {
			sd.metrics.Domains[domain] = &DomainIOMetrics{
				CachePutCount: 1,
				CachePutSize:  putSize,
			}
		}
		sd.metrics.Unlock()
		return
	}

	if old, ok := sd.domains[domain][key]; ok {
		putSize += len(val) - len(old.data)
	} else {
		putSize += len(key) + len(val)
	}
	sd.domains[domain][key] = valWithPrevStep

	if dm, ok := sd.metrics.Domains[domain]; ok {
		dm.CachePutCount++
		dm.CachePutSize += putSize
	} else {
		sd.metrics.Domains[domain] = &DomainIOMetrics{
			CachePutCount: 1,
			CachePutSize:  putSize,
		}
	}
	sd.metrics.Lock()
	sd.metrics.CachePutCount++
	sd.metrics.CachePutSize += putSize
	sd.metrics.Unlock()
}

func (sd *TemporalMemBatch) GetLatest(table kv.Domain, key []byte) (v []byte, prevStep kv.Step, ok bool) {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()

	keyS := toStringZeroCopy(key)
	var dataWithPrevStep dataWithPrevStep
	if table == kv.StorageDomain {
		dataWithPrevStep, ok = sd.storage.Get(keyS)
		return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok

	}

	dataWithPrevStep, ok = sd.domains[table][keyS]
	return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok
}

func (sd *TemporalMemBatch) SizeEstimate() uint64 {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()

	// multiply 2: to cover data-structures overhead (and keep accounting cheap)
	// and muliply 2 more: for Commitment calculation when batch is full
	return uint64(sd.metrics.CachePutSize) * 4
}

func (sd *TemporalMemBatch) ClearRam() {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	for i := range sd.domains {
		sd.domains[i] = map[string]dataWithPrevStep{}
	}

	sd.storage = btree2.NewMap[string, dataWithPrevStep](128)
	sd.metrics.Lock()
	defer sd.metrics.Unlock()
	sd.metrics.CachePutSize = 0
	sd.metrics.CachePutCount = 0
	for _, dm := range sd.metrics.Domains {
		dm.CachePutCount = 0
		dm.CachePutSize = 0
	}
}

func (sd *TemporalMemBatch) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte, step kv.Step) (cont bool, err error)) error {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	var ramIter btree2.MapIter[string, dataWithPrevStep]
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

func (sd *TemporalMemBatch) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	for _, writer := range sd.iiWriters {
		if writer.name == table {
			return writer.Add(key, txNum)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

func (sd *TemporalMemBatch) Close() {
	for _, d := range sd.domainWriters {
		d.Close()
	}
	for _, iiWriter := range sd.iiWriters {
		iiWriter.close()
	}
}
func (sd *TemporalMemBatch) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	if err := sd.flushDiffSet(ctx, tx); err != nil {
		return err
	}
	sd.pastChangesAccumulator = make(map[string]*changeset.StateChangeSet)
	if err := sd.flushWriters(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (sd *TemporalMemBatch) flushDiffSet(ctx context.Context, tx kv.RwTx) error {
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
