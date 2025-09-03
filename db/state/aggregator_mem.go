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

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
)

type dataWithPrevStep struct {
	data     []byte
	prevStep kv.Step
}

type AggMemBatch struct {
	stepSize uint64

	estSize int

	latestStateLock sync.RWMutex
	domains         [kv.DomainLen]map[string]dataWithPrevStep
	storage         *btree2.Map[string, dataWithPrevStep]

	domainWriters [kv.DomainLen]*DomainBufferedWriter
	iiWriters     []*InvertedIndexBufferedWriter

	currentChangesAccumulator *StateChangeSet
	pastChangesAccumulator    map[string]*StateChangeSet
}

func newAggMemBatch(tx kv.TemporalTx) *AggMemBatch {
	sd := &AggMemBatch{
		storage: btree2.NewMap[string, dataWithPrevStep](128),
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

func (sd *AggMemBatch) PutWithPrev(domain kv.Domain, k, v []byte, txNum uint64, preval []byte, prevStep kv.Step) error {
	if len(v) == 0 {
		return sd.domainWriters[domain].DeleteWithPrev(k, txNum, preval, prevStep)
	}
	return sd.domainWriters[domain].PutWithPrev(k, v, txNum, preval, prevStep)
}

func (sd *AggMemBatch) DomainPut(domain kv.Domain, key string, val []byte, txNum uint64) {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	valWithPrevStep := dataWithPrevStep{data: val, prevStep: kv.Step(txNum / sd.stepSize)}
	if domain == kv.StorageDomain {
		if old, ok := sd.storage.Set(key, valWithPrevStep); ok {
			sd.estSize += len(val) - len(old.data)
		} else {
			sd.estSize += len(key) + len(val)
		}
		return
	}

	if old, ok := sd.domains[domain][key]; ok {
		sd.estSize += len(val) - len(old.data)
	} else {
		sd.estSize += len(key) + len(val)
	}
	sd.domains[domain][key] = valWithPrevStep
}

func (sd *AggMemBatch) GetLatest(table kv.Domain, key []byte) (v []byte, prevStep kv.Step, ok bool) {
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

func (sd *AggMemBatch) SizeEstimate() uint64 {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()

	// multiply 2: to cover data-structures overhead (and keep accounting cheap)
	// and muliply 2 more: for Commitment calculation when batch is full
	return uint64(sd.estSize) * 4
}

func (sd *AggMemBatch) ClearRam() {
	sd.latestStateLock.Lock()
	defer sd.latestStateLock.Unlock()
	for i := range sd.domains {
		sd.domains[i] = map[string]dataWithPrevStep{}
	}

	sd.storage = btree2.NewMap[string, dataWithPrevStep](128)
	sd.estSize = 0
}

func (sd *AggMemBatch) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte, step kv.Step) (cont bool, err error)) error {
	sd.latestStateLock.RLock()
	defer sd.latestStateLock.RUnlock()
	var ramIter btree2.MapIter[string, dataWithPrevStep]
	if domain == kv.StorageDomain { //TODO: replace hardcoded domain name to per-config configuration of available Guarantees/AccessMethods (range vs get)
		ramIter = sd.storage.Iter()
	}

	return AggTx(roTx).d[domain].debugIteratePrefixLatest(prefix, ramIter, it, sd.stepSize, roTx)
}

func (sd *AggMemBatch) SetChangesetAccumulator(acc *StateChangeSet) {
	sd.currentChangesAccumulator = acc
	for idx := range sd.domainWriters {
		if sd.currentChangesAccumulator == nil {
			sd.domainWriters[idx].SetDiff(nil)
		} else {
			sd.domainWriters[idx].SetDiff(&sd.currentChangesAccumulator.Diffs[idx])
		}
	}
}
func (sd *AggMemBatch) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *StateChangeSet) {
	if sd.pastChangesAccumulator == nil {
		sd.pastChangesAccumulator = make(map[string]*StateChangeSet)
	}
	key := make([]byte, 40)
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesAccumulator[toStringZeroCopy(key)] = acc
}

func (sd *AggMemBatch) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
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
	return ReadDiffSet(tx, blockNumber, blockHash)
}

func (sd *AggMemBatch) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	for _, writer := range sd.iiWriters {
		if writer.name == table {
			return writer.Add(key, txNum)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

func (sd *AggMemBatch) Close() {
	for _, d := range sd.domainWriters {
		d.Close()
	}
	for _, iiWriter := range sd.iiWriters {
		iiWriter.close()
	}
}
func (sd *AggMemBatch) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	if err := sd.flushDiffSet(ctx, tx); err != nil {
		return err
	}
	sd.pastChangesAccumulator = make(map[string]*StateChangeSet)
	//_, err := sd.ComputeCommitment(ctx, true, sd.BlockNum(), sd.txNum, "flush-commitment")
	//if err != nil {
	//	return err
	//}

	if err := sd.flushWriters(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (sd *AggMemBatch) flushDiffSet(ctx context.Context, tx kv.RwTx) error {
	for key, changeset := range sd.pastChangesAccumulator {
		blockNum := binary.BigEndian.Uint64(toBytesZeroCopy(key[:8]))
		blockHash := common.BytesToHash(toBytesZeroCopy(key[8:]))
		if err := WriteDiffSet(tx, blockNum, blockHash, changeset); err != nil {
			return err
		}
	}
	return nil
}
func (sd *AggMemBatch) flushWriters(ctx context.Context, tx kv.RwTx) error {
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
func (sd *AggMemBatch) DiscardWrites(domain kv.Domain) {
	sd.domainWriters[domain].discard = true
	sd.domainWriters[domain].h.discard = true
}
