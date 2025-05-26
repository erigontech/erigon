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
	"sync/atomic"
	"time"
	"unsafe"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys []string
	Vals [][]byte
}

func (l *KvList) Push(key string, val []byte) {
	l.Keys = append(l.Keys, key)
	l.Vals = append(l.Vals, val)
}

func (l *KvList) Len() int {
	return len(l.Keys)
}

func (l *KvList) Less(i, j int) bool {
	return l.Keys[i] < l.Keys[j]
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
}

type dataWithPrevStep struct {
	data     []byte
	prevStep uint64
}

type SharedDomains struct {
	sdCtx *SharedDomainsCommitmentContext

	stepSize uint64

	logger log.Logger

	txNum    uint64
	blockNum atomic.Uint64
	estSize  int
	trace    bool //nolint
	//muMaps   sync.RWMutex
	//walLock sync.RWMutex

	domains [kv.DomainLen]map[string]dataWithPrevStep
	storage *btree2.Map[string, dataWithPrevStep]

	domainWriters [kv.DomainLen]*DomainBufferedWriter
	iiWriters     []*InvertedIndexBufferedWriter

	currentChangesAccumulator *StateChangeSet
	pastChangesAccumulator    map[string]*StateChangeSet
}

type HasAggTx interface {
	AggTx() any
}
type HasAgg interface {
	Agg() any
}

func NewSharedDomains(tx kv.TemporalTx, logger log.Logger) (*SharedDomains, error) {
	sd := &SharedDomains{
		logger:  logger,
		storage: btree2.NewMap[string, dataWithPrevStep](128),
		//trace:   true,
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

	tv := commitment.VariantHexPatriciaTrie
	if ExperimentalConcurrentCommitment {
		tv = commitment.VariantConcurrentHexPatricia
	}

	sd.sdCtx = NewSharedDomainsCommitmentContext(sd, tx, commitment.ModeDirect, tv, aggTx.a.tmpdir)
	sd.SetTxNum(0)

	if err := sd.SeekCommitment(context.Background(), tx); err != nil {
		return nil, err
	}

	return sd, nil
}

type temporalPutDel struct {
	sd *SharedDomains
	tx kv.Tx
}

func (pd *temporalPutDel) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte, prevStep uint64) error {
	return pd.sd.DomainPut(domain, pd.tx, k, v, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte, prevStep uint64) error {
	return pd.sd.DomainDel(domain, pd.tx, k, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	return pd.sd.DomainDelPrefix(domain, pd.tx, prefix, txNum)
}

func (sd *SharedDomains) AsPutDel(tx kv.Tx) kv.TemporalPutDel {
	return &temporalPutDel{sd, tx}
}

type temporalGetter struct {
	sd *SharedDomains
	tx kv.Tx
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step uint64, err error) {
	return gt.sd.GetLatest(name, gt.tx, k)
}

func (gt *temporalGetter) HasPrefix(name kv.Domain, prefix []byte) (firstKey []byte, ok bool, err error) {
	return gt.sd.HasPrefix(name, prefix, gt.tx)
}

func (sd *SharedDomains) AsGetter(tx kv.Tx) kv.TemporalGetter {
	return &temporalGetter{sd, tx}
}

func (sd *SharedDomains) SetChangesetAccumulator(acc *StateChangeSet) {
	sd.currentChangesAccumulator = acc
	for idx := range sd.domainWriters {
		if sd.currentChangesAccumulator == nil {
			sd.domainWriters[idx].SetDiff(nil)
		} else {
			sd.domainWriters[idx].SetDiff(&sd.currentChangesAccumulator.Diffs[idx])
		}
	}
}

func (sd *SharedDomains) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *StateChangeSet) {
	if sd.pastChangesAccumulator == nil {
		sd.pastChangesAccumulator = make(map[string]*StateChangeSet)
	}
	key := make([]byte, 40)
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesAccumulator[toStringZeroCopy(key)] = acc
}

func (sd *SharedDomains) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
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

// aggregator context should call aggTx.Unwind before this one.
func (sd *SharedDomains) Unwind(ctx context.Context, rwTx kv.TemporalRwTx, blockUnwindTo, txUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) error {
	step := txUnwindTo / sd.stepSize
	sd.logger.Info("aggregator unwind", "step", step,
		"txUnwindTo", txUnwindTo)
	//fmt.Printf("aggregator unwind step %d txUnwindTo %d\n", step, txUnwindTo)
	sf := time.Now()
	defer mxUnwindSharedTook.ObserveDuration(sf)

	if err := sd.Flush(ctx, rwTx); err != nil {
		return err
	}

	if err := rwTx.Unwind(ctx, txUnwindTo, changeset); err != nil {
		return err
	}

	sd.ClearRam(true)
	sd.SetTxNum(txUnwindTo)
	sd.SetBlockNum(blockUnwindTo)
	return sd.Flush(ctx, rwTx)
}

func (sd *SharedDomains) ClearRam(resetCommitment bool) {
	//sd.muMaps.Lock()
	//defer sd.muMaps.Unlock()
	for i := range sd.domains {
		sd.domains[i] = map[string]dataWithPrevStep{}
	}
	if resetCommitment {
		sd.sdCtx.updates.Reset()
		sd.sdCtx.Reset()
	}

	sd.storage = btree2.NewMap[string, dataWithPrevStep](128)
	sd.estSize = 0
}

func (sd *SharedDomains) put(domain kv.Domain, key string, val []byte) {
	// disable mutex - because work on parallel execution postponed after E3 release.
	//sd.muMaps.Lock()
	valWithPrevStep := dataWithPrevStep{data: val, prevStep: sd.txNum / sd.stepSize}
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
	//sd.muMaps.Unlock()
}

// get returns cached value by key. Cache is invalidated when associated WAL is flushed
func (sd *SharedDomains) get(table kv.Domain, key []byte) (v []byte, prevStep uint64, ok bool) {
	//sd.muMaps.RLock()
	keyS := toStringZeroCopy(key)
	var dataWithPrevStep dataWithPrevStep
	if table == kv.StorageDomain {
		dataWithPrevStep, ok = sd.storage.Get(keyS)
		return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok

	}
	dataWithPrevStep, ok = sd.domains[table][keyS]
	return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok
	//sd.muMaps.RUnlock()
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()

	// multiply 2: to cover data-structures overhead (and keep accounting cheap)
	// and muliply 2 more: for Commitment calculation when batch is full
	return uint64(sd.estSize) * 4
}

const CodeSizeTableFake = "CodeSize"

func (sd *SharedDomains) ReadsValid(readLists map[string]*KvList) bool {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()

	for table, list := range readLists {
		switch table {
		case kv.AccountsDomain.String():
			m := sd.domains[kv.AccountsDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case kv.CodeDomain.String():
			m := sd.domains[kv.CodeDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case kv.StorageDomain.String():
			m := sd.storage
			for i, key := range list.Keys {
				if val, ok := m.Get(key); ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case CodeSizeTableFake:
			m := sd.domains[kv.CodeDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(val.data)) {
						return false
					}
				}
			}
		default:
			panic(table)
		}
	}

	return true
}

func (sd *SharedDomains) updateAccountCode(addr, code []byte, txNum uint64, prevCode []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.put(kv.CodeDomain, addrS, code)
	if len(code) == 0 {
		return sd.domainWriters[kv.CodeDomain].DeleteWithPrev(addr, txNum, prevCode, prevStep)
	}
	return sd.domainWriters[kv.CodeDomain].PutWithPrev(addr, code, txNum, prevCode, prevStep)
}

func (sd *SharedDomains) updateCommitmentData(prefix string, data []byte, txNum uint64, prev []byte, prevStep uint64) error {
	sd.put(kv.CommitmentDomain, prefix, data)
	return sd.domainWriters[kv.CommitmentDomain].PutWithPrev(toBytesZeroCopy(prefix), data, txNum, prev, prevStep)
}

func (sd *SharedDomains) deleteAccount(roTx kv.Tx, addr []byte, txNum uint64, prev []byte, prevStep uint64) error {
	addrS := string(addr)
	if err := sd.DomainDelPrefix(kv.StorageDomain, roTx, addr, txNum); err != nil {
		return err
	}

	// commitment delete already has been applied via account
	if err := sd.DomainDel(kv.CodeDomain, roTx, addr, txNum, nil, prevStep); err != nil {
		return err
	}

	sd.put(kv.AccountsDomain, addrS, nil)
	if err := sd.domainWriters[kv.AccountsDomain].DeleteWithPrev(addr, txNum, prev, prevStep); err != nil {
		return err
	}

	return nil
}

func (sd *SharedDomains) writeAccountStorage(k, v, preVal []byte, prevStep, txNum uint64) error {
	sd.put(kv.StorageDomain, string(k), v)
	return sd.domainWriters[kv.StorageDomain].PutWithPrev(k, v, txNum, preVal, prevStep)
}

func (sd *SharedDomains) delAccountStorage(k, preVal []byte, prevStep, txNum uint64) error {
	sd.put(kv.StorageDomain, string(k), nil)
	return sd.domainWriters[kv.StorageDomain].DeleteWithPrev(k, txNum, preVal, prevStep)
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte) (err error) {
	for _, writer := range sd.iiWriters {
		if writer.name == table {
			return writer.Add(key, sd.txNum)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

func (sd *SharedDomains) StepSize() uint64 { return sd.stepSize }

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if aggregationStep is reached
func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.txNum = txNum
	sd.sdCtx.mainTtx.txNum = txNum
}

func (sd *SharedDomains) TxNum() uint64 { return sd.txNum }

func (sd *SharedDomains) BlockNum() uint64 { return sd.blockNum.Load() }

func (sd *SharedDomains) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *SharedDomains) SetTrace(b bool) {
	sd.trace = b
}

func (sd *SharedDomains) HasPrefix(domain kv.Domain, prefix []byte, roTx kv.Tx) ([]byte, bool, error) {
	var firstKey []byte
	var hasPrefix bool
	err := sd.IteratePrefix(domain, prefix, roTx, func(k []byte, v []byte, step uint64) (bool, error) {
		firstKey = common.CopyBytes(k)
		hasPrefix = true
		return false, nil // do not continue, end on first occurrence
	}, roTx)
	return firstKey, hasPrefix, err
}

// IterateStoragePrefix iterates over key-value pairs of the storage domain that start with given prefix
//
// k and v lifetime is bounded by the lifetime of the iterator
func (sd *SharedDomains) IterateStoragePrefix(prefix []byte, roTx kv.Tx, it func(k []byte, v []byte, step uint64) (cont bool, err error)) error {
	return sd.IteratePrefix(kv.StorageDomain, prefix, roTx, it, roTx)
}

func (sd *SharedDomains) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte, step uint64) (cont bool, err error), tx kv.Tx) error {
	var haveRamUpdates bool
	var ramIter btree2.MapIter[string, dataWithPrevStep]
	if domain == kv.StorageDomain {
		haveRamUpdates = sd.storage.Len() > 0
		ramIter = sd.storage.Iter()
	}

	return AggTx(roTx).d[domain].debugIteratePrefix(prefix, haveRamUpdates, ramIter, it, sd.txNum, sd.stepSize, roTx)
}

func (sd *SharedDomains) Close() {
	if sd.sdCtx == nil { //idempotency
		return
	}

	sd.SetBlockNum(0)
	sd.SetTxNum(0)

	//sd.walLock.Lock()
	//defer sd.walLock.Unlock()
	for _, d := range sd.domainWriters {
		d.Close()
	}
	for _, iiWriter := range sd.iiWriters {
		iiWriter.close()
	}

	sd.sdCtx.Close()
	sd.sdCtx = nil
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	for key, changeset := range sd.pastChangesAccumulator {
		blockNum := binary.BigEndian.Uint64(toBytesZeroCopy(key[:8]))
		blockHash := common.BytesToHash(toBytesZeroCopy(key[8:]))
		if err := WriteDiffSet(tx, blockNum, blockHash, changeset); err != nil {
			return err
		}
	}
	sd.pastChangesAccumulator = make(map[string]*StateChangeSet)
	aggTx := AggTx(tx)

	defer mxFlushTook.ObserveDuration(time.Now())
	_, err := sd.ComputeCommitment(ctx, tx, true, sd.BlockNum(), "flush-commitment")
	if err != nil {
		return err
	}

	for di, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
		aggTx.d[di].closeValsCursor() //TODO: why?
	}
	for _, w := range sd.iiWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
	}
	if dbg.PruneOnFlushTimeout != 0 {
		if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, dbg.PruneOnFlushTimeout); err != nil {
			return err
		}
	}

	for _, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		w.Close()
	}
	for _, w := range sd.iiWriters {
		if w == nil {
			continue
		}
		w.close()
	}
	return nil
}

// TemporalDomain satisfaction
func (sd *SharedDomains) GetLatest(domain kv.Domain, tx kv.Tx, k []byte) (v []byte, step uint64, err error) {
	if tx == nil {
		return nil, 0, fmt.Errorf("sd.GetLatest: unexpected nil tx")
	}
	if domain == kv.CommitmentDomain {
		return sd.LatestCommitment(k, tx)
	}
	if v, prevStep, ok := sd.get(domain, k); ok {
		return v, prevStep, nil
	}
	v, step, err = tx.(kv.TemporalTx).GetLatest(domain, k)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}
	return v, step, nil
}

// DomainPut
// Optimizations:
//   - user can provide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainPut(domain kv.Domain, roTx kv.Tx, k, v []byte, txNum uint64, prevVal []byte, prevStep uint64) error {
	if v == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}

	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, roTx, k)
		if err != nil {
			return err
		}
	}
	//fmt.Printf("k %x comp %x S %x\n", k1, composite, compositeS)
	//compositeS := toStringZeroCopy(composite) // composite is leaking pointer: once k1 changed it also changed in maps
	ks := string(k)

	sd.sdCtx.TouchKey(domain, ks, v)
	switch domain {
	case kv.StorageDomain:
		return sd.writeAccountStorage(k, v, prevVal, prevStep, txNum)
	case kv.CodeDomain:
		if bytes.Equal(prevVal, v) {
			return nil
		}
		return sd.updateAccountCode(k, v, 0, prevVal, prevStep)
	case kv.AccountsDomain, kv.CommitmentDomain, kv.RCacheDomain:
		sd.put(domain, ks, v)
		return sd.domainWriters[domain].PutWithPrev(k, v, txNum, prevVal, prevStep)
	default:
		if bytes.Equal(prevVal, v) {
			return nil
		}
		sd.put(domain, ks, v)
		return sd.domainWriters[domain].PutWithPrev(k, v, txNum, prevVal, prevStep)
	}
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, tx kv.Tx, k []byte, txNum uint64, prevVal []byte, prevStep uint64) error {
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, tx, k)
		if err != nil {
			return err
		}
	}

	sd.sdCtx.TouchKey(domain, toStringZeroCopy(k), nil)
	switch domain {
	case kv.AccountsDomain:
		return sd.deleteAccount(tx, k, txNum, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.delAccountStorage(k, prevVal, prevStep, txNum)
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		return sd.updateAccountCode(k, nil, txNum, prevVal, prevStep)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(toStringZeroCopy(k), nil, txNum, prevVal, prevStep)
	default:
		//sd.put(kv.CommitmentDomain, prefix, data)
		//return sd.domainWriters[kv.CommitmentDomain].PutWithPrev(toBytesZeroCopy(prefix), nil, data, sd.txNum, prev, prevStep)
		sd.put(domain, toStringZeroCopy(k), nil)
		return sd.domainWriters[domain].DeleteWithPrev(k, txNum, prevVal, prevStep)
	}
}

func (sd *SharedDomains) DomainDelPrefix(domain kv.Domain, roTx kv.Tx, prefix []byte, txNum uint64) error {
	if domain != kv.StorageDomain {
		return errors.New("DomainDelPrefix: not supported")
	}

	type tuple struct {
		k, v []byte
		step uint64
	}
	tombs := make([]tuple, 0, 8)
	if err := sd.IterateStoragePrefix(prefix, roTx, func(k, v []byte, step uint64) (bool, error) {
		tombs = append(tombs, tuple{k, v, step})
		return true, nil
	}); err != nil {
		return err
	}
	for _, tomb := range tombs {
		if err := sd.DomainDel(kv.StorageDomain, roTx, tomb.k, txNum, tomb.v, tomb.step); err != nil {
			return err
		}
	}

	if assert.Enable {
		forgotten := 0
		if err := sd.IterateStoragePrefix(prefix, roTx, func(k, v []byte, step uint64) (bool, error) {
			forgotten++
			return true, nil
		}); err != nil {
			return err
		}
		if forgotten > 0 {
			panic(fmt.Errorf("DomainDelPrefix: %d forgotten keys after '%x' prefix removal", forgotten, prefix))
		}
	}
	return nil
}

func toStringZeroCopy(v []byte) string { return unsafe.String(&v[0], len(v)) }
func toBytesZeroCopy(s string) []byte  { return unsafe.Slice(unsafe.StringData(s), len(s)) }

func AggTx(tx kv.Tx) *AggregatorRoTx {
	if withAggTx, ok := tx.(interface{ AggTx() any }); ok {
		return withAggTx.AggTx().(*AggregatorRoTx)
	}

	return nil
}
