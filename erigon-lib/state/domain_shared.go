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
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/erigontech/erigon-lib/kv/stream"
	"math"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/erigontech/erigon-lib/seg"
	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common/cryptozerocopy"
	"github.com/erigontech/erigon-lib/log/v3"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/types"
)

var ErrBehindCommitment = errors.New("behind commitment")

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
	aggTx  *AggregatorRoTx
	sdCtx  *SharedDomainsCommitmentContext
	roTx   kv.Tx
	logger log.Logger

	txNum    uint64
	blockNum atomic.Uint64
	estSize  int
	trace    bool //nolint
	//muMaps   sync.RWMutex
	//walLock sync.RWMutex

	domains [kv.DomainLen]map[string]dataWithPrevStep
	storage *btree2.Map[string, dataWithPrevStep]

	domainWriters [kv.DomainLen]*domainBufferedWriter
	iiWriters     [kv.StandaloneIdxLen]*invertedIndexBufferedWriter

	currentChangesAccumulator *StateChangeSet
	pastChangesAccumulator    map[string]*StateChangeSet
}

type HasAggTx interface {
	AggTx() any
}
type HasAgg interface {
	Agg() any
}

func NewSharedDomains(tx kv.Tx, logger log.Logger) (*SharedDomains, error) {

	sd := &SharedDomains{
		logger:  logger,
		storage: btree2.NewMap[string, dataWithPrevStep](128),
		//trace:   true,
	}
	sd.SetTx(tx)

	sd.aggTx.a.DiscardHistory(kv.CommitmentDomain)

	for id, ii := range sd.aggTx.iis {
		sd.iiWriters[id] = ii.NewWriter()
	}

	for id, d := range sd.aggTx.d {
		sd.domains[id] = map[string]dataWithPrevStep{}
		sd.domainWriters[id] = d.NewWriter()
	}

	sd.SetTxNum(0)
	sd.sdCtx = NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	if _, err := sd.SeekCommitment(context.Background(), tx); err != nil {
		return nil, err
	}
	return sd, nil
}

func (sd *SharedDomains) SetChangesetAccumulator(acc *StateChangeSet) {
	sd.currentChangesAccumulator = acc
	for idx := range sd.domainWriters {
		if sd.currentChangesAccumulator == nil {
			sd.domainWriters[idx].diff = nil
		} else {
			sd.domainWriters[idx].diff = &sd.currentChangesAccumulator.Diffs[idx]
		}
	}
}

func (sd *SharedDomains) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *StateChangeSet) {
	if sd.pastChangesAccumulator == nil {
		sd.pastChangesAccumulator = make(map[string]*StateChangeSet)
	}
	var key [40]byte
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesAccumulator[string(key[:])] = acc
}

func (sd *SharedDomains) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]DomainEntryDiff, bool, error) {
	var key [40]byte
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	if changeset, ok := sd.pastChangesAccumulator[string(key[:])]; ok {
		return [kv.DomainLen][]DomainEntryDiff{
			changeset.Diffs[kv.AccountsDomain].GetDiffSet(),
			changeset.Diffs[kv.StorageDomain].GetDiffSet(),
			changeset.Diffs[kv.CodeDomain].GetDiffSet(),
			changeset.Diffs[kv.CommitmentDomain].GetDiffSet(),
		}, true, nil
	}
	return ReadDiffSet(tx, blockNumber, blockHash)
}

func (sd *SharedDomains) AggTx() any { return sd.aggTx }

// aggregator context should call aggTx.Unwind before this one.
func (sd *SharedDomains) Unwind(ctx context.Context, rwTx kv.RwTx, blockUnwindTo, txUnwindTo uint64, changeset *[kv.DomainLen][]DomainEntryDiff) error {
	step := txUnwindTo / sd.aggTx.a.StepSize()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	sd.aggTx.a.logger.Info("aggregator unwind", "step", step,
		"txUnwindTo", txUnwindTo, "stepsRangeInDB", sd.aggTx.a.StepsRangeInDBAsStr(rwTx))
	//fmt.Printf("aggregator unwind step %d txUnwindTo %d stepsRangeInDB %s\n", step, txUnwindTo, sd.aggTx.a.StepsRangeInDBAsStr(rwTx))
	sf := time.Now()
	defer mxUnwindSharedTook.ObserveDuration(sf)

	if err := sd.Flush(ctx, rwTx); err != nil {
		return err
	}

	for idx, d := range sd.aggTx.d {
		if err := d.Unwind(ctx, rwTx, step, txUnwindTo, changeset[idx]); err != nil {
			return err
		}
	}
	for _, ii := range sd.aggTx.iis {
		if err := ii.Unwind(ctx, rwTx, txUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, nil); err != nil {
			return err
		}
	}

	sd.ClearRam(true)
	sd.SetTxNum(txUnwindTo)
	sd.SetBlockNum(blockUnwindTo)
	return sd.Flush(ctx, rwTx)
}

func (sd *SharedDomains) rebuildCommitment(ctx context.Context, roTx kv.Tx, blockNum uint64) ([]byte, error) {
	it, err := sd.aggTx.HistoryRange(kv.AccountsHistory, int(sd.TxNum()), math.MaxInt64, order.Asc, -1, roTx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sd.sdCtx.TouchKey(kv.AccountsDomain, string(k), nil)
	}

	it, err = sd.aggTx.HistoryRange(kv.StorageHistory, int(sd.TxNum()), math.MaxInt64, order.Asc, -1, roTx)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sd.sdCtx.TouchKey(kv.StorageDomain, string(k), nil)
	}

	sd.sdCtx.Reset()
	return sd.ComputeCommitment(ctx, true, blockNum, "rebuild commit")
}

func (sd *SharedDomains) RebuildCommitmentRange(ctx context.Context, db kv.RwDB, blockNum uint64, from, to int) ([]byte, error) {
	roTx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx.Rollback()

	// FROM is not used YET
	it, err := sd.aggTx.DomainRangeAsOf(kv.AccountsDomain, from, to, order.Asc, roTx)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	itS, err := sd.aggTx.DomainRangeAsOf(kv.StorageDomain, from, to, order.Asc, roTx)
	if err != nil {
		return nil, err
	}
	defer itS.Close()

	//itC, err := sd.aggTx.DomainRangeAsOf(kv.CodeDomain, from, to, order.Asc, roTx)
	//if err != nil {
	//	return nil, err
	//}
	//defer itC.Close()

	keyIter := stream.UnionKV(it, itS, -1)
	//keyIter = stream.UnionKV(keyIter, itC, -1)

	sd.domainWriters[kv.AccountsDomain].discard = true
	sd.domainWriters[kv.AccountsDomain].h.discard = true
	sd.domainWriters[kv.StorageDomain].discard = true
	sd.domainWriters[kv.StorageDomain].h.discard = true
	sd.domainWriters[kv.CodeDomain].discard = true
	sd.domainWriters[kv.CodeDomain].h.discard = true
	//sd.domainWriters[kv.CommitmentDomain].h.discard = true

	sd.SetTx(roTx)
	sd.SetTxNum(uint64(to - 1))
	sd.sdCtx.SetLimitReadAsOfTxNum(sd.TxNum() + 1)

	keyCountByDomains := sd.KeyCountInDomainRange(uint64(from), uint64(to))
	totalKeys := keyCountByDomains[kv.AccountsDomain] + keyCountByDomains[kv.StorageDomain]
	batchSize := totalKeys / (uint64(to-from) / sd.StepSize())
	batchFactor := uint64(1)
	mlog := math.Log2(float64(totalKeys / batchSize))
	batchFactor = min(uint64(math.Pow(2, mlog)), 8)

	shardFrom := uint64(from) / sd.StepSize()
	shardTo := shardFrom + batchFactor
	lastShard := uint64(to) / sd.StepSize()
	var processed uint64
	sf := time.Now()

	sd.logger.Info("starting rebuild commitment", "range", fmt.Sprintf("%d-%d", shardFrom, shardTo), "batchFactor", batchFactor, "totalKeys", common.PrettyCounter(totalKeys), "block", blockNum)

	for keyIter.HasNext() {
		k, _, err := keyIter.Next()
		if err != nil {
			return nil, err
		}

		sd.sdCtx.TouchKey(kv.AccountsDomain, string(k), nil)
		processed++
		if shardTo < lastShard && sd.sdCtx.KeysCount()%(batchFactor*batchSize) == 0 {
			rh, err := sd.sdCtx.ComputeCommitment(ctx, true, blockNum, fmt.Sprintf("%d/%d", shardFrom, lastShard))
			if err != nil {
				return nil, err
			}

			err = sd.aggTx.d[kv.CommitmentDomain].d.DumpStepRangeOnDisk(ctx, shardFrom, shardTo, sd.domainWriters[kv.CommitmentDomain], nil)
			if err != nil {
				return nil, err
			}
			sd.logger.Info("Commitment shard done", "processed", fmt.Sprintf("%s/%s", common.PrettyCounter(processed), common.PrettyCounter(totalKeys)),
				"shard", fmt.Sprintf("%d-%d", shardFrom, shardTo), "shard root", hex.EncodeToString(rh))

			if shardTo+batchFactor > lastShard && batchFactor > 1 {
				batchFactor /= 2
			}
			shardFrom += batchFactor
			shardTo += batchFactor
		}
	}
	// if shardTo < lastShard {
	// 	shardTo = lastShard
	// }
	sd.logger.Info("sealing last shard", "shard", fmt.Sprintf("%d-%d", shardFrom, shardTo))
	rh, err := sd.sdCtx.ComputeCommitment(ctx, true, blockNum, fmt.Sprintf("sealing %d-%d", shardFrom, shardTo))
	if err != nil {
		return nil, err
	}

	// rng := MergeRange{
	// 	from: uint64(from),
	// 	to:   uint64(to),
	// }

	// vt, err := sd.aggTx.d[kv.CommitmentDomain].commitmentValTransformDomain(rng, sd.aggTx.d[kv.AccountsDomain], sd.aggTx.d[kv.StorageDomain], nil, nil)
	// if err != nil {
	// 	return nil, err
	// }

	err = sd.aggTx.d[kv.CommitmentDomain].d.DumpStepRangeOnDisk(ctx, shardFrom, shardTo, sd.domainWriters[kv.CommitmentDomain], nil)
	if err != nil {
		return nil, err
	}
	sd.logger.Info("Commitment range finished", "processed", fmt.Sprintf("%s/%s", common.PrettyCounter(processed), common.PrettyCounter(totalKeys)),
		"shard", fmt.Sprintf("%d-%d", shardFrom, shardTo), "root", hex.EncodeToString(rh), "ETA", time.Since(sf).String())
	roTx.Rollback()
	//if err = roTx.Commit(); err != nil {
	//	return nil, err
	//}
	//sd.sdCtx.Reset()
	return rh, nil
}

// SeekCommitment lookups latest available commitment and sets it as current
func (sd *SharedDomains) SeekCommitment(ctx context.Context, tx kv.Tx) (txsFromBlockBeginning uint64, err error) {
	bn, txn, ok, err := sd.sdCtx.SeekCommitment(tx, sd.aggTx.d[kv.CommitmentDomain], 0, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	if ok {
		if bn > 0 {
			lastBn, _, err := rawdbv3.TxNums.Last(tx)
			if err != nil {
				return 0, err
			}
			if lastBn < bn {
				return 0, errors.WithMessage(ErrBehindCommitment, fmt.Sprintf("TxNums index is at block %d and behind commitment %d", lastBn, bn))
			}
		}
		sd.SetBlockNum(bn)
		sd.SetTxNum(txn)
		return 0, nil
	}
	// handle case when we have no commitment, but have executed blocks
	bnBytes, err := tx.GetOne(kv.SyncStageProgress, []byte("Execution")) //TODO: move stages to erigon-lib
	if err != nil {
		return 0, err
	}
	if len(bnBytes) == 8 {
		bn = binary.BigEndian.Uint64(bnBytes)
		txn, err = rawdbv3.TxNums.Max(tx, bn)
		if err != nil {
			return 0, err
		}
	}
	if bn == 0 && txn == 0 {
		sd.SetBlockNum(0)
		sd.SetTxNum(0)
		return 0, nil
	}
	sd.SetBlockNum(bn)
	sd.SetTxNum(txn)
	newRh, err := sd.rebuildCommitment(ctx, tx, bn)
	if err != nil {
		return 0, err
	}
	if bytes.Equal(newRh, commitment.EmptyRootHash) {
		sd.SetBlockNum(0)
		sd.SetTxNum(0)
		return 0, nil
	}
	if sd.trace {
		fmt.Printf("rebuilt commitment %x %d %d\n", newRh, sd.TxNum(), sd.BlockNum())
	}
	sd.SetBlockNum(bn)
	sd.SetTxNum(txn)
	return 0, nil
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
	valWithPrevStep := dataWithPrevStep{data: val, prevStep: sd.txNum / sd.aggTx.a.StepSize()}
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
	keyS := *(*string)(unsafe.Pointer(&key))
	var dataWithPrevStep dataWithPrevStep
	//keyS := string(key)
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

func (sd *SharedDomains) LatestCommitment(prefix []byte) ([]byte, uint64, error) {
	if v, prevStep, ok := sd.get(kv.CommitmentDomain, prefix); ok {
		// sd cache values as is (without transformation) so safe to return
		return v, prevStep, nil
	}
	v, step, found, err := sd.aggTx.d[kv.CommitmentDomain].getLatestFromDb(prefix, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}
	if found {
		// db store values as is (without transformation) so safe to return
		return v, step, nil
	}

	// GetfromFiles doesn't provide same semantics as getLatestFromDB - it returns start/end tx
	// of file where the value is stored (not exact step when kv has been set)
	v, _, startTx, endTx, err := sd.aggTx.d[kv.CommitmentDomain].getFromFiles(prefix)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}

	if !sd.aggTx.a.commitmentValuesTransform || bytes.Equal(prefix, keyCommitmentState) {
		return v, endTx / sd.aggTx.a.StepSize(), nil
	}

	// replace shortened keys in the branch with full keys to allow HPH work seamlessly
	rv, err := sd.replaceShortenedKeysInBranch(prefix, commitment.BranchData(v), startTx, endTx)
	if err != nil {
		return nil, 0, err
	}
	return rv, endTx / sd.aggTx.a.StepSize(), nil
}

// replaceShortenedKeysInBranch replaces shortened keys in the branch with full keys
func (sd *SharedDomains) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	if !sd.aggTx.d[kv.CommitmentDomain].d.replaceKeysInValues && sd.aggTx.a.commitmentValuesTransform {
		panic("domain.replaceKeysInValues is disabled, but agg.commitmentValuesTransform is enabled")
	}

	if !sd.aggTx.a.commitmentValuesTransform ||
		len(branch) == 0 ||
		sd.aggTx.minimaxTxNumInDomainFiles() == 0 ||
		bytes.Equal(prefix, keyCommitmentState) || ((fEndTxNum-fStartTxNum)/sd.aggTx.a.StepSize())%2 != 0 {

		return branch, nil // do not transform, return as is
	}

	sto := sd.aggTx.d[kv.StorageDomain]
	acc := sd.aggTx.d[kv.AccountsDomain]
	storageItem := sto.lookupVisibleFileByItsRange(fStartTxNum, fEndTxNum)
	if storageItem == nil {
		sd.logger.Crit(fmt.Sprintf("storage file of steps %d-%d not found\n", fStartTxNum/sd.aggTx.a.aggregationStep, fEndTxNum/sd.aggTx.a.aggregationStep))
		return nil, errors.New("storage file not found")
	}
	accountItem := acc.lookupVisibleFileByItsRange(fStartTxNum, fEndTxNum)
	if accountItem == nil {
		sd.logger.Crit(fmt.Sprintf("storage file of steps %d-%d not found\n", fStartTxNum/sd.aggTx.a.aggregationStep, fEndTxNum/sd.aggTx.a.aggregationStep))
		return nil, errors.New("account file not found")
	}
	storageGetter := seg.NewReader(storageItem.decompressor.MakeGetter(), sto.d.compression)
	accountGetter := seg.NewReader(accountItem.decompressor.MakeGetter(), acc.d.compression)
	metricI := 0
	for i, f := range sd.aggTx.d[kv.CommitmentDomain].files {
		if i > 5 {
			metricI = 5
			break
		}
		if f.startTxNum == fStartTxNum && f.endTxNum == fEndTxNum {
			metricI = i
		}
	}

	aux := make([]byte, 0, 256)
	return branch.ReplacePlainKeys(aux, func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			if len(key) == length.Addr+length.Hash {
				return nil, nil // save storage key as is
			}
			if dbg.KVReadLevelledMetrics {
				defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
			}
			// Optimised key referencing a state file record (file number and offset within the file)
			storagePlainKey, found := sto.lookupByShortenedKey(key, storageGetter)
			if !found {
				s0, s1 := fStartTxNum/sd.aggTx.a.StepSize(), fEndTxNum/sd.aggTx.a.StepSize()
				sd.logger.Crit("replace back lost storage full key", "shortened", fmt.Sprintf("%x", key),
					"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, decodeShorterKey(key)))
				return nil, fmt.Errorf("replace back lost storage full key: %x", key)
			}
			return storagePlainKey, nil
		}

		if len(key) == length.Addr {
			return nil, nil // save account key as is
		}

		if dbg.KVReadLevelledMetrics {
			defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
		}
		apkBuf, found := acc.lookupByShortenedKey(key, accountGetter)
		if !found {
			s0, s1 := fStartTxNum/sd.aggTx.a.StepSize(), fEndTxNum/sd.aggTx.a.StepSize()
			sd.logger.Crit("replace back lost account full key", "shortened", fmt.Sprintf("%x", key),
				"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, decodeShorterKey(key)))
			return nil, fmt.Errorf("replace back lost account full key: %x", key)
		}
		return apkBuf, nil
	})
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

func (sd *SharedDomains) updateAccountData(addr []byte, account, prevAccount []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.sdCtx.TouchKey(kv.AccountsDomain, addrS, account)
	sd.put(kv.AccountsDomain, addrS, account)
	return sd.domainWriters[kv.AccountsDomain].PutWithPrev(addr, nil, account, prevAccount, prevStep)
}

func (sd *SharedDomains) updateAccountCode(addr, code, prevCode []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.sdCtx.TouchKey(kv.CodeDomain, addrS, code)
	sd.put(kv.CodeDomain, addrS, code)
	if len(code) == 0 {
		return sd.domainWriters[kv.CodeDomain].DeleteWithPrev(addr, nil, prevCode, prevStep)
	}
	return sd.domainWriters[kv.CodeDomain].PutWithPrev(addr, nil, code, prevCode, prevStep)
}

func (sd *SharedDomains) updateCommitmentData(prefix []byte, data, prev []byte, prevStep uint64) error {
	sd.put(kv.CommitmentDomain, string(prefix), data)
	return sd.domainWriters[kv.CommitmentDomain].PutWithPrev(prefix, nil, data, prev, prevStep)
}

func (sd *SharedDomains) deleteAccount(addr, prev []byte, prevStep uint64) error {
	addrS := string(addr)
	if err := sd.DomainDelPrefix(kv.StorageDomain, addr); err != nil {
		return err
	}

	// commitment delete already has been applied via account
	if err := sd.DomainDel(kv.CodeDomain, addr, nil, nil, prevStep); err != nil {
		return err
	}

	sd.sdCtx.TouchKey(kv.AccountsDomain, addrS, nil)
	sd.put(kv.AccountsDomain, addrS, nil)
	if err := sd.domainWriters[kv.AccountsDomain].DeleteWithPrev(addr, nil, prev, prevStep); err != nil {
		return err
	}

	return nil
}

func (sd *SharedDomains) writeAccountStorage(addr, loc []byte, value, preVal []byte, prevStep uint64) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.sdCtx.TouchKey(kv.StorageDomain, compositeS, value)
	sd.put(kv.StorageDomain, compositeS, value)
	return sd.domainWriters[kv.StorageDomain].PutWithPrev(composite, nil, value, preVal, prevStep)
}

func (sd *SharedDomains) delAccountStorage(addr, loc []byte, preVal []byte, prevStep uint64) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.sdCtx.TouchKey(kv.StorageDomain, compositeS, nil)
	sd.put(kv.StorageDomain, compositeS, nil)
	return sd.domainWriters[kv.StorageDomain].DeleteWithPrev(composite, nil, preVal, prevStep)
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte) (err error) {
	switch table {
	case kv.LogAddrIdx, kv.TblLogAddressIdx:
		err = sd.iiWriters[kv.LogAddrIdxPos].Add(key)
	case kv.LogTopicIdx, kv.TblLogTopicsIdx, kv.LogTopicIndex:
		err = sd.iiWriters[kv.LogTopicIdxPos].Add(key)
	case kv.TblTracesToIdx:
		err = sd.iiWriters[kv.TracesToIdxPos].Add(key)
	case kv.TblTracesFromIdx:
		err = sd.iiWriters[kv.TracesFromIdxPos].Add(key)
	default:
		panic(fmt.Errorf("unknown shared index %s", table))
	}
	return err
}

func (sd *SharedDomains) SetTx(tx kv.Tx) {
	if tx == nil {
		panic("tx is nil")
	}
	sd.roTx = tx

	casted, ok := tx.(HasAggTx)
	if !ok {
		panic(fmt.Errorf("type %T need AggTx method", tx))
	}

	sd.aggTx = casted.AggTx().(*AggregatorRoTx)
	if sd.aggTx == nil {
		panic(errors.New("aggtx is nil"))
	}
}

func (sd *SharedDomains) StepSize() uint64 { return sd.aggTx.a.StepSize() }

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if aggregationStep is reached
func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.txNum = txNum
	for _, d := range sd.domainWriters {
		if d != nil {
			d.SetTxNum(txNum)
		}
	}
	for _, iiWriter := range sd.iiWriters {
		if iiWriter != nil {
			iiWriter.SetTxNum(txNum)
		}
	}
}

func (sd *SharedDomains) TxNum() uint64 { return sd.txNum }

func (sd *SharedDomains) BlockNum() uint64 { return sd.blockNum.Load() }

func (sd *SharedDomains) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *SharedDomains) SetTrace(b bool) {
	sd.trace = b
}

func (sd *SharedDomains) ComputeCommitment(ctx context.Context, saveStateAfter bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	rootHash, err = sd.sdCtx.ComputeCommitment(ctx, saveStateAfter, blockNum, logPrefix)
	return
}

// IterateStoragePrefix iterates over key-value pairs of the storage domain that start with given prefix
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
//
// k and v lifetime is bounded by the lifetime of the iterator
func (sd *SharedDomains) IterateStoragePrefix(prefix []byte, it func(k []byte, v []byte, step uint64) error) error {
	// Implementation:
	//     File endTxNum  = last txNum of file step
	//     DB endTxNum    = first txNum of step in db
	//     RAM endTxNum   = current txnum
	//  Example: stepSize=8, file=0-2.kv, db has key of step 2, current tx num is 17
	//     File endTxNum  = 15, because `0-2.kv` has steps 0 and 1, last txNum of step 1 is 15
	//     DB endTxNum    = 16, because db has step 2, and first txNum of step 2 is 16.
	//     RAM endTxNum   = 17, because current tcurrent txNum is 17

	haveRamUpdates := sd.storage.Len() > 0

	var cp CursorHeap
	cpPtr := &cp
	heap.Init(cpPtr)
	var k, v []byte
	var err error

	iter := sd.storage.Iter()
	if iter.Seek(string(prefix)) {
		kx := iter.Key()
		v = iter.Value().data
		k = []byte(kx)

		if len(kx) > 0 && bytes.HasPrefix(k, prefix) {
			heap.Push(cpPtr, &CursorItem{t: RAM_CURSOR, key: common.Copy(k), val: common.Copy(v), step: 0, iter: iter, endTxNum: sd.txNum, reverse: true})
		}
	}

	roTx := sd.roTx
	valsCursor, err := roTx.CursorDupSort(sd.aggTx.a.d[kv.StorageDomain].valsTable)
	if err != nil {
		return err
	}
	defer valsCursor.Close()
	if k, v, err = valsCursor.Seek(prefix); err != nil {
		return err
	}
	if len(k) > 0 && bytes.HasPrefix(k, prefix) {
		step := ^binary.BigEndian.Uint64(v[:8])
		val := v[8:]
		endTxNum := step * sd.StepSize() // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
		if haveRamUpdates && endTxNum >= sd.txNum {
			return fmt.Errorf("probably you didn't set SharedDomains.SetTxNum(). ram must be ahead of db: %d, %d", sd.txNum, endTxNum)
		}

		heap.Push(cpPtr, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(val), step: step, cDup: valsCursor, endTxNum: endTxNum, reverse: true})
	}

	sctx := sd.aggTx.d[kv.StorageDomain]
	for i, item := range sctx.files {
		cursor, err := item.src.bindex.Seek(sctx.statelessGetter(i), prefix)
		if err != nil {
			return err
		}
		if cursor == nil {
			continue
		}

		key := cursor.Key()
		if key != nil && bytes.HasPrefix(key, prefix) {
			val := cursor.Value()
			txNum := item.endTxNum - 1 // !important: .kv files have semantic [from, t)
			heap.Push(cpPtr, &CursorItem{t: FILE_CURSOR, key: key, val: val, step: 0, btCursor: cursor, endTxNum: txNum, reverse: true})
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		lastStep := cp[0].step
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(cpPtr).(*CursorItem)
			switch ci1.t {
			case RAM_CURSOR:
				if ci1.iter.Next() {
					k = []byte(ci1.iter.Key())
					if k != nil && bytes.HasPrefix(k, prefix) {
						ci1.key = common.Copy(k)
						ci1.val = common.Copy(ci1.iter.Value().data)
						heap.Push(cpPtr, ci1)
					}
				}
			case FILE_CURSOR:
				if UseBtree || UseBpsTree {
					if ci1.btCursor.Next() {
						ci1.key = ci1.btCursor.Key()
						if ci1.key != nil && bytes.HasPrefix(ci1.key, prefix) {
							ci1.val = ci1.btCursor.Value()
							heap.Push(cpPtr, ci1)
						}
					}
				} else {
					ci1.dg.Reset(ci1.latestOffset)
					if !ci1.dg.HasNext() {
						break
					}
					key, _ := ci1.dg.Next(nil)
					if key != nil && bytes.HasPrefix(key, prefix) {
						ci1.key = key
						ci1.val, ci1.latestOffset = ci1.dg.Next(nil)
						heap.Push(cpPtr, ci1)
					}
				}
			case DB_CURSOR:
				k, v, err := ci1.cDup.NextNoDup()
				if err != nil {
					return err
				}

				if len(k) > 0 && bytes.HasPrefix(k, prefix) {
					ci1.key = common.Copy(k)
					step := ^binary.BigEndian.Uint64(v[:8])
					endTxNum := step * sd.StepSize() // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
					if haveRamUpdates && endTxNum >= sd.txNum {
						return fmt.Errorf("probably you didn't set SharedDomains.SetTxNum(). ram must be ahead of db: %d, %d", sd.txNum, endTxNum)
					}
					ci1.endTxNum = endTxNum
					ci1.val = common.Copy(v[8:])
					ci1.step = step
					heap.Push(cpPtr, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			if err := it(lastKey, lastVal, lastStep); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sd *SharedDomains) Close() {
	sd.SetBlockNum(0)
	if sd.aggTx != nil {
		sd.SetTxNum(0)

		//sd.walLock.Lock()
		//defer sd.walLock.Unlock()
		for _, d := range sd.domainWriters {
			d.close()
		}
		for _, iiWriter := range sd.iiWriters {
			iiWriter.close()
		}
	}

	if sd.sdCtx != nil {
		sd.sdCtx.Close()
	}
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	for key, changeset := range sd.pastChangesAccumulator {
		blockNum := binary.BigEndian.Uint64([]byte(key[:8]))
		blockHash := common.BytesToHash([]byte(key[8:]))
		if err := WriteDiffSet(tx, blockNum, blockHash, changeset); err != nil {
			return err
		}
	}
	sd.pastChangesAccumulator = make(map[string]*StateChangeSet)

	defer mxFlushTook.ObserveDuration(time.Now())
	fh, err := sd.ComputeCommitment(ctx, true, sd.BlockNum(), "flush-commitment")
	if err != nil {
		return err
	}
	if sd.trace {
		_, f, l, _ := runtime.Caller(1)
		fmt.Printf("[SD aggTx=%d] FLUSHING at tx %d [%x], caller %s:%d\n", sd.aggTx.id, sd.TxNum(), fh, filepath.Base(f), l)
	}
	for _, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
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
		_, err = sd.aggTx.PruneSmallBatches(ctx, dbg.PruneOnFlushTimeout, tx)
		if err != nil {
			return err
		}
	}

	for _, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		w.close()
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
func (sd *SharedDomains) DomainGet(domain kv.Domain, k, k2 []byte) (v []byte, step uint64, err error) {
	if domain == kv.CommitmentDomain {
		return sd.LatestCommitment(k)
	}

	if k2 != nil {
		k = append(k, k2...)
	}
	if v, prevStep, ok := sd.get(domain, k); ok {
		return v, prevStep, nil
	}
	v, step, _, err = sd.aggTx.GetLatest(domain, k, nil, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}
	return v, step, nil
}

func (sd *SharedDomains) DomainGetAsOf(domain kv.Domain, k, k2 []byte, ofMaxTxnum uint64) (v []byte, step uint64, err error) {
	if domain == kv.CommitmentDomain {
		return sd.LatestCommitment(k)
	}

	if k2 != nil {
		k = append(k, k2...)
	}
	//if v, prevStep, ok := sd.get(domain, k); ok {
	//	return v, prevStep, nil
	//}
	//var ok bool
	v, ok, err := sd.aggTx.DomainGetAsOf(sd.roTx, domain, k, ofMaxTxnum)
	if err != nil {
		return nil, 0, fmt.Errorf("domain '%s' %x txn=%d read error: %w", domain, k, ofMaxTxnum, err)
	}
	if !ok {
		return nil, 0, nil
	}
	return v, step, nil
}

// DomainPut
// Optimizations:
//   - user can provide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	if val == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.DomainGet(domain, k1, k2)
		if err != nil {
			return err
		}
	}

	switch domain {
	case kv.AccountsDomain:
		return sd.updateAccountData(k1, val, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.writeAccountStorage(k1, k2, val, prevVal, prevStep)
	case kv.CodeDomain:
		if bytes.Equal(prevVal, val) {
			return nil
		}
		return sd.updateAccountCode(k1, val, prevVal, prevStep)
	case kv.CommitmentDomain:
		sd.put(domain, string(append(k1, k2...)), val)
		return sd.domainWriters[domain].PutWithPrev(k1, k2, val, prevVal, prevStep)
	default:
		if bytes.Equal(prevVal, val) {
			return nil
		}
		sd.put(domain, string(append(k1, k2...)), val)
		return sd.domainWriters[domain].PutWithPrev(k1, k2, val, prevVal, prevStep)
	}
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, k1, k2 []byte, prevVal []byte, prevStep uint64) error {
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.DomainGet(domain, k1, k2)
		if err != nil {
			return err
		}
	}

	switch domain {
	case kv.AccountsDomain:
		return sd.deleteAccount(k1, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.delAccountStorage(k1, k2, prevVal, prevStep)
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		return sd.updateAccountCode(k1, nil, prevVal, prevStep)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(k1, nil, prevVal, prevStep)
	default:
		sd.put(domain, string(append(k1, k2...)), nil)
		return sd.domainWriters[domain].DeleteWithPrev(k1, k2, prevVal, prevStep)
	}
}

func (sd *SharedDomains) DomainDelPrefix(domain kv.Domain, prefix []byte) error {
	if domain != kv.StorageDomain {
		return errors.New("DomainDelPrefix: not supported")
	}

	type tuple struct {
		k, v []byte
		step uint64
	}
	tombs := make([]tuple, 0, 8)
	if err := sd.IterateStoragePrefix(prefix, func(k, v []byte, step uint64) error {
		tombs = append(tombs, tuple{k, v, step})
		return nil
	}); err != nil {
		return err
	}
	for _, tomb := range tombs {
		if err := sd.DomainDel(kv.StorageDomain, tomb.k, nil, tomb.v, tomb.step); err != nil {
			return err
		}
	}

	if assert.Enable {
		forgotten := 0
		if err := sd.IterateStoragePrefix(prefix, func(k, v []byte, step uint64) error {
			forgotten++
			return nil
		}); err != nil {
			return err
		}
		if forgotten > 0 {
			panic(fmt.Errorf("DomainDelPrefix: %d forgotten keys after '%x' prefix removal", forgotten, prefix))
		}
	}
	return nil
}
func (sd *SharedDomains) Tx() kv.Tx { return sd.roTx }

func (sd *SharedDomains) fileRanges() (ranges [kv.DomainLen][]MergeRange) {
	for d, item := range sd.aggTx.d {
		ranges[d] = item.files.MergedRanges()
	}
	return
}

func (sd *SharedDomains) KeyCountInDomainRange(start, end uint64) (ranges [kv.DomainLen]uint64) {
	for d, item := range sd.aggTx.d {
		for _, f := range item.files {
			if f.startTxNum == start && f.endTxNum == end {
				ranges[d] = uint64(f.src.decompressor.Count() / 2)
				break
			}
		}
	}
	return
}

type SharedDomainsCommitmentContext struct {
	sharedDomains *SharedDomains
	discard       bool // could be replaced with using ModeDisabled
	branches      map[string]cachedBranch
	keccak        cryptozerocopy.KeccakState
	updates       *commitment.Updates
	patriciaTrie  commitment.Trie
	justRestored  atomic.Bool

	limitReadAsOfTxNum uint64
}

func (sdc *SharedDomainsCommitmentContext) SetLimitReadAsOfTxNum(txNum uint64) {
	sdc.limitReadAsOfTxNum = txNum
}

func (sdc *SharedDomainsCommitmentContext) Ranges() [kv.DomainLen][]MergeRange {
	return sdc.sharedDomains.fileRanges()
}

func NewSharedDomainsCommitmentContext(sd *SharedDomains, mode commitment.Mode, trieVariant commitment.TrieVariant) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sharedDomains: sd,
		discard:       dbg.DiscardCommitment(),
		branches:      make(map[string]cachedBranch),
		keccak:        sha3.NewLegacyKeccak256().(cryptozerocopy.KeccakState),
	}

	ctx.patriciaTrie, ctx.updates = commitment.InitializeTrieAndUpdates(trieVariant, mode, sd.aggTx.a.tmpdir)
	ctx.patriciaTrie.ResetContext(ctx)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) Close() {
	sdc.updates.Close()
}

type cachedBranch struct {
	data []byte
	step uint64
}

// ResetBranchCache should be called after each commitment computation
func (sdc *SharedDomainsCommitmentContext) ResetBranchCache() {
	clear(sdc.branches)
}

func (sdc *SharedDomainsCommitmentContext) Branch(pref []byte) ([]byte, uint64, error) {
	cached, ok := sdc.branches[string(pref)]
	if ok {
		// cached value is already transformed/clean to read.
		// Cache should ResetBranchCache after each commitment computation
		return cached.data, cached.step, nil
	}

	v, step, err := sdc.sharedDomains.LatestCommitment(pref)
	if err != nil {
		return nil, 0, fmt.Errorf("Branch failed: %w", err)
	}
	if sdc.sharedDomains.trace {
		fmt.Printf("[SDC] Branch: %x: %x\n", pref, v)
	}
	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update, if any, so
	// cache branch until ResetBranchCache called
	sdc.branches[string(pref)] = cachedBranch{data: v, step: step}

	if len(v) == 0 {
		return nil, 0, nil
	}
	return v, step, nil
}

func (sdc *SharedDomainsCommitmentContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep uint64) error {
	if sdc.sharedDomains.trace {
		fmt.Printf("[SDC] PutBranch: %x: %x\n", prefix, data)
	}
	sdc.branches[string(prefix)] = cachedBranch{data: data, step: prevStep}

	return sdc.sharedDomains.updateCommitmentData(prefix, data, prevData, prevStep)
}

func (sdc *SharedDomainsCommitmentContext) Account(plainKey []byte) (u *commitment.Update, err error) {
	var encAccount []byte
	if sdc.limitReadAsOfTxNum == 0 {
		encAccount, _, err = sdc.sharedDomains.DomainGet(kv.AccountsDomain, plainKey, nil)
		if err != nil {
			return nil, fmt.Errorf("GetAccount failed: %w", err)
		}
	} else {
		encAccount, _, err = sdc.sharedDomains.DomainGetAsOf(kv.AccountsDomain, plainKey, nil, sdc.limitReadAsOfTxNum)
		if err != nil {
			return nil, fmt.Errorf("GetAccount failed: %w", err)
		}
	}

	u = new(commitment.Update)
	u.Reset()

	if len(encAccount) > 0 {
		nonce, balance, chash := types.DecodeAccountBytesV3(encAccount)
		u.Flags |= commitment.NonceUpdate
		u.Nonce = nonce
		u.Flags |= commitment.BalanceUpdate
		u.Balance.Set(balance)
		if len(chash) > 0 {
			u.Flags |= commitment.CodeUpdate
			copy(u.CodeHash[:], chash)
		}
	}
	if u.CodeHash == commitment.EmptyCodeHashArray {
		if len(encAccount) == 0 {
			u.Flags = commitment.DeleteUpdate
		}
		return u, nil
	}

	var code []byte
	if sdc.limitReadAsOfTxNum == 0 {
		code, _, err = sdc.sharedDomains.DomainGet(kv.CodeDomain, plainKey, nil)
	} else {
		code, _, err = sdc.sharedDomains.DomainGetAsOf(kv.CodeDomain, plainKey, nil, sdc.limitReadAsOfTxNum)
	}
	if err != nil {
		return nil, fmt.Errorf("GetAccount/Code: failed to read latest code: %w", err)
	}

	if len(code) > 0 {
		sdc.keccak.Reset()
		sdc.keccak.Write(code)
		sdc.keccak.Read(u.CodeHash[:])
		u.Flags |= commitment.CodeUpdate

	} else {
		copy(u.CodeHash[:], commitment.EmptyCodeHashArray[:])
	}

	if len(encAccount) == 0 && len(code) == 0 {
		u.Flags = commitment.DeleteUpdate
	}
	return u, nil
}

func (sdc *SharedDomainsCommitmentContext) Storage(plainKey []byte) (u *commitment.Update, err error) {
	// Look in the summary table first
	var enc []byte
	if sdc.limitReadAsOfTxNum == 0 {
		enc, _, err = sdc.sharedDomains.DomainGet(kv.StorageDomain, plainKey, nil)
	} else {
		enc, _, err = sdc.sharedDomains.DomainGetAsOf(kv.StorageDomain, plainKey, nil, sdc.limitReadAsOfTxNum)
	}
	if err != nil {
		return nil, err
	}
	u = new(commitment.Update)
	u.StorageLen = len(enc)
	if len(enc) == 0 {
		u.Flags = commitment.DeleteUpdate
	} else {
		u.Flags |= commitment.StorageUpdate
		copy(u.Storage[:u.StorageLen], enc)
	}
	return u, nil
}

func (sdc *SharedDomainsCommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
}

func (sdc *SharedDomainsCommitmentContext) TempDir() string {
	return sdc.sharedDomains.aggTx.a.dirs.Tmp
}

func (sdc *SharedDomainsCommitmentContext) KeysCount() uint64 {
	return sdc.updates.Size()
}

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (sdc *SharedDomainsCommitmentContext) TouchKey(d kv.Domain, key string, val []byte) {
	if sdc.discard {
		return
	}
	ks := []byte(key)
	switch d {
	case kv.AccountsDomain:
		sdc.updates.TouchPlainKey(ks, val, sdc.updates.TouchAccount)
	case kv.CodeDomain:
		sdc.updates.TouchPlainKey(ks, val, sdc.updates.TouchCode)
	case kv.StorageDomain:
		sdc.updates.TouchPlainKey(ks, val, sdc.updates.TouchStorage)
	default:
		panic(fmt.Errorf("TouchKey: unknown domain %s", d))
	}
}

// Evaluates commitment for processed state.
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctx context.Context, saveState bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	if dbg.DiscardCommitment() {
		sdc.updates.Reset()
		return nil, nil
	}
	sdc.ResetBranchCache()
	defer sdc.ResetBranchCache()

	mxCommitmentRunning.Inc()
	defer mxCommitmentRunning.Dec()
	defer func(s time.Time) { mxCommitmentTook.ObserveDuration(s) }(time.Now())

	updateCount := sdc.updates.Size()
	if sdc.sharedDomains.trace {
		defer sdc.sharedDomains.logger.Trace("ComputeCommitment", "block", blockNum, "keys", updateCount, "mode", sdc.updates.Mode())
	}
	if updateCount == 0 {
		rootHash, err = sdc.patriciaTrie.RootHash()
		return rootHash, err
	}

	// data accessing functions should be set when domain is opened/shared context updated
	sdc.patriciaTrie.SetTrace(sdc.sharedDomains.trace)
	sdc.Reset()

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix)
	if err != nil {
		return nil, err
	}
	sdc.justRestored.Store(false)

	if saveState {
		if err := sdc.storeCommitmentState(blockNum, rootHash); err != nil {
			return nil, err
		}
	}

	return rootHash, err
}

func (sdc *SharedDomainsCommitmentContext) storeCommitmentState(blockNum uint64, rootHash []byte) error {
	if sdc.sharedDomains.aggTx == nil {
		return fmt.Errorf("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, sdc.sharedDomains.txNum)
	if err != nil {
		return err
	}
	prevState, prevStep, err := sdc.Branch(keyCommitmentState)
	if err != nil {
		return err
	}
	if len(prevState) == 0 && prevState != nil {
		prevState = nil
	}
	// state could be equal but txnum/blocknum could be different.
	// We do skip only full matches
	if bytes.Equal(prevState, encodedState) {
		//fmt.Printf("[commitment] skip store txn %d block %d (prev b=%d t=%d) rh %x\n",
		//	binary.BigEndian.Uint64(prevState[8:16]), binary.BigEndian.Uint64(prevState[:8]), dc.ht.iit.txNum, blockNum, rh)
		return nil
	}
	if sdc.sharedDomains.trace {
		fmt.Printf("[commitment] store txn %d block %d rootHash %x\n", sdc.sharedDomains.txNum, blockNum, rootHash)
	}
	sdc.sharedDomains.put(kv.CommitmentDomain, string(keyCommitmentState), encodedState)
	return sdc.sharedDomains.domainWriters[kv.CommitmentDomain].PutWithPrev(keyCommitmentState, nil, encodedState, prevState, prevStep)
}

func (sdc *SharedDomainsCommitmentContext) encodeCommitmentState(blockNum, txNum uint64) ([]byte, error) {
	var state []byte
	var err error

	switch trie := (sdc.patriciaTrie).(type) {
	case *commitment.HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported state storing for patricia trie type: %T", sdc.patriciaTrie)
	}

	cs := &commitmentState{trieState: state, blockNum: blockNum, txNum: txNum}
	encoded, err := cs.Encode()
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// by that key stored latest root hash and tree state
var keyCommitmentState = []byte("state")

func (sd *SharedDomains) LatestCommitmentState(tx kv.Tx, sinceTx, untilTx uint64) (blockNum, txNum uint64, state []byte, err error) {
	return sd.sdCtx.LatestCommitmentState()
}

func _decodeTxBlockNums(v []byte) (txNum, blockNum uint64) {
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:16])
}

// LatestCommitmentState searches for last encoded state for CommitmentContext.
// Found value does not become current state.
func (sdc *SharedDomainsCommitmentContext) LatestCommitmentState() (blockNum, txNum uint64, state []byte, err error) {
	if dbg.DiscardCommitment() {
		return 0, 0, nil, nil
	}
	if sdc.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie {
		return 0, 0, nil, fmt.Errorf("state storing is only supported hex patricia trie")
	}
	state, _, err = sdc.Branch(keyCommitmentState)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(state) < 16 {
		return 0, 0, nil, nil
	}

	txNum, blockNum = _decodeTxBlockNums(state)
	return blockNum, txNum, state, nil
}

// SeekCommitment [sinceTx, untilTx] searches for last encoded state from DomainCommitted
// and if state found, sets it up to current domain
func (sdc *SharedDomainsCommitmentContext) SeekCommitment(tx kv.Tx, cd *DomainRoTx, sinceTx, untilTx uint64) (blockNum, txNum uint64, ok bool, err error) {
	_, _, state, err := sdc.LatestCommitmentState()
	if err != nil {
		return 0, 0, false, err
	}
	blockNum, txNum, err = sdc.restorePatriciaState(state)
	return blockNum, txNum, true, err
}

// After commitment state is retored, method .Reset() should NOT be called until new updates.
// Otherwise state should be restorePatriciaState()d again.

func (sdc *SharedDomainsCommitmentContext) restorePatriciaState(value []byte) (uint64, uint64, error) {
	cs := new(commitmentState)
	if err := cs.Decode(value); err != nil {
		if len(value) > 0 {
			return 0, 0, fmt.Errorf("failed to decode previous stored commitment state: %w", err)
		}
		// nil value is acceptable for SetState and will reset trie
	}
	if hext, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
		if err := hext.SetState(cs.trieState); err != nil {
			return 0, 0, fmt.Errorf("failed restore state : %w", err)
		}
		sdc.justRestored.Store(true) // to prevent double reset
		// if sdc.sharedDomains.trace {
		rootHash, err := hext.RootHash()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get root hash after state restore: %w", err)
		}
		fmt.Printf("[commitment] restored state: block=%d txn=%d rootHash=%x\n", cs.blockNum, cs.txNum, rootHash)
		// }
	} else {
		return 0, 0, errors.New("state storing is only supported hex patricia trie")
	}
	return cs.blockNum, cs.txNum, nil
}
