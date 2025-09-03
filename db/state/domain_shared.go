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
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
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

type SharedDomains struct {
	sdCtx *SharedDomainsCommitmentContext

	stepSize uint64

	logger log.Logger

	txNum    uint64
	blockNum atomic.Uint64
	trace    bool //nolint

	mem *AggMemBatch
}

type HasAgg interface {
	Agg() any
}

func NewSharedDomains(tx kv.TemporalTx, logger log.Logger) (*SharedDomains, error) {
	sd := &SharedDomains{
		logger: logger,
		//trace:   true,
		mem: newAggMemBatch(tx),
	}
	aggTx := AggTx(tx)
	sd.stepSize = aggTx.StepSize()

	tv := commitment.VariantHexPatriciaTrie
	if statecfg.ExperimentalConcurrentCommitment {
		tv = commitment.VariantConcurrentHexPatricia
	}

	sd.sdCtx = NewSharedDomainsCommitmentContext(sd, tx, commitment.ModeDirect, tv, aggTx.a.dirs.Tmp)

	if err := sd.SeekCommitment(context.Background(), tx); err != nil {
		return nil, err
	}

	return sd, nil
}

type temporalPutDel struct {
	sd *SharedDomains
	tx kv.Tx
}

func (pd *temporalPutDel) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	return pd.sd.DomainPut(domain, pd.tx, k, v, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	return pd.sd.DomainDel(domain, pd.tx, k, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	return pd.sd.DomainDelPrefix(domain, pd.tx, prefix, txNum)
}

func (sd *SharedDomains) AsPutDel(tx kv.Tx) kv.TemporalPutDel {
	return &temporalPutDel{sd, tx}
}
func (sd *SharedDomains) TrieCtxForTests() *SharedDomainsCommitmentContext {
	return sd.sdCtx
}

type temporalGetter struct {
	sd *SharedDomains
	tx kv.Tx
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return gt.sd.GetLatest(name, gt.tx, k)
}

func (gt *temporalGetter) HasPrefix(name kv.Domain, prefix []byte) (firstKey []byte, firstVal []byte, ok bool, err error) {
	return gt.sd.HasPrefix(name, prefix, gt.tx)
}

func (sd *SharedDomains) AsGetter(tx kv.Tx) kv.TemporalGetter {
	return &temporalGetter{sd, tx}
}

func (sd *SharedDomains) SetChangesetAccumulator(acc *StateChangeSet) {
	sd.mem.SetChangesetAccumulator(acc)
}

func (sd *SharedDomains) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *StateChangeSet) {
	sd.mem.SavePastChangesetAccumulator(blockHash, blockNumber, acc)
}

func (sd *SharedDomains) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	return sd.mem.GetDiffset(tx, blockHash, blockNumber)
}

func (sd *SharedDomains) ClearRam(resetCommitment bool) {
	if resetCommitment {
		sd.sdCtx.updates.Reset()
		sd.sdCtx.Reset()
	}
	sd.mem.ClearRam()
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	return sd.mem.SizeEstimate()
}

func (sd *SharedDomains) updateAccountCode(addrS string, code []byte, txNum uint64, prevCode []byte, prevStep kv.Step) error {
	addr := toBytesZeroCopy(addrS)
	sd.mem.DomainPut(kv.CodeDomain, addrS, code, txNum)
	return sd.mem.PutWithPrev(kv.CodeDomain, addr, code, txNum, prevCode, prevStep)
}

func (sd *SharedDomains) updateCommitmentData(prefix string, data []byte, txNum uint64, prev []byte, prevStep kv.Step) error {
	sd.mem.DomainPut(kv.CommitmentDomain, prefix, data, txNum)
	return sd.mem.PutWithPrev(kv.CommitmentDomain, toBytesZeroCopy(prefix), data, txNum, prev, prevStep)
}

func (sd *SharedDomains) deleteAccount(roTx kv.Tx, addrS string, txNum uint64, prev []byte, prevStep kv.Step) error {
	addr := toBytesZeroCopy(addrS)
	if err := sd.DomainDelPrefix(kv.StorageDomain, roTx, addr, txNum); err != nil {
		return err
	}

	// commitment delete already has been applied via account
	if err := sd.DomainDel(kv.CodeDomain, roTx, addr, txNum, nil, prevStep); err != nil {
		return err
	}

	sd.mem.DomainPut(kv.AccountsDomain, addrS, nil, txNum)
	return sd.mem.PutWithPrev(kv.AccountsDomain, addr, nil, txNum, prev, prevStep)
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	return sd.mem.IndexAdd(table, key, txNum)
}

func (sd *SharedDomains) StepSize() uint64 { return sd.stepSize }

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if stepSize is reached
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

func (sd *SharedDomains) HasPrefix(domain kv.Domain, prefix []byte, roTx kv.Tx) ([]byte, []byte, bool, error) {
	var firstKey, firstVal []byte
	var hasPrefix bool
	err := sd.IteratePrefix(domain, prefix, roTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
		firstKey = common.CopyBytes(k)
		firstVal = common.CopyBytes(v)
		hasPrefix = true
		return false, nil // do not continue, end on first occurrence
	})
	return firstKey, firstVal, hasPrefix, err
}

func (sd *SharedDomains) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte, step kv.Step) (cont bool, err error)) error {
	return sd.mem.IteratePrefix(domain, prefix, roTx, it)
}

func (sd *SharedDomains) Close() {
	if sd.sdCtx == nil { //idempotency
		return
	}

	sd.SetBlockNum(0)
	sd.SetTxNum(0)

	//sd.walLock.Lock()
	//defer sd.walLock.Unlock()

	sd.mem.Close()

	sd.sdCtx.Close()
	sd.sdCtx = nil
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	return sd.mem.Flush(ctx, tx)
}

// TemporalDomain satisfaction
func (sd *SharedDomains) GetLatest(domain kv.Domain, tx kv.Tx, k []byte) (v []byte, step kv.Step, err error) {
	if tx == nil {
		return nil, 0, errors.New("sd.GetLatest: unexpected nil tx")
	}
	if v, prevStep, ok := sd.mem.GetLatest(domain, k); ok {
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
func (sd *SharedDomains) DomainPut(domain kv.Domain, roTx kv.Tx, k, v []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	if v == nil {
		return fmt.Errorf("DomainPut: %s, trying to Put nil value. not allowed", domain)
	}

	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, roTx, k)
		if err != nil {
			return err
		}
	}
	ks := string(k)

	sd.sdCtx.TouchKey(domain, ks, v)
	switch domain {
	case kv.CodeDomain:
		if bytes.Equal(prevVal, v) {
			return nil
		}
		sd.mem.DomainPut(domain, ks, v, txNum)
		return sd.mem.PutWithPrev(domain, k, v, txNum, prevVal, prevStep)
	case kv.StorageDomain, kv.AccountsDomain, kv.CommitmentDomain, kv.RCacheDomain:
		sd.mem.DomainPut(domain, ks, v, txNum)
		return sd.mem.PutWithPrev(domain, k, v, txNum, prevVal, prevStep)
	default:
		if bytes.Equal(prevVal, v) {
			return nil
		}
		sd.mem.DomainPut(domain, ks, v, txNum)
		return sd.mem.PutWithPrev(domain, k, v, txNum, prevVal, prevStep)
	}
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, tx kv.Tx, k []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, tx, k)
		if err != nil {
			return err
		}
	}

	ks := string(k)
	sd.sdCtx.TouchKey(domain, ks, nil)
	switch domain {
	case kv.AccountsDomain:
		return sd.deleteAccount(tx, ks, txNum, prevVal, prevStep)
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		sd.mem.DomainPut(kv.CodeDomain, ks, nil, txNum)
		return sd.mem.PutWithPrev(kv.CodeDomain, k, nil, txNum, prevVal, prevStep)
	default:
		sd.mem.DomainPut(domain, ks, nil, txNum)
		return sd.mem.PutWithPrev(domain, k, nil, txNum, prevVal, prevStep)
	}
}

func (sd *SharedDomains) DomainDelPrefix(domain kv.Domain, roTx kv.Tx, prefix []byte, txNum uint64) error {
	if domain != kv.StorageDomain {
		return errors.New("DomainDelPrefix: not supported")
	}

	type tuple struct {
		k, v []byte
		step kv.Step
	}
	tombs := make([]tuple, 0, 8)

	if err := sd.IteratePrefix(kv.StorageDomain, prefix, roTx, func(k, v []byte, step kv.Step) (bool, error) {
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
		if err := sd.IteratePrefix(kv.StorageDomain, prefix, roTx, func(k, v []byte, step kv.Step) (bool, error) {
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

// DiscardWrites disables updates collection for further flushing into db.
// Instead, it keeps them temporarily available until .ClearRam/.Close will make them unavailable.
// Deprecated - need convert to Constructor-Builder configuration
func (sd *SharedDomains) DiscardWrites(d kv.Domain) {
	if d >= kv.DomainLen {
		return
	}
	sd.mem.DiscardWrites(d)
}

func toStringZeroCopy(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len(v))
}

func toBytesZeroCopy(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

func AggTx(tx kv.Tx) *AggregatorRoTx {
	if withAggTx, ok := tx.(interface{ AggTx() any }); ok {
		return withAggTx.AggTx().(*AggregatorRoTx)
	}

	return nil
}
