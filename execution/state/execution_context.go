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
	"math"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

var (
	mxFlushTook = metrics.GetOrCreateSummary("domain_flush_took")
)

type accHolder interface {
	SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet)
	SetChangesetAccumulator(acc *changeset.StateChangeSet)
}

type ValueWithStep[V any] struct {
	Value V
	Step  kv.Step
}

type ExecutionContext struct {
	sdCtx             *commitment.CommitmentContext
	logger            log.Logger
	txNum             uint64
	blockNum          atomic.Uint64
	trace             bool //nolint
	commitmentCapture bool
	mem               kv.TemporalMemBatch
	metrics           DomainMetrics
	accountsDomain    *AccountsDomain
	storageDomain     *StorageDomain
	codeUpdates       map[accounts.Address]ValueWithStep[[]byte]
	branchUpdates     map[commitment.Path]ValueWithStep[commitment.Branch]
}

func NewExecutionContext(ctx context.Context, tx kv.TemporalTx, logger log.Logger) (*ExecutionContext, error) {
	sd := &ExecutionContext{
		logger: logger,
		//trace:   true,
		metrics: DomainMetrics{Domains: map[kv.Domain]*DomainIOMetrics{}},
	}

	sd.mem = tx.Debug().NewMemBatch(&sd.metrics)

	tv := commitment.VariantHexPatriciaTrie
	if statecfg.ExperimentalConcurrentCommitment {
		tv = commitment.VariantConcurrentHexPatricia
	}

	sd.sdCtx = commitment.NewCommitmentContext(commitment.ModeDirect, tv, tx.Debug().Dirs().Tmp)

	if err := sd.SeekCommitment(ctx, tx); err != nil {
		return nil, err
	}

	return sd, nil
}

type temporalPutDel struct {
	sd *ExecutionContext
	tx kv.TemporalTx
}

func (pd *temporalPutDel) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	switch domain {
	case kv.AccountsDomain:
		var a accounts.Account
		if err := accounts.DeserialiseV3(&a, v); err != nil {
			return err
		}
		var pa []ValueWithStep[*accounts.Account]
		if prevVal != nil {
			var a accounts.Account
			if err := accounts.DeserialiseV3(&a, prevVal); err != nil {
				return err
			}
			pa = []ValueWithStep[*accounts.Account]{{Value: &a, Step: prevStep}}
		}
		return pd.sd.PutAccount(context.Background(), accounts.BytesToAddress(k), &a, pd.tx, txNum, pa...)
	case kv.StorageDomain:
		var i uint256.Int
		i.SetBytes(v)
		var prev []ValueWithStep[uint256.Int]
		if prevVal != nil {
			var i uint256.Int
			i.SetBytes(prevVal)
			prev = []ValueWithStep[uint256.Int]{{Value: i, Step: prevStep}}
		}
		return pd.sd.PutStorage(context.Background(),
			accounts.BytesToAddress(k[:length.Addr]), accounts.BytesToKey(k[length.Addr:]), i, pd.tx, txNum, prev...)
	case kv.CodeDomain:
		return pd.sd.PutCode(context.Background(), accounts.BytesToAddress(k), v, pd.tx, txNum, prevVal, prevStep)
	case kv.CommitmentDomain:
		return pd.sd.PutBranch(context.Background(), commitment.InternPath(k), commitment.Branch(v), pd.tx, txNum, commitment.Branch(prevVal), prevStep)
	}
	return pd.sd.mem.DomainPut(domain, k, v, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	switch domain {
	case kv.AccountsDomain:
		var prev []ValueWithStep[*accounts.Account]
		if prevVal != nil {
			var a accounts.Account
			if err := accounts.DeserialiseV3(&a, prevVal); err != nil {
				return err
			}

			prev = []ValueWithStep[*accounts.Account]{{Value: &a, Step: prevStep}}
		}
		return pd.sd.DelAccount(context.Background(), accounts.BytesToAddress(k), pd.tx, txNum, prev...)
	case kv.StorageDomain:
		var pi *uint256.Int
		if prevVal != nil {
			var i uint256.Int
			i.SetBytes(prevVal)
			pi = &i
		}
		return pd.sd.DelStorage(context.Background(),
			accounts.BytesToAddress(k[:length.Addr]), accounts.BytesToKey(k[length.Addr:]), pd.tx, txNum, pi, prevStep)
	case kv.CodeDomain:
		return pd.sd.DelCode(context.Background(), accounts.BytesToAddress(k), pd.tx, txNum, prevVal, prevStep)
	case kv.CommitmentDomain:
		return pd.sd.DelBranch(context.Background(), commitment.InternPath(k), pd.tx, txNum, commitment.Branch(prevVal), prevStep)
	}
	return pd.sd.mem.DomainDel(domain, k, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	if domain == kv.StorageDomain {
		return pd.sd.DelStorage(context.Background(), accounts.BytesToAddress(prefix), accounts.NilKey, pd.tx, txNum)
	}

	return fmt.Errorf("unsupported domain: %s, for del prefix")
}

func (sd *ExecutionContext) AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel {
	return &temporalPutDel{sd, tx}
}

func (sd *ExecutionContext) Merge(other *ExecutionContext) error {
	return sd.mem.Merge(other.mem)
}

type temporalGetter struct {
	sd *ExecutionContext
	tx kv.TemporalTx
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	switch name {
	case kv.AccountsDomain:
		addr := accounts.BytesToAddress(k)
		a, s, ok, err := gt.sd.GetAccount(context.Background(), addr, gt.tx)
		if ok {
			return accounts.SerialiseV3(a), s, nil
		}
		return nil, 0, err
	case kv.StorageDomain:
		addr := accounts.BytesToAddress(k[:length.Addr])
		key := accounts.BytesToKey(k[length.Addr:])
		i, s, ok, err := gt.sd.GetStorage(context.Background(), addr, key, gt.tx)
		if ok {
			return i.Bytes(), s, nil
		}
		return nil, 0, err
	case kv.CodeDomain:
		addr := accounts.BytesToAddress(k)
		c, s, _, err := gt.sd.GetCode(context.Background(), addr, gt.tx)
		return c, s, err
	case kv.CommitmentDomain:
		b, s, ok, err := gt.sd.GetBranch(context.Background(), commitment.InternPath(k), gt.tx)
		if ok {
			return b, s, nil
		}
		return nil, 0, err
	}
	return gt.sd.getLatest(context.Background(), name, gt.tx, k, time.Time{})
}

func (gt *temporalGetter) HasPrefix(name kv.Domain, prefix []byte) (firstKey []byte, firstVal []byte, ok bool, err error) {
	var hasPrefix bool
	err = gt.sd.mem.IteratePrefix(kv.StorageDomain, prefix, gt.tx, func(k []byte, v []byte, step kv.Step) (bool, error) {
		firstKey = common.Copy(k)
		firstVal = common.Copy(v)
		hasPrefix = true
		return false, nil // do not continue, end on first occurrence
	})
	return firstKey, firstVal, hasPrefix, err
}

func (gt *temporalGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return gt.tx.StepsInFiles(entitySet...)
}

type unmarkedPutter struct {
	ec         *ExecutionContext
	forkableId kv.ForkableId
}

func (sd *ExecutionContext) AsUnmarkedPutter(id kv.ForkableId) kv.UnmarkedPutter {
	return &unmarkedPutter{sd, id}
}

func (up *unmarkedPutter) Put(num kv.Num, v []byte) error {
	return up.ec.mem.PutForkable(up.forkableId, num, v)
}

func (sd *ExecutionContext) AsGetter(tx kv.TemporalTx) kv.TemporalGetter {
	return &temporalGetter{sd, tx}
}

func (sd *ExecutionContext) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	sd.mem.(accHolder).SetChangesetAccumulator(acc)
}

func (sd *ExecutionContext) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet) {
	sd.mem.(accHolder).SavePastChangesetAccumulator(blockHash, blockNumber, acc)
}

func (sd *ExecutionContext) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	return sd.mem.GetDiffset(tx, blockHash, blockNumber)
}

func (sd *ExecutionContext) Unwind(txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.mem.Unwind(txNumUnwindTo, changeset)
}

func (sd *ExecutionContext) Trace() bool {
	return sd.trace
}

func (sd *ExecutionContext) CommitmentCapture() bool {
	return sd.commitmentCapture
}

func (sd *ExecutionContext) GetMemBatch() kv.TemporalMemBatch { return sd.mem }
func (sd *ExecutionContext) GetCommitmentCtx() *commitment.CommitmentContext {
	return sd.sdCtx
}
func (sd *ExecutionContext) Logger() log.Logger { return sd.logger }

func (sd *ExecutionContext) ClearRam(resetCommitment bool) {
	if resetCommitment {
		sd.sdCtx.ClearRam()
	}

	sd.accountsDomain.FlushUpdates()

	sd.metrics.Lock()
	defer sd.metrics.Unlock()
	sd.metrics.CachePutCount = 0
	sd.metrics.CachePutSize = 0
	sd.metrics.CachePutKeySize = 0
	sd.metrics.CachePutValueSize = 0
}

func (sd *ExecutionContext) SizeEstimate() uint64 {
	sd.metrics.RLock()
	defer sd.metrics.RUnlock()
	// multiply 2: to cover data-structures overhead (and keep accounting cheap)
	// and muliply 2 more: for Commitment calculation when batch is full
	return uint64(sd.metrics.CachePutSize) * 4
}

const CodeSizeTableFake = "CodeSize"

func (sd *ExecutionContext) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	return sd.mem.IndexAdd(table, key, txNum)
}

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if stepSize is reached
func (sd *ExecutionContext) SetTxNum(txNum uint64) {
	sd.txNum = txNum
}

func (sd *ExecutionContext) TxNum() uint64 { return sd.txNum }

func (sd *ExecutionContext) BlockNum() uint64 { return sd.blockNum.Load() }

func (sd *ExecutionContext) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *ExecutionContext) SetTrace(b, capture bool) []string {
	sd.trace = b
	sd.commitmentCapture = capture
	return sd.sdCtx.GetCapture(true)
}

func (sd *ExecutionContext) HasStorage(ctx context.Context, addr accounts.Address, roTx kv.Tx) ([]byte, []byte, bool, error) {
	var firstKey, firstVal []byte
	var hasPrefix bool
	addrVal := addr.Value()
	err := sd.mem.IteratePrefix(kv.StorageDomain, addrVal[:], roTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
		firstKey = common.Copy(k)
		firstVal = common.Copy(v)
		hasPrefix = true
		return false, nil // do not continue, end on first occurrence
	})
	return firstKey, firstVal, hasPrefix, err
}

func (sd *ExecutionContext) IterateStorage(ctx context.Context, addr accounts.Address, it func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error), roTx kv.Tx) error {
	addrVal := addr.Value()
	return sd.mem.IteratePrefix(kv.StorageDomain, addrVal[:], roTx, func(k []byte, v []byte, step kv.Step) (cont bool, err error) {
		var i uint256.Int
		i.SetBytes(v)
		return it(accounts.BytesToKey(k), i, step)
	})
}

func (sd *ExecutionContext) Close() {
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

func (sd *ExecutionContext) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	sd.accountsDomain.FlushUpdates()
	return sd.mem.Flush(ctx, tx)
}

func (sd *ExecutionContext) GetAccount(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (v *accounts.Account, step kv.Step, ok bool, err error) {
	return sd.accountsDomain.Get(ctx, k, tx)
}

func (sd *ExecutionContext) PutAccount(ctx context.Context, k accounts.Address, v *accounts.Account, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[*accounts.Account]) error {
	return sd.accountsDomain.Put(ctx, k, v, roTx, txNum, prev...)
}

func (sd *ExecutionContext) DelAccount(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[*accounts.Account]) error {
	return sd.accountsDomain.Del(ctx, k, roTx, txNum, prev...)
}

func (sd *ExecutionContext) GetStorage(ctx context.Context, addr accounts.Address, key accounts.StorageKey, tx kv.TemporalTx) (v uint256.Int, step kv.Step, ok bool, err error) {
	return sd.storageDomain.Get(ctx, addr, key, tx)
}

func (sd *ExecutionContext) PutStorage(ctx context.Context, addr accounts.Address, key accounts.StorageKey, v uint256.Int, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[uint256.Int]) error {
	return sd.storageDomain.Put(ctx, addr, key, v, roTx, txNum, prev...)
}

func (sd *ExecutionContext) DelStorage(ctx context.Context, addr accounts.Address, key accounts.StorageKey, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[uint256.Int]) error {
	return sd.storageDomain.Del(ctx, addr, key, roTx, txNum, prev...)
}

func (sd *ExecutionContext) GetCode(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (v []byte, step kv.Step, ok bool, err error) {
	if tx == nil {
		return nil, 0, false, errors.New("sd.GetCode: unexpected nil tx")
	}
	start := time.Now()
	if val, ok := sd.codeUpdates[k]; ok {
		sd.metrics.UpdateCacheReads(kv.CodeDomain, start)
		return val.Value, val.Step, false, nil
	}

	kval := k.Value()
	v, step, err = sd.getLatest(ctx, kv.CodeDomain, tx, kval[:], start)

	if err != nil {
		return nil, 0, false, fmt.Errorf("account %s deserialize error: %w", k, err)
	}

	return v, step, true, nil
}

func (sd *ExecutionContext) PutCode(ctx context.Context, k accounts.Address, v []byte, roTx kv.TemporalTx, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	if v == nil {
		return fmt.Errorf("PutAccount: %s, trying to put nil value. not allowed", kv.AccountsDomain)
	}

	sd.sdCtx.TouchCode(k, v)

	if prevVal == nil {
		var err error
		prevVal, prevStep, _, err = sd.GetCode(ctx, k, roTx)
		if err != nil {
			return err
		}
	}

	if bytes.Equal(v, prevVal) {
		return nil
	}

	kval := k.Value()
	if err := sd.mem.DomainPut(kv.CodeDomain, kval[:], v, txNum, prevVal, prevStep); err != nil {
		return err
	}

	sd.codeUpdates[k] = ValueWithStep[[]byte]{v, kv.Step(txNum / roTx.StepSize())}
	return nil
}

func (sd *ExecutionContext) DelCode(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	sd.sdCtx.TouchCode(k, nil)

	if prevVal == nil {
		var err error
		prevVal, prevStep, _, err = sd.GetCode(ctx, k, roTx)
		if err != nil {
			return err
		}
	}

	if prevVal == nil {
		return nil
	}

	kval := k.Value()
	return sd.mem.DomainDel(kv.AccountsDomain, kval[:], txNum, prevVal, prevStep)
}

func (sd *ExecutionContext) GetBranch(ctx context.Context, k commitment.Path, tx kv.TemporalTx) (v commitment.Branch, step kv.Step, ok bool, err error) {
	if tx == nil {
		return nil, 0, false, errors.New("sd.GetAccount: unexpected nil tx")
	}
	start := time.Now()
	if val, ok := sd.branchUpdates[k]; ok {
		sd.metrics.UpdateCacheReads(kv.CommitmentDomain, start)
		return val.Value, val.Step, false, nil
	}

	var buf bytes.Buffer
	k.Encode(&buf)

	vbytes, step, err := sd.getLatest(ctx, kv.CommitmentDomain, tx, buf.Bytes(), start)

	if err != nil {
		return nil, 0, false, fmt.Errorf("branch %s deserialize error: %w", k, err)
	}

	return commitment.Branch(vbytes), step, true, nil
}

func (sd *ExecutionContext) PutBranch(ctx context.Context, k commitment.Path, v commitment.Branch, roTx kv.TemporalTx, txNum uint64, prevVal commitment.Branch, prevStep kv.Step) error {
	if v == nil {
		return fmt.Errorf("PutBranch: %s, trying to put nil value. not allowed", kv.CommitmentDomain)
	}

	if prevVal == nil {
		var err error
		prevVal, prevStep, _, err = sd.GetBranch(ctx, k, roTx)
		if err != nil {
			return err
		}
	}

	bval := k.Value()
	if err := sd.mem.DomainPut(kv.CommitmentDomain, bval, v, txNum, prevVal, prevStep); err != nil {
		return err
	}

	sd.branchUpdates[k] = ValueWithStep[commitment.Branch]{v, kv.Step(txNum / roTx.StepSize())}
	return nil
}

func (sd *ExecutionContext) DelBranch(ctx context.Context, k commitment.Path, roTx kv.TemporalTx, txNum uint64, prevVal commitment.Branch, prevStep kv.Step) error {
	if prevVal == nil {
		var err error
		prevVal, prevStep, _, err = sd.GetBranch(ctx, k, roTx)
		if err != nil {
			return err
		}
	}

	if prevVal == nil {
		return nil
	}

	kval := k.Value()
	return sd.mem.DomainDel(kv.AccountsDomain, kval[:], txNum, prevVal, prevStep)
}

func (sd *ExecutionContext) getLatest(ctx context.Context, domain kv.Domain, tx kv.TemporalTx, k []byte, start time.Time) (v []byte, step kv.Step, err error) {
	maxStep := kv.Step(math.MaxUint64)

	if v, step, ok := sd.mem.GetLatest(domain, k); ok {
		sd.metrics.UpdateCacheReads(domain, start)
		return v, step, nil
	} else {
		if step > 0 {
			maxStep = step
		}
	}

	type MeteredGetter interface {
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
	}

	if aggTx, ok := tx.AggTx().(MeteredGetter); ok {
		v, step, _, err = aggTx.MeteredGetLatest(domain, k, tx, maxStep, &sd.metrics, start)
	} else {
		v, step, err = tx.GetLatest(domain, k)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("account %s read error: %w", k, err)
	}

	return v, step, nil
}

func (sd *ExecutionContext) Metrics() *DomainMetrics {
	return &sd.metrics
}

func (sd *ExecutionContext) LogMetrics() []any {
	var metrics []any

	sd.metrics.RLock()
	defer sd.metrics.RUnlock()

	if readCount := sd.metrics.CacheReadCount; readCount > 0 {
		metrics = append(metrics, "cache", common.PrettyCounter(readCount),
			"puts", common.PrettyCounter(sd.metrics.CachePutCount),
			"size", fmt.Sprintf("%s(%s/%s)",
				common.PrettyCounter(sd.metrics.CachePutSize), common.PrettyCounter(sd.metrics.CachePutKeySize), common.PrettyCounter(sd.metrics.CachePutValueSize)),
			"gets", common.PrettyCounter(sd.metrics.CacheGetCount), "size", common.PrettyCounter(sd.metrics.CacheGetSize),
			"cdur", common.Round(sd.metrics.CacheReadDuration/time.Duration(readCount), 0))
	}

	if readCount := sd.metrics.DbReadCount; readCount > 0 {
		metrics = append(metrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(sd.metrics.DbReadDuration/time.Duration(readCount), 0))
	}

	if readCount := sd.metrics.FileReadCount; readCount > 0 {
		metrics = append(metrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(sd.metrics.FileReadDuration/time.Duration(readCount), 0))
	}

	return metrics
}

func (sd *ExecutionContext) DomainLogMetrics() map[kv.Domain][]any {
	var logMetrics = map[kv.Domain][]any{}

	sd.metrics.RLock()
	defer sd.metrics.RUnlock()

	for domain, dm := range sd.metrics.Domains {
		var metrics []any

		if readCount := dm.CacheReadCount; readCount > 0 {
			metrics = append(metrics, "cache", common.PrettyCounter(readCount), "cdur", common.Round(dm.CacheReadDuration/time.Duration(readCount), 0))
		}

		if readCount := dm.DbReadCount; readCount > 0 {
			metrics = append(metrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(dm.DbReadDuration/time.Duration(readCount), 0))
		}

		if readCount := dm.FileReadCount; readCount > 0 {
			metrics = append(metrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(dm.DbReadDuration/time.Duration(readCount), 0))
		}

		if len(metrics) > 0 {
			logMetrics[domain] = metrics
		}
	}

	return logMetrics
}

// DiscardWrites disables updates collection for further flushing into db.
// Instead, it keeps them temporarily available until .ClearRam/.Close will make them unavailable.
func (sd *ExecutionContext) DiscardWrites(d kv.Domain) {
	// TODO: Deprecated - need convert this method to Constructor-Builder configuration
	if d >= kv.DomainLen {
		return
	}
	sd.mem.DiscardWrites(d)
}

func (sd *ExecutionContext) GetCommitmentContext() *commitment.CommitmentContext {
	return sd.sdCtx
}

// SeekCommitment lookups latest available commitment and sets it as current
func (sd *ExecutionContext) SeekCommitment(ctx context.Context, tx kv.TemporalTx) (err error) {
	blockNum, txNum, _, err := sd.sdCtx.SeekCommitment(ctx, sd.AsGetter(tx), tx)
	if err != nil {
		return err
	}
	sd.SetBlockNum(blockNum)
	sd.SetTxNum(txNum)
	return nil
}

func (sd *ExecutionContext) ComputeCommitment(ctx context.Context, tx kv.TemporalTx, saveStateAfter bool, blockNum, txNum uint64, logPrefix string, commitProgress chan *commitment.CommitProgress) (rootHash []byte, err error) {
	return sd.sdCtx.ComputeCommitment(ctx, sd.AsGetter(tx), sd.AsPutDel(tx), saveStateAfter, blockNum, txNum, logPrefix, commitProgress)
}
