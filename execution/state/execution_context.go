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

type ValueWithTxNum[V any] struct {
	Value V
	TxNum uint64
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
	blockDomain       *BlockDomain
	accountsDomain    *AccountsDomain
	storageDomain     *StorageDomain
	codeDomain        *CodeDomain
	commitmentDomain  *CommitmentDomain
}

func NewExecutionContext(ctx context.Context, tx kv.TemporalTx, logger log.Logger) (ec *ExecutionContext, err error) {
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

	if sd.commitmentDomain, err = NewCommitmentDomain(sd.mem, sd.sdCtx, &sd.metrics); err != nil {
		return nil, err
	}

	if sd.storageDomain, err = NewStorageDomain(sd.mem, sd.sdCtx, &sd.metrics); err != nil {
		return nil, err
	}

	if sd.codeDomain, err = NewCodeDomain(sd.mem, sd.sdCtx, &sd.metrics); err != nil {
		return nil, err
	}

	if sd.accountsDomain, err = NewAccountsDomain(sd.mem, sd.storageDomain, sd.codeDomain, sd.sdCtx, &sd.metrics); err != nil {
		return nil, err
	}

	if err := sd.SeekCommitment(ctx, tx); err != nil {
		return nil, err
	}

	return sd, nil
}

func (ec *ExecutionContext) WithBlockDomain(tx kv.TemporalTx, tmpDir string, logger log.Logger) *ExecutionContext {
	ec.blockDomain = NewBlockDomain(tx, tmpDir, logger)
	return ec
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
		var pa []ValueWithTxNum[*accounts.Account]
		if prevVal != nil {
			var a accounts.Account
			if err := accounts.DeserialiseV3(&a, prevVal); err != nil {
				return err
			}
			pa = []ValueWithTxNum[*accounts.Account]{{Value: &a, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.PutAccount(context.Background(), accounts.BytesToAddress(k), &a, pd.tx, txNum, pa...)
	case kv.StorageDomain:
		var i uint256.Int
		i.SetBytes(v)
		var prev []ValueWithTxNum[uint256.Int]
		if prevVal != nil {
			var i uint256.Int
			i.SetBytes(prevVal)
			prev = []ValueWithTxNum[uint256.Int]{{Value: i, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.PutStorage(context.Background(),
			accounts.BytesToAddress(k[:length.Addr]), accounts.BytesToKey(k[length.Addr:]), i, pd.tx, txNum, prev...)
	case kv.CodeDomain:
		var prev []CodeWithTxNum
		if prevVal != nil {
			prev = []CodeWithTxNum{{Code: prevVal, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.PutCode(context.Background(), accounts.BytesToAddress(k), accounts.NilCodeHash, v, pd.tx, txNum, prev...)
	case kv.CommitmentDomain:
		var prev []ValueWithTxNum[commitment.BranchData]
		if prevVal != nil {
			prev = []ValueWithTxNum[commitment.BranchData]{{Value: prevVal, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.PutBranch(context.Background(), commitment.InternPath(k), commitment.BranchData(v), pd.tx, txNum, prev...)
	}
	return pd.sd.mem.DomainPut(domain, k, v, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte, prevStep kv.Step) error {
	switch domain {
	case kv.AccountsDomain:
		var prev []ValueWithTxNum[*accounts.Account]
		if prevVal != nil {
			var a accounts.Account
			if err := accounts.DeserialiseV3(&a, prevVal); err != nil {
				return err
			}
			prev = []ValueWithTxNum[*accounts.Account]{{Value: &a, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.DelAccount(context.Background(), accounts.BytesToAddress(k), pd.tx, txNum, prev...)
	case kv.StorageDomain:
		var prev []ValueWithTxNum[uint256.Int]
		if prevVal != nil {
			var i uint256.Int
			i.SetBytes(prevVal)
			prev = []ValueWithTxNum[uint256.Int]{{Value: i, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.DelStorage(context.Background(),
			accounts.BytesToAddress(k[:length.Addr]), accounts.BytesToKey(k[length.Addr:]), pd.tx, txNum, prev...)
	case kv.CodeDomain:
		var prev []CodeWithTxNum
		if prevVal != nil {
			prev = []CodeWithTxNum{{Code: prevVal, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.DelCode(context.Background(), accounts.BytesToAddress(k), pd.tx, txNum, prev...)
	case kv.CommitmentDomain:
		var prev []ValueWithTxNum[commitment.BranchData]
		if prevVal != nil {
			prev = []ValueWithTxNum[commitment.BranchData]{{Value: prevVal, TxNum: prevStep.ToTxNum(pd.tx.StepSize())}}
		}
		return pd.sd.DelBranch(context.Background(), commitment.InternPath(k), pd.tx, txNum, prev...)
	}
	return pd.sd.mem.DomainDel(domain, k, txNum, prevVal, prevStep)
}

func (pd *temporalPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	if domain == kv.StorageDomain {
		return pd.sd.DelStorage(context.Background(), accounts.BytesToAddress(prefix), accounts.NilKey, pd.tx, txNum)
	}

	return fmt.Errorf("unsupported domain: %s, for del prefix", domain)
}

func (sd *ExecutionContext) AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel {
	return &temporalPutDel{sd, tx}
}

func (sd *ExecutionContext) Merge(other *ExecutionContext) error {
	if sd.txNum > other.txNum {
		return fmt.Errorf("can't merge backwards: txnum: %d > %d", sd.txNum, other.txNum)
	}

	if err := sd.mem.Merge(other.mem); err != nil {
		return err
	}
	sd.accountsDomain.Merge(&other.accountsDomain.domain)
	sd.storageDomain.Merge(&other.storageDomain.domain)
	sd.codeDomain.Merge(&other.codeDomain.domain)
	sd.commitmentDomain.Merge(&other.commitmentDomain.domain)
	sd.txNum = other.txNum
	sd.blockNum.Store(other.blockNum.Load())
	return nil
}

type temporalGetter struct {
	sd *ExecutionContext
	tx kv.TemporalTx
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	switch name {
	case kv.AccountsDomain:
		if len(k) == length.Addr {
			addr := accounts.BytesToAddress(k)
			a, txNum, ok, err := gt.sd.GetAccount(context.Background(), addr, gt.tx)
			if ok {
				return accounts.SerialiseV3(a), kv.Step(txNum / gt.tx.StepSize()), nil
			}
			return nil, 0, err
		}
		return nil, 0, nil
	case kv.StorageDomain:
		if len(k) > length.Addr {
			addr := accounts.BytesToAddress(k[:length.Addr])
			key := accounts.BytesToKey(k[length.Addr:])
			i, txNum, ok, err := gt.sd.GetStorage(context.Background(), addr, key, gt.tx)
			if err != nil {
				return nil, 0, err
			}
			if ok {
				return i.Bytes(), kv.Step(txNum / gt.tx.StepSize()), nil
			}
		}
		return nil, 0, nil
	case kv.CodeDomain:
		if len(k) == length.Addr {
			addr := accounts.BytesToAddress(k)
			_, c, txNum, _, err := gt.sd.GetCode(context.Background(), addr, gt.tx)
			return c, kv.Step(txNum / gt.tx.StepSize()), err
		}
		return nil, 0, nil
	case kv.CommitmentDomain:
		b, txNum, ok, err := gt.sd.GetBranch(context.Background(), commitment.InternPath(k), gt.tx)
		if ok {
			return b, kv.Step(txNum / gt.tx.StepSize()), nil
		}
		return nil, 0, err
	default:
		maxStep := kv.Step(math.MaxUint64)
		start := time.Now()

		if v, step, ok := gt.sd.mem.GetLatest(name, k); ok {
			gt.sd.metrics.UpdateCacheReads(name, Put, start)
			return v, step, nil
		} else {
			if step > 0 {
				maxStep = step
			}
		}
		return gt.sd.getLatest(context.Background(), name, gt.tx, maxStep, k, start)
	}
}

func (gt *temporalGetter) HasPrefix(name kv.Domain, prefix []byte) (ok bool, err error) {
	if name == kv.StorageDomain {
		return gt.sd.HasStorage(context.Background(), accounts.BytesToAddress(prefix), gt.tx)
	}
	return false, nil
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

func (sd *ExecutionContext) SizeEstimate() uint64 {
	// multiply 2: to cover data-structures overhead (and keep accounting cheap)
	// and muliply 2 more: for Commitment calculation when batch is full
	return sd.metrics.PutCacheSize.Load() * 4
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
	return sd.sdCtx.SetTraceDomain(b, capture)
}

func (sd *ExecutionContext) HasStorage(ctx context.Context, addr accounts.Address, roTx kv.Tx) (bool, error) {
	return sd.storageDomain.HasStorage(ctx, addr, roTx)
}

func (sd *ExecutionContext) IterateStorage(ctx context.Context, addr accounts.Address, it func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error), roTx kv.Tx) error {
	return sd.storageDomain.IterateStorage(ctx, addr, it, roTx)
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
	if sd.blockDomain != nil {
		if err := sd.blockDomain.Flush(ctx, tx); err != nil {
			return err
		}
	}
	sd.accountsDomain.FlushUpdates()
	sd.storageDomain.FlushUpdates()
	sd.codeDomain.FlushUpdates()
	sd.commitmentDomain.FlushUpdates()
	return sd.mem.Flush(ctx, tx)
}

func (sd *ExecutionContext) GetAccount(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (v *accounts.Account, txNum uint64, ok bool, err error) {
	return sd.accountsDomain.Get(ctx, k, tx)
}

func (sd *ExecutionContext) PutAccount(ctx context.Context, k accounts.Address, v *accounts.Account, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[*accounts.Account]) error {
	return sd.accountsDomain.Put(ctx, k, v, roTx, txNum, prev...)
}

func (sd *ExecutionContext) DelAccount(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[*accounts.Account]) error {
	return sd.accountsDomain.Del(ctx, k, roTx, txNum, prev...)
}

func (sd *ExecutionContext) GetStorage(ctx context.Context, addr accounts.Address, key accounts.StorageKey, tx kv.TemporalTx) (v uint256.Int, txNum uint64, ok bool, err error) {
	return sd.storageDomain.Get(ctx, addr, key, tx)
}

func (sd *ExecutionContext) PutStorage(ctx context.Context, addr accounts.Address, key accounts.StorageKey, v uint256.Int, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[uint256.Int]) error {
	return sd.storageDomain.Put(ctx, addr, key, v, roTx, txNum, prev...)
}

func (sd *ExecutionContext) DelStorage(ctx context.Context, addr accounts.Address, key accounts.StorageKey, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[uint256.Int]) error {
	return sd.storageDomain.Del(ctx, addr, key, roTx, txNum, prev...)
}

func (sd *ExecutionContext) GetCode(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (h accounts.CodeHash, v []byte, txNum uint64, ok bool, err error) {
	return sd.codeDomain.Get(ctx, k, tx)
}

func (sd *ExecutionContext) PutCode(ctx context.Context, k accounts.Address, h accounts.CodeHash, v []byte, roTx kv.TemporalTx, txNum uint64, prev ...CodeWithTxNum) error {
	return sd.codeDomain.Put(ctx, k, h, v, roTx, txNum, prev...)
}

func (sd *ExecutionContext) DelCode(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...CodeWithTxNum) error {
	return sd.codeDomain.Del(ctx, k, roTx, txNum, prev...)
}

func (sd *ExecutionContext) GetBranch(ctx context.Context, k commitment.Path, tx kv.TemporalTx) (v commitment.BranchData, txNum uint64, ok bool, err error) {
	return sd.commitmentDomain.GetBranch(ctx, k, tx)
}

func (sd *ExecutionContext) PutBranch(ctx context.Context, k commitment.Path, v commitment.BranchData, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[commitment.BranchData]) error {
	return sd.commitmentDomain.PutBranch(ctx, k, v, roTx, txNum, prev...)
}

func (sd *ExecutionContext) DelBranch(ctx context.Context, k commitment.Path, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[commitment.BranchData]) error {
	return sd.commitmentDomain.DelBranch(ctx, k, roTx, txNum, prev...)
}

func (sd *ExecutionContext) getLatest(ctx context.Context, domain kv.Domain, tx kv.TemporalTx, maxStep kv.Step, k []byte, start time.Time) (v []byte, step kv.Step, err error) {
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

func (sd *ExecutionContext) GetAsOf(domain kv.Domain, key []byte, txNum uint64) (v []byte, ok bool, err error) {
	switch domain {
	case kv.AccountsDomain:
		addr := accounts.BytesToAddress(key)
		a, ok, err := sd.accountsDomain.GetAsOf(addr, txNum)
		if !ok || err != nil {
			return nil, ok, err
		}
		return accounts.SerialiseV3(a), true, nil
	case kv.StorageDomain:
		if len(key) > length.Addr {
			addr := accounts.BytesToAddress(key[:length.Addr])
			skey := accounts.BytesToKey(key[length.Addr:])
			val, ok, err := sd.storageDomain.GetAsOf(addr, skey, txNum)
			if !ok || err != nil {
				return nil, ok, err
			}
			return val.Bytes(), true, nil
		}
		return nil, false, nil
	case kv.CodeDomain:
		addr := accounts.BytesToAddress(key)
		_, code, ok, err := sd.codeDomain.GetAsOf(addr, txNum)
		return code, ok, err
	case kv.CommitmentDomain:
		path := commitment.InternPath(key)
		b, ok, err := sd.commitmentDomain.GetBranchAsOf(path, txNum)
		if !ok || err != nil {
			return nil, ok, err
		}
		return b, true, nil
	default:
		return nil, false, nil
	}
}

func (sd *ExecutionContext) MetricsSnapshot() *DomainMetricsSnapshot {
	return sd.metrics.Snapshot()
}

func (sd *ExecutionContext) LogMetrics() []any {
	var metrics []any

	if readCount := sd.metrics.CacheReadCount.Load(); readCount > 0 {
		metrics = append(metrics, "cache", common.PrettyCounter(readCount),
			"puts", common.PrettyCounter(sd.metrics.PutCacheCount.Load()),
			"size", fmt.Sprintf("%s(%s/%s)",
				common.PrettyCounter(sd.metrics.PutCacheSize.Load()),
				common.PrettyCounter(sd.metrics.PutCacheKeySize.Load()),
				common.PrettyCounter(sd.metrics.PutCacheValueSize.Load())),
			"gets", fmt.Sprintf("%s(%s/%s)",
				common.PrettyCounter(sd.metrics.CacheReadCount.Load()),
				common.PrettyCounter(sd.metrics.PutCacheReadCount.Load()),
				common.PrettyCounter(sd.metrics.GetCacheReadCount.Load())),
			"size", fmt.Sprintf("%s(%s/%s)",
				common.PrettyCounter(sd.metrics.GetCacheSize.Load()),
				common.PrettyCounter(sd.metrics.GetCacheKeySize.Load()),
				common.PrettyCounter(sd.metrics.GetCacheValueSize.Load())),
			"cdur", fmt.Sprintf("%s(%s/%s)",
				common.Round(sd.metrics.CacheReadDuration.Load()/time.Duration(readCount), 0),
				common.Round(sd.metrics.PutCacheReadDuration.Load()/time.Duration(sd.metrics.PutCacheReadCount.Load()), 0),
				common.Round(sd.metrics.GetCacheReadDuration.Load()/time.Duration(sd.metrics.GetCacheReadCount.Load()), 0)))
	}

	if readCount := sd.metrics.DbReadCount.Load(); readCount > 0 {
		metrics = append(metrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(sd.metrics.DbReadDuration.Load()/time.Duration(readCount), 0))
	}

	if readCount := sd.metrics.FileReadCount.Load(); readCount > 0 {
		metrics = append(metrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(sd.metrics.FileReadDuration.Load()/time.Duration(readCount), 0))
	}

	return metrics
}

func (sd *ExecutionContext) DomainLogMetrics() map[kv.Domain][]any {
	var logMetrics = map[kv.Domain][]any{}

	sd.metrics.RLock()
	defer sd.metrics.RUnlock()

	for domain, dm := range sd.metrics.Domains {
		var metrics []any

		if readCount := dm.CacheReadCount.Load(); readCount > 0 {
			metrics = append(metrics, "cache", common.PrettyCounter(readCount), "cdur", common.Round(dm.CacheReadDuration.Load()/time.Duration(readCount), 0))
		}

		if readCount := dm.DbReadCount.Load(); readCount > 0 {
			metrics = append(metrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(dm.DbReadDuration.Load()/time.Duration(readCount), 0))
		}

		if readCount := dm.FileReadCount.Load(); readCount > 0 {
			metrics = append(metrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(dm.DbReadDuration.Load()/time.Duration(readCount), 0))
		}

		if len(metrics) > 0 {
			logMetrics[domain] = metrics
		}
	}

	return logMetrics
}

// DiscardWrites disables updates collection for further flushing into db.
// Instead, it keeps them temporarily available until .Close will make them unavailable.
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

// ComputeCommitment evaluates commitment for gathered updates.
// If trieWarmup toggle was enabled via EnableTrieWarmup, pre-warms MDBX page cache by reading Branch data in parallel before processing.
func (sd *ExecutionContext) ComputeCommitment(ctx context.Context, tx kv.TemporalTx, saveStateAfter bool, blockNum, txNum uint64, logPrefix string, commitProgress chan *commitment.CommitProgress) (rootHash []byte, err error) {
	return sd.sdCtx.ComputeCommitment(ctx, sd, tx, saveStateAfter, blockNum, txNum, logPrefix, commitProgress)
}

// EnableTrieWarmup enables parallel warmup of MDBX page cache during commitment.
// It requires a DB to be enabled via EnableParaTrieDB.
func (sd *ExecutionContext) EnableTrieWarmup(trieWarmup bool) {
	sd.sdCtx.EnableTrieWarmup(trieWarmup)
}

func (sd *ExecutionContext) EnableParaTrieDB(db kv.TemporalRoDB) {
	sd.sdCtx.EnableParaTrieDB(db)
}

func (sd *ExecutionContext) EnableWarmupCache(enable bool) {
	sd.sdCtx.EnableWarmupCache(enable)
}
