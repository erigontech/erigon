// Copyright 2026 The Erigon Authors
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

package exec

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/erigontech/secp256k1"
	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var preExecOn = dbg.EnvBool("V3_PREEXEC", false)

const (
	preExecWorkers  = 4
	preExecBatch    = 8
	preExecTxGas    = 1 << 24
	preExecBlockGas = 1 << 28
)

type preExecCfg struct {
	config    *chain.Config
	engine    rules.EngineReader
	canonical dbservices.CanonicalReader
}

func (bra *BlockReadAheader) SetPreExec(config *chain.Config, engine rules.EngineReader, canonical dbservices.CanonicalReader) {
	bra.preExec = &preExecCfg{config: config, engine: engine, canonical: canonical}
}

func (bra *BlockReadAheader) preExecTxns(ctx context.Context, db kv.RoDB, header *types.Header, txns types.Transactions, workers int, recovery *SendersRecovery, prefetch *BranchPrefetcher) error {
	pe := bra.preExec
	if header.ExcessBlobGas != nil {
		if _, err := misc.GetBlobGasPrice(pe.config, *header.ExcessBlobGas, header.Time); err != nil {
			return err
		}
	}
	ctx, untrack := trackPreExec(ctx, header.Hash())
	defer untrack()
	var senders []accounts.Address
	if recovery != nil {
		var err error
		if senders, err = recovery.Wait(ctx); err != nil {
			return ctx.Err()
		}
	}
	start := time.Now()
	signer := types.MakeSigner(pe.config, header.Number.Uint64(), header.Time)
	var next, paths, gasLeft atomic.Int64
	gasLeft.Store(preExecBlockGas)
	wg, ctx := errgroup.WithContext(ctx)
	for w := range min(workers, preExecWorkers, secp256k1.NumOfContexts(), len(txns)) {
		cryptoContext := secp256k1.ContextForThread(w)
		wg.Go(func() error {
			tx, err := db.BeginRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			ttx, ok := tx.(kv.TemporalTx)
			if !ok {
				return errors.New("transaction pre-exec requires a temporal read transaction")
			}
			reader := state.NewReaderV3(readAheadGetter(ttx, bra.stateCache, true))
			blockHash := func(n uint64) (common.Hash, error) {
				h, _, err := pe.canonical.CanonicalHash(ctx, ttx, n)
				return h, err
			}
			blockCtx := protocol.NewEVMBlockContext(header, blockHash, pe.engine, accounts.NilAddress, pe.config)
			chainRules := blockCtx.Rules(pe.config)
			toucher := &commitmentToucher{ctx: ctx, out: prefetch, seen: map[PathTouch]struct{}{}}
			defer func() {
				toucher.flush()
				paths.Add(int64(toucher.paths))
			}()
			for i := int(next.Add(1)) - 1; i < len(txns); i = int(next.Add(1)) - 1 {
				if err := ctx.Err(); err != nil {
					return err
				}
				var from accounts.Address
				if len(senders) == len(txns) {
					from = senders[i]
				} else if from, err = signer.SenderWithContext(cryptoContext, txns[i]); err != nil {
					continue
				}
				gas := min(txns[i].GetGasLimit(), preExecTxGas)
				if gasLeft.Add(-int64(gas)) < 0 {
					return nil
				}
				pe.run(ctx, blockCtx, chainRules, from, txns[i], gas, reader, toucher)
			}
			return nil
		})
	}
	err := wg.Wait()
	log.Debug("[preexec] block", "num", header.Number.Uint64(), "txs", len(txns), "ms", time.Since(start).Milliseconds(), "paths", paths.Load(), "err", err)
	return err
}

func (pe *preExecCfg) run(ctx context.Context, blockCtx evmtypes.BlockContext, chainRules *chain.Rules, from accounts.Address, txn types.Transaction, gas uint64, reader state.StateReader, toucher *commitmentToucher) {
	if txn.Type() == types.FrameTxType {
		return
	}
	to := accounts.NilAddress
	if t := txn.GetTo(); t != nil {
		to = accounts.InternAddress(*t)
	}
	gasPrice := txn.GetEffectiveGasTip(&blockCtx.BaseFee)
	gasPrice.Add(&gasPrice, &blockCtx.BaseFee)
	if gasPrice.Gt(txn.GetFeeCap()) {
		gasPrice.Set(txn.GetFeeCap())
	}
	msg := types.NewMessage(from, to, txn.GetNonce(), txn.GetValue(), gas, &gasPrice, txn.GetFeeCap(),
		txn.GetTipCap(), txn.GetData(), txn.GetAccessList(), false, false, true, false, nil)
	if h := txn.GetBlobHashes(); len(h) > 0 {
		msg.SetBlobVersionedHashes(h)
	}
	if a := txn.GetAuthorizations(); len(a) > 0 {
		msg.SetAuthorizations(a)
	}
	ibs := state.NewPooled(reader)
	defer state.ReleasePooled(ibs)
	if err := ibs.SetNonce(from, txn.GetNonce(), tracing.NonceChangeUnspecified); err != nil {
		return
	}
	txCtx := protocol.NewEVMTxContext(msg)
	evm := vm.NewEVM(blockCtx, txCtx, ibs, pe.config, vm.Config{NoBaseFee: true, NoReceipts: true, NoBAL: true})
	defer context.AfterFunc(ctx, evm.Cancel)()
	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	if _, err := protocol.ApplyMessage(evm, msg, gp, true, true, pe.engine); err == nil && ctx.Err() == nil {
		_ = ibs.FinalizeTx(chainRules, toucher)
	}
}

type commitmentToucher struct {
	state.NoopWriter
	ctx   context.Context
	paths int
	out   *BranchPrefetcher
	seen  map[PathTouch]struct{}
	batch []PathTouch
}

type preExecRun struct {
	block  common.Hash
	cancel context.CancelFunc
}

var (
	activePreExec  atomic.Pointer[preExecRun]
	stoppedPreExec atomic.Pointer[common.Hash]
)

func trackPreExec(ctx context.Context, block common.Hash) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	run := &preExecRun{block: block, cancel: cancel}
	activePreExec.Store(run)
	if stopped := stoppedPreExec.Load(); stopped != nil && *stopped == block {
		cancel()
	}
	return ctx, func() {
		activePreExec.CompareAndSwap(run, nil)
		cancel()
	}
}

func StopPreExec(block common.Hash) {
	stoppedPreExec.Store(&block)
	if run := activePreExec.Load(); run != nil && run.block == block {
		run.cancel()
	}
}

func (t *commitmentToucher) flush() {
	if len(t.batch) == 0 {
		return
	}
	slices.SortFunc(t.batch, ComparePathTouch)
	t.out.Add(t.batch...)
	t.batch = t.batch[:0]
}

func (t *commitmentToucher) touch(addrHash, slotHash []byte) {
	if t.out == nil || t.ctx.Err() != nil {
		return
	}
	it := PathTouch{Account: [32]byte(addrHash)}
	if slotHash != nil {
		it.Slot, it.Storage = [32]byte(slotHash), true
	}
	if _, dup := t.seen[it]; dup {
		return
	}
	t.seen[it] = struct{}{}
	t.paths++
	if t.batch = append(t.batch, it); len(t.batch) == preExecBatch {
		t.flush()
	}
}

func (t *commitmentToucher) UpdateAccountData(a accounts.Address, _, _ *accounts.Account) error {
	addr := a.Value()
	h := keccak.Sum256(addr[:])
	t.touch(h[:], nil)
	return nil
}

func (t *commitmentToucher) DeleteAccount(a accounts.Address, _ *accounts.Account) error {
	return t.UpdateAccountData(a, nil, nil)
}

func (t *commitmentToucher) WriteAccountStorage(a accounts.Address, _ uint64, k accounts.StorageKey, _, _ uint256.Int) error {
	addr, slot := a.Value(), k.Value()
	ah, sh := keccak.Sum256(addr[:]), keccak.Sum256(slot[:])
	t.touch(ah[:], sh[:])
	return nil
}
