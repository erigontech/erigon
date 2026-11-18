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
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var (
	preExecArm        = dbg.EnvString("V3_PREEXEC", "")
	preExecCommitment = dbg.EnvBool("V3_PREEXEC_COMMITMENT", false)
)

type preExecCfg struct {
	config    *chain.Config
	engine    rules.EngineReader
	canonical dbservices.CanonicalReader
}

func (bra *BlockReadAheader) SetPreExec(config *chain.Config, engine rules.EngineReader, canonical dbservices.CanonicalReader) {
	bra.preExec = &preExecCfg{config: config, engine: engine, canonical: canonical}
}

func (bra *BlockReadAheader) preExecOn(blockNum uint64) bool {
	if bra.preExec == nil {
		return false
	}
	switch preExecArm {
	case "all":
		return true
	case "odd":
		return blockNum%2 == 1
	case "even":
		return blockNum%2 == 0
	}
	return false
}

func (bra *BlockReadAheader) preExecTxns(ctx context.Context, db kv.RoDB, header *types.Header, txns types.Transactions, workers int) error {
	pe := bra.preExec
	start := time.Now()
	signer := types.MakeSigner(pe.config, header.Number.Uint64(), header.Time)
	var next, paths atomic.Int64
	wg, ctx := errgroup.WithContext(ctx)
	for w := range min(workers, secp256k1.NumOfContexts(), len(txns)) {
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
			var toucher *commitmentToucher
			if preExecCommitment {
				toucher = &commitmentToucher{read: func(key []byte) []byte {
					v, _, err := ttx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
					if err != nil {
						return nil
					}
					return v
				}}
				defer func() { paths.Add(int64(toucher.paths)) }()
			}
			for i := int(next.Add(1)) - 1; i < len(txns); i = int(next.Add(1)) - 1 {
				if err := ctx.Err(); err != nil {
					return err
				}
				from, err := signer.SenderWithContext(cryptoContext, txns[i])
				if err != nil {
					continue
				}
				pe.run(blockCtx, chainRules, from, txns[i], reader, toucher)
			}
			return nil
		})
	}
	err := wg.Wait()
	log.Info("[preexec] block", "num", header.Number.Uint64(), "txs", len(txns), "ms", time.Since(start).Milliseconds(), "paths", paths.Load(), "err", err)
	return err
}

func (pe *preExecCfg) run(blockCtx evmtypes.BlockContext, chainRules *chain.Rules, from accounts.Address, txn types.Transaction, reader state.StateReader, toucher *commitmentToucher) {
	to := accounts.NilAddress
	if t := txn.GetTo(); t != nil {
		to = accounts.InternAddress(*t)
	}
	msg := types.NewMessage(from, to, txn.GetNonce(), txn.GetValue(), txn.GetGasLimit(), txn.GetFeeCap(), txn.GetFeeCap(),
		txn.GetTipCap(), txn.GetData(), txn.GetAccessList(), false, false, true, false, nil)
	if h := txn.GetBlobHashes(); len(h) > 0 {
		msg.SetBlobVersionedHashes(h)
	}
	if a := txn.GetAuthorizations(); len(a) > 0 {
		msg.SetAuthorizations(a)
	}
	ibs := state.NewPooled(reader)
	defer state.ReleasePooled(ibs)
	txCtx := protocol.NewEVMTxContext(msg)
	evm := vm.NewEVM(blockCtx, txCtx, ibs, pe.config, vm.Config{NoBaseFee: true, NoReceipts: true, NoBAL: true})
	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	if _, err := protocol.ApplyMessage(evm, msg, gp, true, true, pe.engine); err == nil && toucher != nil {
		_ = ibs.FinalizeTx(chainRules, toucher)
	}
}

type commitmentToucher struct {
	state.NoopWriter
	read  func(key []byte) []byte
	paths int
}

func (t *commitmentToucher) touch(addrHash, slotHash []byte) {
	v3.PrefetchPath(t.read, addrHash, slotHash)
	t.paths++
}

func (t *commitmentToucher) touchAccount(a accounts.Address) {
	addr := a.Value()
	h := keccak.Sum256(addr[:])
	t.touch(h[:], nil)
}

func (t *commitmentToucher) UpdateAccountData(a accounts.Address, _, _ *accounts.Account) error {
	t.touchAccount(a)
	return nil
}

func (t *commitmentToucher) DeleteAccount(a accounts.Address, _ *accounts.Account) error {
	t.touchAccount(a)
	return nil
}

func (t *commitmentToucher) WriteAccountStorage(a accounts.Address, _ uint64, k accounts.StorageKey, _, _ uint256.Int) error {
	addr, slot := a.Value(), k.Value()
	ah, sh := keccak.Sum256(addr[:]), keccak.Sum256(slot[:])
	t.touch(ah[:], sh[:])
	return nil
}
