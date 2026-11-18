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

	"github.com/erigontech/secp256k1"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var preExecArm = dbg.EnvString("V3_PREEXEC", "")

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
	var next atomic.Int64
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
			for i := int(next.Add(1)) - 1; i < len(txns); i = int(next.Add(1)) - 1 {
				if err := ctx.Err(); err != nil {
					return err
				}
				from, err := signer.SenderWithContext(cryptoContext, txns[i])
				if err != nil {
					continue
				}
				pe.run(blockCtx, chainRules, from, txns[i], reader)
			}
			return nil
		})
	}
	err := wg.Wait()
	log.Info("[preexec] block", "num", header.Number.Uint64(), "txs", len(txns), "ms", time.Since(start).Milliseconds(), "err", err)
	return err
}

func (pe *preExecCfg) run(blockCtx evmtypes.BlockContext, chainRules *chain.Rules, from accounts.Address, txn types.Transaction, reader state.StateReader) {
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
	_, _ = protocol.ApplyMessage(evm, msg, gp, true, true, pe.engine)
}
