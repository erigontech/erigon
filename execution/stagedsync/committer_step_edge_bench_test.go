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

package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkComputeAheadStepEdge times BAL compute-ahead of a test_selfdestruct_existing-like
// block (18 txs, 34k existing contracts whose balance drops 1 -> 0) at step size 3125,
// with and without an interior step edge.
func BenchmarkComputeAheadStepEdge(b *testing.B) {
	const (
		stepSize  = 3125
		contracts = 34_000
		txs       = 18
		seedTxNum = 1
	)
	for _, bc := range []struct {
		name       string
		firstTxNum uint64
	}{
		{"no_edge", 2*stepSize + 5},
		{"one_edge", 2*stepSize - 10},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.Cleanup(statecfg.SetCommitmentV3(true))
			setAheadFlags(b, true)
			code := []byte{0x33, 0xff}
			codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
			addr := func(i int) [20]byte {
				var a [20]byte
				binary.BigEndian.PutUint64(a[:8], uint64(i)*0x9e3779b97f4a7c15)
				binary.BigEndian.PutUint64(a[12:], uint64(i)+1)
				return a
			}
			attack, sender := addr(contracts+1), addr(contracts+2)
			lastTxNum := bc.firstTxNum + txs + 1
			var bl types.BlockAccessList
			perTx := contracts / txs
			for i := range contracts {
				idx := uint32(min(i/perTx, txs-1)) + 1
				bl = append(bl, types.AccountChanges{Address: addr(i), BalanceChanges: []*types.BalanceChange{{Index: idx, Value: *uint256.NewInt(0)}}})
			}
			var attackBal, senderNonce types.AccountChanges
			attackBal.Address, senderNonce.Address = attack, sender
			for idx := uint32(1); idx <= txs; idx++ {
				attackBal.BalanceChanges = append(attackBal.BalanceChanges, &types.BalanceChange{Index: idx, Value: *uint256.NewInt(uint64(idx) * uint64(perTx))})
				senderNonce.NonceChanges = append(senderNonce.NonceChanges, &types.NonceChange{Index: idx, Value: uint64(idx)})
			}
			bl = append(bl, attackBal, senderNonce)

			for range b.N {
				b.StopTimer()
				ctx := context.Background()
				logger := log.New()
				dirs := datadir.New(b.TempDir())
				rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(dirs.Chaindata).MustOpen()
				agg, err := dbstate.NewTest(dirs).StepSize(stepSize).Logger(logger).Open(ctx)
				if err != nil {
					b.Fatal(err)
				}
				db, err := temporal.New(rawDb, agg, nil)
				if err != nil {
					b.Fatal(err)
				}
				tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
				if err != nil {
					b.Fatal(err)
				}
				doms, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithParaTrieDB(db))
				if err != nil {
					b.Fatal(err)
				}
				put := func(a [20]byte, acc accounts.Account) {
					if err := doms.DomainPut(kv.AccountsDomain, tx, a[:], accounts.SerialiseV3(&acc), seedTxNum, nil); err != nil {
						b.Fatal(err)
					}
				}
				for i := range contracts {
					put(addr(i), accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: codeHash})
				}
				put(attack, accounts.Account{Nonce: 1, CodeHash: codeHash})
				put(sender, accounts.Account{Balance: *uint256.NewInt(1 << 60), CodeHash: accounts.EmptyCodeHash})
				if _, err := doms.ComputeCommitment(ctx, tx, true, 1, seedTxNum, "bench", nil); err != nil {
					b.Fatal(err)
				}
				out := make(chan commitmentResult, 64)
				cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "bench", logger, false, 1<<62, make(chan applyResult, 64), nil, out, state.NewLayeredDomainReader(doms, nil, nil), nil)
				if err != nil {
					b.Fatal(err)
				}
				cc.hasFirstBlock, cc.firstBlockNum = true, 2
				req := &blockRequest{blockNum: 2, firstTxNum: bc.firstTxNum, lastTxNum: lastTxNum, stateRoot: common.Hash{0xde, 0xad}, bal: bl}
				b.StartTimer()
				cc.handleBlockRequest(ctx, req)
				b.StopTimer()
				if res := <-out; !errors.Is(res.err, ErrWrongTrieRoot) {
					b.Fatalf("compute-ahead did not reach the root check: %v", res.err)
				}
				cc.Stop()
				doms.Close()
				tx.Rollback()
				agg.Close()
				rawDb.Close()
			}
		})
	}
}
