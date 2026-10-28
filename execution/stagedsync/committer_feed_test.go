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
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	_ "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCalculatorFeedRootMatchesHexPatricia(t *testing.T) {
	defer func(v bool) { statecfg.ExperimentalCommitmentV3 = v }(statecfg.ExperimentalCommitmentV3)
	roots := make(map[bool][][]byte)
	for _, v3 := range []bool{false, true} {
		statecfg.ExperimentalCommitmentV3 = v3
		roots[v3] = calculatorRoots(t, v3)
	}
	require.Len(t, roots[false], 2)
	require.Equal(t, roots[false], roots[true])
}

func calculatorRoots(t *testing.T, wantFeed bool) [][]byte {
	ctx := context.Background()
	db, tx, doms := setupStepTest(t)
	require.Equal(t, wantFeed, doms.GetCommitmentContext().AcceptsFeed())
	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", log.New(), false, 1<<62, in, nil, out)
	require.NoError(t, err)
	defer cc.Stop()

	rnd := rand.New(rand.NewSource(7))
	addrs := make([]accounts.Address, 24)
	for i := range addrs {
		var a common.Address
		rnd.Read(a[:])
		addrs[i] = accounts.InternAddress(a)
	}
	slots := make([]accounts.StorageKey, 12)
	for i := range slots {
		var k common.Hash
		rnd.Read(k[:])
		slots[i] = accounts.InternKey(k)
	}
	balances := make(map[accounts.Address]uint64)
	put := func(txNum uint64, ws *wsb, addr accounts.Address, nonce, balance uint64) {
		acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(balance), CodeHash: accounts.EmptyCodeHash}
		av := addr.Value()
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, av[:], accounts.SerialiseV3(&acc), txNum, nil))
		ws.nonce(addr, state.Version{}, nonce).bal(addr, state.Version{}, *uint256.NewInt(balance)).codeHash(addr, state.Version{}, accounts.EmptyCodeHash)
		balances[addr] = balance
	}
	store := func(txNum uint64, ws *wsb, addr accounts.Address, slot accounts.StorageKey, value uint64) {
		av, sv := addr.Value(), slot.Value()
		v := uint256.NewInt(value)
		require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, append(av[:], sv[:]...), v.Bytes(), txNum, nil))
		ws.stor(addr, slot, state.Version{}, *v)
	}
	var roots [][]byte
	txNum := uint64(0)
	for block := uint64(1); block <= 3; block++ {
		for range 9 {
			txNum++
			ws := newWS()
			for range 3 {
				addr := addrs[rnd.Intn(len(addrs))]
				put(txNum, ws, addr, txNum, uint64(rnd.Intn(1000)+1))
				for range 2 {
					store(txNum, ws, addr, slots[rnd.Intn(len(slots))], uint64(rnd.Intn(3)))
				}
			}
			cc.handleMessage(ctx, &txResult{blockNum: block, txNum: txNum, rules: &chain.Rules{}, writes: ws.build()})
		}
		cc.handleMessage(ctx, newTestBlockResult(block, common.Hash{byte(block)}, txNum, false))
		if block == 2 || block == 3 {
			cc.handleMessage(ctx, &commitComputeRequest{})
			res := <-out
			require.NotEmpty(t, res.rootHash)
			roots = append(roots, res.rootHash)
		}
	}
	return roots
}
