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

package executiontests

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// TestGeneratedChainClearsTouchedEmptyAccount pins agreement between blockgen
// and block import when one transaction leaves an account empty and a later
// EIP-7702 authorization reads its existence in the same block. The generated
// block embeds blockgen's gas and access list, so InsertChain fails on any
// divergence from real execution.
func TestGeneratedChainClearsTouchedEmptyAccount(t *testing.T) {
	for _, tc := range []struct {
		name        string
		preexisting bool
		parallel    bool
	}{
		{name: "fresh_serial"},
		{name: "fresh_parallel", parallel: true},
		{name: "preexisting_serial", preexisting: true},
		{name: "preexisting_parallel", preexisting: true, parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := dbg.Exec3Parallel
			dbg.Exec3Parallel = tc.parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prev })
			runGeneratedChainClearsTouchedEmptyAccount(t, tc.preexisting)
		})
	}
}

func runGeneratedChainClearsTouchedEmptyAccount(t *testing.T, preexisting bool) {
	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderKey.PublicKey)
	emptyKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	emptyAddr := crypto.PubkeyToAddress(emptyKey.PublicKey)

	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 1_000_000_000,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	if preexisting {
		genesis.Alloc[emptyAddr] = types.GenesisAccount{Balance: new(big.Int)}
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(senderKey))

	chainID := m.ChainConfig.ChainID
	signer := types.LatestSignerForChainID(chainID)
	feeCap := uint256.NewInt(1_000_000_000)

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		// A zero-value transfer touches the fresh account, leaving it empty.
		touch, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 0, GasLimit: 100_000, To: &emptyAddr},
			ChainID:  *chainID, TipCap: *feeCap, FeeCap: *feeCap,
		}, *signer, senderKey)
		require.NoError(t, err)
		b.AddTx(touch)

		// The authorization's gas depends on whether the account exists.
		authorization, err := types.SignAuthorization(emptyKey, *chainID, common.Address{0xaa}, 0)
		require.NoError(t, err)
		authorize, err := types.SignTx(&types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: 1, GasLimit: params.MaxTxnGasLimit, To: &senderAddr},
				ChainID:  *chainID, TipCap: *feeCap, FeeCap: *feeCap,
			},
			Authorizations: []types.Authorization{
				authorization,
			},
		}, *signer, senderKey)
		require.NoError(t, err)
		b.AddTx(authorize)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
}
