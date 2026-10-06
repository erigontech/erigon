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

package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

func TestGetContractCreator(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	expectCreator := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	expectCredByTx := common.HexToHash("0x6e25f89e24254ba3eb460291393a4715fd3c33d805334cbd05c1b2efe1080f18")
	t.Run("valid inputs", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, addr)
		require.NoError(err)
		require.Equal(expectCreator, results.Creator)
		require.Equal(expectCredByTx, results.Tx)
	})
	for _, tc := range []struct {
		name            string
		history, blocks prune.BlockAmount
	}{
		{"pruned history", prune.Distance(1), prune.ArchiveMode.Blocks},
		{"pruned transactions", prune.ArchiveMode.History, prune.Distance(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newBaseApiForTest(m)
			base._pruneMode.Store(&prune.Mode{Initialised: true, History: tc.history, Blocks: tc.blocks})
			api := NewOtterscanAPI(base, m.DB, 25)

			result, err := api.GetContractCreator(m.Ctx, addr)
			require.ErrorIs(t, err, state.ErrPruned)
			require.Nil(t, result)
		})
	}
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, common.HexToAddress("0x1234"))
		require.NoError(err)
		require.Nil(results)
	})
	t.Run("pass creator as addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, expectCreator)
		require.NoError(err)
		require.Nil(results)
	})
}

func TestGetContractCreatorDelegatedEOA(t *testing.T) {
	senderKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	authorityKey, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(senderKey.PublicKey)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	delegate := common.HexToAddress("0x000000000000000000000000000000000000cafe")

	pragueConfig := chain.TestChainOsakaConfig.Copy()
	pragueConfig.OsakaTime = nil
	auth, err := types.SignAuthorization(authorityKey, *pragueConfig.ChainID, delegate, 0)
	require.NoError(t, err)
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: pragueConfig,
			Alloc:  types.GenesisAlloc{sender: {Balance: big.NewInt(1_000_000_000_000_000_000)}},
		}),
		execmoduletester.WithKey(senderKey),
	)
	signer := types.LatestSignerForChainID(pragueConfig.ChainID)
	c, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		to := common.HexToAddress("0x000000000000000000000000000000000000beef")
		txn, err := types.SignTx(&types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: b.TxNonce(sender), GasLimit: 500_000, To: &to},
				ChainID:  *pragueConfig.ChainID,
				TipCap:   *uint256.NewInt(1_000_000_000),
				FeeCap:   *uint256.NewInt(10_000_000_000),
			},
			Authorizations: []types.Authorization{auth},
		}, *signer, senderKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)
	hasCode, err := api.HasCode(m.Ctx, authority, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	require.NoError(t, err)
	require.True(t, hasCode, "authorization must have been applied")

	creator, err := api.GetContractCreator(m.Ctx, authority)
	require.NoError(t, err)
	require.Nil(t, creator)
}

func TestGetContractCreatorDelegationLikeContract(t *testing.T) {
	senderKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(senderKey.PublicKey)

	code := types.AddressToDelegation(accounts.InternAddress(common.HexToAddress("0x000000000000000000000000000000000000cafe")))
	initCode := append(append([]byte{0x76}, code...), 0x60, 0x00, 0x52, 0x60, 0x17, 0x60, 0x09, 0xf3)

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{sender: {Balance: big.NewInt(1_000_000_000_000_000_000)}},
		}),
		execmoduletester.WithKey(senderKey),
	)
	signer := types.LatestSignerForChainID(chain.TestChainBerlinConfig.ChainID)
	var deployTx common.Hash
	c, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewContractCreation(b.TxNonce(sender), uint256.NewInt(0), 500_000, uint256.NewInt(1_000_000_000), initCode), *signer, senderKey)
		require.NoError(t, err)
		deployTx = txn.Hash()
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	contract := types.CreateAddress(sender, 0)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)
	hasCode, err := api.HasCode(m.Ctx, contract, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	require.NoError(t, err)
	require.True(t, hasCode)

	creator, err := api.GetContractCreator(m.Ctx, contract)
	require.NoError(t, err)
	require.NotNil(t, creator)
	require.Equal(t, deployTx, creator.Tx)
	require.Equal(t, sender, creator.Creator)
}
