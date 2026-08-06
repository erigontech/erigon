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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type assemblerStateReader struct {
	*state.NoopReader
	address accounts.Address
	account accounts.Account
}

func (r *assemblerStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	if address != r.address {
		return nil, nil
	}
	account := r.account
	return &account, nil
}

func TestBlockAssemblerFlushesVersionedWritesWithoutBAL(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))

	account := accounts.NewAccount()
	account.Balance = *uint256.NewInt(1_000_000)
	reader := &assemblerStateReader{
		NoopReader: state.NewNoopReader(),
		address:    sender,
		account:    account,
	}
	versionMap := state.NewVersionMap(nil)
	ibs := state.NewWithVersionMap(reader, versionMap)
	t.Cleanup(func() { ibs.Release(false) })
	ibs.SetNoMaterialize(true)

	header := &types.Header{
		Number:     *uint256.NewInt(1),
		Time:       1,
		GasLimit:   1_000_000,
		Difficulty: *uint256.NewInt(1),
	}
	engine := ethash.NewFaker()
	assembler := NewBlockAssembler(AssemblerCfg{
		ChainConfig: chain.TestChainBerlinConfig,
		Engine:      engine,
	}, &AssembledBlock{Header: header})
	require.False(t, assembler.HasBAL())

	to := common.Address{0xaa}
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{
			GasLimit: 21_000,
			To:       &to,
			Value:    *uint256.NewInt(1),
		},
		GasPrice: *uint256.NewInt(1),
	}, *types.MakeSigner(chain.TestChainBerlinConfig, header.Number.Uint64(), header.Time), key)
	require.NoError(t, err)

	_, _, err = assembler.AddTransactions(
		t.Context(), nil, types.Transactions{txn}, accounts.InternAddress(common.Address{0xcc}),
		&vm.Config{}, ibs, nil, "test", log.New(),
	)
	require.NoError(t, err)

	nonce, _, ok := versionMap.ReadNonce(sender, 1)
	require.True(t, ok)
	require.Equal(t, uint64(1), nonce)
}
