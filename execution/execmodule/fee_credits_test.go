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

package execmodule_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

func TestFeeCreditsSharedDestination(t *testing.T) {
	for _, fork := range []string{"pre_amsterdam", "amsterdam"} {
		t.Run(fork, func(t *testing.T) { testFeeCreditBlocks(t, fork, "shared") })
	}
}

func TestFeeCreditPrefixConsumers(t *testing.T) {
	for _, fork := range []string{"pre_amsterdam", "amsterdam"} {
		for _, consumer := range []string{"balance_reader", "sender", "new_account"} {
			t.Run(fork+"/"+consumer, func(t *testing.T) { testFeeCreditBlocks(t, fork, consumer) })
		}
	}
}

func testFeeCreditBlocks(t *testing.T, fork, consumer string) {
	t.Helper()

	config := &chain.Config{}
	require.NoError(t, copier.CopyWithOption(config, chain.AllProtocolChanges, copier.Option{DeepCopy: true}))
	if fork == "pre_amsterdam" {
		config.AmsterdamTime = nil
	}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	recipient := common.Address{0x22}
	feeRecipient := common.Address{0x33}
	if consumer == "sender" {
		feeRecipient = sender
	}
	config.BurntContract = map[string]common.Address{"0": feeRecipient}
	genesis := &types.Genesis{
		Config: config,
		Alloc: types.GenesisAlloc{
			sender:    {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			recipient: {Balance: big.NewInt(1)},
		},
	}
	if consumer != "sender" && consumer != "new_account" {
		genesis.Alloc[feeRecipient] = types.GenesisAccount{Balance: big.NewInt(1)}
	}
	if consumer == "balance_reader" {
		genesis.Alloc[recipient] = types.GenesisAccount{Balance: big.NewInt(1), Code: []byte{byte(vm.COINBASE), byte(vm.BALANCE), byte(vm.PUSH0), byte(vm.SSTORE)}}
	}

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	signer := types.LatestSignerForChainID(config.ChainID)
	blocks, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(feeRecipient)
		gasPrice := new(uint256.Int).Add(b.GetHeader().BaseFee, uint256.NewInt(1_000))
		for j := range 4 {
			txn, err := types.SignTx(types.NewTransaction(uint64(4*i+j), recipient, uint256.NewInt(17), 100_000, gasPrice, nil), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), blocks.Blocks))
}
