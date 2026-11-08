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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Before Spurious Dragon, paying the coinbase a zero fee creates it as an
// empty account, so a later CALL to the coinbase in the same block does not
// pay the new-account gas. Parallel import must accept the block that serial
// execution built.
func TestZeroFeeCreatesCoinbaseBeforeSpuriousDragon(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })

	config := &chain.Config{ChainID: uint256.NewInt(1), HomesteadBlock: common.NewUint64(0), TangerineWhistleBlock: common.NewUint64(0)}
	signer := types.MakeFrontierSigner()
	coinbase := common.HexToAddress("0xc0ffee")
	caller := common.HexToAddress("0xca11")
	alloc := types.GenesisAlloc{caller: {Code: callCode(coinbase, 0)}}
	keys := revivalKeys(t, alloc, 2)
	senders := []common.Address{crypto.PubkeyToAddress(keys[0].PublicKey), crypto.PubkeyToAddress(keys[1].PublicKey)}
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}

	dbg.Exec3Parallel = false
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(coinbase)
		zeroFee, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[0]), senders[1], uint256.NewInt(0), 21_000, uint256.NewInt(0), nil), *signer, keys[0])
		require.NoError(t, err)
		b.AddTx(zeroFee)
		call, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[1]), caller, uint256.NewInt(0), 100_000, uint256.NewInt(1), nil), *signer, keys[1])
		require.NoError(t, err)
		b.AddTx(call)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
		exist, err := st.Exist(accounts.InternAddress(coinbase))
		require.NoError(t, err)
		require.True(t, exist)
	})
}
