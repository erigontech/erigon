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

package txpool_test

import (
	"bytes"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

// A withdrawal-only credit must not reset the sender nonce the txpool sees.
func TestWithdrawalCreditKeepsPoolSenderNonce(t *testing.T) {
	prev := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prev })
	dbg.Exec3Parallel = true

	keyR, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	keyP, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	r, p := crypto.PubkeyToAddress(keyR.PublicKey), crypto.PubkeyToAddress(keyP.PublicKey)

	config := chain.TestChainOsakaConfig.Copy()
	config.CancunTime, config.PragueTime, config.OsakaTime = nil, nil, nil
	signer := types.LatestSignerForChainID(config.ChainID)
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		r: {Balance: big.NewInt(1_000_000_000_000_000_000)},
		p: {Balance: big.NewInt(1_000_000_000_000_000_000), Nonce: 311},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keyR), execmoduletester.WithTxPool())

	newTxn := func(k *ecdsa.PrivateKey, nonce uint64) types.Transaction {
		txn, err := types.SignTx(types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(10_000_000_000), nil), *signer, k)
		require.NoError(t, err)
		return txn
	}
	rlp := func(k *ecdsa.PrivateKey, nonce uint64) []byte {
		var buf bytes.Buffer
		require.NoError(t, newTxn(k, nonce).MarshalBinary(&buf))
		return buf.Bytes()
	}

	chainPack, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		if i == 0 {
			for n := range uint64(4) {
				b.AddTx(newTxn(keyR, n))
			}
			return
		}
		b.AddWithdrawal(&types.Withdrawal{Index: 0, Validator: 1, Address: r, Amount: 7})
		b.AddWithdrawal(&types.Withdrawal{Index: 1, Validator: 2, Address: p, Amount: 7})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	require.NoError(t, m.WaitForTxPool(t.Context(), chainPack.TopBlock.Hash()))

	add := func(k *ecdsa.PrivateKey, nonce uint64) {
		reply, err := m.TxPoolGrpcServer.Add(t.Context(), &txpoolproto.AddRequest{RlpTxs: [][]byte{rlp(k, nonce)}})
		require.NoError(t, err)
		require.Equal(t, txpoolproto.ImportResult_SUCCESS, reply.Imported[0], reply.Errors[0])
	}
	add(keyR, 4)
	pending, _, queued := m.TxPool.CountContent()
	require.Equal(t, [2]int{1, 0}, [2]int{pending, queued}, "R nonce 4 must be pending, not queued")
	add(keyP, 311)
	pending, _, queued = m.TxPool.CountContent()
	require.Equal(t, [2]int{2, 0}, [2]int{pending, queued}, "P nonce 311 must be pending")
}
