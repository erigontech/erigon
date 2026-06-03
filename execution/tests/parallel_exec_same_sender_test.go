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
	"context"
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Acceptance test for dependency-free speculative execution (nonce pre-writes
// instead of sender-based scheduling): a block packed with same-sender txs must
// produce exactly the serial executor's state — correct final nonces with no
// double-increment — under both executors. The state root is cross-checked by
// InsertChain against the serially-generated header.
func TestSameSenderTxsInOneBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	data := getGenesis()
	senderA, keyA := data.addresses[0], data.keys[0]
	senderB, keyB := data.addresses[1], data.keys[1]
	recipients := []common.Address{{0xaa}, {0xab}, {0xac}, {0xad}}

	signer := types.LatestSignerForChainID(nil)
	signedTransfer := func(t *testing.T, key *ecdsa.PrivateKey, nonce uint64, to common.Address) types.Transaction {
		t.Helper()
		txn := &types.LegacyTx{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(1_000)},
			GasPrice: *uint256.NewInt(1),
		}
		signed, err := types.SignTx(txn, *signer, key)
		require.NoError(t, err)
		return signed
	}

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(data.genesisSpec),
		execmoduletester.WithKey(keyA),
		execmoduletester.WithExperimentalBAL())

	contractAddress := types.CreateAddress(senderA, 2)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, block *blockgen.BlockGen) {
		block.AddTx(signedTransfer(t, keyA, block.TxNonce(senderA), recipients[0]))
		block.AddTx(signedTransfer(t, keyA, block.TxNonce(senderA), recipients[1]))
		creation := &types.LegacyTx{
			CommonTx: types.CommonTx{Nonce: block.TxNonce(senderA), GasLimit: 60_000},
			GasPrice: *uint256.NewInt(1),
		}
		signedCreation, err := types.SignTx(creation, *signer, keyA)
		require.NoError(t, err)
		block.AddTx(signedCreation)
		block.AddTx(signedTransfer(t, keyA, block.TxNonce(senderA), recipients[2]))
		block.AddTx(signedTransfer(t, keyB, block.TxNonce(senderB), recipients[3]))
		block.AddTx(signedTransfer(t, keyA, block.TxNonce(senderA), recipients[0]))
	})
	require.NoError(t, err)

	readAccounts := func(t *testing.T, m *execmoduletester.ExecModuleTester) map[common.Address]accounts.Account {
		t.Helper()
		out := map[common.Address]accounts.Account{}
		addrs := append([]common.Address{senderA, senderB, contractAddress}, recipients...)
		require.NoError(t, m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
			reader := m.NewStateReader(tx)
			for _, addr := range addrs {
				acc, err := reader.ReadAccountData(accounts.InternAddress(addr))
				if err != nil {
					return err
				}
				require.NotNil(t, acc, "account %x must exist", addr)
				out[addr] = *acc
			}
			return nil
		}))
		return out
	}

	require.NoError(t, m.InsertChain(chain))
	parallelState := readAccounts(t, m)

	require.Equal(t, uint64(5), parallelState[senderA].Nonce)
	require.Equal(t, uint64(1), parallelState[senderB].Nonce)
	require.Equal(t, uint64(1), parallelState[contractAddress].Nonce)
	require.Equal(t, *uint256.NewInt(2_000), parallelState[recipients[0]].Balance)
	require.Equal(t, *uint256.NewInt(1_000), parallelState[recipients[3]].Balance)

	mSerial := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(data.genesisSpec),
		execmoduletester.WithKey(keyA),
		execmoduletester.WithoutExperimentalBAL())
	require.NoError(t, mSerial.InsertChain(chain))
	require.Equal(t, readAccounts(t, mSerial), parallelState)
}

// A same-sender nonce gap must be rejected by both executors: with optimistic
// scheduling the gapped tx reads its predecessor's pre-written nonce, fails the
// nonce check and invalidates the block — same verdict as serial.
func TestSameSenderNonceGapRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	_, validChain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {getBlockTx(from, to, uint256.NewInt(1_000)), fromKey},
	})
	require.NoError(t, err)
	require.Len(t, validChain.Blocks, 1)

	signer := types.LatestSignerForChainID(nil)
	gapped := &types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 2, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(1_000)},
		GasPrice: *uint256.NewInt(1),
	}
	signedGapped, err := types.SignTx(gapped, *signer, fromKey)
	require.NoError(t, err)

	badHeader := types.CopyHeader(validChain.Headers[0])
	badBlock := types.NewBlock(badHeader, append(validChain.Blocks[0].Transactions(), signedGapped),
		validChain.Blocks[0].Uncles(), validChain.Receipts[0], nil)
	badChain := &blockgen.ChainPack{
		Blocks:   []*types.Block{badBlock},
		Headers:  []*types.Header{badBlock.Header()},
		TopBlock: badBlock,
	}

	cases := []struct {
		name string
		opts []execmoduletester.Option
	}{
		{"parallel", []execmoduletester.Option{execmoduletester.WithExperimentalBAL()}},
		{"serial", []execmoduletester.Option{execmoduletester.WithoutExperimentalBAL()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]execmoduletester.Option{
				execmoduletester.WithGenesisSpec(data.genesisSpec),
				execmoduletester.WithKey(fromKey),
			}, tc.opts...)
			m := execmoduletester.New(t, opts...)
			require.Error(t, m.InsertChain(badChain),
				"%s exec must reject a same-sender nonce gap", tc.name)
		})
	}
}
