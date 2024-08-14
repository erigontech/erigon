// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package eth_test

import (
	"math/big"
	"testing"

	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/turbo/jsonrpc/receipts"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)
)

func TestGetBlockReceipts(t *testing.T) {
	if !sentry_multi_client.EnableP2PReceipts {
		t.Skip("")
	}
	// Define three accounts to simulate transactions with
	acc1Key, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	acc2Key, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	acc1Addr := crypto.PubkeyToAddress(acc1Key.PublicKey)
	acc2Addr := crypto.PubkeyToAddress(acc2Key.PublicKey)

	signer := types.LatestSignerForChainID(nil)
	// Create a chain generator with some simple transactions (blatantly stolen from @fjl/chain_markets_test)
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, the test bank sends account #1 some ether.
			tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, testKey)
			block.AddTx(tx)
		case 1:
			// In block 2, the test bank sends some more ether to account #1.
			// acc1Addr passes it on to account #2.
			tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, testKey)
			tx2, _ := types.SignTx(types.NewTransaction(block.TxNonce(acc1Addr), acc2Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, acc1Key)
			block.AddTx(tx1)
			block.AddTx(tx2)
		case 2:
			// Block 3 is empty but was mined by account #2.
			block.SetCoinbase(acc2Addr)
			block.SetExtra([]byte("yeehaw"))
		case 3:
			// Block 4 includes blocks 2 and 3 as uncle headers (with modified extra data).
			b2 := block.PrevBlock(1).Header()
			b2.Extra = []byte("foo")
			block.AddUncle(b2)
			b3 := block.PrevBlock(2).Header()
			b3.Extra = []byte("foo")
			block.AddUncle(b3)
		}
	}
	// Assemble the test environment
	m := mockWithGenerator(t, 4, generator)
	receiptsGetter := receipts.NewGenerator(32, m.BlockReader, m.Engine)
	// Collect the hashes to request, and the response to expect
	var (
		hashes   []libcommon.Hash
		receipts []rlp.RawValue
	)
	err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		for i := uint64(0); i <= rawdb.ReadCurrentHeader(tx).Number.Uint64(); i++ {
			block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, i)
			require.NoError(t, err)

			hashes = append(hashes, block.Hash())
			// If known, encode and queue for response packet

			r, err := receiptsGetter.GetReceipts(m.Ctx, m.ChainConfig, tx, block)
			require.NoError(t, err)
			encoded, err := rlp.EncodeToBytes(r)
			require.NoError(t, err)
			receipts = append(receipts, encoded)
		}
		return nil
	})

	require.NoError(t, err)
	b, err := rlp.EncodeToBytes(eth.GetReceiptsPacket66{RequestId: 1, GetReceiptsPacket: hashes})
	require.NoError(t, err)

	m.StreamWg.Wait()

	m.ReceiveWg.Add(1)
	// Send the hash request and verify the response
	for _, err = range m.Send(&sentry.InboundMessage{Id: eth.ToProto[direct.ETH66][eth.GetReceiptsMsg], Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	expect, err := rlp.EncodeToBytes(eth.ReceiptsRLPPacket66{RequestId: 1, ReceiptsRLPPacket: receipts})
	require.NoError(t, err)
	m.ReceiveWg.Wait()
	sent := m.SentMessage(0)
	require.Equal(t, eth.ToProto[m.SentryClient.Protocol()][eth.ReceiptsMsg], sent.Id)
	require.Equal(t, expect, sent.Data)
}

// newTestBackend creates a chain with a number of explicitly defined blocks and
// wraps it into a mock backend.
func mockWithGenerator(t *testing.T, blocks int, generator func(int, *core.BlockGen)) *mock.MockSentry {
	m := mock.MockWithGenesis(t, &types.Genesis{
		Config: params.TestChainConfig,
		Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}},
	}, testKey, false)
	if blocks > 0 {
		chain, _ := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, blocks, generator)
		err := m.InsertChain(chain)
		require.NoError(t, err)
	}
	return m
}
