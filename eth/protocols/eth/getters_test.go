package eth_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)
)

// Tests that the transaction receipts can be retrieved based on hashes.
func TestGetBlockReceipts65(t *testing.T) { testGetBlockReceipts(t, 65) }
func TestGetBlockReceipts66(t *testing.T) { testGetBlockReceipts(t, 66) }

func testGetBlockReceipts(t *testing.T, protocol uint) {
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

	// Collect the hashes to request, and the response to expect
	var (
		hashes   []common.Hash
		receipts []rlp.RawValue
	)

	err := m.DB.View(m.Ctx, func(tx ethdb.Tx) error {
		for i := uint64(0); i <= rawdb.ReadCurrentHeader(tx).Number.Uint64(); i++ {
			block := rawdb.ReadHeaderByNumber(tx, i)

			hashes = append(hashes, block.Hash())
			// If known, encode and queue for response packet
			r, err := rawdb.ReadReceiptsByHash(tx, block.Hash())
			if err != nil {
				return err
			}
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

	// Send the hash request and verify the response
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: eth.ToProto[eth.ETH66][eth.GetReceiptsMsg], Data: b, PeerId: m.PeerId}) {
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
func mockWithGenerator(t *testing.T, blocks int, generator func(int, *core.BlockGen)) *stages.MockSentry {
	m := stages.MockWithGenesis(t, &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}},
	}, testKey)
	genesis := m.Genesis
	if blocks > 0 {
		chain, _ := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), m.DB, blocks, generator, true)
		err := m.InsertChain(chain)
		require.NoError(t, err)
	}
	return m
}
