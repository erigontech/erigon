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
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// TestGetBlockReceiptsFrozenBlocks covers GetReceipts serving for blocks that live
// only in snapshot segments (headers pruned from the DB). Responses must carry one
// receipt list per requested block in request order — an empty list for empty
// blocks — and end at the first block that cannot be served.
func TestGetBlockReceiptsFrozenBlocks(t *testing.T) {
	t.Parallel()
	const (
		// The chain is one minimal block segment (file names encode block/1000, so
		// segments cannot be smaller) plus a small tail that stays unfrozen in the DB.
		frozenChainLength   = snaptype.Erigon2MinSegmentSize + 10
		frozenEmptyBlockNum = 500
	)
	devp2pTestKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	devp2pTestAddr := crypto.PubkeyToAddress(devp2pTestKey.PublicKey)
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.AllProtocolChanges,
			Alloc:  types.GenesisAlloc{devp2pTestAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}},
		}),
		execmoduletester.WithKey(devp2pTestKey),
		execmoduletester.WithSentryProtocol(direct.ETH70),
	)
	signer := types.LatestSignerForChainID(nil)
	generated, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, frozenChainLength, func(i int, block *blockgen.BlockGen) {
		if i+1 == frozenEmptyBlockNum {
			return // one empty block surrounded by non-empty ones
		}
		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(devp2pTestAddr), common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *signer, devp2pTestKey)
		require.NoError(t, err)
		block.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(generated))
	// Freeze the first segment's blocks into a snapshot file and prune them from
	// the DB, exactly as block retirement does.
	snCfg, _ := snapcfg.KnownCfg(networkname.Mainnet)
	require.NoError(t, freezeblocks.DumpBlocks(m.Ctx, 0, snaptype.Erigon2MinSegmentSize, m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap, m.DB, 1, log.LvlInfo, log.New(), m.BlockReader, snCfg, nil))
	require.NoError(t, m.BlockSnapshots.OpenFolder())
	rwTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	deleted, err := rawdb.PruneBlocks(rwTx, snaptype.Erigon2MinSegmentSize, snaptype.Erigon2MinSegmentSize)
	require.NoError(t, err)
	require.Equal(t, snaptype.Erigon2MinSegmentSize-1, deleted)
	require.NoError(t, rwTx.Commit())
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	blockHash := func(num uint64) common.Hash {
		block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, num)
		require.NoError(t, err)
		require.NotNil(t, block)
		return block.Hash()
	}
	emptyBlockHash := blockHash(frozenEmptyBlockNum)
	// Precondition making the scenario real: the empty block is readable via the
	// snapshot-aware block reader but its header is gone from the DB tables.
	require.GreaterOrEqual(t, m.BlockReader.FrozenBlocks(), uint64(snaptype.Erigon2MinSegmentSize-1))
	prunedHeader, err := rawdb.ReadHeaderByHash(tx, emptyBlockHash)
	require.NoError(t, err)
	require.Nil(t, prunedHeader)
	receiptsGetter := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute)
	encodedReceipts := func(num uint64) rlp.RawValue {
		block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, num)
		require.NoError(t, err)
		r, err := receiptsGetter.GetReceipts(m.Ctx, m.ChainConfig, tx, block, eth.ReceiptsOpts{})
		require.NoError(t, err)
		perReceipt := make([]rlp.RawValue, 0, len(r))
		for _, receipt := range r {
			var buf bytes.Buffer
			require.NoError(t, receipt.EncodeRLP69(&buf))
			perReceipt = append(perReceipt, buf.Bytes())
		}
		encoded, err := rlp.EncodeToBytes(perReceipt)
		require.NoError(t, err)
		return encoded
	}
	var unknownHash common.Hash
	for i := range unknownHash {
		unknownHash[i] = byte(i)
	}
	emptyList, err := rlp.EncodeToBytes([]rlp.RawValue{})
	require.NoError(t, err)
	tests := []struct {
		name   string
		query  []common.Hash
		expect []rlp.RawValue
	}{
		{
			// Baseline: every requested block — frozen or still in the DB — yields
			// its receipt list in request order.
			name: "all blocks available",
			query: []common.Hash{
				blockHash(frozenEmptyBlockNum - 2), blockHash(frozenEmptyBlockNum - 1), blockHash(frozenChainLength - 5),
			},
			expect: []rlp.RawValue{
				encodedReceipts(frozenEmptyBlockNum - 2), encodedReceipts(frozenEmptyBlockNum - 1), encodedReceipts(frozenChainLength - 5),
			},
		},
		{
			// An empty block mid-request must yield an empty receipt list at its
			// position, not be omitted: omission desyncs the positional matching
			// peers use to validate the response, and they drop us for it.
			name: "empty block mid request",
			query: []common.Hash{
				blockHash(frozenEmptyBlockNum - 2), blockHash(frozenEmptyBlockNum - 1),
				emptyBlockHash,
				blockHash(frozenEmptyBlockNum + 1), blockHash(frozenEmptyBlockNum + 2),
			},
			expect: []rlp.RawValue{
				encodedReceipts(frozenEmptyBlockNum - 2), encodedReceipts(frozenEmptyBlockNum - 1),
				emptyList,
				encodedReceipts(frozenEmptyBlockNum + 1), encodedReceipts(frozenEmptyBlockNum + 2),
			},
		},
		{
			// A block we cannot serve ends the response at that block, so what we
			// do send still corresponds positionally to the request prefix.
			name: "unknown hash mid request",
			query: []common.Hash{
				blockHash(frozenEmptyBlockNum - 2), blockHash(frozenEmptyBlockNum - 1),
				unknownHash,
				blockHash(frozenEmptyBlockNum + 1),
			},
			expect: []rlp.RawValue{
				encodedReceipts(frozenEmptyBlockNum - 2), encodedReceipts(frozenEmptyBlockNum - 1),
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := rlp.EncodeToBytes(eth.GetReceiptsPacket70{RequestId: uint64(i + 1), GetReceiptsPacket: tt.query})
			require.NoError(t, err)
			m.StreamWg.Wait()
			m.ReceiveWg.Add(1)
			for _, err = range m.Send(&sentryproto.InboundMessage{Id: eth.ToProto[direct.ETH70][eth.GetReceiptsMsg], Data: b, PeerId: m.PeerId}) {
				require.NoError(t, err)
			}
			m.ReceiveWg.Wait()
			sent, err := m.SentMessage(i)
			require.NoError(t, err)
			require.Equal(t, eth.ToProto[m.SentryClient.Protocol()][eth.ReceiptsMsg], sent.Id)
			var resp eth.ReceiptsRLPPacket70
			require.NoError(t, rlp.DecodeBytes(sent.Data, &resp))
			require.Equal(t, uint64(i+1), resp.RequestId)
			require.False(t, resp.LastBlockIncomplete)
			require.Len(t, resp.ReceiptsRLPPacket, len(tt.expect))
			for pos, expected := range tt.expect {
				require.Equal(t, expected, resp.ReceiptsRLPPacket[pos], "receipt list at position %d", pos)
			}
		})
	}
}

// TestGetBlockAccessListsResponseMatrix drives real eth/71 GetBlockAccessLists
// requests through sentry and pins every response-entry type: stored bytes,
// regenerated bytes, the canonical empty BAL (0xc0), and the not-available
// sentinel (0x80) for pre-Amsterdam, non-canonical, and unknown blocks — plus
// the per-request regeneration budget.
func TestGetBlockAccessListsResponseMatrix(t *testing.T) {
	t.Parallel()
	const (
		chainLength    = 45
		emptyBlockNum  = 42
		storedBlockNum = 43
	)
	keyA, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyB, err := crypto.GenerateKey()
	require.NoError(t, err)
	addrA := crypto.PubkeyToAddress(keyA.PublicKey)
	addrB := crypto.PubkeyToAddress(keyB.PublicKey)
	// blockgen sets block time to 10*blockNum, so Amsterdam at t=15 makes
	// block 1 the only pre-Amsterdam block.
	amsterdamTime := uint64(15)
	cfg := &chain.Config{
		ChainID:                       uint256.NewInt(1337),
		Rules:                         chain.EtHashRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		AmsterdamTime:                 &amsterdamTime,
		DepositContract:               common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa"),
		Ethash:                        new(chain.EthashConfig),
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: cfg,
			Alloc: types.GenesisAlloc{
				addrA: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
				addrB: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			},
		}),
		execmoduletester.WithKey(keyA),
		execmoduletester.WithSentryProtocol(direct.ETH71),
	)
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	canonical, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLength, func(i int, b *blockgen.BlockGen) {
		if i+1 == emptyBlockNum {
			return
		}
		txn, err := types.SignTx(types.NewTransaction(b.TxNonce(addrA), common.Address{1}, uint256.NewInt(1), params.TxGas, baseFee, nil), *signer, keyA)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	fork, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(b.TxNonce(addrB), common.Address{2}, uint256.NewInt(2), params.TxGas, baseFee, nil), *signer, keyB)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.Nil(t, canonical.Blocks[0].Header().BlockAccessListHash, "block 1 should be pre-Amsterdam")
	require.NotNil(t, fork.Blocks[1].Header().BlockAccessListHash, "fork block 2 should be an Amsterdam block")
	require.NoError(t, m.InsertChain(canonical))
	// The fork blocks are inserted without a fork choice update so they stay
	// non-canonical, with no stored BAL sidecar.
	insertRes, err := insertBlocks(m.Ctx, m.ExecModule, fork.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insertRes)
	// Prune every stored BAL row except storedBlockNum's, simulating the
	// exec-stage prune with storedBlockNum inside the reorg window.
	keep := dbutils.BlockBodyKey(storedBlockNum, canonical.Blocks[storedBlockNum-1].Hash())
	rwTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	err = rwTx.ForEach(kv.BlockAccessList, nil, func(k, _ []byte) error {
		if bytes.Equal(k, keep) {
			return nil
		}
		return rwTx.Delete(kv.BlockAccessList, k)
	})
	require.NoError(t, err)
	count, err := rwTx.Count(kv.BlockAccessList)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.NoError(t, rwTx.Commit())
	balBytes := func(num uint64) rlp.RawValue {
		return canonical.BlockAccessLists[num-1]
	}
	blockHash := func(num uint64) common.Hash {
		return canonical.Blocks[num-1].Hash()
	}
	sentinel := rlp.RawValue{0x80}
	var unknownHash common.Hash
	for i := range unknownHash {
		unknownHash[i] = byte(i)
	}
	budgetQuery := make([]common.Hash, 0, 40)
	budgetExpect := make([]rlp.RawValue, 0, eth.MaxBlockAccessListsRegenerate)
	for num := uint64(2); num <= 41; num++ {
		budgetQuery = append(budgetQuery, blockHash(num))
		if len(budgetExpect) < eth.MaxBlockAccessListsRegenerate {
			budgetExpect = append(budgetExpect, balBytes(num))
		}
	}
	tests := []struct {
		name   string
		query  []common.Hash
		expect []rlp.RawValue
	}{
		{
			name:   "stored block serves the stored bytes",
			query:  []common.Hash{blockHash(storedBlockNum)},
			expect: []rlp.RawValue{balBytes(storedBlockNum)},
		},
		{
			name:   "pruned block serves the regenerated bytes",
			query:  []common.Hash{blockHash(2)},
			expect: []rlp.RawValue{balBytes(2)},
		},
		{
			name:   "tx-less block serves its system-call-only BAL",
			query:  []common.Hash{blockHash(emptyBlockNum)},
			expect: []rlp.RawValue{balBytes(emptyBlockNum)},
		},
		{
			name:   "pre-Amsterdam block is not available",
			query:  []common.Hash{blockHash(1)},
			expect: []rlp.RawValue{sentinel},
		},
		{
			name:   "non-canonical block is not available",
			query:  []common.Hash{fork.Blocks[1].Hash()},
			expect: []rlp.RawValue{sentinel},
		},
		{
			name:   "unknown block is not available",
			query:  []common.Hash{unknownHash},
			expect: []rlp.RawValue{sentinel},
		},
		{
			// Every response type in one request: entries must stay
			// positionally aligned with the query.
			name: "mixed request keeps positional alignment",
			query: []common.Hash{
				blockHash(storedBlockNum), blockHash(2), blockHash(emptyBlockNum),
				blockHash(1), fork.Blocks[1].Hash(), unknownHash,
			},
			expect: []rlp.RawValue{
				balBytes(storedBlockNum), balBytes(2), balBytes(emptyBlockNum),
				sentinel, sentinel, sentinel,
			},
		},
		{
			// A request spanning more prunable blocks than the regeneration
			// budget is truncated at the budget; the peer re-requests the rest.
			name:   "regeneration budget truncates",
			query:  budgetQuery,
			expect: budgetExpect,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := rlp.EncodeToBytes(eth.GetBlockAccessListsPacket66{RequestId: uint64(i + 1), GetBlockAccessListsPacket: tt.query})
			require.NoError(t, err)
			m.StreamWg.Wait()
			m.ReceiveWg.Add(1)
			for _, err = range m.Send(&sentryproto.InboundMessage{Id: eth.ToProto[direct.ETH71][eth.GetBlockAccessListsMsg], Data: b, PeerId: m.PeerId}) {
				require.NoError(t, err)
			}
			m.ReceiveWg.Wait()
			sent, err := m.SentMessage(i)
			require.NoError(t, err)
			require.Equal(t, eth.ToProto[direct.ETH71][eth.BlockAccessListsMsg], sent.Id)
			var resp eth.BlockAccessListsPacket66
			require.NoError(t, rlp.DecodeBytes(sent.Data, &resp))
			require.Equal(t, uint64(i+1), resp.RequestId)
			require.Len(t, resp.BlockAccessListsPacket, len(tt.expect))
			for pos, expected := range tt.expect {
				require.Equal(t, expected, resp.BlockAccessListsPacket[pos], "entry at position %d", pos)
			}
		})
	}
}
