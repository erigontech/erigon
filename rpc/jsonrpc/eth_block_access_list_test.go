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

package jsonrpc

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

type blockAccessListRPCCase struct {
	name       string
	selector   string
	want       json.RawMessage
	errCode    int
	errMessage string
}

func TestBlockAccessListRPCSpec(t *testing.T) {
	chainPack, client := newBlockAccessListRPCFixture(t)
	availableJSON := marshalBlockAccessListJSON(t, chainPack.BlockAccessLists[1])
	emptyJSON := marshalBlockAccessListJSON(t, chainPack.BlockAccessLists[2])
	availableRaw := marshalHexBytesJSON(t, chainPack.BlockAccessLists[1])
	emptyRaw := marshalHexBytesJSON(t, chainPack.BlockAccessLists[2])
	t.Run("eth_getBlockAccessList", func(t *testing.T) {
		cases := []blockAccessListRPCCase{
			{name: "available by number", selector: "0x2", want: availableJSON},
			{name: "available by tag", selector: "safe", want: availableJSON},
			{name: "available by hash", selector: chainPack.Blocks[1].Hash().Hex(), want: availableJSON},
			{name: "empty by number", selector: "0x3", want: emptyJSON},
			{name: "empty by tag", selector: "latest", want: emptyJSON},
			{name: "empty by hash", selector: chainPack.Blocks[2].Hash().Hex(), want: emptyJSON},
			{name: "unknown number", selector: "0xff", want: json.RawMessage("null")},
			{name: "unknown hash", selector: common.Hash{}.Hex(), want: json.RawMessage("null")},
			{name: "pending", selector: "pending", want: json.RawMessage("null")},
			{name: "pre-Amsterdam by number", selector: "0x1", errCode: -32001, errMessage: "Resource not found"},
			{name: "pre-Amsterdam by tag", selector: "earliest", errCode: -32001, errMessage: "Resource not found"},
			{name: "pre-Amsterdam by hash", selector: chainPack.Blocks[0].Hash().Hex(), errCode: -32001, errMessage: "Resource not found"},
			{name: "pruned by number", selector: "0x4", errCode: 4444, errMessage: "Pruned history unavailable"},
			{name: "pruned by tag", selector: "finalized", errCode: 4444, errMessage: "Pruned history unavailable"},
			{name: "pruned by hash", selector: chainPack.Blocks[3].Hash().Hex(), errCode: 4444, errMessage: "Pruned history unavailable"},
		}
		runBlockAccessListRPCCases(t, client, "eth_getBlockAccessList", cases)
	})
	t.Run("debug_getRawBlockAccessList", func(t *testing.T) {
		cases := []blockAccessListRPCCase{
			{name: "available by number", selector: "0x2", want: availableRaw},
			{name: "available by tag", selector: "safe", want: availableRaw},
			{name: "available by hash", selector: chainPack.Blocks[1].Hash().Hex(), want: availableRaw},
			{name: "empty by number", selector: "0x3", want: emptyRaw},
			{name: "empty by tag", selector: "latest", want: emptyRaw},
			{name: "empty by hash", selector: chainPack.Blocks[2].Hash().Hex(), want: emptyRaw},
			{name: "unknown number", selector: "0xff", errCode: -32001, errMessage: "Resource not found"},
			{name: "unknown hash", selector: common.Hash{}.Hex(), errCode: -32001, errMessage: "Resource not found"},
			{name: "pending", selector: "pending", errCode: -32001, errMessage: "Resource not found"},
			{name: "pre-Amsterdam by number", selector: "0x1", errCode: -32001, errMessage: "Resource not found"},
			{name: "pre-Amsterdam by tag", selector: "earliest", errCode: -32001, errMessage: "Resource not found"},
			{name: "pre-Amsterdam by hash", selector: chainPack.Blocks[0].Hash().Hex(), errCode: -32001, errMessage: "Resource not found"},
			{name: "pruned by number", selector: "0x4", errCode: 4444, errMessage: "Pruned history unavailable"},
			{name: "pruned by tag", selector: "finalized", errCode: 4444, errMessage: "Pruned history unavailable"},
			{name: "pruned by hash", selector: chainPack.Blocks[3].Hash().Hex(), errCode: 4444, errMessage: "Pruned history unavailable"},
		}
		runBlockAccessListRPCCases(t, client, "debug_getRawBlockAccessList", cases)
	})
}

func newBlockAccessListRPCFixture(t *testing.T) (*blockgen.ChainPack, *rpc.Client) {
	t.Helper()
	m, chainPack := rpcdaemontest.CreateTestBlockAccessListExecModule(t)
	base := newBaseApiForTest(m)
	ethAPI := newEthApiForTest(base, m.DB, nil, nil)
	debugAPI := NewPrivateDebugAPI(base, m.DB, nil, 0, false)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("eth", EthAPI(ethAPI)))
	require.NoError(t, server.RegisterName("debug", PrivateDebugAPI(debugAPI)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() {
		client.Close()
		server.Stop()
	})
	return chainPack, client
}

func marshalBlockAccessListJSON(t *testing.T, data []byte) json.RawMessage {
	t.Helper()
	bal, err := types.DecodeBlockAccessListBytes(data)
	require.NoError(t, err)
	encoded, err := json.Marshal(ethapi.MarshalBlockAccessList(bal))
	require.NoError(t, err)
	return encoded
}

func marshalHexBytesJSON(t *testing.T, data []byte) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(hexutil.Bytes(data))
	require.NoError(t, err)
	return encoded
}

func runBlockAccessListRPCCases(t *testing.T, client *rpc.Client, method string, cases []blockAccessListRPCCase) {
	t.Helper()
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var got json.RawMessage
			err := client.CallContext(t.Context(), &got, method, testCase.selector)
			if testCase.errCode != 0 {
				require.Error(t, err)
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, testCase.errCode, rpcErr.ErrorCode())
				require.Equal(t, testCase.errMessage, err.Error())
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, string(testCase.want), string(got))
		})
	}
}

// TestGetBlockAccessListRegeneratesPrunedBAL verifies that eth_getBlockAccessList
// serves BALs whose stored copy has been pruned (kept only for the reorg window)
// by regenerating them via re-execution.
func TestGetBlockAccessListRegeneratesPrunedBAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(uint64(i), common.Address{1}, uint256.NewInt(10_000), 50_000, baseFee, nil), *signer, privKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)
	// Simulate the exec-stage prune: drop every stored BAL row.
	err = m.DB.Update(ctx, func(tx kv.RwTx) error {
		return tx.ForEach(kv.BlockAccessList, nil, func(k, _ []byte) error {
			return tx.Delete(kv.BlockAccessList, k)
		})
	})
	require.NoError(t, err)
	err = m.DB.View(ctx, func(tx kv.Tx) error {
		count, err := tx.Count(kv.BlockAccessList)
		require.NoError(t, err)
		require.Zero(t, count, "stored BALs should be fully pruned")
		return nil
	})
	require.NoError(t, err)
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, &rpccfg.EthApiConfig{}, log.New())
	for i, block := range chainPack.Blocks {
		blockNum := rpc.BlockNumber(block.NumberU64())
		got, err := api.GetBlockAccessList(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})
		require.NoError(t, err, "block %d", block.NumberU64())
		canonical, err := types.DecodeBlockAccessListBytes(chainPack.BlockAccessLists[i])
		require.NoError(t, err)
		require.Equal(t, ethapi.MarshalBlockAccessList(canonical), got, "block %d", block.NumberU64())
	}
}
