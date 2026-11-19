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

package execution_client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestLocalExecutionClientsGetHeaderByHash(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	block := chainPack.Blocks[0]
	reader := chainreader.NewChainReaderEth1(m.ChainConfig, m.ExecModule, time.Second)
	direct, err := NewExecutionClientDirect(reader, nil)
	require.NoError(t, err)
	clients := map[string]ExecutionEngine{
		"direct": direct,
		"engine": &ExecutionClientEngine{chainRW: &reader},
	}

	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			t.Run("known", func(t *testing.T) {
				header, err := client.GetHeaderByHash(t.Context(), block.Hash())
				require.NoError(t, err)
				require.NotNil(t, header)
				require.Equal(t, block.Hash(), header.Hash())
			})
			t.Run("unknown", func(t *testing.T) {
				header, err := client.GetHeaderByHash(t.Context(), common.Hash{1})
				require.NoError(t, err)
				require.Nil(t, header)
			})
		})
	}
}

func TestLocalExecutionClientsGetHeaderByHashError(t *testing.T) {
	wantErr := errors.New("header lookup failed")
	reader := chainreader.NewChainReaderEth1(chain.AllProtocolChanges, headerErrorExecutionModule{err: wantErr}, time.Second)
	direct, err := NewExecutionClientDirect(reader, nil)
	require.NoError(t, err)
	clients := map[string]ExecutionEngine{
		"direct": direct,
		"engine": &ExecutionClientEngine{chainRW: &reader},
	}

	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			header, err := client.GetHeaderByHash(t.Context(), common.Hash{1})
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, header)
		})
	}
}

type headerErrorExecutionModule struct {
	execmodule.ExecutionModule
	err error
}

func (m headerErrorExecutionModule) GetHeader(context.Context, *common.Hash, *uint64) (*types.Header, error) {
	return nil, m.err
}

func TestLocalExecutionClientsGetPayloadBodiesByHash(t *testing.T) {
	for _, name := range []string{"direct", "engine"} {
		t.Run(name, func(t *testing.T) {
			m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
			chainPack, err := m.GenerateChain(1, nil)
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(chainPack))
			block := chainPack.Blocks[0]
			err = m.DB.Update(t.Context(), func(tx kv.RwTx) error {
				return tx.Delete(kv.BlockAccessList, dbutils.BlockBodyKey(block.NumberU64(), block.Hash()))
			})
			require.NoError(t, err)
			want, err := types.EncodeBlockAccessListBytes(block.BlockAccessList())
			require.NoError(t, err)

			reader := chainreader.NewChainReaderEth1(m.ChainConfig, m.ExecModule, time.Second)
			var client ExecutionEngine
			if name == "direct" {
				client, err = NewExecutionClientDirect(reader, nil)
				require.NoError(t, err)
			} else {
				client = &ExecutionClientEngine{chainRW: &reader}
			}

			bodies, err := client.GetPayloadBodiesByHash(t.Context(), []common.Hash{block.Hash(), {1}})
			require.NoError(t, err)
			require.Len(t, bodies, 2)
			require.NotNil(t, bodies[0])
			require.NotNil(t, bodies[0].BlockAccessList)
			require.Equal(t, hexutil.Bytes(want), *bodies[0].BlockAccessList)
			require.Nil(t, bodies[1])
		})
	}
}

func TestExecutionClientEngineRemoteGetPayloadBodiesByHashV2(t *testing.T) {
	bal := hexutil.Bytes{0xc0}
	want := engine_types.ExecutionPayloadBodiesV2{
		{Transactions: []hexutil.Bytes{{1}}, Withdrawals: types.Withdrawals{}, BlockAccessList: &bal},
		nil,
	}
	client, calls := newPayloadDataRPCClient(t, want)

	bodies, err := client.GetPayloadBodiesByHash(t.Context(), []common.Hash{{1}, {2}})
	require.NoError(t, err)
	require.Len(t, bodies, 2)
	require.Equal(t, want, engine_types.ExecutionPayloadBodiesV2(bodies))
	call := <-calls
	require.Equal(t, "engine_getPayloadBodiesByHashV2", call.Method)
}

func TestExecutionClientEngineRemoteRejectsWrongPayloadBodyCount(t *testing.T) {
	client, calls := newPayloadDataRPCClient(t, engine_types.ExecutionPayloadBodiesV2{{}})

	bodies, err := client.GetPayloadBodiesByHash(t.Context(), []common.Hash{{1}, {2}})
	require.ErrorContains(t, err, "returned 1 bodies for 2 hashes")
	require.Nil(t, bodies)
	call := <-calls
	require.Equal(t, "engine_getPayloadBodiesByHashV2", call.Method)
}

func TestExecutionClientEngineRemoteGetHeaderByHash(t *testing.T) {
	header := newPayloadDataTestHeader()
	hash := header.Hash()

	t.Run("known", func(t *testing.T) {
		block := types.NewBlockWithHeader(header, nil)
		blockJSON, err := jsonstream.Marshal(ethapi.RPCMarshalBlock(block, true, false))
		require.NoError(t, err)
		client, calls := newPayloadDataRPCClient(t, json.RawMessage(blockJSON))
		got, err := client.GetHeaderByHash(t.Context(), hash)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, hash, got.Hash())
		call := <-calls
		require.Equal(t, "eth_getBlockByHash", call.Method)
		require.JSONEq(t, `[`+`"`+hash.Hex()+`"`+`,false]`, string(call.Params))
	})

	t.Run("unknown", func(t *testing.T) {
		client, calls := newPayloadDataRPCClient(t, nil)
		got, err := client.GetHeaderByHash(t.Context(), common.Hash{1})
		require.NoError(t, err)
		require.Nil(t, got)
		call := <-calls
		require.Equal(t, "eth_getBlockByHash", call.Method)
	})
}

func TestExecutionClientEngineRemoteRejectsWrongHeaderHash(t *testing.T) {
	header := newPayloadDataTestHeader()
	block := types.NewBlockWithHeader(header, nil)
	blockJSON, err := jsonstream.Marshal(ethapi.RPCMarshalBlock(block, true, false))
	require.NoError(t, err)
	client, calls := newPayloadDataRPCClient(t, json.RawMessage(blockJSON))

	got, err := client.GetHeaderByHash(t.Context(), common.Hash{1})
	require.ErrorContains(t, err, "returned header")
	require.Nil(t, got)
	call := <-calls
	require.Equal(t, "eth_getBlockByHash", call.Method)
}

type payloadDataRPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func newPayloadDataRPCClient(t *testing.T, result any) (*ExecutionClientEngine, <-chan payloadDataRPCRequest) {
	t.Helper()
	resultJSON, err := json.Marshal(result)
	require.NoError(t, err)
	calls := make(chan payloadDataRPCRequest, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request payloadDataRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		calls <- request
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"result":  json.RawMessage(resultJSON),
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	rpcClient, err := rpc.DialHTTP(srv.URL, log.New())
	require.NoError(t, err)
	t.Cleanup(rpcClient.Close)
	return &ExecutionClientEngine{
		engine:    &EngineAPIRPCClient{client: rpcClient},
		rpcClient: rpcClient,
	}, calls
}

func newPayloadDataTestHeader() *types.Header {
	hash := common.HexToHash("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	num := uint64(7)
	return &types.Header{
		ParentHash:            hash,
		UncleHash:             empty.UncleHash,
		Coinbase:              common.HexToAddress("0x1234567890123456789012345678901234567890"),
		Root:                  hash,
		TxHash:                hash,
		ReceiptHash:           hash,
		Difficulty:            *merge.ProofOfStakeDifficulty,
		Number:                *uint256.NewInt(42),
		GasLimit:              30_000_000,
		GasUsed:               21_000,
		Time:                  1_700_000_000,
		Extra:                 []byte{0xde, 0xad},
		MixDigest:             hash,
		Nonce:                 merge.ProofOfStakeNonce,
		BaseFee:               uint256.NewInt(1_000_000_000),
		WithdrawalsHash:       &hash,
		BlobGasUsed:           &num,
		ExcessBlobGas:         &num,
		ParentBeaconBlockRoot: &hash,
		RequestsHash:          &hash,
		BlockAccessListHash:   &hash,
		SlotNumber:            &num,
	}
}
