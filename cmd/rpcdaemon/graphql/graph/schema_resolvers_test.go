// Copyright 2024 The Erigon Authors
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

package graph

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	ethapi "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// TestBlockResolver_Account_InvalidAddress verifies that a malformed address
// string is rejected before any GraphQL API call is made.
func TestBlockResolver_Account_InvalidAddress(t *testing.T) {
	r := &blockResolver{&Resolver{}} // GraphQLAPI is nil; validation fires before it is called

	tests := []struct {
		name    string
		address string
	}{
		{"empty string", ""},
		{"not hex", "not-an-address"},
		{"too short", "0x1234"},
		{"non-hex chars", "0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := r.Account(context.Background(), &model.Block{Number: 1}, tt.address)
			if err == nil {
				t.Errorf("address %q: expected error, got nil", tt.address)
			}
		})
	}
}

// TestNewAccountAtBlock verifies that the constructor sets BlockNum correctly.
func TestNewAccountAtBlock(t *testing.T) {
	acc := model.NewAccountAtBlock(42)
	if acc == nil {
		t.Fatal("expected non-nil Account")
	}
	if acc.BlockNum != 42 {
		t.Errorf("expected BlockNum=42, got %d", acc.BlockNum)
	}
}

func TestBlockResolver_TransactionAt(t *testing.T) {
	r := &blockResolver{&Resolver{}}

	tx0 := &model.Transaction{Hash: "0xaaa"}
	tx1 := &model.Transaction{Hash: "0xbbb"}
	block := &model.Block{Transactions: []*model.Transaction{tx0, tx1}}

	tests := []struct {
		name  string
		index int
		want  *model.Transaction
	}{
		{"first", 0, tx0},
		{"second", 1, tx1},
		{"out of range", 2, nil},
		{"negative", -1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.TransactionAt(context.Background(), block, tt.index)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TransactionAt(%d) = %v, want %v", tt.index, got, tt.want)
			}
		})
	}
}

func TestAddressesFromModel(t *testing.T) {
	addrs, err := addressesFromModel(nil)
	if err != nil || len(addrs) != 0 {
		t.Fatal("expected empty slice for nil input")
	}

	addrs, err = addressesFromModel([]string{"0x1234567890123456789012345678901234567890"})
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 || addrs[0] != common.HexToAddress("0x1234567890123456789012345678901234567890") {
		t.Fatal("unexpected address value")
	}

	_, err = addressesFromModel([]string{"not-an-address"})
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestTopicsFromModel(t *testing.T) {
	topics, err := topicsFromModel(nil)
	if err != nil || len(topics) != 0 {
		t.Fatal("expected empty for nil input")
	}

	validTopic := "0x" + strings.Repeat("ab", 32)
	topics, err = topicsFromModel([][]string{{validTopic}})
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || len(topics[0]) != 1 {
		t.Fatal("unexpected topic shape")
	}

	_, err = topicsFromModel([][]string{{"not-hex"}})
	if err == nil {
		t.Fatal("expected error for invalid topic")
	}
}

func TestDecodeOptionalAddress(t *testing.T) {
	addr, err := decodeOptionalAddress(nil, "from")
	if err != nil || addr != nil {
		t.Fatal("expected nil, nil for nil input")
	}

	invalid := "not-an-address"
	_, err = decodeOptionalAddress(&invalid, "from")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}

	valid := "0x1234567890123456789012345678901234567890"
	addr, err = decodeOptionalAddress(&valid, "from")
	if err != nil {
		t.Fatal(err)
	}
	if addr == nil || *addr != common.HexToAddress(valid) {
		t.Fatal("unexpected address value")
	}
}

func TestDecodeOptionalBig(t *testing.T) {
	b, err := decodeOptionalBig(nil, "gasPrice")
	if err != nil || b != nil {
		t.Fatal("expected nil, nil for nil input")
	}

	invalid := "not-hex"
	_, err = decodeOptionalBig(&invalid, "gasPrice")
	if err == nil {
		t.Fatal("expected error for invalid hex")
	}

	valid := "0x1234"
	b, err = decodeOptionalBig(&valid, "gasPrice")
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.ToInt().Uint64() != 0x1234 {
		t.Fatal("unexpected big value")
	}
}

func TestRpcLogsToModel(t *testing.T) {
	result := rpcLogsToModel(nil)
	if len(result) != 0 {
		t.Fatal("expected empty for nil input")
	}

	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	blockHash := common.HexToHash("0x" + strings.Repeat("bb", 32))
	txHash := common.HexToHash("0x" + strings.Repeat("cc", 32))
	topic := common.HexToHash("0x" + strings.Repeat("dd", 32))

	logs := types.RPCLogs{
		{Log: types.Log{
			Address:     addr,
			Topics:      []common.Hash{topic},
			Data:        hexutil.Bytes{0x01, 0x02},
			BlockNumber: 42,
			BlockHash:   blockHash,
			TxHash:      txHash,
			Index:       3,
		}},
	}
	result = rpcLogsToModel(logs)
	if len(result) != 1 {
		t.Fatalf("expected 1 log, got %d", len(result))
	}
	ml := result[0]
	if ml.Index != 3 {
		t.Errorf("unexpected Index: %d", ml.Index)
	}
	if ml.Account == nil || ml.Account.Address != strings.ToLower(addr.Hex()) {
		t.Errorf("unexpected Account.Address: %v", ml.Account)
	}
	if ml.Account.BlockNum != 42 {
		t.Errorf("unexpected Account.BlockNum: %d", ml.Account.BlockNum)
	}
	if len(ml.Topics) != 1 || ml.Topics[0] != topic.Hex() {
		t.Errorf("unexpected Topics: %v", ml.Topics)
	}
	if ml.Transaction == nil || ml.Transaction.Hash != txHash.Hex() {
		t.Errorf("unexpected Transaction.Hash: %v", ml.Transaction)
	}
	if ml.Transaction.Block == nil || ml.Transaction.Block.Hash != blockHash.Hex() {
		t.Errorf("unexpected Transaction.Block.Hash: %v", ml.Transaction.Block)
	}
}

// mockGraphQLAPI is a minimal stub of jsonrpc.GraphQLAPI for resolver-level tests.
type mockGraphQLAPI struct {
	getLogsCrit   filters.FilterCriteria
	getLogsResult types.RPCLogs
	getLogsErr    error

	callBlockNum rpc.BlockNumber
	callArgs     ethapi.CallArgs
	callResult   *jsonrpc.GraphQLCallResult
	callErr      error

	estimateGasBlockNum rpc.BlockNumber
	estimateGasArgs     ethapi.CallArgs
	estimateGasResult   uint64
	estimateGasErr      error

	gasPriceResult string
	gasPriceErr    error

	sendRawTx     hexutil.Bytes
	sendRawResult common.Hash
	sendRawErr    error

	pendingTxns []types.Transaction
	pendingErr  error
}

func (m *mockGraphQLAPI) GetBlockDetails(_ context.Context, _ rpc.BlockNumber) (map[string]any, error) {
	return nil, nil
}
func (m *mockGraphQLAPI) GetBlockDetailsByHash(_ context.Context, _ common.Hash) (map[string]any, error) {
	return nil, nil
}
func (m *mockGraphQLAPI) GetLatestBlockNumber(_ context.Context) (uint64, error) { return 0, nil }
func (m *mockGraphQLAPI) GetChainID(_ context.Context) (*uint256.Int, error)     { return nil, nil }
func (m *mockGraphQLAPI) GetAccountInfo(_ context.Context, _ common.Address, _ rpc.BlockNumber) (string, uint64, string, error) {
	return "", 0, "", nil
}
func (m *mockGraphQLAPI) GetAccountStorage(_ context.Context, _ common.Address, _ string, _ rpc.BlockNumber) (string, error) {
	return "", nil
}
func (m *mockGraphQLAPI) GetBlockNumberForTx(_ context.Context, _ common.Hash) (uint64, bool, error) {
	return 0, false, nil
}
func (m *mockGraphQLAPI) SendRawTransaction(_ context.Context, data hexutil.Bytes) (common.Hash, error) {
	m.sendRawTx = data
	return m.sendRawResult, m.sendRawErr
}
func (m *mockGraphQLAPI) Call(_ context.Context, blockNumber rpc.BlockNumber, args ethapi.CallArgs) (*jsonrpc.GraphQLCallResult, error) {
	m.callBlockNum = blockNumber
	m.callArgs = args
	return m.callResult, m.callErr
}
func (m *mockGraphQLAPI) EstimateGas(_ context.Context, blockNumber rpc.BlockNumber, args ethapi.CallArgs) (uint64, error) {
	m.estimateGasBlockNum = blockNumber
	m.estimateGasArgs = args
	return m.estimateGasResult, m.estimateGasErr
}
func (m *mockGraphQLAPI) GasPrice(_ context.Context) (string, error) {
	return m.gasPriceResult, m.gasPriceErr
}
func (m *mockGraphQLAPI) GetLogs(_ context.Context, crit filters.FilterCriteria) (types.RPCLogs, error) {
	m.getLogsCrit = crit
	return m.getLogsResult, m.getLogsErr
}
func (m *mockGraphQLAPI) GetPendingTransactions(_ context.Context) ([]types.Transaction, error) {
	return m.pendingTxns, m.pendingErr
}

func TestBlockResolver_Logs(t *testing.T) {
	blockHash := "0x" + strings.Repeat("aa", 32)
	addr := "0x1234567890123456789012345678901234567890"
	topic := "0x" + strings.Repeat("bb", 32)

	t.Run("success — criteria and result", func(t *testing.T) {
		mock := &mockGraphQLAPI{
			getLogsResult: types.RPCLogs{
				{Log: types.Log{
					Address:     common.HexToAddress(addr),
					BlockNumber: 10,
					BlockHash:   common.HexToHash(blockHash),
					TxHash:      common.HexToHash("0x" + strings.Repeat("cc", 32)),
				}},
			},
		}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		obj := &model.Block{Number: 10, Hash: blockHash}
		filter := model.BlockFilterCriteria{
			Addresses: []string{addr},
			Topics:    [][]string{{topic}},
		}

		got, err := r.Logs(context.Background(), obj, filter)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.getLogsCrit.BlockHash == nil || mock.getLogsCrit.BlockHash.Hex() != blockHash {
			t.Errorf("BlockHash not set correctly in crit: %v", mock.getLogsCrit.BlockHash)
		}
		if len(mock.getLogsCrit.Addresses) != 1 || mock.getLogsCrit.Addresses[0] != common.HexToAddress(addr) {
			t.Errorf("Addresses not set correctly: %v", mock.getLogsCrit.Addresses)
		}
		if len(mock.getLogsCrit.Topics) != 1 || len(mock.getLogsCrit.Topics[0]) != 1 {
			t.Errorf("Topics not set correctly: %v", mock.getLogsCrit.Topics)
		}
		if len(got) != 1 {
			t.Errorf("expected 1 log, got %d", len(got))
		}
	})

	t.Run("error from GetLogs propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{getLogsErr: errors.New("db error")}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Logs(context.Background(), &model.Block{Hash: blockHash}, model.BlockFilterCriteria{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid address returns error", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Logs(context.Background(), &model.Block{Hash: blockHash}, model.BlockFilterCriteria{
			Addresses: []string{"not-an-address"},
		})
		if err == nil {
			t.Fatal("expected error for invalid address, got nil")
		}
	})

	t.Run("invalid topic returns error", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Logs(context.Background(), &model.Block{Hash: blockHash}, model.BlockFilterCriteria{
			Topics: [][]string{{"not-hex"}},
		})
		if err == nil {
			t.Fatal("expected error for invalid topic, got nil")
		}
	})
}

func TestBlockResolver_Call(t *testing.T) {
	toAddr := "0x1234567890123456789012345678901234567890"

	t.Run("success — args and result", func(t *testing.T) {
		mock := &mockGraphQLAPI{
			callResult: &jsonrpc.GraphQLCallResult{
				Data:    hexutil.Bytes{0xde, 0xad},
				GasUsed: 21000,
				Status:  1,
			},
		}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		obj := &model.Block{Number: 5}
		got, err := r.Call(context.Background(), obj, model.CallData{To: &toAddr})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.callBlockNum != 5 {
			t.Errorf("block number not forwarded: got %d", mock.callBlockNum)
		}
		if mock.callArgs.To == nil || *mock.callArgs.To != common.HexToAddress(toAddr) {
			t.Errorf("To address not set: %v", mock.callArgs.To)
		}
		if got.GasUsed != 21000 {
			t.Errorf("GasUsed: want 21000, got %d", got.GasUsed)
		}
		if got.Status != 1 {
			t.Errorf("Status: want 1, got %d", got.Status)
		}
		if got.Data != hexutil.Encode([]byte{0xde, 0xad}) {
			t.Errorf("Data: unexpected value %q", got.Data)
		}
	})

	t.Run("revert — status 0 and gas used", func(t *testing.T) {
		mock := &mockGraphQLAPI{
			callResult: &jsonrpc.GraphQLCallResult{
				Data:    hexutil.Bytes{0x08, 0xc3, 0x79, 0xa0},
				GasUsed: 15000,
				Status:  0,
			},
		}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		got, err := r.Call(context.Background(), &model.Block{Number: 1}, model.CallData{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Status != 0 {
			t.Errorf("Status: want 0, got %d", got.Status)
		}
		if got.GasUsed != 15000 {
			t.Errorf("GasUsed: want 15000, got %d", got.GasUsed)
		}
	})

	t.Run("error from Call propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{callErr: errors.New("execution error")}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Call(context.Background(), &model.Block{Number: 1}, model.CallData{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid to address returns error", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		bad := "not-an-address"
		_, err := r.Call(context.Background(), &model.Block{Number: 1}, model.CallData{To: &bad})
		if err == nil {
			t.Fatal("expected error for invalid to address, got nil")
		}
	})
}

func TestQueryResolver_Logs(t *testing.T) {
	t.Run("fromBlock and toBlock set correctly", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		from := uint64(100)
		to := uint64(200)
		_, err := r.Logs(context.Background(), model.FilterCriteria{FromBlock: &from, ToBlock: &to})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.getLogsCrit.FromBlock == nil || mock.getLogsCrit.FromBlock.Uint64() != 100 {
			t.Errorf("FromBlock: want 100, got %v", mock.getLogsCrit.FromBlock)
		}
		if mock.getLogsCrit.ToBlock == nil || mock.getLogsCrit.ToBlock.Uint64() != 200 {
			t.Errorf("ToBlock: want 200, got %v", mock.getLogsCrit.ToBlock)
		}
	})

	t.Run("nil fromBlock and toBlock not set", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Logs(context.Background(), model.FilterCriteria{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.getLogsCrit.FromBlock != nil || mock.getLogsCrit.ToBlock != nil {
			t.Error("expected nil FromBlock and ToBlock when not provided")
		}
	})

	t.Run("error from GetLogs propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{getLogsErr: errors.New("storage error")}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Logs(context.Background(), model.FilterCriteria{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid address returns error", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Logs(context.Background(), model.FilterCriteria{Addresses: []string{"bad"}})
		if err == nil {
			t.Fatal("expected error for invalid address, got nil")
		}
	})
}

func TestMutationResolver_SendRawTransaction(t *testing.T) {
	validHex := "0x" + strings.Repeat("ab", 20)
	wantHash := common.HexToHash("0x" + strings.Repeat("cc", 32))

	t.Run("success — decodes hex and returns hash", func(t *testing.T) {
		mock := &mockGraphQLAPI{sendRawResult: wantHash}
		r := &mutationResolver{&Resolver{GraphQLAPI: mock}}
		got, err := r.SendRawTransaction(context.Background(), validHex)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != wantHash.Hex() {
			t.Errorf("hash: want %q, got %q", wantHash.Hex(), got)
		}
		wantBytes, _ := hexutil.Decode(validHex)
		if string(mock.sendRawTx) != string(wantBytes) {
			t.Errorf("decoded bytes not forwarded correctly")
		}
	})

	t.Run("invalid hex returns error", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &mutationResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.SendRawTransaction(context.Background(), "not-hex")
		if err == nil {
			t.Fatal("expected error for invalid hex, got nil")
		}
	})

	t.Run("error from SendRawTransaction propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{sendRawErr: errors.New("nonce too low")}
		r := &mutationResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.SendRawTransaction(context.Background(), validHex)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestBlockResolver_EstimateGas(t *testing.T) {
	toAddr := "0x1234567890123456789012345678901234567890"

	t.Run("success — block number and args forwarded", func(t *testing.T) {
		mock := &mockGraphQLAPI{estimateGasResult: 42000}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		got, err := r.EstimateGas(context.Background(), &model.Block{Number: 7}, model.CallData{To: &toAddr})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 42000 {
			t.Errorf("gas: want 42000, got %d", got)
		}
		if mock.estimateGasBlockNum != 7 {
			t.Errorf("block number not forwarded: got %d", mock.estimateGasBlockNum)
		}
		if mock.estimateGasArgs.To == nil || *mock.estimateGasArgs.To != common.HexToAddress(toAddr) {
			t.Errorf("To address not forwarded: %v", mock.estimateGasArgs.To)
		}
	})

	t.Run("error from EstimateGas propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{estimateGasErr: errors.New("out of gas")}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.EstimateGas(context.Background(), &model.Block{Number: 1}, model.CallData{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid calldata returns error before API call", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &blockResolver{&Resolver{GraphQLAPI: mock}}
		bad := "not-an-address"
		_, err := r.EstimateGas(context.Background(), &model.Block{Number: 1}, model.CallData{To: &bad})
		if err == nil {
			t.Fatal("expected error for invalid to address, got nil")
		}
		if mock.estimateGasBlockNum != 0 {
			t.Error("API should not be called when calldata is invalid")
		}
	})
}

func TestQueryResolver_GasPrice(t *testing.T) {
	t.Run("success — returns price string", func(t *testing.T) {
		mock := &mockGraphQLAPI{gasPriceResult: "0x3b9aca00"}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		got, err := r.GasPrice(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "0x3b9aca00" {
			t.Errorf("gasPrice: want %q, got %q", "0x3b9aca00", got)
		}
	})

	t.Run("error from GasPrice propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{gasPriceErr: errors.New("node not ready")}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.GasPrice(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestAccountResolver_Storage(t *testing.T) {
	validAddr := "0x1234567890123456789012345678901234567890"
	validSlot := "0x" + strings.Repeat("ab", 32)

	t.Run("success", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &accountResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Storage(context.Background(), &model.Account{Address: validAddr}, validSlot)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	invalidAddrs := []struct {
		name string
		addr string
	}{
		{"empty", ""},
		{"not hex", "not-an-address"},
		{"too short", "0x1234"},
	}
	for _, tt := range invalidAddrs {
		t.Run("invalid address — "+tt.name, func(t *testing.T) {
			r := &accountResolver{&Resolver{}}
			_, err := r.Storage(context.Background(), &model.Account{Address: tt.addr}, validSlot)
			if err == nil {
				t.Fatalf("address %q: expected error, got nil", tt.addr)
			}
		})
	}

	invalidSlots := []struct {
		name string
		slot string
	}{
		{"no 0x prefix", strings.Repeat("ab", 32)},
		{"too short", "0xabcd"},
		{"too long", "0x" + strings.Repeat("ab", 33)},
		{"decimal string", "12345"},
	}
	for _, tt := range invalidSlots {
		t.Run("invalid slot — "+tt.name, func(t *testing.T) {
			r := &accountResolver{&Resolver{}}
			_, err := r.Storage(context.Background(), &model.Account{Address: validAddr}, tt.slot)
			if err == nil {
				t.Fatalf("slot %q: expected error, got nil", tt.slot)
			}
		})
	}
}

func TestQueryResolver_Transaction_InvalidHash(t *testing.T) {
	r := &queryResolver{&Resolver{}} // GraphQLAPI is nil; error fires before it is called

	tests := []struct {
		name string
		hash string
	}{
		{"empty", ""},
		{"no 0x prefix", "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"},
		{"invalid hex chars", "0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := r.Transaction(context.Background(), tt.hash)
			if err == nil {
				t.Errorf("hash %q: expected error, got nil", tt.hash)
			}
		})
	}
}

func TestQueryResolver_Pending(t *testing.T) {
	t.Run("empty pool — zero count and empty slice", func(t *testing.T) {
		mock := &mockGraphQLAPI{}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		got, err := r.Pending(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected non-nil Pending")
		}
		if got.TransactionCount != 0 {
			t.Errorf("TransactionCount: want 0, got %d", got.TransactionCount)
		}
		if len(got.Transactions) != 0 {
			t.Errorf("Transactions: want empty, got %d", len(got.Transactions))
		}
	})

	t.Run("one pending tx — count and nonce/gas populated", func(t *testing.T) {
		tx := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 50, GasLimit: 1048575}}
		mock := &mockGraphQLAPI{pendingTxns: []types.Transaction{tx}}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		got, err := r.Pending(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.TransactionCount != 1 {
			t.Errorf("TransactionCount: want 1, got %d", got.TransactionCount)
		}
		if len(got.Transactions) != 1 {
			t.Fatalf("Transactions: want 1 entry, got %d", len(got.Transactions))
		}
		if got.Transactions[0].Nonce != "0x32" {
			t.Errorf("Nonce: want 0x32, got %q", got.Transactions[0].Nonce)
		}
		if got.Transactions[0].Gas != 1048575 {
			t.Errorf("Gas: want 1048575, got %d", got.Transactions[0].Gas)
		}
	})

	t.Run("error from GetPendingTransactions propagates", func(t *testing.T) {
		mock := &mockGraphQLAPI{pendingErr: errors.New("pool unavailable")}
		r := &queryResolver{&Resolver{GraphQLAPI: mock}}
		_, err := r.Pending(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestPendingResolver_Account_InvalidAddress(t *testing.T) {
	r := &pendingResolver{&Resolver{}}
	_, err := r.Account(context.Background(), &model.Pending{}, "not-an-address")
	if err == nil {
		t.Fatal("expected error for invalid address, got nil")
	}
}

func TestPendingResolver_Call_ForwardsPendingBlockNumber(t *testing.T) {
	mock := &mockGraphQLAPI{callResult: &jsonrpc.GraphQLCallResult{Status: 1}}
	r := &pendingResolver{&Resolver{GraphQLAPI: mock}}
	_, err := r.Call(context.Background(), &model.Pending{}, model.CallData{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.callBlockNum != rpc.PendingBlockNumber {
		t.Errorf("expected PendingBlockNumber, got %d", mock.callBlockNum)
	}
}

func TestPendingResolver_EstimateGas_ForwardsPendingBlockNumber(t *testing.T) {
	mock := &mockGraphQLAPI{estimateGasResult: 21000}
	r := &pendingResolver{&Resolver{GraphQLAPI: mock}}
	got, err := r.EstimateGas(context.Background(), &model.Pending{}, model.CallData{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 21000 {
		t.Errorf("gas: want 21000, got %d", got)
	}
	if mock.estimateGasBlockNum != rpc.PendingBlockNumber {
		t.Errorf("expected PendingBlockNumber, got %d", mock.estimateGasBlockNum)
	}
}
