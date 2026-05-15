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
	"strings"
	"testing"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
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
