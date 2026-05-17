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
	"math"
	"math/big"
	"testing"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc"
)

type mockGraphQLAPI struct {
	getAccountInfo func(ctx context.Context, address common.Address, blockNumber rpc.BlockNumber) (string, uint64, string, error)
}

func (m mockGraphQLAPI) GetBlockDetails(context.Context, rpc.BlockNumber) (map[string]any, error) {
	return nil, nil
}

func (m mockGraphQLAPI) GetChainID(context.Context) (*big.Int, error) {
	return big.NewInt(1), nil
}

func (m mockGraphQLAPI) GetAccountInfo(ctx context.Context, address common.Address, blockNumber rpc.BlockNumber) (string, uint64, string, error) {
	if m.getAccountInfo != nil {
		return m.getAccountInfo(ctx, address, blockNumber)
	}
	return "0x0", 0, "0x", nil
}

func (m mockGraphQLAPI) GetAccountStorage(context.Context, common.Address, string, rpc.BlockNumber) (string, error) {
	return "0x0", nil
}

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

func TestBlockResolverLogsAppliesFilter(t *testing.T) {
	block := &model.Block{
		Logs: []*model.Log{
			{
				Index:   0,
				Account: &model.Account{Address: "0x1111111111111111111111111111111111111111"},
				Topics: []string{
					"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				},
			},
			{
				Index:   1,
				Account: &model.Account{Address: "0x2222222222222222222222222222222222222222"},
				Topics: []string{
					"0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
			},
		},
	}

	resolver := &blockResolver{&Resolver{}}
	logs, err := resolver.Logs(context.Background(), block, model.BlockFilterCriteria{
		Addresses: []string{"0x1111111111111111111111111111111111111111"},
		Topics: [][]string{
			{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			{"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}
	if logs[0].Index != 0 {
		t.Fatalf("expected first log to match, got index %d", logs[0].Index)
	}
}

func TestBlockResolverLogsTopicWildcardDoesNotRequireTopicPosition(t *testing.T) {
	block := &model.Block{
		Logs: []*model.Log{
			{
				Index: 0,
				Topics: []string{
					"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
			},
		},
	}

	resolver := &blockResolver{&Resolver{}}
	logs, err := resolver.Logs(context.Background(), block, model.BlockFilterCriteria{
		Topics: [][]string{
			{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}
}

func TestBlockResolverLogsFallsBackToFlattenTransactionsWhenLogsNil(t *testing.T) {
	block := &model.Block{
		Logs: nil,
		Transactions: []*model.Transaction{
			{
				Logs: []*model.Log{
					{Index: 7},
				},
			},
		},
	}

	resolver := &blockResolver{&Resolver{}}
	logs, err := resolver.Logs(context.Background(), block, model.BlockFilterCriteria{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 flattened log, got %d", len(logs))
	}
	if logs[0].Index != 7 {
		t.Fatalf("expected flattened log index 7, got %d", logs[0].Index)
	}
}

func TestTransactionResolverFromUsesRequestedBlock(t *testing.T) {
	const address = "0x1111111111111111111111111111111111111111"

	var gotAddress common.Address
	var gotBlock rpc.BlockNumber

	resolver := &transactionResolver{&Resolver{
		GraphQLAPI: mockGraphQLAPI{
			getAccountInfo: func(_ context.Context, addr common.Address, blockNumber rpc.BlockNumber) (string, uint64, string, error) {
				gotAddress = addr
				gotBlock = blockNumber
				return "0x10", 7, "0x6000", nil
			},
		},
	}}

	tx := &model.Transaction{From: model.NewAccountAtBlock(12)}
	tx.From.Address = address
	targetBlock := uint64(99)

	account, err := resolver.From(context.Background(), tx, &targetBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAddress != common.HexToAddress(address) {
		t.Fatalf("expected address %s, got %s", address, gotAddress)
	}
	if gotBlock != rpc.BlockNumber(targetBlock) {
		t.Fatalf("expected block %d, got %d", targetBlock, gotBlock)
	}
	if account.BlockNum != targetBlock {
		t.Fatalf("expected result block %d, got %d", targetBlock, account.BlockNum)
	}
}

func TestLogResolverAccountFallsBackToEmbeddedBlock(t *testing.T) {
	const address = "0x3333333333333333333333333333333333333333"

	var gotBlock rpc.BlockNumber

	resolver := &logResolver{&Resolver{
		GraphQLAPI: mockGraphQLAPI{
			getAccountInfo: func(_ context.Context, _ common.Address, blockNumber rpc.BlockNumber) (string, uint64, string, error) {
				gotBlock = blockNumber
				return "0x20", 3, "0x6001", nil
			},
		},
	}}

	logEntry := &model.Log{Account: model.NewAccountAtBlock(55)}
	logEntry.Account.Address = address

	account, err := resolver.Account(context.Background(), logEntry, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotBlock != rpc.BlockNumber(55) {
		t.Fatalf("expected embedded block 55, got %d", gotBlock)
	}
	if account.BlockNum != 55 {
		t.Fatalf("expected result block 55, got %d", account.BlockNum)
	}
}

func TestTransactionResolverFromRejectsOverflowRequestedBlock(t *testing.T) {
	const address = "0x1111111111111111111111111111111111111111"

	called := false
	resolver := &transactionResolver{&Resolver{
		GraphQLAPI: mockGraphQLAPI{
			getAccountInfo: func(_ context.Context, _ common.Address, _ rpc.BlockNumber) (string, uint64, string, error) {
				called = true
				return "0x10", 7, "0x6000", nil
			},
		},
	}}

	tx := &model.Transaction{From: model.NewAccountAtBlock(12)}
	tx.From.Address = address
	overflow := uint64(math.MaxInt64) + 1

	_, err := resolver.From(context.Background(), tx, &overflow)
	if err == nil {
		t.Fatal("expected overflow error, got nil")
	}
	if called {
		t.Fatal("expected resolver to reject overflow before GetAccountInfo call")
	}
}
