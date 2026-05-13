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
	"testing"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
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
