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

package jsonrpc

import (
	"context"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc"
)

// TestGetAccountStorage_InvalidSlot checks that malformed slot strings are
// rejected with InvalidParamsError before any DB access occurs.
func TestGetAccountStorage_InvalidSlot(t *testing.T) {
	api := &GraphQLAPIImpl{} // db is nil; validation must fire before BeginTemporalRo

	tests := []struct {
		name string
		slot string
	}{
		{"non-hex string", "not-hex"},
		{"too long (33 bytes)", "0x" + strings.Repeat("ab", 33)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := api.GetAccountStorage(context.Background(), common.Address{}, tt.slot, rpc.BlockNumber(0))
			if _, ok := err.(*rpc.InvalidParamsError); !ok {
				t.Errorf("expected *rpc.InvalidParamsError, got %T: %v", err, err)
			}
		})
	}
}
