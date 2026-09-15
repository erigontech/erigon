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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
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
			if _, ok := errors.AsType[*rpc.InvalidParamsError](err); !ok {
				t.Errorf("expected *rpc.InvalidParamsError, got %T: %v", err, err)
			}
		})
	}
}

// The graphql_ block responses hand back a map[string]any, so each value carries
// its own encoding rather than the one a struct tag would impose. This pins all
// four to what a direct JSON-RPC caller sees.
func TestMarshalWithdrawalsEncoding(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(marshalWithdrawals(types.Withdrawals{{
		Index:     19_000_042,
		Validator: 881_234,
		Address:   common.HexToAddress("0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"),
		Amount:    63_012_345,
	}}))
	require.NoError(t, err)
	assert.JSONEq(t, `[{"index":"0x121eaea","validator":"0xd7252",`+
		`"address":"0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f","amount":"0x3c17df9"}]`, string(got))

	// A withdrawal amount above 2^63 stays a plain quantity.
	got, err = json.Marshal(marshalWithdrawals(types.Withdrawals{{Amount: hexutil.Uint64(1) << 63}}))
	require.NoError(t, err)
	assert.Contains(t, string(got), `"amount":"0x8000000000000000"`)
}

func TestMarshalWithdrawalsEmpty(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(marshalWithdrawals(nil))
	require.NoError(t, err)
	assert.Equal(t, "[]", string(got))
}
