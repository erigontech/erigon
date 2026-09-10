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

package types

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestWithdrawalsHash(t *testing.T) {
	t.Parallel()
	w := &Withdrawal{
		Index:     0,
		Validator: 0,
		Address:   common.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"),
		Amount:    1,
	}
	withdrawals := Withdrawals([]*Withdrawal{w})
	hash := DeriveSha(withdrawals)
	// The only trie node is short (its RLP < 32 bytes).
	// Its Keccak should be returned, not the node itself.
	assert.Equal(t, common.HexToHash("82cc6fbe74c41496b382fcdf25216c5af7bdbb5a3929e8f2e61bd6445ab66436"), hash)
}

// goldenWithdrawals covers a single-byte, a multi-byte and a max-uint64 value
// for each of the three integer fields.
func goldenWithdrawals() Withdrawals {
	return Withdrawals{
		{Index: 0, Validator: 0, Address: common.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"), Amount: 1},
		{Index: 19_000_042, Validator: 881_234, Address: common.HexToAddress("0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"), Amount: 63_012_345},
		{Index: math.MaxUint64, Validator: math.MaxUint64, Address: common.HexToAddress("0xffffffffffffffffffffffffffffffffffffffff"), Amount: math.MaxUint64},
	}
}

func TestWithdrawalRLPGolden(t *testing.T) {
	t.Parallel()
	want := []string{
		"d88080946295ee1b4f6dd65047762f924ecd367c17eabf8f01",
		"e3840121eaea830d725294b9d7934878b5fb9610b3fe8a5e441e8fad7e293f8403c17df9",
		"f088ffffffffffffffff88ffffffffffffffff94ffffffffffffffffffffffffffffffffffffffff88ffffffffffffffff",
	}
	wantSize := []int{24, 35, 48}

	ws := goldenWithdrawals()
	for i, w := range ws {
		var buf bytes.Buffer
		require.NoError(t, w.EncodeRLP(&buf))
		assert.Equal(t, want[i], hex.EncodeToString(buf.Bytes()))
		assert.Equal(t, wantSize[i], w.EncodingSize())

		var got Withdrawal
		require.NoError(t, rlp.DecodeBytes(buf.Bytes(), &got))
		assert.Equal(t, *w, got)
	}

	assert.Equal(t, common.HexToHash("0xa8777398fb18ac804a6a35b2fe689f25cd731e2d7cacc305eac1372495a21368"), DeriveSha(ws))
}

func TestWithdrawalJSONGolden(t *testing.T) {
	t.Parallel()
	const want = `[{"index":"0x0","validatorIndex":"0x0","address":"0x6295ee1b4f6dd65047762f924ecd367c17eabf8f","amount":"0x1"},` +
		`{"index":"0x121eaea","validatorIndex":"0xd7252","address":"0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f","amount":"0x3c17df9"},` +
		`{"index":"0xffffffffffffffff","validatorIndex":"0xffffffffffffffff","address":"0xffffffffffffffffffffffffffffffffffffffff","amount":"0xffffffffffffffff"}]`

	ws := goldenWithdrawals()
	got, err := json.Marshal(ws)
	require.NoError(t, err)
	assert.JSONEq(t, want, string(got))

	var back Withdrawals
	require.NoError(t, json.Unmarshal(got, &back))
	assert.Equal(t, ws, back)
}

// Null is rejected because hexutil.Uint64 and common.Address carry their own
// UnmarshalJSON. The generated decoder tolerated it for the opposite reason: it
// decoded into pointer fields, and encoding/json nils the pointer on null
// without reaching the element type at all, which its non-nil guard then read
// as an absent field.
func TestWithdrawalJSONRejectsNull(t *testing.T) {
	t.Parallel()
	for _, input := range []string{
		`{"index":null}`,
		`{"validatorIndex":null}`,
		`{"address":null}`,
		`{"amount":null}`,
		`{"index":1}`,
	} {
		var w Withdrawal
		assert.Error(t, json.Unmarshal([]byte(input), &w), input)
	}
}
