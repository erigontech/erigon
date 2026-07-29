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

package commitment

import (
	"encoding/hex"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// The expectations here are hand-written hex, never the encoder's own output:
// the Task 4 oracle consumes this same encoder, so a differential root test
// cannot see a value-encoding bug.
func TestPBinEncodeBasicData(t *testing.T) {
	t.Parallel()

	maxU128 := new(uint256.Int).Sub(new(uint256.Int).Lsh(uint256.NewInt(1), 128), uint256.NewInt(1))

	for _, tc := range []struct {
		name     string
		codeSize uint64
		nonce    uint64
		balance  *uint256.Int
		want     string
	}{
		{
			name:    "empty account",
			balance: uint256.NewInt(0),
			want:    "0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			name:     "distinct bytes in every field",
			codeSize: 0xDEADBEEF,
			nonce:    0x0102030405060708,
			balance:  new(uint256.Int).SetBytes(common.FromHex("0x0102030405060708090a0b0c0d0e0f10")),
			want:     "00000000deadbeef01020304050607080102030405060708090a0b0c0d0e0f10",
		},
		{
			name:     "every field at its maximum",
			codeSize: 0xFFFFFFFF,
			nonce:    0xFFFFFFFFFFFFFFFF,
			balance:  maxU128,
			want:     "00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
		{
			name:     "code_size occupies offsets 4..7 only",
			codeSize: 1,
			balance:  uint256.NewInt(0),
			want:     "0000000000000001000000000000000000000000000000000000000000000000",
		},
		{
			name:    "nonce occupies offsets 8..15 only",
			nonce:   1,
			balance: uint256.NewInt(0),
			want:    "0000000000000000000000000000000100000000000000000000000000000000",
		},
		{
			name:    "balance occupies offsets 16..31 only",
			balance: uint256.NewInt(1),
			want:    "0000000000000000000000000000000000000000000000000000000000000001",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := pbinEncodeBasicData(tc.nonce, tc.balance, tc.codeSize)
			require.NoError(t, err)
			require.Equal(t, tc.want, hex.EncodeToString(got[:]))
			require.Len(t, got, pbinValueLength)
		})
	}
}

func TestPBinEncodeBasicDataVersionAndReservedAreZero(t *testing.T) {
	t.Parallel()

	got, err := pbinEncodeBasicData(0xFFFFFFFFFFFFFFFF, uint256.NewInt(0), 0xFFFFFFFF)
	require.NoError(t, err)
	require.Equal(t, byte(0), got[0], "version")
	require.Equal(t, []byte{0, 0, 0}, got[1:4], "reserved")
}

func TestPBinEncodeBasicDataBalanceOverflow(t *testing.T) {
	t.Parallel()

	twoPow128 := new(uint256.Int).Lsh(uint256.NewInt(1), 128)

	_, err := pbinEncodeBasicData(0, twoPow128, 0)
	require.ErrorIs(t, err, errPBinBalanceOverflow)

	_, err = pbinEncodeBasicData(0, new(uint256.Int).Sub(twoPow128, uint256.NewInt(1)), 0)
	require.NoError(t, err, "2^128-1 is the largest representable balance")

	_, err = pbinEncodeBasicData(0, new(uint256.Int).SetAllOne(), 0)
	require.ErrorIs(t, err, errPBinBalanceOverflow)
}

func TestPBinEncodeBasicDataCodeSizeOverflow(t *testing.T) {
	t.Parallel()

	_, err := pbinEncodeBasicData(0, uint256.NewInt(0), 1<<32)
	require.ErrorIs(t, err, errPBinCodeSizeOverflow)

	_, err = pbinEncodeBasicData(0, uint256.NewInt(0), 1<<32-1)
	require.NoError(t, err)
}

func TestPBinCodeHashValue(t *testing.T) {
	t.Parallel()

	// keccak256("") — what a codeless account's CODE_HASH leaf holds.
	const emptyCodeHash = "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"

	t.Run("contract code hash passes through", func(t *testing.T) {
		t.Parallel()
		h := common.HexToHash("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
		got := pbinCodeHashValue(h)
		require.Equal(t, "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", hex.EncodeToString(got[:]))
	})

	t.Run("zero hash becomes the empty-code hash", func(t *testing.T) {
		t.Parallel()
		got := pbinCodeHashValue(common.Hash{})
		require.Equal(t, emptyCodeHash, hex.EncodeToString(got[:]))
	})

	t.Run("empty-code hash passes through", func(t *testing.T) {
		t.Parallel()
		got := pbinCodeHashValue(common.HexToHash("0x" + emptyCodeHash))
		require.Equal(t, emptyCodeHash, hex.EncodeToString(got[:]))
	})
}

func TestPBinEncodeStorageValue(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "absent value is 32 zero bytes",
			value: "",
			want:  "0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			name:  "one byte is left-padded",
			value: "05",
			want:  "0000000000000000000000000000000000000000000000000000000000000005",
		},
		{
			name:  "short value keeps its byte order",
			value: "0102",
			want:  "0000000000000000000000000000000000000000000000000000000000000102",
		},
		{
			name:  "full-width value passes through",
			value: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			want:  "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		},
		{
			name:  "leading zero byte is preserved",
			value: "0002030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			want:  "0002030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := hex.DecodeString(tc.value)
			require.NoError(t, err)
			got := pbinEncodeStorageValue(raw)
			require.Equal(t, tc.want, hex.EncodeToString(got[:]))
			require.Len(t, got, pbinValueLength)
		})
	}
}

func TestPBinEncodeStorageValueRejectsOversizedValue(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() { pbinEncodeStorageValue(make([]byte, 33)) })
}
