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

package accounts

import (
	"bytes"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
)

func nonEmptyCodeHash(b ...byte) CodeHash {
	return InternCodeHash(common.BytesToHash(crypto.Keccak256(b)))
}

// accountVariants exercises every fieldset bit (nonce/balance/incarnation/codehash)
// individually and in combination so the length/encode/decode paths all run.
func accountVariants() map[string]Account {
	return map[string]Account{
		"empty":            {Root: empty.RootHash, CodeHash: EmptyCodeHash},
		"nonce only":       {Nonce: 42, Root: empty.RootHash, CodeHash: EmptyCodeHash},
		"balance only":     {Balance: *uint256.NewInt(1_000_000), Root: empty.RootHash, CodeHash: EmptyCodeHash},
		"incarnation only": {Incarnation: 7, Root: empty.RootHash, CodeHash: EmptyCodeHash},
		"codehash only":    {Root: empty.RootHash, CodeHash: nonEmptyCodeHash(1, 2, 3)},
		"all fields": {
			Nonce:       2,
			Balance:     *uint256.NewInt(1000),
			Root:        common.HexToHash("0x21"),
			CodeHash:    nonEmptyCodeHash(1, 2, 3),
			Incarnation: 4,
		},
		"large nonce/balance": {
			Nonce:    ^uint64(0),
			Balance:  *new(uint256.Int).SetAllOne(),
			Root:     empty.RootHash,
			CodeHash: EmptyCodeHash,
		},
	}
}

func TestNewAccount(t *testing.T) {
	t.Parallel()
	a := NewAccount()
	require.Equal(t, uint64(0), a.Nonce)
	require.True(t, a.Balance.IsZero())
	require.Equal(t, empty.RootHash, a.Root)
	require.Equal(t, EmptyCodeHash, a.CodeHash)
	require.True(t, a.IsEmptyCodeHash())
}

func TestEncodeDecodeForStorage_RoundTrip(t *testing.T) {
	t.Parallel()
	for name, a := range accountVariants() {
		a := a
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			buf := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(buf)

			var got Account
			require.NoError(t, got.DecodeForStorage(buf))

			// Storage encoding omits Root, so compare only the persisted fields.
			require.Equal(t, a.Nonce, got.Nonce)
			require.Equal(t, 0, a.Balance.Cmp(&got.Balance))
			require.Equal(t, a.Incarnation, got.Incarnation)
			require.Equal(t, a.CodeHash, got.CodeHash)
		})
	}
}

func TestDecodeForStorage_Empty(t *testing.T) {
	t.Parallel()
	var a Account
	a.Nonce = 99 // ensure Reset() actually runs
	require.NoError(t, a.DecodeForStorage(nil))
	require.Equal(t, uint64(0), a.Nonce)
}

func TestDecodeForStorage_Malformed(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		enc    []byte
		errSub string
	}{
		"truncated nonce":       {[]byte{0x01, 0x05}, "Account.Nonce"},
		"truncated balance":     {[]byte{0x02, 0x05}, "Account.Nonce"}, // balance path reuses the Nonce message
		"truncated incarnation": {[]byte{0x04, 0x05}, "Account.Incarnation"},
		"codehash wrong length": {[]byte{0x08, 0x10}, "codehash should be 32 bytes long"},
		"truncated codehash":    {[]byte{0x08, 0x20}, "Account.CodeHash"},
	}
	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var a Account
			err := a.DecodeForStorage(tc.enc)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.errSub)
		})
	}
}

func TestEncodeDecodeForHashing_RoundTrip(t *testing.T) {
	t.Parallel()
	for name, a := range accountVariants() {
		a := a
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			enc := a.RLP()
			require.Len(t, enc, int(a.EncodingLengthForHashing()))

			var got Account
			require.NoError(t, got.DecodeForHashing(enc))

			// Hashing encoding omits Incarnation; Root is included.
			require.Equal(t, a.Nonce, got.Nonce)
			require.Equal(t, 0, a.Balance.Cmp(&got.Balance))
			require.Equal(t, a.Root, got.Root)
			require.Equal(t, a.CodeHash, got.CodeHash)
		})
	}
}

func TestEncodeRLP_MatchesRLP(t *testing.T) {
	t.Parallel()
	a := accountVariants()["all fields"]
	var buf bytes.Buffer
	require.NoError(t, a.EncodeRLP(&buf))
	require.Equal(t, a.RLP(), buf.Bytes())
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

func TestEncodeRLP_WriterError(t *testing.T) {
	t.Parallel()
	a := accountVariants()["all fields"]
	require.ErrorContains(t, a.EncodeRLP(failingWriter{}), "boom")
}

func TestDecodeRLP_Stream(t *testing.T) {
	t.Parallel()
	a := accountVariants()["all fields"]
	s := rlp.NewStream(bytes.NewReader(a.RLP()), 0)

	var got Account
	require.NoError(t, got.DecodeRLP(s))
	require.Equal(t, a.Nonce, got.Nonce)
	require.Equal(t, a.Root, got.Root)
	require.Equal(t, a.CodeHash, got.CodeHash)
}

func TestDecodeRLP_StreamError(t *testing.T) {
	t.Parallel()
	var got Account
	s := rlp.NewStream(bytes.NewReader(nil), 0)
	require.Error(t, got.DecodeRLP(s))
}

func TestDecodeForHashing_Errors(t *testing.T) {
	t.Parallel()
	// A struct carrying nonce(5), balance(6), a 32-byte root, then a codehash
	// field that declares a non-32 length — exercises the CodeHash size check.
	codeHashWrongSize := append([]byte{0xe7, 0x05, 0x06, 0xa0}, make([]byte, 32)...)
	codeHashWrongSize = append(codeHashWrongSize, 0x83, 0x01, 0x02, 0x03)

	tests := map[string]struct {
		enc    []byte
		errSub string
	}{
		"length mismatch":     {[]byte{0xc1, 0x00, 0x00}, "malformed RLP"},
		"byte array struct":   {[]byte{0x82, 0x01, 0x02}, "should be RLP struct"},
		"nonce as struct":     {[]byte{0xc1, 0xc0}, "Nonce should be byte array"},
		"balance as struct":   {[]byte{0xc2, 0x05, 0xc0}, "Balance should be byte array"},
		"root wrong size":     {[]byte{0xc6, 0x05, 0x06, 0x83, 0x01, 0x02, 0x03}, "Root should have size 32"},
		"root truncated":      {[]byte{0xc5, 0x05, 0x06, 0xa0, 0x01, 0x02}, "Account.Root"},
		"codehash wrong size": {codeHashWrongSize, "CodeHash should have size 32"},
	}
	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var a Account
			err := a.DecodeForHashing(tc.enc)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.errSub)
		})
	}
}

func TestDecodeForHashing_EmptyStruct(t *testing.T) {
	t.Parallel()
	var a Account
	a.Nonce = 5
	require.NoError(t, a.DecodeForHashing([]byte{0xc0}))
	require.Equal(t, uint64(0), a.Nonce)
	require.Equal(t, empty.RootHash, a.Root)
}

func TestCopyAndSelfCopy_Independent(t *testing.T) {
	t.Parallel()
	src := accountVariants()["all fields"]

	var dst Account
	dst.Copy(&src)
	require.True(t, dst.Equals(&src))
	require.Equal(t, src.Root, dst.Root)

	// Mutating the source must not affect the copy.
	src.Balance.SetUint64(1)
	src.Nonce = 999
	require.NotEqual(t, src.Nonce, dst.Nonce)
	require.NotEqual(t, 0, dst.Balance.Cmp(&src.Balance))

	clone := dst.SelfCopy()
	require.True(t, clone.Equals(&dst))
	require.NotSame(t, &dst, clone)
}

func TestEmpty(t *testing.T) {
	t.Parallel()
	require.True(t, (*Account)(nil).Empty())

	a := NewAccount()
	require.True(t, a.Empty())

	a.Nonce = 1
	require.False(t, a.Empty())
}

func TestEquals(t *testing.T) {
	t.Parallel()
	base := accountVariants()["all fields"]

	same := base.SelfCopy()
	require.True(t, base.Equals(same))

	for name, mutate := range map[string]func(*Account){
		"nonce":       func(x *Account) { x.Nonce++ },
		"incarnation": func(x *Account) { x.Incarnation++ },
		"balance":     func(x *Account) { x.Balance.AddUint64(&x.Balance, 1) },
		"codehash":    func(x *Account) { x.CodeHash = nonEmptyCodeHash(9, 9, 9) },
	} {
		other := base.SelfCopy()
		mutate(other)
		require.False(t, base.Equals(other), "should differ on %s", name)
	}
}

func TestIsEmptyRootAndIncarnationAccessors(t *testing.T) {
	t.Parallel()
	a := NewAccount()
	require.True(t, a.IsEmptyRoot())

	a.Root = common.Hash{}
	require.True(t, a.IsEmptyRoot())

	a.Root = common.HexToHash("0x1234")
	require.False(t, a.IsEmptyRoot())

	a.SetIncarnation(11)
	require.Equal(t, uint64(11), a.GetIncarnation())
}

func TestDecodeIncarnationFromStorage(t *testing.T) {
	t.Parallel()
	for name, a := range accountVariants() {
		a := a
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			buf := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(buf)
			got, err := DecodeIncarnationFromStorage(buf)
			require.NoError(t, err)
			require.Equal(t, a.Incarnation, got)
		})
	}
}

func TestDecodeIncarnationFromStorage_Errors(t *testing.T) {
	t.Parallel()
	got, err := DecodeIncarnationFromStorage(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)

	tests := map[string][]byte{
		"truncated nonce":       {0x01, 0x05},
		"truncated balance":     {0x02, 0x05},
		"truncated incarnation": {0x04, 0x05},
	}
	for name, enc := range tests {
		enc := enc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeIncarnationFromStorage(enc)
			require.Error(t, err)
		})
	}
}

func TestSerialiseDeserialiseV3_RoundTrip(t *testing.T) {
	t.Parallel()
	for name, a := range accountVariants() {
		a := a
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			enc := SerialiseV3(&a)

			var got Account
			require.NoError(t, DeserialiseV3(&got, enc))
			require.Equal(t, a.Nonce, got.Nonce)
			require.Equal(t, 0, a.Balance.Cmp(&got.Balance))
			require.Equal(t, a.Incarnation, got.Incarnation)
			require.Equal(t, a.CodeHash, got.CodeHash)
		})
	}
}

func TestDeserialiseV3_Truncated(t *testing.T) {
	t.Parallel()
	// nonce=0, balance=0, codehash=0 then the buffer ends before the
	// incarnation length byte.
	require.ErrorContains(t, DeserialiseV3(&Account{}, []byte{0x00, 0x00, 0x00}), "deserialse2")
}
