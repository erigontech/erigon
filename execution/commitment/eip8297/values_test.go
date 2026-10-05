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

package eip8297

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

// The expectations are hand-written hex, never the encoder's own output: the
// oracle consumes this same encoder, so a differential root test would not see
// a value-encoding bug.
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
			got, err := EncodeBasicData(tc.nonce, tc.balance, tc.codeSize)
			require.NoError(t, err)
			require.Equal(t, tc.want, hex.EncodeToString(got[:]))
			require.Len(t, got, ValueLength)
		})
	}
}

func TestPBinEncodeBasicDataVersionAndReservedAreZero(t *testing.T) {
	t.Parallel()

	got, err := EncodeBasicData(0xFFFFFFFFFFFFFFFF, uint256.NewInt(0), 0xFFFFFFFF)
	require.NoError(t, err)
	require.Equal(t, byte(0), got[0], "version")
	require.Equal(t, []byte{0, 0, 0}, got[1:4], "reserved")
}

func TestPBinEncodeBasicDataBalanceOverflow(t *testing.T) {
	t.Parallel()

	twoPow128 := new(uint256.Int).Lsh(uint256.NewInt(1), 128)

	_, err := EncodeBasicData(0, twoPow128, 0)
	require.ErrorIs(t, err, ErrBalanceOverflow)

	_, err = EncodeBasicData(0, new(uint256.Int).Sub(twoPow128, uint256.NewInt(1)), 0)
	require.NoError(t, err, "2^128-1 is the largest representable balance")

	_, err = EncodeBasicData(0, new(uint256.Int).SetAllOne(), 0)
	require.ErrorIs(t, err, ErrBalanceOverflow)
}

func TestPBinEncodeBasicDataCodeSizeOverflow(t *testing.T) {
	t.Parallel()

	_, err := EncodeBasicData(0, uint256.NewInt(0), 1<<32)
	require.ErrorIs(t, err, ErrCodeSizeOverflow)

	_, err = EncodeBasicData(0, uint256.NewInt(0), 1<<32-1)
	require.NoError(t, err)
}

func TestPBinCodeHashValue(t *testing.T) {
	t.Parallel()

	// keccak256("") — what a codeless account's CODE_HASH leaf holds.
	const emptyCodeHash = "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"

	t.Run("contract code hash passes through", func(t *testing.T) {
		t.Parallel()
		h := common.HexToHash("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
		got := CodeHashValue(h)
		require.Equal(t, "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", hex.EncodeToString(got[:]))
	})

	t.Run("zero hash becomes the empty-code hash", func(t *testing.T) {
		t.Parallel()
		got := CodeHashValue(common.Hash{})
		require.Equal(t, emptyCodeHash, hex.EncodeToString(got[:]))
	})

	t.Run("empty-code hash passes through", func(t *testing.T) {
		t.Parallel()
		got := CodeHashValue(common.HexToHash("0x" + emptyCodeHash))
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
			got := EncodeStorageValue(raw)
			require.Equal(t, tc.want, hex.EncodeToString(got[:]))
			require.Len(t, got, ValueLength)
		})
	}
}

func TestPBinEncodeStorageValueRejectsOversizedValue(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() { EncodeStorageValue(make([]byte, 33)) })
}

func TestPBinLeafValueCodecRoundTrip(t *testing.T) {
	t.Parallel()

	basic := [ValueLength]byte{
		5: 0x01, 6: 0x02, 7: 0x03,
		12: 0x04, 13: 0x05, 14: 0x06, 15: 0x07,
		27: 0x08, 28: 0x09, 29: 0x0a, 30: 0x0b, 31: 0x0c,
	}
	codeHash := [ValueLength]byte{0x01, 0x02, 0x03}
	delegation := [ValueLength]byte{0xef, 0x01, 0x00}
	copy(delegation[3:23], []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	})
	header := [ValueLength]byte{31: 0x2a}
	reserved := [ValueLength]byte{0x01, 0x02, 0x03}
	chunk := [ValueLength]byte{0x01, 0x60, 0x02}
	storage := [ValueLength]byte{29: 0x2a, 30: 0xbb, 31: 0xcc}

	for _, tc := range []struct {
		name string
		key  []byte
		word [ValueLength]byte
		enc  string
	}{
		{
			name: "basic data",
			key:  leafCodecKey(AccountZone, BasicDataLeafKey),
			word: basic,
			enc:  "06850102030405060708090a0b0c",
		},
		{
			name: "code hash",
			key:  leafCodecKey(AccountZone, CodeHashLeafKey),
			word: codeHash,
			enc:  hex.EncodeToString(codeHash[:]),
		},
		{
			name: "delegation",
			key:  leafCodecKey(AccountZone, DelegationLeafKey),
			word: delegation,
			enc:  "0102030405060708090a0b0c0d0e0f1011121314",
		},
		{
			name: "header storage",
			key:  leafCodecKey(AccountZone, HeaderStorageOffset),
			word: header,
			enc:  "2a",
		},
		{
			name: "reserved low sub-index",
			key:  leafCodecKey(AccountZone, 3),
			word: reserved,
			enc:  hex.EncodeToString(reserved[:]),
		},
		{
			name: "reserved high sub-index",
			key:  leafCodecKey(AccountZone, 128),
			word: reserved,
			enc:  hex.EncodeToString(reserved[:]),
		},
		{
			name: "code chunk",
			key:  leafCodecKey(CodeZone, 7),
			word: chunk,
			enc:  "016002",
		},
		{
			name: "storage slot",
			key:  leafCodecKey(StorageZone, 7),
			word: storage,
			enc:  "2abbcc",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enc, err := EncodeLeafValue(tc.key, &tc.word)
			require.NoError(t, err)
			require.Equal(t, tc.enc, hex.EncodeToString(enc))
			got, err := DecodeLeafValue(tc.key, enc)
			require.NoError(t, err)
			require.Equal(t, tc.word, got)
		})
	}
}

func TestPBinLeafValueCodecZeroLength(t *testing.T) {
	t.Parallel()

	codeHashKey := leafCodecKey(AccountZone, CodeHashLeafKey)
	got, err := DecodeLeafValue(codeHashKey, nil)
	require.NoError(t, err)
	require.Equal(t, [ValueLength]byte(empty.CodeHash), got)

	storageKey := leafCodecKey(StorageZone, 7)
	_, err = DecodeLeafValue(storageKey, nil)
	require.Error(t, err)

	zero := [ValueLength]byte{}
	_, err = EncodeLeafValue(storageKey, &zero)
	require.Error(t, err)

	codeKey := leafCodecKey(CodeZone, 7)
	_, err = EncodeLeafValue(codeKey, &zero)
	require.Error(t, err)
	_, err = DecodeLeafValue(codeKey, nil)
	require.Error(t, err)
}

func TestPBinLeafValueCodecRejectsNonCanonicalReencoding(t *testing.T) {
	t.Parallel()

	codeHashKey := leafCodecKey(AccountZone, CodeHashLeafKey)
	storageKey := leafCodecKey(StorageZone, 7)
	codeKey := leafCodecKey(CodeZone, 7)

	for _, tc := range []struct {
		name string
		key  []byte
		enc  []byte
	}{
		{name: "empty code hash alias", key: codeHashKey, enc: empty.CodeHash[:]},
		{name: "storage leading zero", key: storageKey, enc: []byte{0, 0x42}},
		{name: "code trailing zero", key: codeKey, enc: []byte{0x42, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeLeafValue(tc.key, tc.enc)
			require.ErrorIs(t, err, ErrLeafValue)
		})
	}
}

func TestPBinLeafValueCodecDirection(t *testing.T) {
	t.Parallel()

	chunkKey := leafCodecKey(CodeZone, 7)
	chunk := [ValueLength]byte{0x01, 0x02, 0x03}
	chunkEnc, err := EncodeLeafValue(chunkKey, &chunk)
	require.NoError(t, err)
	wordKey := leafCodecKey(AccountZone, HeaderStorageOffset)
	got, err := DecodeLeafValue(wordKey, chunkEnc)
	require.NoError(t, err)
	require.NotEqual(t, chunk, got)

	word := [ValueLength]byte{29: 0x01, 30: 0x02, 31: 0x03}
	wordEnc, err := EncodeLeafValue(wordKey, &word)
	require.NoError(t, err)
	got, err = DecodeLeafValue(chunkKey, wordEnc)
	require.NoError(t, err)
	require.NotEqual(t, word, got)
}

func TestPBinLeafValueCodecRejectsMalformedBasicData(t *testing.T) {
	t.Parallel()

	key := leafCodecKey(AccountZone, BasicDataLeafKey)
	for _, tc := range []struct {
		name string
		word [ValueLength]byte
	}{
		{
			name: "non-zero version",
			word: [ValueLength]byte{0: 1},
		},
		{
			name: "non-zero reserved byte",
			word: [ValueLength]byte{1: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := EncodeLeafValue(key, &tc.word)
			require.Error(t, err, tc.name)
		})
	}
}

func TestPBinLeafValueCodecRejectsMalformedDelegation(t *testing.T) {
	t.Parallel()

	key := leafCodecKey(AccountZone, DelegationLeafKey)
	for _, tc := range []struct {
		name string
		word [ValueLength]byte
	}{
		{
			name: "wrong marker",
			word: [ValueLength]byte{0xEF, 0x01, 0x01},
		},
		{
			name: "non-zero trailing byte",
			word: [ValueLength]byte{0xEF, 0x01, 0x00, 31: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := EncodeLeafValue(key, &tc.word)
			require.Error(t, err, tc.name)
		})
	}
}

func FuzzPBinLeafValueCodec(f *testing.F) {
	words := []struct {
		zone byte
		sub  byte
		word [ValueLength]byte
	}{
		{zone: AccountZone, sub: BasicDataLeafKey, word: [ValueLength]byte{5: 1, 12: 2, 31: 3}},
		{zone: AccountZone, sub: CodeHashLeafKey, word: [ValueLength]byte{0x01}},
		{zone: AccountZone, sub: DelegationLeafKey, word: [ValueLength]byte{0xEF, 0x01, 0x00, 3: 1}},
		{zone: AccountZone, sub: HeaderStorageOffset, word: [ValueLength]byte{31: 1}},
		{zone: AccountZone, sub: 3, word: [ValueLength]byte{0x01}},
		{zone: CodeZone, sub: 7, word: [ValueLength]byte{0x01, 0x02}},
		{zone: StorageZone, sub: 7, word: [ValueLength]byte{31: 1}},
	}
	for _, seed := range words {
		f.Add(seed.zone, seed.sub,
			binary.BigEndian.Uint64(seed.word[0:8]),
			binary.BigEndian.Uint64(seed.word[8:16]),
			binary.BigEndian.Uint64(seed.word[16:24]),
			binary.BigEndian.Uint64(seed.word[24:32]))
	}

	f.Fuzz(func(t *testing.T, zone, sub byte, a, b, c, d uint64) {
		key := leafCodecKey(zone, sub)
		word := [ValueLength]byte{}
		binary.BigEndian.PutUint64(word[0:8], a)
		binary.BigEndian.PutUint64(word[8:16], b)
		binary.BigEndian.PutUint64(word[16:24], c)
		binary.BigEndian.PutUint64(word[24:32], d)

		enc, err := EncodeLeafValue(key, &word)
		if err != nil {
			return
		}
		got, err := DecodeLeafValue(key, enc)
		require.NoError(t, err)
		require.Equal(t, word, got)
	})
}

func leafCodecKey(zone, sub byte) []byte {
	keyLen, known := ZoneKeyLength(zone)
	if !known {
		keyLen = AccountKeyLength
	}
	key := make([]byte, keyLen)
	key[0] = zone
	key[len(key)-1] = sub
	return key
}
