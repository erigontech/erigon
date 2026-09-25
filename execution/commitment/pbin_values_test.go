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

func TestPBinLeafValueCodecRoundTrip(t *testing.T) {
	t.Parallel()

	basic := [pbinValueLength]byte{
		5: 0x01, 6: 0x02, 7: 0x03,
		12: 0x04, 13: 0x05, 14: 0x06, 15: 0x07,
		27: 0x08, 28: 0x09, 29: 0x0a, 30: 0x0b, 31: 0x0c,
	}
	codeHash := [pbinValueLength]byte{0x01, 0x02, 0x03}
	delegation := [pbinValueLength]byte{0xef, 0x01, 0x00}
	copy(delegation[3:23], []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	})
	header := [pbinValueLength]byte{31: 0x2a}
	reserved := [pbinValueLength]byte{0x01, 0x02, 0x03}
	chunk := [pbinValueLength]byte{0x01, 0x60, 0x02}
	storage := [pbinValueLength]byte{29: 0x2a, 30: 0xbb, 31: 0xcc}

	for _, tc := range []struct {
		name string
		key  []byte
		word [pbinValueLength]byte
		enc  string
	}{
		{
			name: "basic data",
			key:  pbinLeafCodecKey(pbinAccountZone, pbinBasicDataLeafKey),
			word: basic,
			enc:  "06850102030405060708090a0b0c",
		},
		{
			name: "code hash",
			key:  pbinLeafCodecKey(pbinAccountZone, pbinCodeHashLeafKey),
			word: codeHash,
			enc:  hex.EncodeToString(codeHash[:]),
		},
		{
			name: "delegation",
			key:  pbinLeafCodecKey(pbinAccountZone, pbinDelegationLeafKey),
			word: delegation,
			enc:  "0102030405060708090a0b0c0d0e0f1011121314",
		},
		{
			name: "header storage",
			key:  pbinLeafCodecKey(pbinAccountZone, pbinHeaderStorageOffset),
			word: header,
			enc:  "2a",
		},
		{
			name: "reserved low sub-index",
			key:  pbinLeafCodecKey(pbinAccountZone, 3),
			word: reserved,
			enc:  hex.EncodeToString(reserved[:]),
		},
		{
			name: "reserved high sub-index",
			key:  pbinLeafCodecKey(pbinAccountZone, 128),
			word: reserved,
			enc:  hex.EncodeToString(reserved[:]),
		},
		{
			name: "code chunk",
			key:  pbinLeafCodecKey(pbinCodeZone, 7),
			word: chunk,
			enc:  "016002",
		},
		{
			name: "storage slot",
			key:  pbinLeafCodecKey(pbinStorageZone, 7),
			word: storage,
			enc:  "2abbcc",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enc, err := pbinEncodeLeafValue(tc.key, &tc.word)
			require.NoError(t, err)
			require.Equal(t, tc.enc, hex.EncodeToString(enc))
			got, err := pbinDecodeLeafValue(tc.key, enc)
			require.NoError(t, err)
			require.Equal(t, tc.word, got)
		})
	}
}

func TestPBinLeafValueCodecZeroLength(t *testing.T) {
	t.Parallel()

	codeHashKey := pbinLeafCodecKey(pbinAccountZone, pbinCodeHashLeafKey)
	got, err := pbinDecodeLeafValue(codeHashKey, nil)
	require.NoError(t, err)
	require.Equal(t, [pbinValueLength]byte(empty.CodeHash), got)

	storageKey := pbinLeafCodecKey(pbinStorageZone, 7)
	_, err = pbinDecodeLeafValue(storageKey, nil)
	require.Error(t, err)

	zero := [pbinValueLength]byte{}
	_, err = pbinEncodeLeafValue(storageKey, &zero)
	require.Error(t, err)

	codeKey := pbinLeafCodecKey(pbinCodeZone, 7)
	_, err = pbinEncodeLeafValue(codeKey, &zero)
	require.Error(t, err)
	_, err = pbinDecodeLeafValue(codeKey, nil)
	require.Error(t, err)
}

func TestPBinLeafValueCodecDirection(t *testing.T) {
	t.Parallel()

	chunkKey := pbinLeafCodecKey(pbinCodeZone, 7)
	chunk := [pbinValueLength]byte{0x01, 0x02, 0x03}
	chunkEnc, err := pbinEncodeLeafValue(chunkKey, &chunk)
	require.NoError(t, err)
	wordKey := pbinLeafCodecKey(pbinAccountZone, pbinHeaderStorageOffset)
	got, err := pbinDecodeLeafValue(wordKey, chunkEnc)
	require.NoError(t, err)
	require.NotEqual(t, chunk, got)

	word := [pbinValueLength]byte{29: 0x01, 30: 0x02, 31: 0x03}
	wordEnc, err := pbinEncodeLeafValue(wordKey, &word)
	require.NoError(t, err)
	got, err = pbinDecodeLeafValue(chunkKey, wordEnc)
	require.NoError(t, err)
	require.NotEqual(t, word, got)
}

func TestPBinLeafValueCodecRejectsMalformedBasicData(t *testing.T) {
	t.Parallel()

	key := pbinLeafCodecKey(pbinAccountZone, pbinBasicDataLeafKey)
	for _, tc := range []struct {
		name string
		word [pbinValueLength]byte
	}{
		{
			name: "non-zero version",
			word: [pbinValueLength]byte{0: 1},
		},
		{
			name: "non-zero reserved byte",
			word: [pbinValueLength]byte{1: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pbinEncodeLeafValue(key, &tc.word)
			require.Error(t, err, tc.name)
		})
	}
}

func TestPBinLeafValueCodecRejectsMalformedDelegation(t *testing.T) {
	t.Parallel()

	key := pbinLeafCodecKey(pbinAccountZone, pbinDelegationLeafKey)
	for _, tc := range []struct {
		name string
		word [pbinValueLength]byte
	}{
		{
			name: "wrong marker",
			word: [pbinValueLength]byte{0xEF, 0x01, 0x01},
		},
		{
			name: "non-zero trailing byte",
			word: [pbinValueLength]byte{0xEF, 0x01, 0x00, 31: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pbinEncodeLeafValue(key, &tc.word)
			require.Error(t, err, tc.name)
		})
	}
}

func FuzzPBinLeafValueCodec(f *testing.F) {
	words := []struct {
		zone byte
		sub  byte
		word [pbinValueLength]byte
	}{
		{zone: pbinAccountZone, sub: pbinBasicDataLeafKey, word: [pbinValueLength]byte{5: 1, 12: 2, 31: 3}},
		{zone: pbinAccountZone, sub: pbinCodeHashLeafKey, word: [pbinValueLength]byte{0x01}},
		{zone: pbinAccountZone, sub: pbinDelegationLeafKey, word: [pbinValueLength]byte{0xEF, 0x01, 0x00, 3: 1}},
		{zone: pbinAccountZone, sub: pbinHeaderStorageOffset, word: [pbinValueLength]byte{31: 1}},
		{zone: pbinAccountZone, sub: 3, word: [pbinValueLength]byte{0x01}},
		{zone: pbinCodeZone, sub: 7, word: [pbinValueLength]byte{0x01, 0x02}},
		{zone: pbinStorageZone, sub: 7, word: [pbinValueLength]byte{31: 1}},
	}
	for _, seed := range words {
		f.Add(seed.zone, seed.sub,
			binary.BigEndian.Uint64(seed.word[0:8]),
			binary.BigEndian.Uint64(seed.word[8:16]),
			binary.BigEndian.Uint64(seed.word[16:24]),
			binary.BigEndian.Uint64(seed.word[24:32]))
	}

	f.Fuzz(func(t *testing.T, zone, sub byte, a, b, c, d uint64) {
		key := pbinLeafCodecKey(zone, sub)
		word := [pbinValueLength]byte{}
		binary.BigEndian.PutUint64(word[0:8], a)
		binary.BigEndian.PutUint64(word[8:16], b)
		binary.BigEndian.PutUint64(word[16:24], c)
		binary.BigEndian.PutUint64(word[24:32], d)

		enc, err := pbinEncodeLeafValue(key, &word)
		if err != nil {
			return
		}
		got, err := pbinDecodeLeafValue(key, enc)
		require.NoError(t, err)
		require.Equal(t, word, got)
	})
}

func pbinLeafCodecKey(zone, sub byte) []byte {
	keyLen, known := pbinZoneKeyLength(zone)
	if !known {
		keyLen = pbinAccountKeyLength
	}
	key := make([]byte, keyLen)
	key[0] = zone
	key[len(key)-1] = sub
	return key
}
