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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

// writeLeafHeaderPerByte reproduces the header byte stream completeLeafHash emitted
// before it assembled the header in one buffer: list prefix, key prefix, then the
// compact key one byte per Write.
func writeLeafHeaderPerByte(w io.Writer, totalLen, compactLen int, key []byte, compact0 byte, ni int) error {
	var lenPrefix [4]byte
	pl := rlp.EncodeListPrefixToBuf(totalLen, lenPrefix[:])
	if _, err := w.Write(lenPrefix[:pl]); err != nil {
		return err
	}
	if compactLen > 1 {
		keyPrefix := [1]byte{0x80 + byte(compactLen)}
		if _, err := w.Write(keyPrefix[:]); err != nil {
			return err
		}
	}
	b := [1]byte{compact0}
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := w.Write(b[:]); err != nil {
			return err
		}
		ni += 2
	}
	return nil
}

// Storage and account leaves at depth 0 produce the longest key completeLeafHash
// ever compacts, which is where its single fixed header buffer would overflow or
// truncate first.
func TestCompleteLeafHashMatchesPerByteHeader(t *testing.T) {
	t.Parallel()

	maxKey := make([]byte, 65)
	for i := range maxKey {
		maxKey[i] = byte(i % 16)
	}
	maxKey[64] = terminatorHexByte

	// accountLeafHashWithKey takes its longer compactLen (len/2+1 rather than
	// (len-1)/2+1) when the key carries no terminator, so that is the branch the
	// header buffer must be sized for.
	noTermOddKey := make([]byte, 65)
	for i := range noTermOddKey {
		noTermOddKey[i] = byte(i % 16)
	}
	noTermEvenKey := maxKey[:64]

	shortKey := []byte{0x1, 0x2, terminatorHexByte}

	storageVal := make([]byte, 32)
	for i := range storageVal {
		storageVal[i] = byte(0xa0 + i)
	}
	accountVal := make([]byte, 70)
	for i := range accountVal {
		accountVal[i] = byte(i)
	}

	for _, tc := range []struct {
		name      string
		key       []byte
		singleton bool
		account   bool
	}{
		{"storage max-length key, singleton", maxKey, true, false},
		{"storage max-length key, embeddable", maxKey, false, false},
		{"storage even-length key", noTermEvenKey, false, false},
		{"storage short key", shortKey, false, false},
		{"account max-length key", maxKey, true, true},
		{"account max-length key, no terminator", noTermOddKey, true, true},
		{"account even-length key, no terminator", noTermEvenKey, true, true},
		{"account short key", shortKey, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hph := NewHexPatriciaHashed(length.Addr, nil, TrieConfig{})
			defer hph.Release()

			var got []byte
			var err error
			var val rlp.RlpSerializable
			if tc.account {
				val = rlp.RlpEncodedBytes(accountVal)
				got, err = hph.accountLeafHashWithKey(nil, tc.key, val)
			} else {
				storage := rlp.RlpSerializableBytes(storageVal)
				val = storage
				got, err = hph.leafHashWithKeyVal(nil, tc.key, storage, tc.singleton)
			}
			require.NoError(t, err)

			compactLen, compact0, ni := compactKeyParams(tc.key, tc.account)
			kp := 0
			kl := 1
			if compactLen > 1 {
				kp, kl = 1, compactLen
			}
			totalLen := kp + kl + val.DoubleRLPLen()

			ref := NewHexPatriciaHashed(length.Addr, nil, TrieConfig{})
			defer ref.Release()
			singleton := tc.singleton || tc.account
			canEmbed := !singleton && totalLen+rlp.ListPrefixLen(totalLen) < length.Hash

			var writer io.Writer
			if canEmbed {
				ref.auxBuffer.Reset()
				writer = ref.auxBuffer
			} else {
				ref.keccak.Reset()
				writer = ref.keccak
			}
			require.NoError(t, writeLeafHeaderPerByte(writer, totalLen, compactLen, tc.key, compact0, ni))
			var prefixBuf [8]byte
			require.NoError(t, val.ToDoubleRLP(writer, prefixBuf[:]))

			var want []byte
			if canEmbed {
				want = ref.auxBuffer.Bytes()
			} else {
				var hashBuf [33]byte
				hashBuf[0] = 0x80 + length.Hash
				_, err := ref.keccak.Read(hashBuf[1:])
				require.NoError(t, err)
				want = hashBuf[:]
			}
			require.Equal(t, want, got)
		})
	}
}

// compactKeyParams mirrors the compact-encoding decisions leafHashWithKeyVal and
// accountLeafHashWithKey make before delegating to completeLeafHash.
func compactKeyParams(key []byte, account bool) (compactLen int, compact0 byte, ni int) {
	if account {
		if nibbles.HasTerm(key) {
			compactLen = (len(key)-1)/2 + 1
			if len(key)&1 == 0 {
				return compactLen, 48 + key[0], 1
			}
			return compactLen, 32, 0
		}
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			return compactLen, terminatorHexByte + key[0], 1
		}
		return compactLen, 0, 0
	}
	compactLen = (len(key)-1)/2 + 1
	if len(key)&1 == 0 {
		return compactLen, 0x30 + key[0], 1
	}
	return compactLen, 0x20, 0
}
