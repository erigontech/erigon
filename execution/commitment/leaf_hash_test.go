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

	// Storage keys reach an even length by starting deeper, never by dropping the
	// terminator, which the call sites always append.
	evenKey := maxKey[1:]

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
		{"storage even-length key", evenKey, false, false},
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

			ref := NewHexPatriciaHashed(length.Addr, nil, TrieConfig{})
			defer ref.Release()
			want, err := referenceLeafHash(ref, tc.key, val, tc.account, tc.singleton || tc.account)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// The table above pins named shapes; this sweeps every key length the nibble
// arrays permit, so an undersized leafRlpBuf cannot pass by staying below the
// longest key the buffer is sized for.
func TestCompleteLeafHashAllKeyLengths(t *testing.T) {
	t.Parallel()

	storageVal := make([]byte, 32)
	for i := range storageVal {
		storageVal[i] = byte(0xa0 + i)
	}
	accountVal := make([]byte, 110)
	for i := range accountVal {
		accountVal[i] = byte(i)
	}

	for keyLen := 1; keyLen <= len(cell{}.hashedExtension); keyLen++ {
		for _, terminated := range []bool{false, true} {
			for _, account := range []bool{false, true} {
				key := make([]byte, keyLen)
				for i := range key {
					key[i] = byte(i % 16)
				}
				if terminated {
					key[keyLen-1] = terminatorHexByte
				}

				hph := NewHexPatriciaHashed(length.Addr, nil, TrieConfig{})
				var got []byte
				var err error
				var val rlp.RlpSerializable
				if account {
					val = rlp.RlpEncodedBytes(accountVal)
					got, err = hph.accountLeafHashWithKey(nil, key, val)
				} else {
					storage := rlp.RlpSerializableBytes(storageVal)
					val = storage
					got, err = hph.leafHashWithKeyVal(nil, key, storage, true)
				}
				require.NoError(t, err, "keyLen=%d terminated=%v account=%v", keyLen, terminated, account)

				ref := NewHexPatriciaHashed(length.Addr, nil, TrieConfig{})
				want, err := referenceLeafHash(ref, key, val, account, true)
				require.NoError(t, err)
				require.Equal(t, want, got, "keyLen=%d terminated=%v account=%v", keyLen, terminated, account)

				ref.Release()
				hph.Release()
			}
		}
	}
}

// referenceLeafHash reproduces completeLeafHash as it behaved before the header was
// assembled in one buffer. It writes through the same witness wrapper production uses,
// so enabling witness tracing cannot make the reference and the real path diverge.
func referenceLeafHash(ref *HexPatriciaHashed, key []byte, val rlp.RlpSerializable, account, singleton bool) ([]byte, error) {
	compactLen, compact0, ni := compactKeyParams(key, account)
	kp, kl := 0, 1
	if compactLen > 1 {
		kp, kl = 1, compactLen
	}
	totalLen := kp + kl + val.DoubleRLPLen()
	canEmbed := !singleton && totalLen+rlp.ListPrefixLen(totalLen) < length.Hash

	var writer io.Writer
	if canEmbed {
		ref.auxBuffer.Reset()
		writer = ref.auxBuffer
	} else {
		ref.keccak.Reset()
		writer = ref.witness.leafWriter(ref.keccak)
	}
	if err := writeLeafHeaderPerByte(writer, totalLen, compactLen, key, compact0, ni); err != nil {
		return nil, err
	}
	var prefixBuf [8]byte
	if err := val.ToDoubleRLP(writer, prefixBuf[:]); err != nil {
		return nil, err
	}
	if canEmbed {
		return ref.auxBuffer.Bytes(), nil
	}
	hashBuf := make([]byte, 33)
	hashBuf[0] = 0x80 + length.Hash
	if _, err := ref.keccak.Read(hashBuf[1:]); err != nil {
		return nil, err
	}
	return hashBuf, nil
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
