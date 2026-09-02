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
	"bytes"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
)

// TestPBinIsDelegationClassifiesByBytes pins that classification reads the code
// bytes and nothing else: 23 bytes opening with the marker. Code whose keccak
// hash begins with the marker is still code.
func TestPBinIsDelegationClassifiesByBytes(t *testing.T) {
	t.Parallel()

	marker := []byte{0xEF, 0x01, 0x00}
	indicator := append(bytes.Clone(marker), bytes.Repeat([]byte{0xAB}, 20)...)
	require.True(t, pbinIsDelegation(indicator))

	hashGrindsToMarker := pbinMustHex(t, "0x0000000000000000000000000000000000000000637401")
	require.Len(t, hashGrindsToMarker, pbinDelegationCodeLength)
	h := keccak.Sum256(hashGrindsToMarker)
	require.Equal(t, marker, h[:3], "the ground value must still hash to the marker")
	require.False(t, pbinIsDelegation(hashGrindsToMarker))

	require.False(t, pbinIsDelegation(append(bytes.Clone(marker), bytes.Repeat([]byte{0xAB}, 19)...)))
	require.False(t, pbinIsDelegation(append(bytes.Clone(marker), bytes.Repeat([]byte{0xAB}, 21)...)))
	require.False(t, pbinIsDelegation(marker))
	require.False(t, pbinIsDelegation(nil))
}

func TestPBinEncodeDelegationPadsToThirtyTwo(t *testing.T) {
	t.Parallel()

	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0xCD}, 20)...)
	v := pbinEncodeDelegation(indicator)
	require.Equal(t, indicator, v[:pbinDelegationCodeLength])
	require.Equal(t, make([]byte, pbinValueLength-pbinDelegationCodeLength), v[pbinDelegationCodeLength:])

	chunk := pbinChunkifyCode(indicator)[0]
	require.NotEqual(t, chunk, v,
		"an indicator is not chunk-encoded: byte 0 carries code, not a PUSHDATA count")
}

// TestPBinDelegationLeafIsExclusive pins the header rule: an account holds
// exactly one of the CODE_HASH and DELEGATION leaves, decided by its current
// code bytes, and every write removes the other leaf.
func TestPBinDelegationLeafIsExclusive(t *testing.T) {
	t.Parallel()

	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0x11}, 20)...)

	t.Run("fresh EOA delegates", func(t *testing.T) {
		t.Parallel()

		addr := pbinOracleAddr(91)
		corpus := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, indicator)
		_, root := corpus.process(t)

		basic, err := pbinEncodeBasicData(1, &corpus.updates[0].Balance, pbinDelegationCodeLength)
		require.NoError(t, err)
		delegation := pbinEncodeDelegation(indicator)
		want := pbinOracleRoot([]pbinOracleEntry{
			{key: pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), value: basic[:]},
			{key: pbinTreeKeyAccount(addr, pbinDelegationLeafKey), value: delegation[:]},
		})
		require.Equal(t, want[:], root, "a delegated account is BASIC_DATA plus the indicator: no code-hash leaf, no chunks")
	})

	t.Run("delegation replaces contract code", func(t *testing.T) {
		t.Parallel()

		addr := pbinOracleAddr(92)
		code := pbinTestCode(62)
		deploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, code)
		delegate := new(pbinTestCorpus).accountWithCodeBytes(addr, 2, 10, indicator)
		_, _, forward := pbinTestBatches(t, deploy, delegate)

		want := delegate.entries(t)
		oldHash := keccak.Sum256(code)
		for i, chunk := range pbinChunkifyCode(code) {
			want = append(want, pbinOracleEntry{key: pbinTreeKeyCodeChunk(oldHash, i), value: chunk[:]})
		}
		wantRoot := pbinOracleRoot(want)
		require.Equal(t, wantRoot[:], forward,
			"the code-hash leaf goes; the old chunks stay, content-addressed by the old hash")
	})

	t.Run("delegation cleared to empty code", func(t *testing.T) {
		t.Parallel()

		addr := pbinOracleAddr(93)
		delegate := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, indicator)
		cleared := new(pbinTestCorpus).account(addr, 2, 10, empty.CodeHash)
		_, _, forward := pbinTestBatches(t, delegate, cleared)

		require.Equal(t, cleared.oracleRoot(t), forward,
			"clearing restores the empty-code CODE_HASH leaf and removes the indicator")
	})

	t.Run("two authorities one target", func(t *testing.T) {
		t.Parallel()

		a, b := pbinOracleAddr(94), pbinOracleAddr(95)
		corpus := new(pbinTestCorpus).
			accountWithCodeBytes(a, 1, 10, indicator).
			accountWithCodeBytes(b, 2, 20, indicator)
		_, root := corpus.process(t)

		basicA, err := pbinEncodeBasicData(1, &corpus.updates[0].Balance, pbinDelegationCodeLength)
		require.NoError(t, err)
		basicB, err := pbinEncodeBasicData(2, &corpus.updates[1].Balance, pbinDelegationCodeLength)
		require.NoError(t, err)
		delegation := pbinEncodeDelegation(indicator)
		want := pbinOracleRoot([]pbinOracleEntry{
			{key: pbinTreeKeyAccount(a, pbinBasicDataLeafKey), value: basicA[:]},
			{key: pbinTreeKeyAccount(a, pbinDelegationLeafKey), value: delegation[:]},
			{key: pbinTreeKeyAccount(b, pbinBasicDataLeafKey), value: basicB[:]},
			{key: pbinTreeKeyAccount(b, pbinDelegationLeafKey), value: delegation[:]},
		})
		require.Equal(t, want[:], root,
			"each authority holds its own header leaf; the shared target adds no shared leaf")
	})
}
