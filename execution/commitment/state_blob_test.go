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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func encodeLegacyStateBlob(t *testing.T, root []byte, rootFlags stateRootFlag, afterMap [128]uint16) []byte {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.BigEndian, int8(rootFlags)))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, uint16(len(root))))
	_, err := buf.Write(root)
	require.NoError(t, err)
	require.NoError(t, binary.Write(&buf, binary.BigEndian, make([]byte, 128)))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, [128]uint16{}))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, afterMap))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, uint64(0)))
	require.NoError(t, binary.Write(&buf, binary.BigEndian, uint64(0)))
	return buf.Bytes()
}

func TestStateBlobRoundTrip(t *testing.T) {
	t.Parallel()

	s := state{
		Root:        bytes.Repeat([]byte{0x37}, 128),
		RootMask:    0xa55a,
		RootPresent: true,
		RootTouched: true,
		RootChecked: true,
	}

	encoded, err := s.Encode(nil)
	require.NoError(t, err)
	require.Equal(t, len(s.Root)+5, len(encoded))

	var decoded state
	require.NoError(t, decoded.Decode(encoded))
	require.Equal(t, s.Root, decoded.Root)
	require.Equal(t, s.RootMask, decoded.RootMask)
	require.Equal(t, s.RootPresent, decoded.RootPresent)
	require.Equal(t, s.RootTouched, decoded.RootTouched)
	require.Equal(t, s.RootChecked, decoded.RootChecked)
}

func TestStateBlobDecodesLegacyState(t *testing.T) {
	t.Parallel()

	root := bytes.Repeat([]byte{0x42}, 128)
	var afterMap [128]uint16
	afterMap[0] = 0x1234
	legacy := encodeLegacyStateBlob(t, root, stateRootPresent|stateRootTouched, afterMap)

	var decoded state
	require.NoError(t, decoded.Decode(legacy))
	require.Equal(t, root, decoded.Root)
	require.Equal(t, uint16(0x1234), decoded.RootMask)
	require.True(t, decoded.RootPresent)
	require.True(t, decoded.RootTouched)
	require.False(t, decoded.RootChecked)
}

func TestStateBlobSizeReduction(t *testing.T) {
	t.Parallel()

	root := bytes.Repeat([]byte{0x91}, 128)
	var afterMap [128]uint16
	legacy := encodeLegacyStateBlob(t, root, 0, afterMap)
	current, err := (&state{Root: root}).Encode(nil)
	require.NoError(t, err)

	require.Equal(t, 654, len(legacy)-len(current))
}

func TestStateBlobRejectsActiveRows(t *testing.T) {
	t.Parallel()

	hph := newHexPatriciaHashed()
	hph.currentKeyLen = 1
	require.PanicsWithValue(t, "currentKeyLen > 0", func() {
		_, _ = hph.EncodeCurrentState(nil)
	})

	hph.currentKeyLen = 0
	hph.activeRows = 1
	encoded, err := (&state{}).Encode(nil)
	require.NoError(t, err)
	require.ErrorContains(t, hph.SetState(encoded), "active rows")
}

func TestCommitmentStateKeyOrdering(t *testing.T) {
	t.Parallel()

	require.Equal(t, []byte{0x00}, KeyCommitmentState)
	for _, path := range [][]byte{{}, {0x00}, {0x0f}, {0x00, 0x0f}, {0x0f, 0x00, 0x0a}} {
		nodeKey := nibbles.EncodeKeyV3(path)
		for nibble := range 16 {
			childKey := nibbles.ChildKeyV3(nodeKey, byte(nibble))
			require.Less(t, bytes.Compare(KeyCommitmentState, childKey), 0)
		}
	}

	isPersistedNodeKey := func(key []byte) bool {
		if len(key) < 2 {
			return false
		}
		_, err := nibbles.DecodeKeyV3(key)
		return err == nil
	}
	require.False(t, isPersistedNodeKey(KeyCommitmentState))
	require.False(t, nibbles.IsChildKeyV3(KeyCommitmentState))
}
