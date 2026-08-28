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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
)

type readV3Context struct {
	branch []byte
	calls  int
	mask   uint16
}

func (c *readV3Context) Branch([]byte) ([]byte, kv.Step, error) { return nil, 0, nil }

func (c *readV3Context) BranchWithMask(_ []byte, mask uint16, _ bool) ([]byte, kv.Step, [16]uint16, uint16, error) {
	c.calls++
	c.mask = mask
	return c.branch, 0, [16]uint16{}, 0, nil
}

func (*readV3Context) PutBranch([]byte, []byte, []byte) error { return nil }
func (*readV3Context) Account([]byte) (*Update, error)        { return nil, nil }
func (*readV3Context) Storage([]byte) (*Update, error)        { return nil, nil }

func TestHexPatriciaHashedReadsBranchWithMask(t *testing.T) {
	t.Parallel()

	var cells [16]cellEncodeData
	cells[3] = recordTestData("account", []byte{1, 2})
	legacy, err := NewBranchEncoder(1024).EncodeBranch(1<<3, 1<<3, 1<<3, &cells)
	require.NoError(t, err)

	ctx := &readV3Context{branch: legacy}
	hph := newHexPatriciaHashed()
	hph.cfg.EdgeRecords = true
	hph.ctx = ctx
	hph.rootPresent = true
	hph.rootMask = 1 << 3

	require.NoError(t, hph.unfold([]byte{3}, 1))
	require.Equal(t, 1, ctx.calls)
	require.Equal(t, uint16(1<<3), ctx.mask)
	require.Equal(t, int16(length.Addr), hph.grid[0][3].accountAddrLen)
}

func TestSynthesizeBranchRowMatchesBundledRow(t *testing.T) {
	t.Parallel()

	var source [16]cellEncodeData
	mask := uint16(1<<1 | 1<<5 | 1<<9)
	for nibble, shape := range map[int]string{1: "branch", 5: "account", 9: "storage"} {
		source[nibble] = recordTestData(shape, []byte{1, 2, 3})
		if source[nibble].storageAddrLen > length.Hash {
			source[nibble].storageAddrLen = length.Hash
		}
	}
	legacy, err := NewBranchEncoder(4096).EncodeBranch(mask, mask, mask, &source)
	require.NoError(t, err)

	var records [16][]byte
	for bitset := mask; bitset != 0; bitset &= bitset - 1 {
		nibble := bitsTrailingZeros16(bitset)
		if source[nibble].accountAddrLen > 0 || source[nibble].storageAddrLen > 0 {
			records[nibble] = EncodeLeafChild(&source[nibble])
		} else {
			records[nibble] = EncodeBranchChild(0x4567, &source[nibble])
		}
	}

	read, err := SynthesizeBranchRow(mask, true, records, mask, legacy)
	require.NoError(t, err)

	var bundledCells, synthesizedCells [16]cell
	bundledMaps, err := DecodeBranchInto(legacy[2:], false, &bundledCells)
	require.NoError(t, err)
	synthesizedMaps, err := DecodeBranchInto(read.Data[2:], false, &synthesizedCells)
	require.NoError(t, err)
	require.Equal(t, bundledMaps, synthesizedMaps)
	for bitset := mask; bitset != 0; bitset &= bitset - 1 {
		nibble := bitsTrailingZeros16(bitset)
		require.Equal(t, cellEncodeDataFromCell(&bundledCells[nibble]), cellEncodeDataFromCell(&synthesizedCells[nibble]), "nibble %d", nibble)
	}
}

func TestSynthesizeBranchRowIgnoresClearedMaskRecords(t *testing.T) {
	t.Parallel()

	var source [16]cellEncodeData
	source[2] = recordTestData("account", nil)
	legacy, err := NewBranchEncoder(1024).EncodeBranch(1<<2, 1<<2, 1<<2, &source)
	require.NoError(t, err)

	stale := recordTestData("account", nil)
	var records [16][]byte
	records[7] = EncodeLeafChild(&stale)
	read, err := SynthesizeBranchRow(1<<2, true, records, 1<<7, legacy)
	require.NoError(t, err)

	var cells [16]cell
	maps, err := DecodeBranchInto(read.Data[2:], false, &cells)
	require.NoError(t, err)
	require.Equal(t, uint16(1<<2), maps.AfterMap)
	require.Zero(t, cells[7].accountAddrLen)
}
