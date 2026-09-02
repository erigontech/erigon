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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type maskCall struct {
	prefix    string
	mask      uint16
	maskKnown bool
}

type maskRecordingCtx struct {
	rows  map[string][]byte
	masks map[string][16]uint16
	known map[string]uint16
	calls []maskCall
}

func (c *maskRecordingCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	c.calls = append(c.calls, maskCall{prefix: string(prefix)})
	return c.rows[string(prefix)], 0, nil
}

func (c *maskRecordingCtx) BranchWithMask(prefix []byte, mask uint16, maskKnown bool) ([]byte, kv.Step, [16]uint16, uint16, error) {
	c.calls = append(c.calls, maskCall{prefix: string(prefix), mask: mask, maskKnown: maskKnown})
	return c.rows[string(prefix)], 0, c.masks[string(prefix)], c.known[string(prefix)], nil
}

func (*maskRecordingCtx) PutBranch([]byte, []byte, []byte) error { return nil }
func (*maskRecordingCtx) Account([]byte) (*Update, error)        { return nil, nil }
func (*maskRecordingCtx) Storage([]byte) (*Update, error)        { return nil, nil }

// A v3 parent record carries its child's bitmap. Descending without it makes the next node read
// probe all 16 nibbles and scan every commitment file instead of popcount(mask) point lookups.
func TestWarmupCarriesChildMaskIntoTheNextDescent(t *testing.T) {
	t.Parallel()

	const nibble = 3
	const childMask = uint16(0x00f0)

	var cells [16]cellEncodeData
	cells[nibble] = recordTestData("branch", nil)
	row, err := NewBranchEncoder(1024).EncodeBranch(1<<nibble, 1<<nibble, 1<<nibble, &cells)
	require.NoError(t, err)

	rootPrefix := string(nibbles.HexToCompact(nil))
	var advertised [16]uint16
	advertised[nibble] = childMask

	ctx := &maskRecordingCtx{
		rows:  map[string][]byte{rootPrefix: append([]byte(nil), row...)},
		masks: map[string][16]uint16{rootPrefix: advertised},
		known: map[string]uint16{rootPrefix: 1 << nibble},
	}

	w := &Warmuper{maxDepth: 64}
	w.warmupKey(ctx, []byte{nibble, 5, 7}, 0)

	require.GreaterOrEqual(t, len(ctx.calls), 2, "warmup must descend past the root")
	require.False(t, ctx.calls[0].maskKnown, "the root has no parent to learn its mask from")
	require.True(t, ctx.calls[1].maskKnown, "the child mask the parent record carried was discarded")
	require.Equal(t, childMask, ctx.calls[1].mask)
}

// The storage-root read hands back the masks for its children; without stamping them, the first
// descent below each row-0 cell probes all 16 nibbles again.
func TestUnfoldStorageBaseStampsChildMasks(t *testing.T) {
	t.Parallel()

	const nibble = 5
	const childMask = uint16(0x0303)

	var cells [16]cellEncodeData
	cells[nibble] = recordTestData("branch", nil)
	row, err := NewBranchEncoder(1024).EncodeBranch(1<<nibble, 1<<nibble, 1<<nibble, &cells)
	require.NoError(t, err)

	accPrefix := make([]byte, 64)
	for i := range accPrefix {
		accPrefix[i] = byte(i % 16)
	}
	key := string(nibbles.HexToCompact(accPrefix))

	var advertised [16]uint16
	advertised[nibble] = childMask
	ctx := &maskRecordingCtx{
		rows:  map[string][]byte{key: append([]byte(nil), row...)},
		masks: map[string][16]uint16{key: advertised},
		known: map[string]uint16{key: 1 << nibble},
	}

	base := newHexPatriciaHashed()
	base.cfg.EdgeRecords = true
	base.ctx = ctx
	require.NoError(t, unfoldStorageBase(base, accPrefix))

	require.True(t, base.grid[0][nibble].branchMaskKnown, "storage-root children lost the masks the record carried")
	require.Equal(t, childMask, base.grid[0][nibble].branchMask)
}
