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

type recordWarmupCtx struct {
	maskRecordingCtx
	edgeRecords bool
	records     map[string][16][]byte
	present     map[string]uint16
	recordCalls []maskCall
}

func (c *recordWarmupCtx) EdgeRecords() bool { return c.edgeRecords }

func (c *recordWarmupCtx) BranchRecords(prefix []byte, mask uint16, maskKnown bool) ([16][]byte, uint16, kv.Step, error) {
	c.recordCalls = append(c.recordCalls, maskCall{prefix: string(prefix), mask: mask, maskKnown: maskKnown})
	return c.records[string(prefix)], c.present[string(prefix)], 0, nil
}

// The v3 warmup reads records directly and carries each child's own bitmap down.
func TestWarmupRecordDescentCarriesChildMask(t *testing.T) {
	t.Parallel()

	const nibble = 3
	const childMask = uint16(0x0f0f)

	var branchCell cellEncodeData
	branchCell.hashLen = length.Hash
	for i := range branchCell.hash {
		branchCell.hash[i] = byte(i + 1)
	}
	var records [16][]byte
	records[nibble] = EncodeBranchChild(childMask, &branchCell)

	rootPrefix := string(nibbles.HexToCompact(nil))
	ctx := &recordWarmupCtx{
		edgeRecords: true,
		records:     map[string][16][]byte{rootPrefix: records},
		present:     map[string]uint16{rootPrefix: 1 << nibble},
	}

	w := &Warmuper{maxDepth: 64}
	w.warmupKey(ctx, []byte{nibble, 5, 7}, 0)

	require.GreaterOrEqual(t, len(ctx.recordCalls), 2, "v3 warmup must descend past the root")
	require.Empty(t, ctx.calls, "v3 warmup must not build a legacy row")
	require.False(t, ctx.recordCalls[0].maskKnown)
	require.True(t, ctx.recordCalls[1].maskKnown, "the child's bitmap was not carried down")
	require.Equal(t, childMask, ctx.recordCalls[1].mask)
}

// A v2 context implements BranchRecordReader too; taking the record path there would warm nothing.
func TestWarmupV2ContextDoesNotTakeTheRecordPath(t *testing.T) {
	t.Parallel()

	var cells [16]cellEncodeData
	cells[3] = recordTestData("branch", nil)
	row, err := NewBranchEncoder(1024).EncodeBranch(1<<3, 1<<3, 1<<3, &cells)
	require.NoError(t, err)

	rootPrefix := string(nibbles.HexToCompact(nil))
	ctx := &recordWarmupCtx{edgeRecords: false}
	ctx.rows = map[string][]byte{rootPrefix: append([]byte(nil), row...)}

	w := &Warmuper{maxDepth: 64}
	w.warmupKey(ctx, []byte{3, 5, 7}, 0)

	require.Empty(t, ctx.recordCalls, "a v2 context must not be read as records")
	require.NotEmpty(t, ctx.calls, "v2 warmup must still descend through the legacy row")
}
