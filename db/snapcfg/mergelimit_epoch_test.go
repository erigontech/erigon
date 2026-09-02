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

package snapcfg_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
)

// With epoch=true the block merge limit is the epoch frozen size and the merge steps are the epoch
// ladder. Caplin keeps its own regime.
func TestMergeLimitEpochRounding(t *testing.T) {
	cfg := &snapcfg.Cfg{}

	require.Equal(t, uint64(snaptype.EpochMergeLimit), snapcfg.MergeLimitFromCfg(cfg, snaptype.Unknown, true, 21_000_000))
	require.Equal(t, []uint64{524_288, 65_536, 8_192}, snapcfg.MergeStepsFromCfg(cfg, snaptype.Unknown, true, 21_000_000))

	// Caplin shards by slot and keeps its own limit, even when asked in the epoch regime.
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), cfg.MergeLimit(snaptype.CaplinEnums.BeaconBlocks, false, 21_000_000))
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), cfg.MergeLimit(snaptype.CaplinEnums.BeaconBlocks, true, 21_000_000))
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), cfg.MergeLimit(snaptype.CaplinEnums.BlobSidecars, true, 21_000_000))
}

// The decimal merge limit/steps are what a non-epoch chain (Aura) gets; the regime comes from the
// explicit epoch argument, not from the type.
func TestMergeLimitDecimal(t *testing.T) {
	cfg := &snapcfg.Cfg{}
	require.Equal(t, uint64(snaptype.Erigon2MergeLimit), cfg.MergeLimit(snaptype.Unknown, false, 21_000_000))
	require.Equal(t, []uint64{100_000, 10_000}, snapcfg.MergeStepsFromCfg(cfg, snaptype.Unknown, false, 21_000_000))
}

// A full-size (524288) epoch segment is frozen and seedable; a smaller tier is neither. The ranges
// are 1024- (not 1000-) aligned, so the FileInfo is built directly to keep the exact block bounds.
func TestSeedableFrozenEpoch(t *testing.T) {
	cfg := &snapcfg.Cfg{}

	frozen := snaptype.FileInfo{Epoch: true, From: 0, To: snaptype.EpochMergeLimit, Type: snaptype2.Headers} // 524288
	require.True(t, cfg.IsFrozen(frozen))
	require.True(t, cfg.Seedable(frozen))

	small := snaptype.FileInfo{Epoch: true, From: 0, To: 8_192, Type: snaptype2.Headers} // one era1 tier
	require.False(t, cfg.IsFrozen(small))
	require.False(t, cfg.Seedable(small))
}
