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

package state

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// writeV4KVFixture writes a minimal, well-formed v4-shape .kv into the
// given domain's snapshot dir at the v4 naming for (fromTxN, toTxN).
// Returns the final path. Two keys are written so seg's compressor
// produces a non-empty stream.
func writeV4KVFixture(t *testing.T, ctx context.Context, agg *Aggregator, domain kv.Domain, fromTxN, toTxN uint64) string {
	t.Helper()
	finalPath := agg.DomainKVFilePathV4(domain, fromTxN, toTxN)
	tmpDir := t.TempDir()
	comp, err := seg.NewCompressor(ctx, "v4-tail-fixture", finalPath, tmpDir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	writer := seg.NewWriter(comp, agg.d[domain].Compression)
	for _, kvp := range []struct{ k, v []byte }{
		{[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("value-1")},
		{[]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("value-2")},
	} {
		_, err = writer.Write(kvp.k)
		require.NoError(t, err)
		_, err = writer.Write(kvp.v)
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
	_, err = os.Stat(finalPath)
	require.NoError(t, err, "expected v4 fixture at %s", finalPath)
	return finalPath
}

// TestDomain_IntegrateDirtyFiles_V4TailUsesRawTxNFromFile pins that when
// retire hands integrateDirtyFiles a v4-shaped on-disk file (mode-C v4
// tail case), the resulting FilesItem carries the raw (fromTxN, toTxN)
// parsed from the file name, NOT the step-aligned (txNumFrom, txNumTo)
// the caller passed. Without this, IsRawTxN misclassifies the v4 tail
// and V4PairForStep can't recognise the pair — the background merger
// never composes them, leaving the pre-target range without coverage.
func TestDomain_IntegrateDirtyFiles_V4TailUsesRawTxNFromFile(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	d := agg.d[kv.AccountsDomain]

	// v4 tail file at [1050, 1100) — mid-step start, step-aligned end.
	const fromTxN, toTxN = uint64(1050), uint64(1100)
	finalPath := writeV4KVFixture(t, ctx, agg, kv.AccountsDomain, fromTxN, toTxN)

	dec, err := seg.NewDecompressor(finalPath)
	require.NoError(t, err)
	t.Cleanup(dec.Close)

	sf := StaticFiles{valuesDecomp: dec}

	// Retire calls integrateDirtyFiles with STEP-ALIGNED args
	// (FirstTxNumOfStep(step), FirstTxNumOfStep(step+1)) even though the
	// on-disk file is only a tail. Simulate that here.
	d.integrateDirtyFiles(sf, 1000, 1100)

	// Assert the FilesItem in dirtyFiles has RAW coords matching the
	// on-disk file, not the step-aligned (1000, 1100) the caller passed.
	var found *FilesItem
	d.dirtyFiles.Scan(func(item *FilesItem) bool {
		if item.decompressor == dec {
			found = item
			return false
		}
		return true
	})
	require.NotNil(t, found, "integrateDirtyFiles must insert a FilesItem carrying the v4 fixture")
	require.Equal(t, fromTxN, found.startTxNum, "startTxNum must be the raw fromTxN parsed from the v4 file name")
	require.Equal(t, toTxN, found.endTxNum, "endTxNum must be the raw toTxN parsed from the v4 file name")
	require.True(t, found.IsRawTxN(d.stepSize), "FilesItem must classify as v4 (IsRawTxN=true) so V4PairForStep can pair it")
}

// TestAggregator_IntegrateDirtyFiles_V4TailSkipsSubsumptionOfV4One pins
// the retire-time invariant under the universal two-v4-then-merge
// lifecycle: when retire's per-domain output is a v4 tail, the
// pre-existing v4 #1 boundary file MUST stay alive in dirtyFiles.
// The background merger owns composition of the pair into a
// step-aligned file; subsuming v4 #1 at retire time would orphan the
// pre-target range.
func TestAggregator_IntegrateDirtyFiles_V4TailSkipsSubsumptionOfV4One(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	d := agg.d[kv.AccountsDomain]

	// Seed a v4 #1 boundary file in dirtyFiles covering [1000, 1050)
	// — step-aligned start, mid-step end. Simulates the mode-C emit
	// that lands before retire fires.
	const v41From, v41To = uint64(1000), uint64(1050)
	v41Path := writeV4KVFixture(t, ctx, agg, kv.AccountsDomain, v41From, v41To)
	v41Dec, err := seg.NewDecompressor(v41Path)
	require.NoError(t, err)
	t.Cleanup(v41Dec.Close)
	v41Item := newFilesItem(v41From, v41To)
	v41Item.decompressor = v41Dec
	d.dirtyFiles.Set(v41Item)

	// Retire produces v4 #2 tail covering [1050, 1100) — mid-step
	// start, step-aligned end.
	const v42From, v42To = uint64(1050), uint64(1100)
	v42Path := writeV4KVFixture(t, ctx, agg, kv.AccountsDomain, v42From, v42To)
	v42Dec, err := seg.NewDecompressor(v42Path)
	require.NoError(t, err)
	t.Cleanup(v42Dec.Close)

	sf := &AggV3StaticFiles{}
	sf.d[kv.AccountsDomain] = StaticFiles{valuesDecomp: v42Dec}

	// Retire always passes STEP-ALIGNED (FirstTxNumOfStep(step),
	// FirstTxNumOfStep(step+1)) — even when the actual output is a v4
	// tail. Simulate that here.
	agg.IntegrateDirtyFiles(sf, 1000, 1100)

	// v4 #1 must still be present in dirtyFiles — subsumption must be
	// deferred to the background merger.
	stillPresent := false
	d.dirtyFiles.Scan(func(item *FilesItem) bool {
		if item == v41Item {
			stillPresent = true
			return false
		}
		return true
	})
	require.True(t, stillPresent, "v4 #1 boundary must NOT be retired at retire time — background merger owns composition")
	require.False(t, v41Item.canDelete.Load(), "v4 #1 must not be marked canDelete under a v4 tail retire")
}
