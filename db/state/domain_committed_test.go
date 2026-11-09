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
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

func TestCommitmentBranchReferenced(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)
	// commitment.DefaultKeyReferencingMinSteps == 2, so a range >= 2 steps reaches the threshold.
	const fromAbove = uint64(0)
	const toAbove = uint64(20) // 2 steps -> threshold reached
	const fromBelow = uint64(0)
	const toBelow = uint64(10) // 1 step -> below threshold

	cases := []struct {
		name        string
		fileVersion version.Version
		from, to    uint64
		want        bool
	}{
		{"v2.0 above threshold is referenced", version.V2_0, fromAbove, toAbove, true},
		{"v1.0 above threshold is referenced", version.V1_0, fromAbove, toAbove, true},
		{"v2.1 above threshold is referenced", version.V2_1, fromAbove, toAbove, true},
		{"v2.0 below threshold is plain", version.V2_0, fromBelow, toBelow, false},
		{"v2.1 below threshold is plain", version.V2_1, fromBelow, toBelow, false},
		{"v2.2 above threshold is plain", version.V2_2, fromAbove, toAbove, false},
		{"v2.2 below threshold is plain", version.V2_2, fromBelow, toBelow, false},
		{"zero version (missing file) above threshold is referenced", version.Version{}, fromAbove, toAbove, true},
		{"zero version (missing file) below threshold is plain", version.Version{}, fromBelow, toBelow, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CommitmentBranchReferenced(tc.fileVersion, stepSize, tc.from, tc.to)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLookupByShortenedKeyDoesNotKeepAnMmapSlice(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-storage.0-1.kv")
	c, err := seg.NewCompressor(t.Context(), t.Name(), path, t.TempDir(), seg.DefaultCfg, log.LvlError, log.New())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.AddUncompressedWord([]byte("key")))
	require.NoError(t, c.Compress())
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	dt := &DomainRoTx{}

	got, found := dt.lookupByShortenedKey(EncodeReferenceKey(make([]byte, binary.MaxVarintLen64), 0), seg.NewReader(d.MakeGetter(), seg.CompressNone))

	require.True(t, found)
	require.Equal(t, "key", string(got))
	got[0] = 'x' // faults on a read-only mapping
}
