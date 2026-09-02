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

package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snaptype2"
)

// An epoch block segment at the merge limit must be seedable like its decimal counterpart. Its name
// carries the "-ep" marker as an extra dash-separated part, which the pre-filter in
// seedableSegmentFiles must tolerate — otherwise no epoch segment ever gets a .torrent and the chain
// cannot be published at all.
func TestSeedableSegmentFilesAcceptsEpoch(t *testing.T) {
	dir := t.TempDir()
	full := snaptype2.Headers.FileInfo(dir, true, 0, 524_288)  // at EpochMergeLimit -> seedable
	partial := snaptype2.Headers.FileInfo(dir, true, 0, 8_192) // below it -> not seedable
	for _, f := range []string{full.Name(), partial.Name()} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte{}, 0o644))
	}

	got, err := seedableSegmentFiles(dir, "mainnet", false)
	require.NoError(t, err)
	require.Equal(t, []string{full.Name()}, got)
}
