// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/db/snaptype"
)

// A locally rebuilt snapshot has no metainfo matching the preverified hash, which used to send it
// through invalidateData and rename it to .part with no log.
func TestInvalidateDataPanicsOnCompleteLocalFile(t *testing.T) {
	ctx := t.Context()
	test := newDownloaderTest(t)
	const name = "a.seg"
	dataPath := filepath.Join(test.dirs.Snap, name)
	require.NoError(t, os.WriteFile(dataPath, []byte("locally rebuilt"), 0o644))

	require.Panics(t, func() {
		//nolint:errcheck // the panic is the assertion
		test.downloader.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("aa"), name)
	})

	_, err := os.Stat(dataPath)
	require.NoError(t, err, "data file must survive the panic")
	_, err = os.Stat(dataPath + ".part")
	require.ErrorIs(t, err, os.ErrNotExist, "data file must not be renamed to .part")
}

func TestInvalidateDataAllowsMissingFile(t *testing.T) {
	ctx := t.Context()
	test := newDownloaderTest(t)
	require.NotPanics(t, func() {
		//nolint:errcheck // only checking that the missing-file path stays a no-op
		test.downloader.testStartSingleDownloadNoWait(ctx, snaptype.Hex2InfoHash("aa"), "b.seg")
	})
}
