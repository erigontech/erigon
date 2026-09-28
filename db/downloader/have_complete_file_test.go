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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

// writeMetainfo writes a sidecar whose info section parses, and returns the
// infohash it declares.
func writeMetainfo(t *testing.T, path string, length int64) metainfo.Hash {
	t.Helper()
	info := metainfo.Info{Name: filepath.Base(path), Length: length, PieceLength: 1 << 14}
	require.NoError(t, info.GeneratePieces(func(metainfo.FileInfo) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(make([]byte, length))), nil
	}))
	mi := metainfo.MetaInfo{}
	var err error
	mi.InfoBytes, err = bencode.Marshal(info)
	require.NoError(t, err)
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, mi.Write(f))
	require.NoError(t, f.Close())
	return mi.HashInfoBytes()
}

// TestHaveCompletePayload pins the check that keeps a download from rewriting
// a file we already hold.
//
// Rewriting truncates, which invalidates any mapping the aggregator holds over
// that file and faults the next reader with SIGBUS. State files live in a kind
// subdir, so the check has to resolve through the layout — a bare join finds
// nothing and lets the rewrite through.
func TestHaveCompletePayload(t *testing.T) {
	snapDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(snapDir, "domain"), 0o755))

	write := func(rel string) {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, rel), []byte("x"), 0o644))
	}

	const stateName = "v2.1-commitment.331-332.kvi"
	var anyHash metainfo.Hash
	require.False(t, haveCompletePayload(snapDir, stateName, anyHash),
		"nothing on disk yet")

	write("domain/" + stateName)
	require.False(t, haveCompletePayload(snapDir, stateName, anyHash),
		"payload without its sidecar is an unfinished download; fetching it is correct")

	held := writeMetainfo(t, filepath.Join(snapDir, "domain", stateName+".torrent"), 1)
	require.True(t, haveCompletePayload(snapDir, stateName, held),
		"payload plus sidecar means we hold it in full — rewriting it would fault "+
			"any reader holding a mapping")

	other := writeMetainfo(t, filepath.Join(snapDir, "domain", stateName+".other"), 2)
	require.NotEqual(t, held, other)
	require.False(t, haveCompletePayload(snapDir, stateName, other),
		"another generation of the same name is not the payload that was asked for; "+
			"reporting it as held pairs it with the requested generation's siblings")

	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "domain", stateName+".torrent"), []byte("not bencode"), 0o644))
	require.False(t, haveCompletePayload(snapDir, stateName, held),
		"a malformed sidecar proves nothing; the repair path must still re-fetch")

	const blockName = "v1.1-003680-003690-headers.seg"
	write(blockName)
	blockHash := writeMetainfo(t, filepath.Join(snapDir, blockName+".torrent"), 1)
	require.True(t, haveCompletePayload(snapDir, blockName, blockHash),
		"block files live at the root and must resolve too")
}
