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
	"crypto/sha1"
	"testing"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

func metainfoOverContent(t *testing.T, name string, content []byte) *metainfo.MetaInfo {
	t.Helper()
	const pieceLength = 1 << 14
	var pieces []byte
	for off := 0; off < len(content); off += pieceLength {
		end := min(off+pieceLength, len(content))
		sum := sha1.Sum(content[off:end])
		pieces = append(pieces, sum[:]...)
	}
	info := metainfo.Info{
		Name:        name,
		Length:      int64(len(content)),
		PieceLength: pieceLength,
		Pieces:      pieces,
	}
	infoBytes, err := bencode.Marshal(info)
	require.NoError(t, err)
	return &metainfo.MetaInfo{InfoBytes: infoBytes}
}

// Metainfo is an input to a torrent, taken from peers and sources that are
// equally untrusted. Installing info bytes that belong to another generation
// replaces the piece hashes the payload is verified against, so that payload
// completes under the infohash we asked for and nothing downstream notices.
func TestInfoBytesMatchInfoHash(t *testing.T) {
	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	const name = "domain/v2.2-commitment.332-333.kv"
	wanted := metainfoOverContent(t, name, bytes.Repeat([]byte("a"), 3<<14))
	foreign := metainfoOverContent(t, name, bytes.Repeat([]byte("b"), 4<<14))
	require.NotEqual(t, wanted.HashInfoBytes(), foreign.HashInfoBytes())

	swapped, _ := client.AddTorrentInfoHash(wanted.HashInfoBytes())
	require.NoError(t, swapped.SetInfoBytes(foreign.InfoBytes),
		"info bytes are installed as given; validating them is the caller's job")
	require.False(t, infoBytesMatchInfoHash(swapped))

	intact, _ := client.AddTorrentInfoHash(wanted.HashInfoBytes())
	require.NoError(t, intact.SetInfoBytes(wanted.InfoBytes))
	require.True(t, infoBytesMatchInfoHash(intact))
}
