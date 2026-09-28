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

package downloadercfg

import (
	"bytes"
	"crypto/sha1"
	"testing"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"
)

// metainfoOver builds a single-file metainfo over content, as a publisher of
// that generation would.
func metainfoOver(t *testing.T, name string, content []byte) *metainfo.MetaInfo {
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

// A webseed is an untrusted source like any peer, and one serving a different
// generation of a file under the same name answers the metainfo-source request
// with that generation's metainfo. Installed unchecked, it leaves the torrent
// holding the requested infohash while it downloads and completes the other
// generation's payload.
func TestMetainfoSourcesMergerRejectsForeignInfohash(t *testing.T) {
	cfg := newTestCfg(t, NewCfgOpts{})
	client, err := torrent.NewClient(cfg.ClientConfig)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	const name = "domain/v2.2-commitment.332-333.kv"
	wanted := metainfoOver(t, name, bytes.Repeat([]byte("a"), 3<<14))
	foreign := metainfoOver(t, name, bytes.Repeat([]byte("b"), 4<<14))
	require.NotEqual(t, wanted.HashInfoBytes(), foreign.HashInfoBytes())

	tor, _ := client.AddTorrentInfoHash(wanted.HashInfoBytes())

	require.Error(t, cfg.ClientConfig.MetainfoSourcesMerger(tor, foreign))
	require.Nil(t, tor.Info())

	require.NoError(t, cfg.ClientConfig.MetainfoSourcesMerger(tor, wanted))
	require.NotNil(t, tor.Info())
}
