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

	"github.com/anacrolix/torrent/metainfo"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
)

// haveCompletePayload reports whether the payload requested for name under
// want is already on disk and finished. The metainfo sidecar is written only
// once the payload completes, so a sidecar that parses and declares want,
// beside the data file, is the completion signal. A malformed or info-less
// sidecar proves nothing and leaves the file to be re-fetched, which is the
// existing repair path.
//
// Re-downloading a finished file is not harmless: the torrent client opens the
// data file for writing, which truncates it, and any mapping the aggregator
// holds over that file is invalidated — the next read faults with SIGBUS,
// killing the process. A file we already hold in full is never worth that.
// A file held under another infohash is a different generation: it is not what
// was asked for, and reporting it as held pairs it with the requested
// generation's siblings.
func haveCompletePayload(snapDir, name string, want metainfo.Hash) bool {
	dataPath := snapshotinv.ResolveExistingPath(snapDir, name)
	if _, err := os.Stat(dataPath); err != nil {
		return false
	}
	mi, err := metainfo.LoadFromFile(dataPath + ".torrent")
	if err != nil {
		return false
	}
	if _, err := mi.UnmarshalInfo(); err != nil {
		return false
	}
	return mi.HashInfoBytes() == want
}
