package downloader

import (
	"io"
	"strings"

	"github.com/erigontech/erigon/db/snaptype"
)

// NewReaderForFile returns a new io.ReadSeekCloser backed by the torrent for
// the named snapshot file. Each call creates a fresh Reader so callers don't
// share seek positions. Returns nil, 0, false if no torrent exists.
//
// This method satisfies the snapshotsync.TorrentLookup interface.
func (d *Downloader) NewReaderForFile(fileName string) (io.ReadSeekCloser, int64, bool) {
	t, ok := d.TorrentByName(fileName)
	if !ok {
		return nil, 0, false
	}

	// Wait for torrent metadata (info dict) before creating a reader
	select {
	case <-t.GotInfo():
	default:
		// Info not available yet — can't create a reader
		return nil, 0, false
	}

	r := t.NewReader()
	r.SetReadahead(64 * 1024) // 64KB readahead — enough for a single record
	return r, t.Length(), true
}

// FindFileForBlock scans known torrents for a .seg file of the given type name
// (e.g. "headers", "bodies", "transactions") that covers blockNum.
// Returns the filename, block range, and ok=true if found.
//
// This method satisfies the snapshotsync.TorrentLookup interface.
func (d *Downloader) FindFileForBlock(typeName string, blockNum uint64) (fileName string, from, to uint64, ok bool) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var bestName string
	var bestFrom, bestTo uint64
	var bestFound bool

	for name := range d.torrentsByName {
		// Only consider .seg files
		if !strings.HasSuffix(name, ".seg") {
			continue
		}

		info, _, parsed := snaptype.ParseFileName("", name)
		if !parsed {
			continue
		}
		if info.TypeString != typeName {
			continue
		}
		if blockNum < info.From || blockNum >= info.To {
			continue
		}

		// Prefer the smallest covering range (most specific segment)
		rangeSize := info.To - info.From
		if !bestFound || rangeSize < (bestTo-bestFrom) {
			bestName = name
			bestFrom = info.From
			bestTo = info.To
			bestFound = true
		}
	}

	if !bestFound {
		return "", 0, 0, false
	}
	return bestName, bestFrom, bestTo, true
}
