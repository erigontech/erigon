package downloader

import (
	"encoding/hex"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon/common/log/v3"
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

	// Wait for torrent metadata (info dict) before creating a reader.
	// For sparse mode, metadata may still be fetching from peers, so
	// block with a timeout rather than returning immediately.
	select {
	case <-t.GotInfo():
	case <-time.After(2 * time.Minute):
		d.log(log.LvlWarn, "timeout waiting for torrent metadata", "name", fileName)
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

// FindReaderForFileMask searches known torrents for a file matching the given
// glob-like mask (e.g. "*-accounts.0-2048.kv") within the specified directory
// prefix (e.g. "domain"). This handles version differences between what code
// expects and what torrents have.
//
// This method satisfies the state.SparseTorrentLookup interface.
func (d *Downloader) FindReaderForFileMask(dirPrefix, fileNameMask string) (io.ReadSeekCloser, int64, string, bool) {
	d.lock.RLock()
	names := make([]string, 0, len(d.torrentsByName))
	for name := range d.torrentsByName {
		names = append(names, name)
	}
	d.lock.RUnlock()

	for _, name := range names {
		// Match against dirPrefix/fileNameMask pattern
		baseName := name
		if dirPrefix != "" {
			if !strings.HasPrefix(name, dirPrefix+"/") {
				continue
			}
			baseName = name[len(dirPrefix)+1:]
		}

		matched, _ := filepath.Match(fileNameMask, baseName)
		if !matched {
			continue
		}

		reader, fileSize, ok := d.NewReaderForFile(name)
		if ok {
			return reader, fileSize, name, true
		}
	}
	return nil, 0, "", false
}

// SparseEntry is a name+hash pair for a torrent that should be registered
// for metadata-only access (no data download).
type SparseEntry struct {
	Name string
	Hash string // hex-encoded info hash
}

// AddSparseMetadataTorrents registers torrents for metadata-only access.
// These torrents will fetch their info dict from peers but will NOT download
// data locally. They exist in torrentsByName so that FindReaderForFileMask
// can create on-demand readers for sparse domain access.
//
// Returns a channel that closes when all domain .kv torrents have their
// metadata available. The caller should wait on this before re-opening
// the aggregator so that sparse domain files can be found.
func (d *Downloader) AddSparseMetadataTorrents(entries []SparseEntry) <-chan struct{} {
	d.lock.Lock()

	var added int
	var domainTorrents []*torrent.Torrent
	for _, entry := range entries {
		// Convert hex hash to metainfo.Hash
		var ih metainfo.Hash
		hashBytes, err := hex.DecodeString(entry.Hash)
		if err != nil {
			d.log(log.LvlWarn, "sparse: invalid hash", "name", entry.Name, "err", err)
			continue
		}
		copy(ih[:], hashBytes)

		// Check if already loaded
		if _, exists := d.torrentsByName[snapshotName(entry.Name)]; exists {
			continue
		}

		t, isNew, err := d.addTorrent(snapshotName(entry.Name), ih)
		if err != nil {
			d.log(log.LvlWarn, "sparse: failed to add torrent", "name", entry.Name, "err", err)
			continue
		}
		if !isNew {
			continue
		}

		// Mark as sparse so it's excluded from download stats
		if d.sparseTorrents == nil {
			d.sparseTorrents = make(map[snapshotName]struct{})
		}
		d.sparseTorrents[snapshotName(entry.Name)] = struct{}{}

		// Add trackers and webseed sources so the torrent can fetch metadata
		// from peers. Allow data download so pieces can be fetched on-demand
		// when the reader reads from the torrent. Note: without DownloadAll(),
		// only pieces that are actually read will be downloaded.
		t.AddTrackers(Trackers)
		t.AddWebSeeds(d.cfg.WebSeedUrls, d.addWebSeedOpts...)
		t.AllowDataDownload()
		t.AllowDataUpload()

		isDomain := strings.HasPrefix(entry.Name, "domain/") && strings.HasSuffix(entry.Name, ".kv")
		if isDomain {
			domainTorrents = append(domainTorrents, t)
		}

		// Start background metadata fetch logging
		go func(t2 *torrent.Torrent, name string) {
			select {
			case <-t2.GotInfo():
				d.log(log.LvlInfo, "sparse: got torrent metadata", "name", name)
			case <-t2.Closed():
			}
		}(t, entry.Name)

		added++
	}

	d.lock.Unlock()

	if added > 0 {
		d.log(log.LvlInfo, "sparse: registered metadata-only torrents", "count", added, "total", len(entries), "domain", len(domainTorrents))
	}

	// Return a channel that closes when all domain .kv torrents have metadata.
	ch := make(chan struct{})
	go func() {
		for _, t := range domainTorrents {
			select {
			case <-t.GotInfo():
			case <-t.Closed():
			}
		}
		close(ch)
	}()
	return ch
}
