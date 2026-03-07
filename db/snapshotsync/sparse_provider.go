package snapshotsync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
)

// TorrentLookup provides access to torrent readers for snapshot files.
// This interface decouples the snapshot layer from the downloader package.
type TorrentLookup interface {
	// NewReaderForFile returns an io.ReadSeekCloser for the named snapshot file
	// and the file's total size. The reader fetches data on demand from the
	// BitTorrent network. Returns nil, 0, false if no torrent exists for the file.
	NewReaderForFile(fileName string) (reader io.ReadSeekCloser, fileSize int64, ok bool)

	// FindFileForBlock scans known torrents for a .seg file of the given type
	// that covers blockNum. Returns the filename and block range, or ok=false
	// if no matching torrent is found.
	FindFileForBlock(typeName string, blockNum uint64) (fileName string, from, to uint64, ok bool)
}

// SparseProvider creates on-demand DirtySegments backed by torrent data.
// It requires index files (.idx) to be locally present — only the .seg data
// is loaded sparsely from the network.
type SparseProvider struct {
	mu       sync.RWMutex
	torrents TorrentLookup
	snapDir  string // directory containing index files

	// Cache of created sparse segments, keyed by seg filename
	segments map[string]*DirtySegment
}

// NewSparseProvider creates a provider that creates sparse segments on demand.
// snapDir is the directory where index files (.idx) are stored locally.
func NewSparseProvider(torrents TorrentLookup, snapDir string) *SparseProvider {
	return &SparseProvider{
		torrents: torrents,
		snapDir:  snapDir,
		segments: make(map[string]*DirtySegment),
	}
}

func (sp *SparseProvider) getOrCreateSegment(segType snaptype.Type, segFileName string, from, to uint64) *DirtySegment {
	sp.mu.RLock()
	if ds, ok := sp.segments[segFileName]; ok {
		sp.mu.RUnlock()
		return ds
	}
	sp.mu.RUnlock()

	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Double-check after acquiring write lock
	if ds, ok := sp.segments[segFileName]; ok {
		return ds
	}

	ds := sp.createSparseSegment(segType, segFileName, from, to)
	if ds == nil {
		return nil
	}

	sp.segments[segFileName] = ds
	return ds
}

func (sp *SparseProvider) createSparseSegment(segType snaptype.Type, segFileName string, from, to uint64) *DirtySegment {
	// Parse the version from the actual torrent filename
	info, _, parsed := snaptype.ParseFileName("", segFileName)
	segVersion := segType.Versions().Current
	if parsed {
		segVersion = info.Version
	}

	ds := NewDirtySegment(segType, segVersion, from, to, true)

	// Get torrent reader for this segment file
	reader, fileSize, ok := sp.torrents.NewReaderForFile(segFileName)
	if !ok {
		return nil
	}

	// Create sparse-backed Decompressor (reads header, stores reader for on-demand access)
	d, err := seg.NewDecompressorFromReader(reader, fileSize, segFileName)
	if err != nil {
		log.Debug("[sparse] failed to create decompressor from reader", "file", segFileName, "err", err)
		reader.Close() //nolint:errcheck
		return nil
	}
	ds.Decompressor = d

	// Open index files — these must be locally present
	if err := sp.openSparseIdx(ds); err != nil {
		log.Debug("[sparse] index not available, closing reader", "file", segFileName, "err", err)
		ds.Close()
		ds.Decompressor = nil
		return nil
	}

	log.Info("[sparse] created sparse segment", "file", segFileName, "size", fileSize)
	return ds
}

// openSparseIdx opens index files for a sparse segment.
// Unlike the normal openIdx which checks s.Decompressor != nil,
// this works with segments that have no local Decompressor.
func (sp *SparseProvider) openSparseIdx(s *DirtySegment) error {
	idxFileNames := s.Type().IdxFileNames(s.from, s.to)
	if len(idxFileNames) == 0 {
		return nil
	}

	for len(s.indexes) < len(s.Type().Indexes()) {
		s.indexes = append(s.indexes, nil)
	}

	for i, fileName := range idxFileNames {
		fPath := filepath.Join(sp.snapDir, fileName)
		if _, err := os.Stat(fPath); err != nil {
			return fmt.Errorf("index file missing: %s", fPath)
		}

		index, err := recsplit.OpenIndex(fPath)
		if err != nil {
			return fmt.Errorf("open index %s: %w", fileName, err)
		}
		s.indexes[i] = index
	}

	return nil
}

// ViewSingleFile returns a sparse VisibleSegment for the given type and block number.
// Returns nil, false if no sparse segment covers the requested block.
func (sp *SparseProvider) ViewSingleFile(segType snaptype.Type, blockNum uint64) (*VisibleSegment, bool) {
	if sp == nil || sp.torrents == nil {
		return nil, false
	}

	// Ask the torrent layer which file covers this block
	segFileName, from, to, ok := sp.torrents.FindFileForBlock(segType.Name(), blockNum)
	if !ok {
		return nil, false
	}

	ds := sp.getOrCreateSegment(segType, segFileName, from, to)
	if ds == nil {
		return nil, false
	}

	return &VisibleSegment{
		Range:   Range{from, to},
		segType: segType,
		src:     ds,
	}, true
}

// Close releases all sparse segments.
func (sp *SparseProvider) Close() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for key, ds := range sp.segments {
		ds.close()
		delete(sp.segments, key)
	}
}
