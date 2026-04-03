package qmtree

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
)

const (
	// Each record in the .kv file: 32-byte keyHash + 8-byte txNum (big-endian).
	keyIndexRecordSize = 40

	// File naming: v1.0-qmtree-keyindex.{fromStep}-{toStep}.kv / .kvi
	keyIndexVersion = "v1.0"
	keyIndexName    = "qmtree-keyindex"
	keyIndexExtKV   = ".kv"
	keyIndexExtKVI  = ".kvi"
)

// keyIndexSegment represents one persisted segment: a .kv data file and its
// .kvi RecSplit index. Each segment covers one flush (a step range).
type keyIndexSegment struct {
	fromStep uint64
	toStep   uint64
	kvPath   string
	kviPath  string
	data     []byte          // mmap'd .kv file (nil until opened)
	index    *recsplit.Index // opened .kvi (nil until opened)
	count    int             // number of records in the .kv
}

// close releases the segment's resources.
func (s *keyIndexSegment) close() {
	if s.index != nil {
		s.index.Close()
		s.index = nil
	}
	s.data = nil
}

// KeyIndexFile manages persisted KeyIndex segments on disk.
type KeyIndexFile struct {
	dir      string
	segments []*keyIndexSegment // sorted by fromStep ascending
}

// NewKeyIndexFile creates a KeyIndexFile rooted at the given directory.
// The directory is created if it doesn't exist.
func NewKeyIndexFile(dir string) (*KeyIndexFile, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create keyindex dir: %w", err)
	}
	return &KeyIndexFile{dir: dir}, nil
}

func keyIndexKVPath(dir string, fromStep, toStep uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s.%d-%d%s", keyIndexVersion, keyIndexName, fromStep, toStep, keyIndexExtKV))
}

func keyIndexKVIPath(dir string, fromStep, toStep uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s.%d-%d%s", keyIndexVersion, keyIndexName, fromStep, toStep, keyIndexExtKVI))
}

// FlushDelta writes a delta of dirty key-index entries to a new .kv file and
// builds a .kvi RecSplit index over it. The entries must be sorted by keyHash.
func (kf *KeyIndexFile) FlushDelta(ctx context.Context, entries []KeyIndexEntry, fromStep, toStep uint64) error {
	if len(entries) == 0 {
		return nil
	}

	// Sort entries by keyHash.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].KeyHash.Cmp(entries[j].KeyHash) < 0
	})

	kvPath := keyIndexKVPath(kf.dir, fromStep, toStep)
	kviPath := keyIndexKVIPath(kf.dir, fromStep, toStep)

	// Write .kv data file: fixed-size records, no compression.
	f, err := os.Create(kvPath)
	if err != nil {
		return fmt.Errorf("create keyindex .kv: %w", err)
	}
	var buf [keyIndexRecordSize]byte
	for _, e := range entries {
		copy(buf[:32], e.KeyHash[:])
		binary.BigEndian.PutUint64(buf[32:40], e.TxNum)
		if _, err := f.Write(buf[:]); err != nil {
			f.Close()
			return fmt.Errorf("write keyindex record: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync keyindex .kv: %w", err)
	}
	f.Close()

	// Build .kvi RecSplit index: key=keyHash, value=byte offset into .kv.
	if err := buildKeyIndexRecSplit(ctx, kvPath, kviPath, len(entries)); err != nil {
		os.Remove(kvPath)
		return fmt.Errorf("build keyindex .kvi: %w", err)
	}

	// Open and register the segment.
	seg, err := openKeyIndexSegment(kvPath, kviPath, fromStep, toStep)
	if err != nil {
		os.Remove(kvPath)
		os.Remove(kviPath)
		return err
	}
	kf.segments = append(kf.segments, seg)
	return nil
}

// buildKeyIndexRecSplit builds a RecSplit perfect hash index over a .kv file.
func buildKeyIndexRecSplit(ctx context.Context, kvPath, kviPath string, keyCount int) error {
	tmpDir := filepath.Dir(kviPath)
	for {
		rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:           keyCount,
			BucketSize:         recsplit.DefaultBucketSize,
			LeafSize:           recsplit.DefaultLeafSize,
			IndexFile:          kviPath,
			TmpDir:             tmpDir,
			Enums:              false,
			LessFalsePositives: true,
		}, log.Root())
		if err != nil {
			return err
		}

		// Read .kv and add each keyHash with its byte offset.
		data, err := os.ReadFile(kvPath)
		if err != nil {
			return err
		}
		for i := 0; i < len(data); i += keyIndexRecordSize {
			key := data[i : i+32]
			if err := rs.AddKey(key, uint64(i)); err != nil {
				return err
			}
		}
		if err := rs.Build(ctx); err != nil {
			if rs.Collision() {
				rs.ResetNextSalt()
				continue // retry with new salt
			}
			return err
		}
		return nil
	}
}

// openKeyIndexSegment opens an existing .kv/.kvi pair.
func openKeyIndexSegment(kvPath, kviPath string, fromStep, toStep uint64) (*keyIndexSegment, error) {
	data, err := os.ReadFile(kvPath)
	if err != nil {
		return nil, fmt.Errorf("read keyindex .kv: %w", err)
	}
	idx, err := recsplit.OpenIndex(kviPath)
	if err != nil {
		return nil, fmt.Errorf("open keyindex .kvi: %w", err)
	}
	return &keyIndexSegment{
		fromStep: fromStep,
		toStep:   toStep,
		kvPath:   kvPath,
		kviPath:  kviPath,
		data:     data,
		index:    idx,
		count:    len(data) / keyIndexRecordSize,
	}, nil
}

// Lookup searches all segments (newest first) for the given keyHash.
// Returns (txNum, true) if found, (0, false) otherwise.
func (kf *KeyIndexFile) Lookup(keyHash common.Hash) (uint64, bool) {
	for i := len(kf.segments) - 1; i >= 0; i-- {
		seg := kf.segments[i]
		if seg.index == nil {
			continue
		}
		reader := recsplit.NewIndexReader(seg.index)
		offset, ok := reader.Lookup(keyHash[:])
		if !ok {
			continue
		}
		// Verify the key matches (RecSplit can have false positives).
		if int(offset)+keyIndexRecordSize > len(seg.data) {
			continue
		}
		var storedKey common.Hash
		copy(storedKey[:], seg.data[offset:offset+32])
		if storedKey != keyHash {
			continue // false positive
		}
		txNum := binary.BigEndian.Uint64(seg.data[offset+32 : offset+40])
		return txNum, true
	}
	return 0, false
}

// LoadAll scans the directory for existing .kv/.kvi files and opens them.
// Returns the highest toStep found (0 if no files).
func (kf *KeyIndexFile) LoadAll() (uint64, error) {
	entries, err := os.ReadDir(kf.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var maxStep uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), keyIndexExtKV) {
			continue
		}
		var fromStep, toStep uint64
		pattern := keyIndexVersion + "-" + keyIndexName + ".%d-%d" + keyIndexExtKV
		n, _ := fmt.Sscanf(e.Name(), pattern, &fromStep, &toStep)
		if n != 2 {
			continue
		}
		kvPath := filepath.Join(kf.dir, e.Name())
		kviPath := kvPath[:len(kvPath)-len(keyIndexExtKV)] + keyIndexExtKVI

		// Skip if .kvi doesn't exist (incomplete flush).
		if _, err := os.Stat(kviPath); os.IsNotExist(err) {
			log.Warn("keyindex: skipping .kv without .kvi", "file", kvPath)
			continue
		}

		seg, err := openKeyIndexSegment(kvPath, kviPath, fromStep, toStep)
		if err != nil {
			log.Warn("keyindex: failed to open segment", "file", kvPath, "err", err)
			continue
		}
		kf.segments = append(kf.segments, seg)
		if toStep > maxStep {
			maxStep = toStep
		}
	}

	// Sort segments by fromStep ascending.
	sort.Slice(kf.segments, func(i, j int) bool {
		return kf.segments[i].fromStep < kf.segments[j].fromStep
	})
	return maxStep, nil
}

// PopulateKeyIndex reads all segments and populates the given KeyIndex.
// Segments are read oldest-first so newer entries override older ones.
func (kf *KeyIndexFile) PopulateKeyIndex(ki *KeyIndex) {
	for _, seg := range kf.segments {
		for off := 0; off+keyIndexRecordSize <= len(seg.data); off += keyIndexRecordSize {
			var kh common.Hash
			copy(kh[:], seg.data[off:off+32])
			txNum := binary.BigEndian.Uint64(seg.data[off+32 : off+40])
			ki.UpdateKey(kh, txNum)
		}
	}
}

// TruncateAfterStep removes all segments with fromStep >= step and deletes
// their files from disk.
func (kf *KeyIndexFile) TruncateAfterStep(step uint64) {
	var kept []*keyIndexSegment
	for _, seg := range kf.segments {
		if seg.fromStep >= step {
			seg.close()
			os.Remove(seg.kvPath)
			os.Remove(seg.kviPath)
		} else {
			kept = append(kept, seg)
		}
	}
	kf.segments = kept
}

// Close releases all segment resources.
func (kf *KeyIndexFile) Close() {
	for _, seg := range kf.segments {
		seg.close()
	}
	kf.segments = nil
}

// SegmentCount returns the number of loaded segments.
func (kf *KeyIndexFile) SegmentCount() int { return len(kf.segments) }
