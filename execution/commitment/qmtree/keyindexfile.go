package qmtree

import (
	"context"
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
	// File naming: v1.0-qmtree-keyindex.{fromStep}-{toStep}.kvi
	// Single RecSplit index mapping keyHash (32B) → txNum.
	keyIndexVersion = "v1.0"
	keyIndexName    = "qmtree-keyindex"
)

// keyIndexSegment represents one persisted RecSplit index mapping keyHash → txNum.
type keyIndexSegment struct {
	fromStep uint64
	toStep   uint64
	path     string
	index    *recsplit.Index
}

func (s *keyIndexSegment) close() {
	if s.index != nil {
		s.index.Close()
		s.index = nil
	}
}

// KeyIndexFile manages persisted KeyIndex segments on disk.
// Each segment is a single .kvi RecSplit file (no .kv data file).
type KeyIndexFile struct {
	dir      string
	segments []*keyIndexSegment // sorted by fromStep ascending
}

// NewKeyIndexFile creates a KeyIndexFile rooted at the given directory.
func NewKeyIndexFile(dir string) (*KeyIndexFile, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create keyindex dir: %w", err)
	}
	return &KeyIndexFile{dir: dir}, nil
}

func keyIndexPath(dir string, fromStep, toStep uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s.%d-%d.kvi", keyIndexVersion, keyIndexName, fromStep, toStep))
}

// FlushDelta builds a RecSplit index from dirty key-index entries.
// The RecSplit maps keyHash → txNum directly (txNum stored as the "offset" value).
func (kf *KeyIndexFile) FlushDelta(ctx context.Context, entries []KeyIndexEntry, fromStep, toStep uint64) error {
	if len(entries) == 0 {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].KeyHash.Cmp(entries[j].KeyHash) < 0
	})

	path := keyIndexPath(kf.dir, fromStep, toStep)

	if err := buildKeyIndexKVI(ctx, path, entries); err != nil {
		return fmt.Errorf("build keyindex: %w", err)
	}

	seg, err := openKeyIndexSegment(path, fromStep, toStep)
	if err != nil {
		os.Remove(path)
		return err
	}
	kf.segments = append(kf.segments, seg)
	return nil
}

// buildKeyIndexKVI builds a RecSplit index mapping keyHash → txNum.
func buildKeyIndexKVI(ctx context.Context, path string, entries []KeyIndexEntry) error {
	tmpDir := filepath.Dir(path)
	for {
		rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:           len(entries),
			BucketSize:         recsplit.DefaultBucketSize,
			LeafSize:           recsplit.DefaultLeafSize,
			IndexFile:          path,
			TmpDir:             tmpDir,
			Enums:              false,
			LessFalsePositives: true,
		}, log.Root())
		if err != nil {
			return err
		}

		for _, e := range entries {
			if err := rs.AddKey(e.KeyHash[:], e.TxNum); err != nil {
				return err
			}
		}
		if err := rs.Build(ctx); err != nil {
			if rs.Collision() {
				rs.ResetNextSalt()
				continue
			}
			return err
		}
		return nil
	}
}

func openKeyIndexSegment(path string, fromStep, toStep uint64) (*keyIndexSegment, error) {
	idx, err := recsplit.OpenIndex(path)
	if err != nil {
		return nil, fmt.Errorf("open keyindex: %w", err)
	}
	return &keyIndexSegment{
		fromStep: fromStep,
		toStep:   toStep,
		path:     path,
		index:    idx,
	}, nil
}

// Lookup searches all segments (newest first) for the given keyHash.
// Returns (txNum, true) if found, (0, false) otherwise.
// Note: RecSplit may return false positives; callers should verify
// against the entry data if needed.
func (kf *KeyIndexFile) Lookup(keyHash common.Hash) (uint64, bool) {
	for i := len(kf.segments) - 1; i >= 0; i-- {
		seg := kf.segments[i]
		if seg.index == nil {
			continue
		}
		reader := recsplit.NewIndexReader(seg.index)
		txNum, ok := reader.Lookup(keyHash[:])
		if ok {
			return txNum, true
		}
	}
	return 0, false
}

// LoadAll scans the directory for existing .kvi files and opens them.
func (kf *KeyIndexFile) LoadAll() (uint64, error) {
	entries, err := os.ReadDir(kf.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var maxStep uint64
	prefix := keyIndexVersion + "-" + keyIndexName + "."
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".kvi") || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		var fromStep, toStep uint64
		n, _ := fmt.Sscanf(e.Name(), prefix+"%d-%d.kvi", &fromStep, &toStep)
		if n != 2 {
			continue
		}
		path := filepath.Join(kf.dir, e.Name())
		seg, err := openKeyIndexSegment(path, fromStep, toStep)
		if err != nil {
			log.Warn("keyindex: failed to open segment", "file", path, "err", err)
			continue
		}
		kf.segments = append(kf.segments, seg)
		if toStep > maxStep {
			maxStep = toStep
		}
	}

	sort.Slice(kf.segments, func(i, j int) bool {
		return kf.segments[i].fromStep < kf.segments[j].fromStep
	})
	return maxStep, nil
}

// PopulateKeyIndex is no longer needed — the RecSplit index IS the lookup.
// Kept for backward compat with LoadFromDB which populates the in-memory KeyIndex.
// This iterates all segments but cannot extract entries from RecSplit (it's a
// hash function, not a key-value store). The in-memory KeyIndex is populated
// from MDBX via LoadFromDB instead.
func (kf *KeyIndexFile) PopulateKeyIndex(ki *KeyIndex) {
	// RecSplit is a perfect hash function — it can look up known keys but
	// cannot enumerate all keys. The in-memory KeyIndex must be populated
	// from MDBX (which has the full key→txNum mapping).
}

// TruncateAfterStep removes all segments with fromStep >= step.
func (kf *KeyIndexFile) TruncateAfterStep(step uint64) {
	var kept []*keyIndexSegment
	for _, seg := range kf.segments {
		if seg.fromStep >= step {
			seg.close()
			os.Remove(seg.path)
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
