package qmtree

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
)

const (
	entrySnapshotVersion = "v1.0"
	entrySnapshotName    = "qmtree-entries"
	entrySnapshotExtKV   = ".kv"
	entrySnapshotExtKVI  = ".kvi"
	// Each entry in the snapshot: txNum (8B) + pre(32B) + sc(32B) + trans(32B) = 104 bytes
	snapshotEntrySize = 104
)

// entrySnapshot represents a frozen snapshot of entries for one step range.
// Data is NOT held in memory — reads go through the RecSplit index + file I/O.
type entrySnapshot struct {
	fromStep uint64
	toStep   uint64
	kvPath   string
	kviPath  string
	index    *recsplit.Index // RecSplit for txNum → offset lookup
	size     int64           // .kv file size in bytes
	count    int             // number of entries (size / snapshotEntrySize)
}

func (s *entrySnapshot) close() {
	if s.index != nil {
		s.index.Close()
		s.index = nil
	}
}

// SnapshotManager handles collation, pruning, and merging of qmtree data
// from MDBX hot tables to frozen snapshot files in snapshots/domain/.
type SnapshotManager struct {
	domainDir string // path to snapshots/domain/ (shared with other domains)
	stepSize  uint64
	entries   []*entrySnapshot // sorted by fromStep ascending
}

// NewSnapshotManager creates a manager for qmtree snapshot files.
// domainDir should be the snapshots/domain/ directory (shared with accounts, storage, etc.).
func NewSnapshotManager(domainDir string, stepSize uint64) (*SnapshotManager, error) {
	if err := os.MkdirAll(domainDir, 0755); err != nil {
		return nil, fmt.Errorf("create domain dir: %w", err)
	}
	return &SnapshotManager{
		domainDir: domainDir,
		stepSize:  stepSize,
	}, nil
}

func entrySnapshotFilename(dir string, fromStep, toStep uint64, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s.%d-%d%s", entrySnapshotVersion, entrySnapshotName, fromStep, toStep, ext))
}

func entryKVPath(dir string, fromStep, toStep uint64) string {
	return entrySnapshotFilename(dir, fromStep, toStep, entrySnapshotExtKV)
}

func entryKVIPath(dir string, fromStep, toStep uint64) string {
	return entrySnapshotFilename(dir, fromStep, toStep, entrySnapshotExtKVI)
}

// -------------------------------------------------------------------
// Phase 2: Collation — freeze completed steps from MDBX to snapshots
// -------------------------------------------------------------------

// CollateEntries reads entries for the given step range from MDBX and writes
// them to a .kv snapshot file with a .kvi RecSplit index.
func (sm *SnapshotManager) CollateEntries(ctx context.Context, tx kv.Tx, step uint64) error {
	fromSN := step * sm.stepSize
	toSN := (step + 1) * sm.stepSize

	kvPath := entryKVPath(sm.domainDir, step, step+1)
	kviPath := entryKVIPath(sm.domainDir, step, step+1)

	// Read entries from MDBX and write to .kv file.
	f, err := os.Create(kvPath)
	if err != nil {
		return fmt.Errorf("create entry snapshot: %w", err)
	}

	var buf [snapshotEntrySize]byte
	count := 0
	for sn := fromSN; sn < toSN; sn++ {
		pre, sc, trans, err := GetEntry(tx, sn)
		if err != nil {
			f.Close()
			os.Remove(kvPath)
			// Entry doesn't exist yet — step is not complete.
			if count == 0 {
				return fmt.Errorf("step %d has no entries at sn=%d", step, sn)
			}
			break
		}
		binary.BigEndian.PutUint64(buf[0:8], sn)
		copy(buf[8:40], pre[:])
		copy(buf[40:72], sc[:])
		copy(buf[72:104], trans[:])
		if _, err := f.Write(buf[:]); err != nil {
			f.Close()
			os.Remove(kvPath)
			return fmt.Errorf("write entry snapshot: %w", err)
		}
		count++
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(kvPath)
		return fmt.Errorf("sync entry snapshot: %w", err)
	}
	f.Close()

	if count == 0 {
		os.Remove(kvPath)
		return fmt.Errorf("step %d: no entries to collate", step)
	}

	// Build RecSplit index: key=txNum (8B), value=byte offset.
	if err := buildEntryRecSplit(ctx, kvPath, kviPath, count); err != nil {
		os.Remove(kvPath)
		return fmt.Errorf("build entry RecSplit: %w", err)
	}

	// Open and register.
	snap, err := openEntrySnapshot(kvPath, kviPath, step, step+1)
	if err != nil {
		os.Remove(kvPath)
		os.Remove(kviPath)
		return err
	}
	sm.entries = append(sm.entries, snap)

	log.Info("qmtree: collated entry snapshot",
		"step", step,
		"entries", count,
		"file", filepath.Base(kvPath),
	)
	return nil
}


func buildEntryRecSplit(ctx context.Context, kvPath, kviPath string, keyCount int) error {
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

		// Stream through the .kv file — don't load into memory.
		f, err := os.Open(kvPath)
		if err != nil {
			return err
		}
		var buf [snapshotEntrySize]byte
		offset := uint64(0)
		for {
			_, err := io.ReadFull(f, buf[:])
			if err != nil {
				break // EOF or error
			}
			if err := rs.AddKey(buf[:8], offset); err != nil {
				f.Close()
				return err
			}
			offset += snapshotEntrySize
		}
		f.Close()

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

func openEntrySnapshot(kvPath, kviPath string, fromStep, toStep uint64) (*entrySnapshot, error) {
	fi, err := os.Stat(kvPath)
	if err != nil {
		return nil, fmt.Errorf("stat entry snapshot: %w", err)
	}
	idx, err := recsplit.OpenIndex(kviPath)
	if err != nil {
		return nil, fmt.Errorf("open entry snapshot index: %w", err)
	}
	return &entrySnapshot{
		fromStep: fromStep,
		toStep:   toStep,
		kvPath:   kvPath,
		kviPath:  kviPath,
		index:    idx,
		size:     fi.Size(),
		count:    int(fi.Size()) / snapshotEntrySize,
	}, nil
}

// readEntryAt reads a single entry from the .kv file at the given byte offset.
func readEntryAt(kvPath string, offset uint64) (pre, sc, trans common.Hash, err error) {
	f, err := os.Open(kvPath)
	if err != nil {
		return common.Hash{}, common.Hash{}, common.Hash{}, err
	}
	defer f.Close()
	var buf [snapshotEntrySize]byte
	if _, err := f.ReadAt(buf[:], int64(offset)); err != nil {
		return common.Hash{}, common.Hash{}, common.Hash{}, err
	}
	copy(pre[:], buf[8:40])
	copy(sc[:], buf[40:72])
	copy(trans[:], buf[72:104])
	return pre, sc, trans, nil
}

// GetEntryFromSnapshots looks up an entry by txNum in frozen snapshots.
// Returns (pre, sc, trans, true) if found, (zero, zero, zero, false) if not.
func (sm *SnapshotManager) GetEntryFromSnapshots(txNum uint64) (pre, sc, trans common.Hash, found bool) {
	step := txNum / sm.stepSize
	for _, snap := range sm.entries {
		if step >= snap.fromStep && step < snap.toStep {
			reader := recsplit.NewIndexReader(snap.index)
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], txNum)
			offset, ok := reader.Lookup(key[:])
			if !ok || int64(offset)+int64(snapshotEntrySize) > snap.size {
				return common.Hash{}, common.Hash{}, common.Hash{}, false
			}
			// Read entry from file at offset.
			pre, sc, trans, err := readEntryAt(snap.kvPath, offset)
			if err != nil {
				return common.Hash{}, common.Hash{}, common.Hash{}, false
			}
			return pre, sc, trans, true
		}
	}
	return common.Hash{}, common.Hash{}, common.Hash{}, false
}

// -------------------------------------------------------------------
// Phase 3: Pruning — remove frozen data from MDBX
// -------------------------------------------------------------------

// PruneEntries deletes entries from MDBX that have been frozen into snapshots.
func (sm *SnapshotManager) PruneEntries(tx kv.RwTx, step uint64) (int, error) {
	fromSN := step * sm.stepSize
	toSN := (step + 1) * sm.stepSize

	var fromKey, toKey [8]byte
	binary.BigEndian.PutUint64(fromKey[:], fromSN)
	binary.BigEndian.PutUint64(toKey[:], toSN)

	c, err := tx.Cursor(kv.TblQMTreeEntries)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	pruned := 0
	for k, _, err := c.Seek(fromKey[:]); k != nil; k, _, err = c.Next() {
		if err != nil {
			return pruned, err
		}
		sn := binary.BigEndian.Uint64(k)
		if sn >= toSN {
			break
		}
		if err := tx.Delete(kv.TblQMTreeEntries, k); err != nil {
			return pruned, err
		}
		pruned++
	}

	log.Info("qmtree: pruned entries from MDBX",
		"step", step,
		"pruned", pruned,
	)
	return pruned, nil
}

// CollateAndPrune performs collation + pruning for a completed step.
// This is the typical call from the Tracker at step boundaries.
func (sm *SnapshotManager) CollateAndPrune(ctx context.Context, roTx kv.Tx, rwTx kv.RwTx, step uint64) error {
	if err := sm.CollateEntries(ctx, roTx, step); err != nil {
		return fmt.Errorf("collate entries step %d: %w", step, err)
	}
	if _, err := sm.PruneEntries(rwTx, step); err != nil {
		return fmt.Errorf("prune entries step %d: %w", step, err)
	}
	return nil
}

// -------------------------------------------------------------------
// Phase 4: Merging — combine small step files into larger ones
// -------------------------------------------------------------------

// MergeEntries merges multiple consecutive entry snapshots into a single file.
// Since entries are ordered by txNum (monotonically increasing), merging
// is a simple concatenation of sorted ranges.
func (sm *SnapshotManager) MergeEntries(ctx context.Context, fromStep, toStep uint64) error {
	// Find snapshots to merge.
	var toMerge []*entrySnapshot
	for _, snap := range sm.entries {
		if snap.fromStep >= fromStep && snap.toStep <= toStep {
			toMerge = append(toMerge, snap)
		}
	}
	if len(toMerge) <= 1 {
		return nil // nothing to merge
	}

	// Sort by fromStep.
	sort.Slice(toMerge, func(i, j int) bool {
		return toMerge[i].fromStep < toMerge[j].fromStep
	})

	// Verify contiguous range.
	for i := 1; i < len(toMerge); i++ {
		if toMerge[i].fromStep != toMerge[i-1].toStep {
			return fmt.Errorf("non-contiguous merge: gap between step %d and %d",
				toMerge[i-1].toStep, toMerge[i].fromStep)
		}
	}

	mergedFrom := toMerge[0].fromStep
	mergedTo := toMerge[len(toMerge)-1].toStep

	kvPath := entryKVPath(sm.domainDir, mergedFrom, mergedTo)
	kviPath := entryKVIPath(sm.domainDir, mergedFrom, mergedTo)

	// Stream-concatenate data files — no full-file memory allocation.
	f, err := os.Create(kvPath)
	if err != nil {
		return fmt.Errorf("create merged snapshot: %w", err)
	}
	totalCount := 0
	for _, snap := range toMerge {
		src, err := os.Open(snap.kvPath)
		if err != nil {
			f.Close()
			os.Remove(kvPath)
			return fmt.Errorf("open source for merge: %w", err)
		}
		if _, err := io.Copy(f, src); err != nil {
			src.Close()
			f.Close()
			os.Remove(kvPath)
			return fmt.Errorf("copy merged data: %w", err)
		}
		src.Close()
		totalCount += snap.count
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(kvPath)
		return err
	}
	f.Close()

	// Build merged RecSplit index.
	if err := buildEntryRecSplit(ctx, kvPath, kviPath, totalCount); err != nil {
		os.Remove(kvPath)
		return fmt.Errorf("build merged RecSplit: %w", err)
	}

	// Open merged snapshot.
	merged, err := openEntrySnapshot(kvPath, kviPath, mergedFrom, mergedTo)
	if err != nil {
		os.Remove(kvPath)
		os.Remove(kviPath)
		return err
	}

	// Merge corresponding twig root files.
	mergeRootFiles(sm.domainDir, mergedFrom, mergedTo, sm.stepSize)

	// Close and delete old snapshots.
	var kept []*entrySnapshot
	for _, snap := range sm.entries {
		if snap.fromStep >= mergedFrom && snap.toStep <= mergedTo {
			snap.close()
			os.Remove(snap.kvPath)
			os.Remove(snap.kviPath)
		} else {
			kept = append(kept, snap)
		}
	}
	kept = append(kept, merged)
	sort.Slice(kept, func(i, j int) bool {
		return kept[i].fromStep < kept[j].fromStep
	})
	sm.entries = kept

	log.Info("qmtree: merged entry snapshots",
		"fromStep", mergedFrom,
		"toStep", mergedTo,
		"totalEntries", totalCount,
		"removedFiles", len(toMerge),
	)
	return nil
}

// MaybeMerge finds a mergeable range using binary-doubling (same algorithm
// as the Aggregator) and merges if possible. Call after each collation.
//
// Binary-doubling: for endStep, find the largest power-of-2 span that fits.
// E.g., after step 4: merge 0-4. After step 6: merge 4-6. After step 8: merge 0-8.
func (sm *SnapshotManager) MaybeMerge(ctx context.Context) {
	high := sm.HighestFrozenStep()
	if high < 2 {
		return
	}

	// Find the largest power-of-2 aligned merge ending at high.
	// spanStep = high & -high extracts the rightmost set bit.
	spanStep := high & (^high + 1) // same as high & -high
	if spanStep < 2 {
		return
	}
	fromStep := high - spanStep
	toStep := high

	// Check that we have all the individual step files needed.
	haveAll := true
	for s := fromStep; s < toStep; s++ {
		found := false
		for _, snap := range sm.entries {
			if snap.fromStep == s && snap.toStep == s+1 {
				found = true
				break
			}
		}
		// Also check if there's already a merged file covering this range.
		for _, snap := range sm.entries {
			if snap.fromStep <= s && snap.toStep > s {
				found = true
				break
			}
		}
		if !found {
			haveAll = false
			break
		}
	}
	if !haveAll {
		return
	}

	// Check we don't already have a merged file for this exact range.
	for _, snap := range sm.entries {
		if snap.fromStep == fromStep && snap.toStep == toStep {
			return // already merged
		}
	}

	if err := sm.MergeEntries(ctx, fromStep, toStep); err != nil {
		log.Warn("qmtree: merge failed", "from", fromStep, "to", toStep, "err", err)
	}
}

// -------------------------------------------------------------------
// Loading existing snapshots from disk
// -------------------------------------------------------------------

// LoadSnapshots scans the snapshot directory for existing .kv/.kvi files
// and opens them. Returns the highest frozen step.
func (sm *SnapshotManager) LoadSnapshots() (uint64, error) {
	dir := sm.domainDir
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var maxStep uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), entrySnapshotExtKV) {
			continue
		}
		if !strings.HasPrefix(e.Name(), entrySnapshotVersion+"-"+entrySnapshotName+".") {
			continue
		}
		var fromStep, toStep uint64
		pattern := entrySnapshotVersion + "-" + entrySnapshotName + ".%d-%d" + entrySnapshotExtKV
		n, _ := fmt.Sscanf(e.Name(), pattern, &fromStep, &toStep)
		if n != 2 {
			continue
		}
		kvPath := filepath.Join(dir, e.Name())
		kviPath := kvPath[:len(kvPath)-len(entrySnapshotExtKV)] + entrySnapshotExtKVI
		if _, err := os.Stat(kviPath); os.IsNotExist(err) {
			log.Warn("qmtree: skipping snapshot without index", "file", kvPath)
			continue
		}
		snap, err := openEntrySnapshot(kvPath, kviPath, fromStep, toStep)
		if err != nil {
			log.Warn("qmtree: failed to open snapshot", "file", kvPath, "err", err)
			continue
		}
		sm.entries = append(sm.entries, snap)
		if toStep > maxStep {
			maxStep = toStep
		}
	}

	sort.Slice(sm.entries, func(i, j int) bool {
		return sm.entries[i].fromStep < sm.entries[j].fromStep
	})

	if len(sm.entries) > 0 {
		log.Info("qmtree: loaded entry snapshots",
			"count", len(sm.entries),
			"maxStep", maxStep,
		)
	}
	return maxStep, nil
}

// FrozenEntries returns the total number of entries in frozen snapshots.
func (sm *SnapshotManager) FrozenEntries() int {
	total := 0
	for _, snap := range sm.entries {
		total += snap.count
	}
	return total
}

// HighestFrozenStep returns the highest step that has been fully frozen.
func (sm *SnapshotManager) HighestFrozenStep() uint64 {
	var max uint64
	for _, snap := range sm.entries {
		if snap.toStep > max {
			max = snap.toStep
		}
	}
	return max
}

// Close releases all snapshot resources.
func (sm *SnapshotManager) Close() {
	for _, snap := range sm.entries {
		snap.close()
	}
	sm.entries = nil
}
