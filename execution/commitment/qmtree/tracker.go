package qmtree

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// proofEntry is a minimal Entry for the production pipeline.
// It stores the three raw hash components; the leaf hash is computed from them
// plus the chained previousLeafHash (tracked externally by Tracker.prevLeaf).
type proofEntry struct {
	sn          uint64
	hash        common.Hash // precomputed leaf hash, used by tree for twig building
	pre         common.Hash
	stateChange common.Hash
	transition  common.Hash
}

func (e *proofEntry) SerialNumber() uint64 { return e.sn }
func (e *proofEntry) Hash() common.Hash    { return e.hash }
func (e *proofEntry) Len() int64           { return 0 }
func (e *proofEntry) Components() (pre, stateChange, transition common.Hash) {
	return e.pre, e.stateChange, e.transition
}

// Tracker holds per-sync qmtree state for the serial executor.
// When a datadir is provided, the tree is backed by disk files (EntryFile +
// TwigFile) so it survives restarts and can generate proofs after the fact.
// It also retains leaf component data for witness generation.
//
// Storage is segmented into steps aligned with erigon's domain step size.
// Each HPFile segment covers one step's worth of entries/twigs, making
// completed segments individually distributable.
type Tracker struct {
	tree     *Tree
	hasher   *Keccak256Hasher
	NextSN   uint64
	prevLeaf common.Hash

	// Leaf component storage for witness generation.
	// Keyed by serial number. In the future this could be persisted
	// to disk, but for the PoC it's held in memory.
	leafData map[uint64]LeafData

	// Disk storage handles (nil when in-memory only).
	entryFile *EntryFile
	twigFile  *TwigFile

	// Step tracking.
	stepSize       uint64 // entries per step (matches config3.DefaultStepSize)
	lastStepLogged uint64 // last completed step that was logged
}

const (
	trackerSubdir      = "qmtree" // lives under snapshots/
	trackerEntrySubdir = "entries"
	trackerTwigSubdir  = "twigs"

	// Entry segments: one step = stepSize entries × 96 bytes/entry (3 × 32B components).
	// Buffer size chosen so segmentSize % bufferSize == 0.
	// 150,000,000 / 1,500,000 = 100.
	trackerEntryBufSize = 1_500_000

	// Twig segments: one step ≈ 763 twigs (ceil(stepSize / 2048)).
	// Buffer size = TWIG_SIZE so each buffer flush writes exactly one twig.
	trackerTwigBufSize = 73_708 // = TWIG_SIZE = 12 + (4095-1792)*32

	// Twigs per step: ceil(stepSize / LEAF_COUNT_IN_TWIG).
	trackerTwigsPerStep = 763 // ceil(1_562_500 / 2048)
)

// NewTracker creates a qmtree tracker. If snapDir is non-empty, the tree is
// backed by disk files under <snapDir>/qmtree/{entries,twigs}/. This matches
// the existing snapshot layout (e.g. snapshots/domain/, snapshots/history/).
//
// Storage is segmented by step: each HPFile segment covers exactly one step
// worth of data. Entry segment size = stepSize × 32 bytes. Twig segment
// size = ceil(stepSize/2048) × TWIG_SIZE bytes.
func NewTracker(snapDir string, stepSize uint64) (*Tracker, error) {
	hasher := &Keccak256Hasher{}
	qt := &Tracker{
		hasher:   hasher,
		leafData: make(map[uint64]LeafData),
		stepSize: stepSize,
	}

	if snapDir != "" {
		qmdir := filepath.Join(snapDir, trackerSubdir)
		entryDir := filepath.Join(qmdir, trackerEntrySubdir)
		twigDir := filepath.Join(qmdir, trackerTwigSubdir)
		if err := os.MkdirAll(entryDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree entry dir: %w", err)
		}
		if err := os.MkdirAll(twigDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree twig dir: %w", err)
		}

		entrySegSize := stepSize * entrySize // one step per segment (stepSize × 96 bytes)
		twigSegSize := uint64(trackerTwigsPerStep) * uint64(trackerTwigBufSize)

		ef, err := NewEntryFile(trackerEntryBufSize, entrySegSize, entryDir)
		if err != nil {
			return nil, fmt.Errorf("open qmtree entry file: %w", err)
		}
		tf, err := NewTwigFile(trackerTwigBufSize, twigSegSize, twigDir, hasher)
		if err != nil {
			ef.Close()
			return nil, fmt.Errorf("open qmtree twig file: %w", err)
		}

		qt.entryFile = ef
		qt.twigFile = tf
		qt.tree = NewTree(hasher, 0, ef, tf)
	} else {
		qt.tree = NewTree(hasher, 0, nil, nil)
	}

	return qt, nil
}

// AppendLeaf builds a proof leaf from individual hash components and appends
// it to the qmtree. The preStateHash, stateChangeHash, and transitionHash
// come from execution; previousLeafHash is chained automatically.
func (qt *Tracker) AppendLeaf(preStateHash, stateChangeHash, transitionHash common.Hash) {
	ld := LeafData{
		SerialNum:        qt.NextSN,
		PreStateHash:     preStateHash,
		StateChangeHash:  stateChangeHash,
		TransitionHash:   transitionHash,
		PreviousLeafHash: qt.prevLeaf,
	}
	leafHash := ld.LeafHash()

	entry := &proofEntry{sn: qt.NextSN, hash: leafHash, pre: preStateHash, stateChange: stateChangeHash, transition: transitionHash}
	qt.tree.AppendEntry(entry)

	qt.leafData[qt.NextSN] = ld
	qt.prevLeaf = leafHash
	qt.NextSN++
}

// SyncRoot computes and returns the current qmtree root.
func (qt *Tracker) SyncRoot() common.Hash {
	return common.Hash(qt.tree.SyncAndRoot(qt.hasher))
}

// currentStep returns the step number for the current serial number.
func (qt *Tracker) currentStep() uint64 {
	if qt.NextSN == 0 {
		return 0
	}
	return (qt.NextSN - 1) / qt.stepSize
}

// completedSteps returns the number of fully completed steps.
func (qt *Tracker) completedSteps() uint64 {
	return qt.NextSN / qt.stepSize
}

// LogStepProgress logs when a new step is completed.
func (qt *Tracker) LogStepProgress(logPrefix string) {
	completed := qt.completedSteps()
	if completed > qt.lastStepLogged {
		for step := qt.lastStepLogged + 1; step <= completed; step++ {
			entryBytes := int64(0)
			twigBytes := int64(0)
			if qt.entryFile != nil {
				entryBytes = qt.entryFile.Size()
			}
			if qt.twigFile != nil {
				twigBytes = qt.twigFile.Size()
			}
			log.Info(fmt.Sprintf("[%s] qmtree step completed", logPrefix),
				"step", step,
				"entries", step*qt.stepSize,
				"entryStorage", common.ByteCount(uint64(entryBytes)),
				"twigStorage", common.ByteCount(uint64(twigBytes)),
				"totalStorage", common.ByteCount(uint64(entryBytes+twigBytes)),
			)
		}
		qt.lastStepLogged = completed
	}
}

// StorageStats returns current storage sizes.
type StorageStats struct {
	Entries       uint64 // total entries (leaves)
	Steps         uint64 // completed steps
	CurrentStep   uint64 // current (possibly incomplete) step
	EntryBytes    int64  // entry file size
	TwigBytes     int64  // twig file size
	TotalBytes    int64  // total storage
	LeafDataCount int    // in-memory leaf data entries
}

func (qt *Tracker) StorageStats() StorageStats {
	stats := StorageStats{
		Entries:       qt.NextSN,
		Steps:         qt.completedSteps(),
		CurrentStep:   qt.currentStep(),
		LeafDataCount: len(qt.leafData),
	}
	if qt.entryFile != nil {
		stats.EntryBytes = qt.entryFile.Size()
	}
	if qt.twigFile != nil {
		stats.TwigBytes = qt.twigFile.Size()
	}
	stats.TotalBytes = stats.EntryBytes + stats.TwigBytes
	return stats
}

// GetWitness generates a Witness for a single leaf by serial number.
// The tree must be synced (SyncRoot called) before calling this.
func (qt *Tracker) GetWitness(sn uint64) (*Witness, error) {
	ld, ok := qt.leafData[sn]
	if !ok {
		return nil, fmt.Errorf("leaf data not found for sn=%d", sn)
	}
	proof, err := qt.tree.GetProof(sn)
	if err != nil {
		return nil, fmt.Errorf("get proof for sn=%d: %w", sn, err)
	}
	return &Witness{
		Proof:            proof,
		PreStateHash:     ld.PreStateHash,
		StateChangeHash:  ld.StateChangeHash,
		TransitionHash:   ld.TransitionHash,
		PreviousLeafHash: ld.PreviousLeafHash,
	}, nil
}

// GetRangeWitness generates a RangeWitness for a contiguous range of leaves.
// The tree must be synced before calling this.
func (qt *Tracker) GetRangeWitness(fromSN, toSN uint64) (*RangeWitness, error) {
	if fromSN > toSN {
		return nil, fmt.Errorf("invalid range: from=%d > to=%d", fromSN, toSN)
	}

	leaves := make([]LeafData, 0, toSN-fromSN+1)
	for sn := fromSN; sn <= toSN; sn++ {
		ld, ok := qt.leafData[sn]
		if !ok {
			return nil, fmt.Errorf("leaf data not found for sn=%d", sn)
		}
		leaves = append(leaves, ld)
	}

	firstProof, err := qt.tree.GetProof(fromSN)
	if err != nil {
		return nil, fmt.Errorf("get first proof for sn=%d: %w", fromSN, err)
	}
	lastProof, err := qt.tree.GetProof(toSN)
	if err != nil {
		return nil, fmt.Errorf("get last proof for sn=%d: %w", toSN, err)
	}

	return &RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     leaves,
	}, nil
}

// Flush writes buffered entry/twig data to disk without closing files.
// Called at commit boundaries to keep qmtree storage in sync with domain commits.
// Implements execctx.AppendOnlyFlusher.
func (qt *Tracker) Flush() {
	if qt.entryFile != nil {
		qt.entryFile.Flush()
	}
	if qt.twigFile != nil {
		qt.twigFile.Flush()
	}
}

// UnwindTo truncates the tree and storage back to the given serial number.
// All entries with sn >= toSN are discarded, and in-memory leaf data for
// the unwound range is removed. The target SN is the first entry to discard
// (i.e., entries 0..toSN-1 are kept).
func (qt *Tracker) UnwindTo(toSN uint64) {
	if toSN >= qt.NextSN {
		return // nothing to unwind
	}

	if toSN == 0 {
		// Unwind everything — reset tree to empty state.
		qt.tree.UnwindTo(0, nil)
	} else {
		// Tree.UnwindTo(targetSN) keeps entries 0..targetSN.
		lastKept := toSN - 1
		twigStart := lastKept &^ TWIG_MASK // first SN in the twig
		posInTwig := lastKept & TWIG_MASK

		// Collect entry hashes for partial twig rebuild.
		var entryHashes []common.Hash
		if posInTwig < TWIG_MASK { // partial twig needs rebuild
			entryHashes = make([]common.Hash, posInTwig+1)
			for i := uint64(0); i <= posInTwig; i++ {
				if ld, ok := qt.leafData[twigStart+i]; ok {
					entryHashes[i] = ld.LeafHash()
				}
			}
		}
		qt.tree.UnwindTo(lastKept, entryHashes)
	}

	// Remove leaf data for unwound entries.
	for sn := toSN; sn < qt.NextSN; sn++ {
		delete(qt.leafData, sn)
	}
	qt.NextSN = toSN

	// Recompute prevLeaf from the last remaining entry.
	if toSN > 0 {
		if ld, ok := qt.leafData[toSN-1]; ok {
			qt.prevLeaf = ld.LeafHash()
		}
	} else {
		qt.prevLeaf = common.Hash{}
	}
}

// LoadFromDisk rebuilds in-memory tree state from existing entry files on disk.
// Call this after NewTracker when the snapDir already contains data from a
// previous run, before calling GetWitness or SyncRoot.
//
// Strategy: truncate the twig file to 0 and rebuild it by replaying all entries
// from the entry file with correctly-chained leaf hashes. The entry file is the
// authoritative source of truth (storing raw component triplets); the twig file
// is a derived structure that must be rebuilt to ensure the twig Merkle trees
// reflect the correct prevLeaf chain. This handles the case where a prior run
// was killed mid-twig and restarted without LoadFromDisk (causing the twig file
// to be written with a reset prevLeaf=0).
func (qt *Tracker) LoadFromDisk() error {
	if qt.entryFile == nil {
		return nil // in-memory only, nothing to load
	}
	fileSize := qt.entryFile.Size()
	if fileSize == 0 {
		return nil // empty, nothing to do
	}
	if fileSize%int64(entrySize) != 0 {
		return fmt.Errorf("qmtree entry file size %d is not aligned to entrySize=%d", fileSize, entrySize)
	}
	count := uint64(fileSize) / uint64(entrySize)

	// Truncate the twig file so we rebuild it from scratch with correct leaf hashes.
	// The entry file stores the authoritative (pre, stateChange, transition) triplets;
	// the twig file is derived from them and must match the correctly-chained leaf hashes.
	if qt.twigFile != nil {
		if err := qt.twigFile.Truncate(0); err != nil {
			return fmt.Errorf("qmtree LoadFromDisk: truncate twig file: %w", err)
		}
	}

	// Build a replay tree with nil entry storage (entries are already on disk;
	// don't re-append them) and real twig storage so completed twigs are written
	// to disk with the correctly-computed leaf hashes.
	replayTree := NewTree(qt.hasher, 0, nil, qt.twigFile)

	var prevLeaf common.Hash
	leafData := make(map[uint64]LeafData, count)

	for sn := uint64(0); sn < count; sn++ {
		pre, stateChange, transition, err := qt.entryFile.ReadComponents(sn)
		if err != nil {
			return fmt.Errorf("qmtree LoadFromDisk: read components sn=%d: %w", sn, err)
		}
		ld := LeafData{
			SerialNum:        sn,
			PreStateHash:     pre,
			StateChangeHash:  stateChange,
			TransitionHash:   transition,
			PreviousLeafHash: prevLeaf,
		}
		leafHash := ld.LeafHash()
		entry := &proofEntry{sn: sn, hash: leafHash, pre: pre, stateChange: stateChange, transition: transition}
		replayTree.AppendEntry(entry) //nolint:errcheck
		leafData[sn] = ld
		prevLeaf = leafHash
	}

	// Attach the real entry storage to the replay tree. The caller is responsible
	// for calling SyncRoot() which will trigger the first (and only) SyncAndRoot
	// call to build the upper-tree node cache.
	replayTree.entryStorage = qt.tree.entryStorage

	qt.tree = replayTree
	qt.leafData = leafData
	qt.prevLeaf = prevLeaf
	qt.NextSN = count

	log.Info("qmtree: loaded from disk",
		"entries", count,
		"entryStorage", common.ByteCount(uint64(fileSize)),
	)
	return nil
}

// Close flushes and closes disk storage.
// Implements execctx.AppendOnlyFlusher.
func (qt *Tracker) Close() {
	qt.Flush()
	if qt.entryFile != nil {
		qt.entryFile.Close()
	}
	if qt.twigFile != nil {
		qt.twigFile.Close()
	}
}
