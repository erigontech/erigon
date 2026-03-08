package stagedsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
	"github.com/erigontech/erigon/execution/exec"
)

// proofEntry is a minimal qmtree.Entry for the production pipeline.
// It wraps a serial number and a pre-computed leaf hash.
type proofEntry struct {
	sn   uint64
	hash common.Hash
}

func (e *proofEntry) SerialNumber() uint64 { return e.sn }
func (e *proofEntry) Hash() common.Hash    { return e.hash }
func (e *proofEntry) Len() int64           { return 0 }

// qmtreeTracker holds per-sync qmtree state for the serial executor.
// When a datadir is provided, the tree is backed by disk files (EntryFile +
// TwigFile) so it survives restarts and can generate proofs after the fact.
// It also retains leaf component data for witness generation.
//
// Storage is segmented into steps aligned with erigon's domain step size.
// Each HPFile segment covers one step's worth of entries/twigs, making
// completed segments individually distributable.
type qmtreeTracker struct {
	tree     *qmtree.Tree
	hasher   *qmtree.Keccak256Hasher
	nextSN   uint64
	prevLeaf common.Hash

	// Leaf component storage for witness generation.
	// Keyed by serial number. In the future this could be persisted
	// to disk, but for the PoC it's held in memory.
	leafData map[uint64]qmtree.LeafData

	// Disk storage handles (nil when in-memory only).
	entryFile *qmtree.EntryFile
	twigFile  *qmtree.TwigFile

	// Step tracking.
	stepSize       uint64 // entries per step (matches config3.DefaultStepSize)
	lastStepLogged uint64 // last completed step that was logged
}

const (
	qmtreeSubdir      = "qmtree" // lives under snapshots/
	qmtreeEntrySubdir = "entries"
	qmtreeTwigSubdir  = "twigs"

	// Entry segments: one step = stepSize entries × 32 bytes/entry.
	// Buffer size chosen so segmentSize % bufferSize == 0.
	// 50,000,000 / 500,000 = 100.
	qmtreeEntryBufSize = 500_000

	// Twig segments: one step ≈ 763 twigs (ceil(stepSize / 2048)).
	// Buffer size = TWIG_SIZE so each buffer flush writes exactly one twig.
	qmtreeTwigBufSize = 73_708 // = TWIG_SIZE = 12 + (4095-1792)*32

	// Twigs per step: ceil(stepSize / LEAF_COUNT_IN_TWIG).
	qmtreeTwigsPerStep = 763 // ceil(1_562_500 / 2048)
)

// newQmtreeTracker creates a qmtree tracker. If snapDir is non-empty, the
// tree is backed by disk files under <snapDir>/qmtree/{entries,twigs}/.
// This matches the existing snapshot layout (e.g. snapshots/domain/,
// snapshots/history/).
//
// Storage is segmented by step: each HPFile segment covers exactly one step
// worth of data. Entry segment size = stepSize × 32 bytes. Twig segment
// size = ceil(stepSize/2048) × TWIG_SIZE bytes.
func newQmtreeTracker(snapDir string) (*qmtreeTracker, error) {
	hasher := &qmtree.Keccak256Hasher{}
	stepSize := uint64(config3.DefaultStepSize)
	qt := &qmtreeTracker{
		hasher:   hasher,
		leafData: make(map[uint64]qmtree.LeafData),
		stepSize: stepSize,
	}

	if snapDir != "" {
		qmdir := filepath.Join(snapDir, qmtreeSubdir)
		entryDir := filepath.Join(qmdir, qmtreeEntrySubdir)
		twigDir := filepath.Join(qmdir, qmtreeTwigSubdir)
		if err := os.MkdirAll(entryDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree entry dir: %w", err)
		}
		if err := os.MkdirAll(twigDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree twig dir: %w", err)
		}

		entrySegSize := stepSize * 32 // one step per segment
		twigSegSize := uint64(qmtreeTwigsPerStep) * uint64(qmtreeTwigBufSize)

		ef, err := qmtree.NewEntryFile(qmtreeEntryBufSize, entrySegSize, entryDir)
		if err != nil {
			return nil, fmt.Errorf("open qmtree entry file: %w", err)
		}
		tf, err := qmtree.NewTwigFile(qmtreeTwigBufSize, twigSegSize, twigDir, hasher)
		if err != nil {
			ef.Close()
			return nil, fmt.Errorf("open qmtree twig file: %w", err)
		}

		qt.entryFile = ef
		qt.twigFile = tf
		qt.tree = qmtree.NewTree(hasher, 0, ef, tf)
	} else {
		qt.tree = qmtree.NewTree(hasher, 0, nil, nil)
	}

	return qt, nil
}

// appendTxResult builds a proof leaf from a transaction execution result
// and appends it to the qmtree.
func (qt *qmtreeTracker) appendTxResult(result *exec.TxResult) {
	ld := qmtree.LeafData{
		SerialNum:        qt.nextSN,
		PreStateHash:     result.ExecutionResult.PreStateHash,
		StateChangeHash:  result.ExecutionResult.StateChangeHash,
		TransitionHash:   result.ExecutionResult.TransitionHash,
		PreviousLeafHash: qt.prevLeaf,
	}
	leafHash := ld.LeafHash()

	entry := &proofEntry{sn: qt.nextSN, hash: leafHash}
	qt.tree.AppendEntry(entry)

	qt.leafData[qt.nextSN] = ld
	qt.prevLeaf = leafHash
	qt.nextSN++
}

// syncRoot computes and returns the current qmtree root.
func (qt *qmtreeTracker) syncRoot() common.Hash {
	return common.Hash(qt.tree.SyncAndRoot(qt.hasher))
}

// currentStep returns the step number for the current serial number.
func (qt *qmtreeTracker) currentStep() uint64 {
	if qt.nextSN == 0 {
		return 0
	}
	return (qt.nextSN - 1) / qt.stepSize
}

// completedSteps returns the number of fully completed steps.
func (qt *qmtreeTracker) completedSteps() uint64 {
	return qt.nextSN / qt.stepSize
}

// logStepProgress logs when a new step is completed.
func (qt *qmtreeTracker) logStepProgress(logPrefix string) {
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

// storageStats returns current storage sizes.
type qmtreeStorageStats struct {
	Entries       uint64 // total entries (leaves)
	Steps         uint64 // completed steps
	CurrentStep   uint64 // current (possibly incomplete) step
	EntryBytes    int64  // entry file size
	TwigBytes     int64  // twig file size
	TotalBytes    int64  // total storage
	LeafDataCount int    // in-memory leaf data entries
}

func (qt *qmtreeTracker) storageStats() qmtreeStorageStats {
	stats := qmtreeStorageStats{
		Entries:       qt.nextSN,
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

// getWitness generates a Witness for a single leaf by serial number.
// The tree must be synced (syncRoot called) before calling this.
func (qt *qmtreeTracker) getWitness(sn uint64) (*qmtree.Witness, error) {
	ld, ok := qt.leafData[sn]
	if !ok {
		return nil, fmt.Errorf("leaf data not found for sn=%d", sn)
	}
	proof, err := qt.tree.GetProof(sn)
	if err != nil {
		return nil, fmt.Errorf("get proof for sn=%d: %w", sn, err)
	}
	return &qmtree.Witness{
		Proof:            proof,
		PreStateHash:     ld.PreStateHash,
		StateChangeHash:  ld.StateChangeHash,
		TransitionHash:   ld.TransitionHash,
		PreviousLeafHash: ld.PreviousLeafHash,
	}, nil
}

// getRangeWitness generates a RangeWitness for a contiguous range of leaves.
// The tree must be synced before calling this.
func (qt *qmtreeTracker) getRangeWitness(fromSN, toSN uint64) (*qmtree.RangeWitness, error) {
	if fromSN > toSN {
		return nil, fmt.Errorf("invalid range: from=%d > to=%d", fromSN, toSN)
	}

	leaves := make([]qmtree.LeafData, 0, toSN-fromSN+1)
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

	return &qmtree.RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     leaves,
	}, nil
}

// close flushes and closes disk storage.
func (qt *qmtreeTracker) close() {
	if qt.entryFile != nil {
		qt.entryFile.Flush()
		qt.entryFile.Close()
	}
	if qt.twigFile != nil {
		qt.twigFile.Flush()
		qt.twigFile.Close()
	}
}
