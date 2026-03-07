package stagedsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
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
}

const (
	qmtreeSubdir         = "qmtree"
	qmtreeEntrySubdir    = "entries"
	qmtreeTwigSubdir     = "twigs"
	qmtreeBufferSize     = 1 << 20 // 1 MB buffer
	qmtreeEntrySegSize   = 1 << 30 // 1 GB segments
	qmtreeTwigSegSize    = 1 << 30
)

// newQmtreeTracker creates a qmtree tracker. If dataDir is non-empty, the
// tree is backed by disk files under <dataDir>/qmtree/. Otherwise it
// operates purely in memory.
func newQmtreeTracker(dataDir string) (*qmtreeTracker, error) {
	hasher := &qmtree.Keccak256Hasher{}
	qt := &qmtreeTracker{
		hasher:   hasher,
		leafData: make(map[uint64]qmtree.LeafData),
	}

	if dataDir != "" {
		qmdir := filepath.Join(dataDir, qmtreeSubdir)
		entryDir := filepath.Join(qmdir, qmtreeEntrySubdir)
		twigDir := filepath.Join(qmdir, qmtreeTwigSubdir)
		if err := os.MkdirAll(entryDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree entry dir: %w", err)
		}
		if err := os.MkdirAll(twigDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree twig dir: %w", err)
		}

		ef, err := qmtree.NewEntryFile(qmtreeBufferSize, qmtreeEntrySegSize, entryDir)
		if err != nil {
			return nil, fmt.Errorf("open qmtree entry file: %w", err)
		}
		tf, err := qmtree.NewTwigFile(qmtreeBufferSize, qmtreeTwigSegSize, twigDir, hasher)
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
	var zero common.Hash
	ld := qmtree.LeafData{
		SerialNum:        qt.nextSN,
		PreStateHash:     zero, // future: from PreStateHasher
		StateChangeHash:  zero, // future: from state writer
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
