package qmtree

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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

	// keyIndex tracks the latest txNum for each (domain, key) pair written
	// during execution. It supports exclusion proofs: proving that key K
	// was last written at txNum T, or was never written at all.
	keyIndex *KeyIndex

	// keyIndexFile persists the KeyIndex to disk as segmented .kv/.kvi files.
	// nil when in-memory only (no snapDir).
	keyIndexFile *KeyIndexFile

	// snapManager handles collation (MDBX → snapshot), pruning, and merging.
	// nil when in-memory only (no snapDir).
	snapManager *SnapshotManager

	// lastCollatedStep is the last step that was collated to snapshots.
	lastCollatedStep uint64

	// keyIndexDirty tracks keys updated since the last KeyIndex flush.
	// On Flush(), only these entries are written to a new segment file.
	keyIndexDirty map[common.Hash]uint64

	// keyIndexLastFlushedQStep is the quarter-step number at the last KeyIndex flush.
	// Quarter-steps = NextSN / (stepSize/4). Segments are named by quarter-step range.
	keyIndexLastFlushedQStep uint64

	// leafData is a bounded LRU cache of LeafData keyed by serial number.
	// PreviousLeafHash is stored in each entry but is NOT persisted to disk;
	// it is recomputed on cache miss using twigPrevLeaf as an O(twig-size)
	// anchor, avoiding full-history traversal.
	leafData *lru.Cache[uint64, LeafData]

	// twigPrevLeaf[twigId] is the prevLeaf value at the START of that twig
	// (i.e. leafHash of the last entry of the prior twig, or zero for twig 0).
	// This lets us reconstruct any entry's PreviousLeafHash in at most
	// LEAF_COUNT_IN_TWIG steps from the entry file. Not persisted — rebuilt
	// from the entry file during LoadFromDisk.
	twigPrevLeaf []common.Hash

	// Disk storage handles.
	// MDBX mode: rwTx is set → entries/keyindex written to MDBX tables.
	// HPFile mode (legacy): entryFile/twigFile set → written to HPFile segments.
	// In-memory mode: both nil.
	rwTx      kv.RwTx   // current write transaction (set per batch via SetTx)
	entryFile *EntryFile // legacy HPFile storage (nil in MDBX mode)
	twigFile  *TwigFile  // legacy HPFile storage (nil in MDBX mode)

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

	// DefaultLeafCacheSize is the default number of LeafData entries to keep
	// in the bounded LRU cache. Each entry is ~160 bytes, so 200k ≈ 32 MB.
	DefaultLeafCacheSize = 200_000
)

// NewTracker creates a qmtree tracker. If snapDir is non-empty, the tree is
// backed by disk files under <snapDir>/qmtree/{entries,twigs}/. This matches
// the existing snapshot layout (e.g. snapshots/domain/, snapshots/history/).
//
// Storage is segmented by step: each HPFile segment covers exactly one step
// worth of data. Entry segment size = stepSize × 96 bytes. Twig segment
// size = ceil(stepSize/2048) × TWIG_SIZE bytes.
func NewTracker(snapDir string, stepSize uint64) (*Tracker, error) {
	hasher := &Keccak256Hasher{}
	cache, err := lru.New[uint64, LeafData](DefaultLeafCacheSize)
	if err != nil {
		return nil, fmt.Errorf("create leaf data cache: %w", err)
	}
	qt := &Tracker{
		hasher:        hasher,
		leafData:      cache,
		twigPrevLeaf:  []common.Hash{{}}, // twig 0 starts with zero prevLeaf
		stepSize:      stepSize,
		keyIndex:      NewKeyIndex(),
		keyIndexDirty: make(map[common.Hash]uint64),
	}

	if snapDir != "" {
		qmdir := filepath.Join(snapDir, trackerSubdir)
		entryDir := filepath.Join(qmdir, trackerEntrySubdir)
		twigDir := filepath.Join(qmdir, trackerTwigSubdir)
		keyIdxDir := filepath.Join(qmdir, keyIndexSubdir)
		if err := os.MkdirAll(entryDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree entry dir: %w", err)
		}
		if err := os.MkdirAll(twigDir, 0755); err != nil {
			return nil, fmt.Errorf("create qmtree twig dir: %w", err)
		}
		kif, err := NewKeyIndexFile(keyIdxDir)
		if err != nil {
			return nil, fmt.Errorf("create keyindex file: %w", err)
		}
		qt.keyIndexFile = kif

		sm, err := NewSnapshotManager(qmdir, stepSize)
		if err != nil {
			return nil, fmt.Errorf("create snapshot manager: %w", err)
		}
		qt.snapManager = sm

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

// SetTx sets the current MDBX write transaction for this batch. When set,
// entries and key-index updates are written to MDBX tables instead of HPFile.
// Call with nil to detach.
func (qt *Tracker) SetTx(tx kv.RwTx) { qt.rwTx = tx }

// AppendLeaf builds a proof leaf from individual hash components and appends
// it to the qmtree. The preStateHash, stateChangeHash, and transitionHash
// come from execution; previousLeafHash is chained automatically.
func (qt *Tracker) AppendLeaf(preStateHash, stateChangeHash, transitionHash common.Hash) {
	// Record the prevLeaf at the start of each new twig so that cache misses
	// can be reconstructed in O(LEAF_COUNT_IN_TWIG) steps.
	if qt.NextSN%LEAF_COUNT_IN_TWIG == 0 {
		twigId := qt.NextSN / LEAF_COUNT_IN_TWIG
		for uint64(len(qt.twigPrevLeaf)) <= twigId {
			qt.twigPrevLeaf = append(qt.twigPrevLeaf, common.Hash{})
		}
		qt.twigPrevLeaf[twigId] = qt.prevLeaf
	}

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

	// Write to MDBX if a transaction is set.
	if qt.rwTx != nil {
		if err := PutEntry(qt.rwTx, qt.NextSN, preStateHash, stateChangeHash, transitionHash); err != nil {
			log.Warn("qmtree: failed to write entry to MDBX", "sn", qt.NextSN, "err", err)
		}
	}

	qt.leafData.Add(qt.NextSN, ld)
	qt.prevLeaf = leafHash
	qt.NextSN++

	// Auto-collate when a step boundary is crossed.
	qt.maybeCollate()
}

// SyncRoot computes and returns the current qmtree root.
func (qt *Tracker) SyncRoot() common.Hash {
	return common.Hash(qt.tree.SyncAndRoot(qt.hasher))
}

// NotifyKeyWrites records that each key hash in keyHashes was written at txNum.
// Call this after AppendLeaf for each transaction with the set of (domain, key)
// hashes produced by the transaction's state writes.
// keyHashes is computed as keccak256(domain_byte || key_bytes) per write.
func (qt *Tracker) NotifyKeyWrites(keyHashes []common.Hash, txNum uint64) {
	for _, kh := range keyHashes {
		qt.keyIndex.UpdateKey(kh, txNum)
		qt.keyIndexDirty[kh] = txNum
		// Write to MDBX if a transaction is set.
		if qt.rwTx != nil {
			if err := PutKeyIndex(qt.rwTx, kh, txNum); err != nil {
				log.Warn("qmtree: failed to write keyindex to MDBX", "err", err)
			}
		}
	}
	qt.maybeFlushKeyIndex()
}

// KeyIndexRoot returns the Merkle root of the current key index.
// This commits to the set {(keyHash, latestTxNum)} for all keys written so far.
func (qt *Tracker) KeyIndexRoot() common.Hash {
	return qt.keyIndex.Root()
}

// KeyIndexLen returns the number of distinct keys in the index.
func (qt *Tracker) KeyIndexLen() int {
	return qt.keyIndex.Len()
}

// GetExclusionProof returns a proof that the given key hash was last written
// at the returned txNum (inclusion), or was never written (non-membership).
// The proof is verified against KeyIndexRoot().
func (qt *Tracker) GetExclusionProof(keyHash common.Hash) *ExclusionProof {
	return qt.keyIndex.GetProof(keyHash)
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
	LeafDataCount int    // cached leaf data entries
}

func (qt *Tracker) StorageStats() StorageStats {
	stats := StorageStats{
		Entries:       qt.NextSN,
		Steps:         qt.completedSteps(),
		CurrentStep:   qt.currentStep(),
		LeafDataCount: qt.leafData.Len(),
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

// twigStartPrevLeaf returns the prevLeaf value at the start of the given twig.
func (qt *Tracker) twigStartPrevLeaf(twigId uint64) common.Hash {
	if twigId < uint64(len(qt.twigPrevLeaf)) {
		return qt.twigPrevLeaf[twigId]
	}
	return common.Hash{}
}

// getLeafData returns LeafData for sn from the LRU cache, reconstructing from
// the entry file on a cache miss. Reconstruction starts from twigPrevLeaf[twigId]
// so the chain walk is at most LEAF_COUNT_IN_TWIG (2048) steps.
func (qt *Tracker) getLeafData(sn uint64) (LeafData, bool) {
	if ld, ok := qt.leafData.Get(sn); ok {
		return ld, true
	}
	if qt.entryFile == nil {
		return LeafData{}, false
	}
	twigId := sn >> TWIG_SHIFT
	twigBase := twigId * LEAF_COUNT_IN_TWIG
	prevLeaf := qt.twigStartPrevLeaf(twigId)

	for s := twigBase; s <= sn; s++ {
		if _, ok := qt.leafData.Peek(s); ok {
			// Already cached: get (updates LRU order) and advance prevLeaf.
			ld, _ := qt.leafData.Get(s)
			prevLeaf = ld.LeafHash()
			continue
		}
		pre, sc, trans, err := qt.entryFile.ReadComponents(s)
		if err != nil {
			return LeafData{}, false
		}
		ld := LeafData{
			SerialNum:        s,
			PreStateHash:     pre,
			StateChangeHash:  sc,
			TransitionHash:   trans,
			PreviousLeafHash: prevLeaf,
		}
		qt.leafData.Add(s, ld)
		prevLeaf = ld.LeafHash()
	}
	ld, ok := qt.leafData.Get(sn)
	return ld, ok
}

// getTwigLeafHashes returns all LEAF_COUNT_IN_TWIG leaf hashes for the given twig,
// using the LRU cache where possible and the entry file for misses.
// The returned slice always has length LEAF_COUNT_IN_TWIG; positions beyond
// NextSN-1 are the null entry hash.
func (qt *Tracker) getTwigLeafHashes(twigId uint64) ([]common.Hash, error) {
	twigBase := twigId * LEAF_COUNT_IN_TWIG
	hashes := make([]common.Hash, LEAF_COUNT_IN_TWIG)

	prevLeaf := qt.twigStartPrevLeaf(twigId)
	end := min(twigBase+LEAF_COUNT_IN_TWIG, qt.NextSN)

	for sn := twigBase; sn < end; sn++ {
		i := sn - twigBase
		if ld, ok := qt.leafData.Get(sn); ok {
			hashes[i] = ld.LeafHash()
			prevLeaf = hashes[i]
			continue
		}
		if qt.entryFile == nil {
			return nil, fmt.Errorf("entry file unavailable for sn=%d", sn)
		}
		pre, sc, trans, err := qt.entryFile.ReadComponents(sn)
		if err != nil {
			return nil, fmt.Errorf("read entry sn=%d: %w", sn, err)
		}
		ld := LeafData{
			SerialNum:        sn,
			PreStateHash:     pre,
			StateChangeHash:  sc,
			TransitionHash:   trans,
			PreviousLeafHash: prevLeaf,
		}
		hashes[i] = ld.LeafHash()
		qt.leafData.Add(sn, ld)
		prevLeaf = hashes[i]
	}
	return hashes, nil
}

// buildTwigMT constructs a full TwigMT (4096 nodes) from 2048 leaf hashes
// by computing all internal Merkle nodes bottom-up.
func buildTwigMT(hasher Hasher, leafHashes []common.Hash) TwigMT {
	mt := hasher.nullMtForTwig().Clone()
	copy(mt[LEAF_COUNT_IN_TWIG:], leafHashes)
	mt.Sync(hasher, 0, int32(LEAF_COUNT_IN_TWIG-1))
	return mt
}

// getProof builds a ProofPath for sn with LeftOfTwig derived from the LRU
// cache and entry file rather than from the twig file. This ensures correct
// proofs regardless of the twig file's state (handles datasets written with
// a stale prevLeaf due to a prior restart without LoadFromDisk).
//
// For completed twigs the proof is assembled directly from tree internals,
// never reading the twig file, to avoid EOF panics on incomplete twig data.
func (qt *Tracker) getProof(sn uint64) (ProofPath, error) {
	twigId := sn >> TWIG_SHIFT

	if twigId == qt.tree.youngestTwigId {
		// Youngest twig: tree has the correct TwigMT in memory already.
		return qt.tree.GetProof(sn)
	}

	// Completed twig: assemble the proof without touching the twig file.
	// The twig file may be corrupt (stale prevLeaf) or incomplete (EOF) due
	// to a prior restart without LoadFromDisk, so we never read it here.

	// UpperPath + Root: from the upper tree (in-memory, always correct after LoadFromDisk).
	upperPath, root := qt.tree.getUpperPathAndRoot(twigId)
	if len(upperPath) == 0 {
		return ProofPath{}, fmt.Errorf("cannot find upper path for twig=%d", twigId)
	}

	// LeftOfTwig: compute from LRU cache + entry file (authoritative).
	leafHashes, err := qt.getTwigLeafHashes(twigId)
	if err != nil {
		return ProofPath{}, fmt.Errorf("get twig leaf hashes twig=%d: %w", twigId, err)
	}
	mt := buildTwigMT(qt.hasher, leafHashes)

	return ProofPath{
		SerialNum:  sn,
		LeftOfTwig: GetLeftPathInMem(mt, sn),
		UpperPath:  upperPath,
		Root:       common.Hash(root),
	}, nil
}

// GetWitness generates a Witness for a single leaf by serial number.
// The tree must be synced (SyncRoot called) before calling this.
func (qt *Tracker) GetWitness(sn uint64) (*Witness, error) {
	ld, ok := qt.getLeafData(sn)
	if !ok {
		return nil, fmt.Errorf("leaf data not found for sn=%d", sn)
	}
	proof, err := qt.getProof(sn)
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
		ld, ok := qt.getLeafData(sn)
		if !ok {
			return nil, fmt.Errorf("leaf data not found for sn=%d", sn)
		}
		leaves = append(leaves, ld)
	}

	firstProof, err := qt.getProof(fromSN)
	if err != nil {
		return nil, fmt.Errorf("get first proof for sn=%d: %w", fromSN, err)
	}
	lastProof, err := qt.getProof(toSN)
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
	qt.flushKeyIndex()
	// Persist metadata to MDBX so LoadFromDB can resume.
	if qt.rwTx != nil {
		if err := PutNextSN(qt.rwTx, qt.NextSN); err != nil {
			log.Warn("qmtree: failed to write nextSN to MDBX", "err", err)
		}
		if err := PutPrevLeaf(qt.rwTx, qt.prevLeaf); err != nil {
			log.Warn("qmtree: failed to write prevLeaf to MDBX", "err", err)
		}
	}
}

// maybeCollate checks if a full step has been completed since the last
// collation and, if so, freezes that step's entries from MDBX to a snapshot
// file and prunes the MDBX rows. Requires rwTx to be set.
func (qt *Tracker) maybeCollate() {
	if qt.snapManager == nil || qt.rwTx == nil {
		return
	}
	completedStep := qt.completedSteps()
	if completedStep <= qt.lastCollatedStep {
		return
	}
	// Collate all steps that have completed since the last collation.
	for step := qt.lastCollatedStep; step < completedStep; step++ {
		if err := qt.snapManager.CollateAndPrune(context.Background(), qt.rwTx, qt.rwTx, step); err != nil {
			log.Warn("qmtree: collation failed", "step", step, "err", err)
			return
		}
		qt.lastCollatedStep = step + 1
	}
}

// quarterStep returns the current quarter-step number: NextSN / (stepSize/4).
func (qt *Tracker) quarterStep() uint64 {
	qSize := qt.stepSize / 4
	if qSize == 0 {
		qSize = 1
	}
	return qt.NextSN / qSize
}

// maybeFlushKeyIndex checks if we've crossed a quarter-step boundary since
// the last flush and, if so, writes the dirty KeyIndex entries to a new segment.
// Called automatically from NotifyKeyWrites to keep flush frequency aligned
// with Erigon's step structure (4 flushes per step).
func (qt *Tracker) maybeFlushKeyIndex() {
	currentQStep := qt.quarterStep()
	if currentQStep <= qt.keyIndexLastFlushedQStep {
		return
	}
	qt.flushKeyIndex()
}

// flushKeyIndex writes dirty KeyIndex entries to a new segment file.
func (qt *Tracker) flushKeyIndex() {
	if qt.keyIndexFile == nil || len(qt.keyIndexDirty) == 0 {
		return
	}
	entries := make([]KeyIndexEntry, 0, len(qt.keyIndexDirty))
	for kh, txNum := range qt.keyIndexDirty {
		entries = append(entries, KeyIndexEntry{KeyHash: kh, TxNum: txNum})
	}
	currentQStep := qt.quarterStep()
	fromQStep := qt.keyIndexLastFlushedQStep
	toQStep := currentQStep
	if toQStep <= fromQStep {
		toQStep = fromQStep + 1 // ensure unique range
	}
	if err := qt.keyIndexFile.FlushDelta(context.Background(), entries, fromQStep, toQStep); err != nil {
		log.Warn("qmtree: failed to flush keyindex", "err", err)
		return
	}
	qt.keyIndexDirty = make(map[common.Hash]uint64)
	qt.keyIndexLastFlushedQStep = toQStep
	log.Info("qmtree: flushed keyindex delta",
		"entries", len(entries),
		"fromQStep", fromQStep,
		"toQStep", toQStep,
		"segments", qt.keyIndexFile.SegmentCount(),
	)
}

// UnwindTo truncates the tree and storage back to the given serial number.
// All entries with sn >= toSN are discarded. The target SN is the first
// entry to discard (i.e. entries 0..toSN-1 are kept).
func (qt *Tracker) UnwindTo(toSN uint64) {
	if toSN >= qt.NextSN {
		return // nothing to unwind
	}

	if toSN == 0 {
		qt.tree.UnwindTo(0, nil)
	} else {
		lastKept := toSN - 1
		twigStart := lastKept &^ TWIG_MASK
		posInTwig := lastKept & TWIG_MASK

		var entryHashes []common.Hash
		if posInTwig < TWIG_MASK {
			entryHashes = make([]common.Hash, posInTwig+1)
			for i := uint64(0); i <= posInTwig; i++ {
				if ld, ok := qt.leafData.Peek(twigStart + i); ok {
					entryHashes[i] = ld.LeafHash()
				}
			}
		}
		qt.tree.UnwindTo(lastKept, entryHashes)
	}

	// Remove unwound entries from the LRU cache.
	for sn := toSN; sn < qt.NextSN; sn++ {
		qt.leafData.Remove(sn)
	}
	qt.NextSN = toSN

	// Truncate twigPrevLeaf to the twigs that remain.
	if toSN > 0 {
		lastKeptTwig := (toSN - 1) >> TWIG_SHIFT
		if lastKeptTwig+1 < uint64(len(qt.twigPrevLeaf)) {
			qt.twigPrevLeaf = qt.twigPrevLeaf[:lastKeptTwig+1]
		}
	} else {
		qt.twigPrevLeaf = qt.twigPrevLeaf[:1] // keep twig 0's zero anchor
	}

	// Recompute prevLeaf from the last remaining entry.
	if toSN > 0 {
		if ld, ok := qt.getLeafData(toSN - 1); ok {
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
// Strategy: replay all entries through an in-memory-only tree (nil storage so
// no data is re-written to disk). The twig file is left unchanged — LeftOfTwig
// paths for completed twigs are computed from the LRU cache and twigPrevLeaf
// checkpoints via Tracker.getProof, not from the twig file directly.
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

	// Replay through an in-memory tree (nil storage — don't re-write files).
	replayTree := NewTree(qt.hasher, 0, nil, nil)

	var prevLeaf common.Hash
	twigPrevLeaf := []common.Hash{{}} // twig 0 starts with zero prevLeaf

	for sn := uint64(0); sn < count; sn++ {
		// Record twig-start prevLeaf checkpoint.
		if sn > 0 && sn%LEAF_COUNT_IN_TWIG == 0 {
			twigId := sn / LEAF_COUNT_IN_TWIG
			for uint64(len(twigPrevLeaf)) <= twigId {
				twigPrevLeaf = append(twigPrevLeaf, common.Hash{})
			}
			twigPrevLeaf[twigId] = prevLeaf
		}

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
		qt.leafData.Add(sn, ld)      // LRU evicts oldest entries automatically
		prevLeaf = leafHash
	}

	// Attach the real disk handles to the replay tree. The caller is responsible
	// for calling SyncRoot() to build the upper-tree node cache.
	replayTree.entryStorage = qt.tree.entryStorage
	replayTree.twigStorage = qt.tree.twigStorage

	qt.tree = replayTree
	qt.twigPrevLeaf = twigPrevLeaf
	qt.prevLeaf = prevLeaf
	qt.NextSN = count

	// Load persisted KeyIndex segments if available. If no segments exist,
	// the KeyIndex was already rebuilt during the entry replay above (via
	// the caller's NotifyKeyWrites calls). On first run after adding
	// persistence, the caller should flush to bootstrap the .kv/.kvi files.
	if qt.keyIndexFile != nil {
		maxStep, err := qt.keyIndexFile.LoadAll()
		if err != nil {
			log.Warn("qmtree: failed to load keyindex files, will rebuild from entries", "err", err)
		} else if maxStep > 0 {
			qt.keyIndexFile.PopulateKeyIndex(qt.keyIndex)
			qt.keyIndexLastFlushedQStep = maxStep
			log.Info("qmtree: loaded keyindex from disk",
				"keys", qt.keyIndex.Len(),
				"segments", qt.keyIndexFile.SegmentCount(),
				"maxStep", maxStep,
			)
		}
	}

	log.Info("qmtree: loaded from disk",
		"entries", count,
		"entryStorage", common.ByteCount(uint64(fileSize)),
		"cachedLeaves", qt.leafData.Len(),
		"keyIndexKeys", qt.keyIndex.Len(),
	)
	return nil
}

// LoadFromDB rebuilds the in-memory tree from QMTreeEntries in MDBX.
// This is faster than LoadFromDisk (HPFile replay) because MDBX reads are
// sequential and the metadata (NextSN, prevLeaf) is stored directly.
func (qt *Tracker) LoadFromDB(tx kv.Tx) error {
	nextSN, err := GetNextSN(tx)
	if err != nil {
		return fmt.Errorf("qmtree LoadFromDB: read nextSN: %w", err)
	}
	if nextSN == 0 {
		return nil // empty, nothing to load
	}

	prevLeaf, err := GetPrevLeaf(tx)
	if err != nil {
		return fmt.Errorf("qmtree LoadFromDB: read prevLeaf: %w", err)
	}

	// Replay through an in-memory tree.
	replayTree := NewTree(qt.hasher, 0, nil, nil)
	var currentPrevLeaf common.Hash
	twigPrevLeaf := []common.Hash{{}}

	for sn := uint64(0); sn < nextSN; sn++ {
		if sn > 0 && sn%LEAF_COUNT_IN_TWIG == 0 {
			twigId := sn / LEAF_COUNT_IN_TWIG
			for uint64(len(twigPrevLeaf)) <= twigId {
				twigPrevLeaf = append(twigPrevLeaf, common.Hash{})
			}
			twigPrevLeaf[twigId] = currentPrevLeaf
		}

		pre, sc, trans, err := GetEntry(tx, sn)
		if err != nil {
			return fmt.Errorf("qmtree LoadFromDB: read entry sn=%d: %w", sn, err)
		}
		ld := LeafData{
			SerialNum:        sn,
			PreStateHash:     pre,
			StateChangeHash:  sc,
			TransitionHash:   trans,
			PreviousLeafHash: currentPrevLeaf,
		}
		leafHash := ld.LeafHash()
		entry := &proofEntry{sn: sn, hash: leafHash, pre: pre, stateChange: sc, transition: trans}
		replayTree.AppendEntry(entry)
		qt.leafData.Add(sn, ld)
		currentPrevLeaf = leafHash
	}

	// Attach disk handles from existing tree if any.
	replayTree.entryStorage = qt.tree.entryStorage
	replayTree.twigStorage = qt.tree.twigStorage

	qt.tree = replayTree
	qt.twigPrevLeaf = twigPrevLeaf
	qt.prevLeaf = prevLeaf
	qt.NextSN = nextSN

	// Load existing entry snapshots so we know what's already frozen.
	if qt.snapManager != nil {
		maxFrozenStep, err := qt.snapManager.LoadSnapshots()
		if err != nil {
			log.Warn("qmtree: failed to load snapshots", "err", err)
		} else {
			qt.lastCollatedStep = maxFrozenStep
		}
	}

	log.Info("qmtree: loaded from MDBX",
		"entries", nextSN,
		"cachedLeaves", qt.leafData.Len(),
		"frozenSteps", qt.lastCollatedStep,
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
	if qt.keyIndexFile != nil {
		qt.keyIndexFile.Close()
	}
	if qt.snapManager != nil {
		qt.snapManager.Close()
	}
}
