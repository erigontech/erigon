package qmtree

import (
	"context"
	"fmt"
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
	txNum       uint64
	hash        common.Hash // precomputed leaf hash, used by tree for twig building
	pre         common.Hash
	stateChange common.Hash
	transition  common.Hash
}

func (e *proofEntry) TxNum() uint64 { return e.txNum }
func (e *proofEntry) Hash() common.Hash    { return e.hash }
func (e *proofEntry) Len() int64           { return 0 }
func (e *proofEntry) Components() (pre, stateChange, transition common.Hash) {
	return e.pre, e.stateChange, e.transition
}

// Tracker holds per-sync qmtree state for the serial executor.
// Entry data is written to MDBX tables during execution and frozen to
// snapshot files (.kv/.kvi) at step boundaries via the SnapshotManager.
// Leaf component data is cached in an LRU for witness/proof generation.
type Tracker struct {
	tree     *Tree
	hasher   *Keccak256Hasher
	NextTxNum uint64
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

	// keyIndexLastFlushedStep is the step number at the last KeyIndex flush.
	// Aligned to the same step boundaries as entry collation.
	keyIndexLastFlushedStep uint64

	// leafData is a bounded LRU cache of LeafData keyed by serial number.
	// PreviousLeafHash is stored in each entry but is NOT persisted to disk;
	// it is recomputed on cache miss using twigPrevLeaf as an O(twig-size)
	// anchor, avoiding full-history traversal.
	leafData *lru.Cache[uint64, LeafData]

	// twigPrevLeaf[twigId] is the prevLeaf value at the START of that twig
	// (i.e. leafHash of the last entry of the prior twig, or zero for twig 0).
	// This lets us reconstruct any entry's PreviousLeafHash in at most
	// LEAF_COUNT_IN_TWIG steps. Rebuilt from entries during LoadFromDB.
	twigPrevLeaf []common.Hash

	// MDBX write transaction for the current batch. Set via SetTx().
	// When set, entries and key-index updates are written to MDBX tables.
	rwTx kv.RwTx

	// Step tracking.
	stepSize       uint64 // entries per step (matches config3.DefaultStepSize)
	lastStepLogged uint64 // last completed step that was logged
}

const (
	// DefaultLeafCacheSize is the default number of LeafData entries to keep
	// in the bounded LRU cache. Each entry is ~160 bytes, so 200k ≈ 32 MB.
	DefaultLeafCacheSize = 200_000
)

// NewTracker creates a qmtree tracker. If snapDir is non-empty, KeyIndex
// and SnapshotManager are initialized for snapshot persistence.
// Entry data is written to MDBX via SetTx(); the tree is always in-memory.
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
		tree:          NewTree(hasher, 0, nil, nil),
	}

	if snapDir != "" {
		// All qmtree snapshot files go to snapshots/domain/ alongside
		// other domain files, distinguished by the .qmtree.kv extension.
		domainDir := filepath.Join(snapDir, "domain")

		kif, err := NewKeyIndexFile(domainDir)
		if err != nil {
			return nil, fmt.Errorf("create keyindex file: %w", err)
		}
		qt.keyIndexFile = kif

		sm, err := NewSnapshotManager(domainDir, stepSize)
		if err != nil {
			return nil, fmt.Errorf("create snapshot manager: %w", err)
		}
		qt.snapManager = sm
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
	if qt.NextTxNum%LEAF_COUNT_IN_TWIG == 0 {
		twigId := qt.NextTxNum / LEAF_COUNT_IN_TWIG
		for uint64(len(qt.twigPrevLeaf)) <= twigId {
			qt.twigPrevLeaf = append(qt.twigPrevLeaf, common.Hash{})
		}
		qt.twigPrevLeaf[twigId] = qt.prevLeaf
	}

	ld := LeafData{
		TxNum:            qt.NextTxNum,
		PreStateHash:     preStateHash,
		StateChangeHash:  stateChangeHash,
		TransitionHash:   transitionHash,
		PreviousLeafHash: qt.prevLeaf,
	}
	leafHash := ld.LeafHash()

	entry := &proofEntry{txNum: qt.NextTxNum, hash: leafHash, pre: preStateHash, stateChange: stateChangeHash, transition: transitionHash}
	qt.tree.AppendEntry(entry)

	// Write to MDBX if a transaction is set.
	if qt.rwTx != nil {
		if err := PutEntry(qt.rwTx, qt.NextTxNum, preStateHash, stateChangeHash, transitionHash); err != nil {
			log.Warn("qmtree: failed to write entry to MDBX", "txNum", qt.NextTxNum, "err", err)
		}
	}

	qt.leafData.Add(qt.NextTxNum, ld)
	qt.prevLeaf = leafHash
	qt.NextTxNum++

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
	if qt.NextTxNum == 0 {
		return 0
	}
	return (qt.NextTxNum - 1) / qt.stepSize
}

// completedSteps returns the number of fully completed steps.
func (qt *Tracker) completedSteps() uint64 {
	return qt.NextTxNum / qt.stepSize
}

// LogStepProgress logs when a new step is completed.
func (qt *Tracker) LogStepProgress(logPrefix string) {
	completed := qt.completedSteps()
	if completed > qt.lastStepLogged {
		for step := qt.lastStepLogged + 1; step <= completed; step++ {
			frozen := 0
			if qt.snapManager != nil {
				frozen = qt.snapManager.FrozenEntries()
			}
			log.Info(fmt.Sprintf("[%s] qmtree step completed", logPrefix),
				"step", step,
				"entries", step*qt.stepSize,
				"frozenEntries", frozen,
				"frozenSteps", qt.lastCollatedStep,
			)
		}
		qt.lastStepLogged = completed
	}
}

// StorageStats returns current storage sizes.
type StorageStats struct {
	Entries        uint64 // total entries (leaves)
	Steps          uint64 // completed steps
	CurrentStep    uint64 // current (possibly incomplete) step
	FrozenEntries  int    // entries in frozen snapshots
	FrozenSteps    uint64 // steps frozen to snapshots
	LeafDataCount  int    // cached leaf data entries
	KeyIndexKeys   int    // distinct keys in key index
}

func (qt *Tracker) StorageStats() StorageStats {
	stats := StorageStats{
		Entries:       qt.NextTxNum,
		Steps:         qt.completedSteps(),
		CurrentStep:   qt.currentStep(),
		FrozenSteps:   qt.lastCollatedStep,
		LeafDataCount: qt.leafData.Len(),
		KeyIndexKeys:  qt.keyIndex.Len(),
	}
	if qt.snapManager != nil {
		stats.FrozenEntries = qt.snapManager.FrozenEntries()
	}
	return stats
}

// twigStartPrevLeaf returns the prevLeaf value at the start of the given twig.
func (qt *Tracker) twigStartPrevLeaf(twigId uint64) common.Hash {
	if twigId < uint64(len(qt.twigPrevLeaf)) {
		return qt.twigPrevLeaf[twigId]
	}
	return common.Hash{}
}

// readComponents reads entry components for the given txNum.
// Checks: MDBX (hot) → snapshots (frozen). Returns error if not found.
func (qt *Tracker) readComponents(txNum uint64) (pre, sc, trans common.Hash, err error) {
	// Try MDBX first (hot data).
	if qt.rwTx != nil {
		pre, sc, trans, err = GetEntry(qt.rwTx, txNum)
		if err == nil {
			return
		}
	}
	// Try frozen snapshots.
	if qt.snapManager != nil {
		pre, sc, trans, found := qt.snapManager.GetEntryFromSnapshots(txNum)
		if found {
			return pre, sc, trans, nil
		}
	}
	return common.Hash{}, common.Hash{}, common.Hash{}, fmt.Errorf("entry not found: txNum=%d", txNum)
}

// getLeafData returns LeafData for txNum from the LRU cache, reconstructing from
// MDBX/snapshots on a cache miss. Reconstruction starts from twigPrevLeaf[twigId]
// so the chain walk is at most LEAF_COUNT_IN_TWIG (2048) steps.
func (qt *Tracker) getLeafData(txNum uint64) (LeafData, bool) {
	if ld, ok := qt.leafData.Get(txNum); ok {
		return ld, true
	}
	twigId := txNum >> TWIG_SHIFT
	twigBase := twigId * LEAF_COUNT_IN_TWIG
	prevLeaf := qt.twigStartPrevLeaf(twigId)

	for s := twigBase; s <= txNum; s++ {
		if _, ok := qt.leafData.Peek(s); ok {
			ld, _ := qt.leafData.Get(s)
			prevLeaf = ld.LeafHash()
			continue
		}
		pre, sc, trans, err := qt.readComponents(s)
		if err != nil {
			return LeafData{}, false
		}
		ld := LeafData{
			TxNum:            s,
			PreStateHash:     pre,
			StateChangeHash:  sc,
			TransitionHash:   trans,
			PreviousLeafHash: prevLeaf,
		}
		qt.leafData.Add(s, ld)
		prevLeaf = ld.LeafHash()
	}
	ld, ok := qt.leafData.Get(txNum)
	return ld, ok
}

// getTwigLeafHashes returns all LEAF_COUNT_IN_TWIG leaf hashes for the given twig,
// using the LRU cache where possible and MDBX/snapshots for misses.
// The returned slice always has length LEAF_COUNT_IN_TWIG; positions beyond
// NextTxNum-1 are the null entry hash.
func (qt *Tracker) getTwigLeafHashes(twigId uint64) ([]common.Hash, error) {
	twigBase := twigId * LEAF_COUNT_IN_TWIG
	hashes := make([]common.Hash, LEAF_COUNT_IN_TWIG)

	prevLeaf := qt.twigStartPrevLeaf(twigId)
	end := min(twigBase+LEAF_COUNT_IN_TWIG, qt.NextTxNum)

	for txNum := twigBase; txNum < end; txNum++ {
		i := txNum - twigBase
		if ld, ok := qt.leafData.Get(txNum); ok {
			hashes[i] = ld.LeafHash()
			prevLeaf = hashes[i]
			continue
		}
		pre, sc, trans, err := qt.readComponents(txNum)
		if err != nil {
			return nil, fmt.Errorf("read entry txNum=%d: %w", txNum, err)
		}
		ld := LeafData{
			TxNum:            txNum,
			PreStateHash:     pre,
			StateChangeHash:  sc,
			TransitionHash:   trans,
			PreviousLeafHash: prevLeaf,
		}
		hashes[i] = ld.LeafHash()
		qt.leafData.Add(txNum, ld)
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

// getProof builds a ProofPath for txNum with LeftOfTwig derived from the LRU
// cache and MDBX/snapshot entries rather than from a twig file.
//
// For completed twigs the proof is assembled from tree internals and entry
// data reconstructed via readComponents.
func (qt *Tracker) getProof(txNum uint64) (ProofPath, error) {
	twigId := txNum >> TWIG_SHIFT

	if twigId == qt.tree.youngestTwigId {
		// Youngest twig: tree has the correct TwigMT in memory already.
		return qt.tree.GetProof(txNum)
	}

	// Completed twig: assemble the proof from entry data and upper tree.

	// UpperPath + Root: from the upper tree (in-memory, always correct after LoadFromDB).
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
		TxNum:  txNum,
		LeftOfTwig: GetLeftPathInMem(mt, txNum),
		UpperPath:  upperPath,
		Root:       common.Hash(root),
	}, nil
}

// GetWitness generates a Witness for a single leaf by txNum.
// The tree must be synced (SyncRoot called) before calling this.
func (qt *Tracker) GetWitness(txNum uint64) (*Witness, error) {
	ld, ok := qt.getLeafData(txNum)
	if !ok {
		return nil, fmt.Errorf("leaf data not found for txNum=%d", txNum)
	}
	proof, err := qt.getProof(txNum)
	if err != nil {
		return nil, fmt.Errorf("get proof for txNum=%d: %w", txNum, err)
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
	for txNum := fromSN; txNum <= toSN; txNum++ {
		ld, ok := qt.getLeafData(txNum)
		if !ok {
			return nil, fmt.Errorf("leaf data not found for txNum=%d", txNum)
		}
		leaves = append(leaves, ld)
	}

	firstProof, err := qt.getProof(fromSN)
	if err != nil {
		return nil, fmt.Errorf("get first proof for txNum=%d: %w", fromSN, err)
	}
	lastProof, err := qt.getProof(toSN)
	if err != nil {
		return nil, fmt.Errorf("get last proof for txNum=%d: %w", toSN, err)
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
	qt.flushKeyIndex()
	// Persist metadata to MDBX so LoadFromDB can resume.
	if qt.rwTx != nil {
		if err := PutNextTxNum(qt.rwTx, qt.NextTxNum); err != nil {
			log.Warn("qmtree: failed to write nextTxNum to MDBX", "err", err)
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

// maybeFlushKeyIndex checks if we've crossed a step boundary since the last
// flush and, if so, writes the dirty KeyIndex entries to a new segment.
// Aligned to the same step boundaries as entry collation.
func (qt *Tracker) maybeFlushKeyIndex() {
	currentStep := qt.completedSteps()
	if currentStep <= qt.keyIndexLastFlushedStep {
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
	currentStep := qt.completedSteps()
	fromStep := qt.keyIndexLastFlushedStep
	toStep := currentStep
	if toStep <= fromStep {
		toStep = fromStep + 1
	}
	if err := qt.keyIndexFile.FlushDelta(context.Background(), entries, fromStep, toStep); err != nil {
		log.Warn("qmtree: failed to flush keyindex", "err", err)
		return
	}
	qt.keyIndexDirty = make(map[common.Hash]uint64)
	qt.keyIndexLastFlushedStep = toStep
	log.Info("qmtree: flushed keyindex delta",
		"entries", len(entries),
		"fromStep", fromStep,
		"toStep", toStep,
		"segments", qt.keyIndexFile.SegmentCount(),
	)
}

// UnwindTo truncates the tree and storage back to the given serial number.
// All entries with sn >= toSN are discarded. The target SN is the first
// entry to discard (i.e. entries 0..toSN-1 are kept).
func (qt *Tracker) UnwindTo(toSN uint64) {
	if toSN >= qt.NextTxNum {
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
	for txNum := toSN; txNum < qt.NextTxNum; txNum++ {
		qt.leafData.Remove(txNum)
	}
	qt.NextTxNum = toSN

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

// LoadFromDB rebuilds the in-memory tree from QMTreeEntries in MDBX and
// frozen snapshot files. Reads NextTxNum/prevLeaf from QMTreeMeta, then replays
// all entries (snapshots + MDBX hot) through the in-memory tree.
func (qt *Tracker) LoadFromDB(tx kv.Tx) error {
	nextTxNum, err := GetNextTxNum(tx)
	if err != nil {
		return fmt.Errorf("qmtree LoadFromDB: read nextTxNum: %w", err)
	}
	if nextTxNum == 0 {
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

	for txNum := uint64(0); txNum < nextTxNum; txNum++ {
		if txNum > 0 && txNum%LEAF_COUNT_IN_TWIG == 0 {
			twigId := txNum / LEAF_COUNT_IN_TWIG
			for uint64(len(twigPrevLeaf)) <= twigId {
				twigPrevLeaf = append(twigPrevLeaf, common.Hash{})
			}
			twigPrevLeaf[twigId] = currentPrevLeaf
		}

		pre, sc, trans, err := GetEntry(tx, txNum)
		if err != nil {
			return fmt.Errorf("qmtree LoadFromDB: read entry txNum=%d: %w", txNum, err)
		}
		ld := LeafData{
			TxNum:            txNum,
			PreStateHash:     pre,
			StateChangeHash:  sc,
			TransitionHash:   trans,
			PreviousLeafHash: currentPrevLeaf,
		}
		leafHash := ld.LeafHash()
		entry := &proofEntry{txNum: txNum, hash: leafHash, pre: pre, stateChange: sc, transition: trans}
		replayTree.AppendEntry(entry)
		qt.leafData.Add(txNum, ld)
		currentPrevLeaf = leafHash
	}

	// Attach disk handles from existing tree if any.
	replayTree.entryStorage = qt.tree.entryStorage
	replayTree.twigStorage = qt.tree.twigStorage

	qt.tree = replayTree
	qt.twigPrevLeaf = twigPrevLeaf
	qt.prevLeaf = prevLeaf
	qt.NextTxNum = nextTxNum

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
		"entries", nextTxNum,
		"cachedLeaves", qt.leafData.Len(),
		"frozenSteps", qt.lastCollatedStep,
	)
	return nil
}

// Close flushes and closes disk storage.
// Implements execctx.AppendOnlyFlusher.
func (qt *Tracker) Close() {
	qt.Flush()
	if qt.keyIndexFile != nil {
		qt.keyIndexFile.Close()
	}
	if qt.snapManager != nil {
		qt.snapManager.Close()
	}
}
