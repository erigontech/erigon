// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitmentdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/execution/commitment"
)

// RecomputeAtTxNumWithoutSD computes the patricia trie root + encoded
// trie state at toTxNum WITHOUT opening a SharedDomains. It bypasses
// NewSharedDomains' internal SeekCommitment + behind-commitment guard
// — those assume forward-execution consistency that admin SetHead
// mode B deliberately breaks (TxNums is wiped past toBlock; the
// over-step commitment file still surfaces via mmap until aggregator
// OpenFolder runs, which can't run mid-tx).
//
// The function:
//
//  1. Looks up a file-side commitment baseline whose end step ≤
//     maxStep, via tx.Debug().GetLatestFromFilesUpToStep. If found,
//     restores the patricia trie state from that record's encoded
//     trieState bytes (the per-file commitment record is exactly the
//     shape NewCommitmentState writes).
//  2. If no baseline found, the trie starts empty (genesis recompute).
//  3. Touches every account/storage/code key changed in
//     (baselineTxNum, toTxNum+1] via tx.Debug().HistoryKeyTxNumRange.
//     Accounts/storage/code domains have history tracking; this
//     range query is well-defined.
//  4. Calls trie.Process(updates) to fold the touches into the trie
//     and compute the root.
//  5. Encodes the resulting trie state via EncodeCurrentState.
//
// State reader: SplitStateReader with the commitment branch reads
// going through StepBoundedFilesStateReader (so the over-step file's
// commitment branches don't pollute the in-progress trie hashing)
// and the plain-state reads going through HistoryStateReader at
// toTxNum (accounts/storage/code's GetAsOf works correctly because
// those domains DO have history).
//
// Returns (root, encodedTrieState, baselineTxNum, branches, error).
//
// The branches collector captures every commitment branch recomputed
// by trie.Process (via PutBranch). Caller drains it after the
// boundary-step wipe to repopulate commitment branches at lastTxNum
// — see Provider.Unwind's compute/apply split. branches is non-nil
// on successful return; caller owns Close().
//
// The baselineTxNum return lets the caller decide whether to also
// write the new commitment entry into the writable shadow at toTxNum.
func RecomputeAtTxNumWithoutSD(
	ctx context.Context,
	tx kv.TemporalTx,
	tmpDir string,
	toTxNum uint64,
	maxStep kv.Step,
	stepSize uint64,
) (root []byte, encodedTrieState []byte, baselineTxNum uint64, branches *etl.Collector, err error) {
	return recomputeAtTxNumWithoutSDInternal(ctx, tx, tmpDir, toTxNum, maxStep, stepSize, false)
}

// RecomputeAtTxNumWithoutSDBounded is the dual-root diagnostic variant.
// Same as RecomputeAtTxNumWithoutSD but the plainReader falls back to
// GetLatestFromFilesUpToStep(maxStep) instead of unbounded GetAsOf on
// HistorySeek miss. Runs post-mismatch to test whether the unbounded
// fallback is the root cause of the wrong compute root.
func RecomputeAtTxNumWithoutSDBounded(
	ctx context.Context,
	tx kv.TemporalTx,
	tmpDir string,
	toTxNum uint64,
	maxStep kv.Step,
	stepSize uint64,
) (root []byte, encodedTrieState []byte, baselineTxNum uint64, branches *etl.Collector, err error) {
	return recomputeAtTxNumWithoutSDInternal(ctx, tx, tmpDir, toTxNum, maxStep, stepSize, true)
}

func recomputeAtTxNumWithoutSDInternal(
	ctx context.Context,
	tx kv.TemporalTx,
	tmpDir string,
	toTxNum uint64,
	maxStep kv.Step,
	stepSize uint64,
	boundedPlainReader bool,
) (root []byte, encodedTrieState []byte, baselineTxNum uint64, branches *etl.Collector, err error) {
	if tx == nil {
		return nil, nil, 0, nil, fmt.Errorf("RecomputeAtTxNumWithoutSD: nil tx")
	}
	if stepSize == 0 {
		return nil, nil, 0, nil, fmt.Errorf("RecomputeAtTxNumWithoutSD: stepSize == 0")
	}

	// Baseline lookup: files only, bounded by maxStep. The file at
	// endTxNum ≤ maxStep*stepSize always has a commitment record at
	// endTxNum-1, which is by construction ≤ toTxNum. Touches in
	// (baselineTxNum, toTxNum+1] cover the rest. For aligned cuts the
	// file containing lastTxNum is directly the baseline; for
	// non-aligned cuts an earlier file is the baseline and history
	// covers the gap.
	var baselineBytes []byte
	var baselineFound bool
	var baselineFileEndStep kv.Step
	filesVal, fileEndStep, filesHas, err := tx.Debug().GetLatestFromFilesUpToStep(kv.CommitmentDomain, KeyCommitmentState, maxStep)
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("GetLatestFromFilesUpToStep(commitment, maxStep=%d): %w", maxStep, err)
	}
	if filesHas {
		baselineBytes = filesVal
		baselineFound = true
		baselineFileEndStep = fileEndStep
	}

	// Bug#6 diagnostic: capture the visible-file set the compute will read
	// from. Same-block same-input-files runs of this function under a
	// live erigon vs a standalone recompute-at tool have historically
	// produced different roots (2026-07-08 investigation). Logging the
	// visible-set names here at INFO makes the next divergence
	// attributable without needing to re-run months of investigation —
	// a live vs tool diff on these names names the aggregator-state gap.
	logger := log.New()
	summariseFiles := func(dom kv.Domain) string {
		return tx.Debug().DomainFiles(dom).String()
	}
	logger.Info("[commitment-recompute-sdless] compute entry",
		"toTxNum", toTxNum,
		"maxStep", maxStep,
		"stepSize", stepSize,
		"baselineFound", baselineFound,
		"baselineFileEndStep", baselineFileEndStep,
		"commitmentFiles", summariseFiles(kv.CommitmentDomain),
		"accountsFiles", summariseFiles(kv.AccountsDomain),
		"storageFiles", summariseFiles(kv.StorageDomain),
		"codeFiles", summariseFiles(kv.CodeDomain),
	)

	// State reader composition.
	//
	// Plain state (accounts/storage/code): HistoryStateReader at
	// toTxNum+1. GetAsOf(key, ts) returns the value JUST BEFORE ts,
	// so passing toTxNum+1 reads inclusively of writes at
	// txnum=toTxNum.
	//
	// Commitment branches: StepBoundedFilesStateReader at maxStep.
	// Commitment domain is HistoryDisabled, so we cannot time-travel
	// via GetAsOf. The writable shadow may hold commitment branches
	// for post-toBlock state during mode-B unwind, so we read from
	// files only (bounded to maxStep = the file containing toBlock's
	// baseline) and rely on the trie's in-memory state (restored from
	// baseline via SetState + folded by Process) for everything else.
	var plainReader StateReader
	if boundedPlainReader {
		plainReader = NewHistoryStateReaderBounded(tx, toTxNum+1)
	} else {
		plainReader = NewHistoryStateReader(tx, toTxNum+1)
	}
	commitmentReader := NewStepBoundedFilesStateReader(tx, maxStep)
	stateReader := NewCommitmentSplitStateReader(commitmentReader, plainReader, true /* withHistory */)

	// Branch collector — captures every commitment branch Process
	// emits via PutBranch. The caller drains this after wiping
	// shadow's stale boundary-step branches and writes the captured
	// branches back at txnum=lastTxNum.
	branches = etl.NewCollectorWithAllocator("commitment-recompute-branches", tmpDir, etl.SmallSortableBuffers, log.New())
	defer func() {
		if err != nil && branches != nil {
			branches.Close()
			branches = nil
		}
	}()

	trieCtx := NewTrieContextWithBranchCollector(stateReader, stepSize, branches)
	trie := commitment.NewHexPatriciaHashed(length.Addr, trieCtx, commitment.DefaultTrieConfig())
	defer trie.Release()

	updates := commitment.NewUpdates(commitment.ModeDirect, tmpDir, commitment.KeyToHexNibbleHash)
	defer updates.Close()

	if baselineFound && len(baselineBytes) >= 16 {
		// Decode the baseline commitment state record. Layout matches
		// what encodeCommitmentState produces: 8B txNum + 8B blockNum
		// + 2B trieStateLen + trieState.
		cs := new(commitmentState)
		if err = cs.Decode(baselineBytes); err != nil {
			return nil, nil, 0, nil, fmt.Errorf("decode baseline commitment state: %w", err)
		}
		baselineTxNum = cs.txNum
		if err = trie.SetState(cs.trieState); err != nil {
			return nil, nil, 0, nil, fmt.Errorf("restore baseline trie state (txNum=%d): %w", cs.txNum, err)
		}
	}

	// Touch every key changed in (baselineTxNum, toTxNum+1]. The
	// range is exclusive on the low end so we don't re-touch keys
	// already folded into the baseline trie.
	touchFromTxNum := baselineTxNum
	if baselineTxNum > 0 {
		touchFromTxNum = baselineTxNum + 1
	}
	// mode-D residual wrong-root diagnostic (2026-08-19): reset the
	// per-compute counters so post-compute log reflects THIS compute's
	// HistorySeek hit/miss/divergence stats only.
	HistReaderResetCounters()

	// Snapshot the visible-file set at compute start so we can detect
	// retire/merge flipping it mid-compute. If it changes, log a WARN
	// at compute exit and dump both snapshots so we can correlate.
	visibleAtEntry := map[kv.Domain]string{
		kv.AccountsDomain:   summariseFiles(kv.AccountsDomain),
		kv.StorageDomain:    summariseFiles(kv.StorageDomain),
		kv.CodeDomain:       summariseFiles(kv.CodeDomain),
		kv.CommitmentDomain: summariseFiles(kv.CommitmentDomain),
	}

	touchesByDomain := make(map[kv.Domain]uint64, 3)
	// Per-domain FNV hash of the walked (key, txN) tuple sequence. Two
	// runs of the same compute on the same visible-file set MUST produce
	// the same hash — if they don't, the walk itself is non-deterministic
	// (heap order, file iteration, MDBX merge). If they do, the walk is
	// deterministic and any root divergence lives downstream in
	// trie.Process (fold order, warmup config, PutBranch collector).
	// Cheap, log-once diagnostic. Remove after root-cause.
	touchHashByDomain := make(map[kv.Domain]uint64, 3)
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		it, ierr := tx.Debug().HistoryKeyTxNumRange(d, int(touchFromTxNum), int(toTxNum+1), order.Asc, -1)
		if ierr != nil {
			return nil, nil, 0, nil, fmt.Errorf("HistoryKeyTxNumRange(%s, [%d, %d)): %w", d, touchFromTxNum, toTxNum+1, ierr)
		}
		touchFn := updates.TouchAccount
		switch d {
		case kv.StorageDomain:
			touchFn = updates.TouchStorage
		case kv.CodeDomain:
			touchFn = updates.TouchCode
		}
		domHash := fnv.New64a()
		var txNumBuf [8]byte
		for it.HasNext() {
			k, txN, terr := it.Next()
			if terr != nil {
				it.Close()
				return nil, nil, 0, nil, fmt.Errorf("HistoryKeyTxNumRange next(%s): %w", d, terr)
			}
			domHash.Write(k)
			binary.BigEndian.PutUint64(txNumBuf[:], txN)
			domHash.Write(txNumBuf[:])
			updates.TouchPlainKey(string(k), nil, touchFn)
			touchesByDomain[d]++
		}
		it.Close()
		touchHashByDomain[d] = domHash.Sum64()
	}

	// Refuse to return the baseline root unchanged when the walk range
	// is non-empty. A gap in on-disk history for (baselineTxNum, toTxNum]
	// — typical when the source publisher runs --prune.mode=minimal and
	// consumers bootstrap its manifest without on-demand history download
	// — makes HistoryKeyTxNumRange silently emit zero touches, and
	// trie.Process against zero touches returns the SetState root as-is.
	// That root reflects state at baselineTxNum, not at toTxNum, and it
	// almost always mismatches the header the caller is validating
	// against. Fail loudly so Provider.Unwind cannot commit a wrong root.
	if toTxNum > baselineTxNum {
		total := touchesByDomain[kv.AccountsDomain] + touchesByDomain[kv.StorageDomain] + touchesByDomain[kv.CodeDomain]
		if total == 0 {
			return nil, nil, baselineTxNum, nil, fmt.Errorf(
				"commitment recompute: walk range (%d, %d] is non-empty but HistoryKeyTxNumRange returned zero touches across accounts/storage/code — history .v/.ef coverage is missing for this range (baseline=%d target=%d span=%d)",
				baselineTxNum, toTxNum, baselineTxNum, toTxNum, toTxNum-baselineTxNum)
		}
	}

	root, err = trie.Process(ctx, updates, "commitment-recompute-no-sd", nil, commitment.WarmupConfig{})
	if err != nil {
		return nil, nil, baselineTxNum, nil, fmt.Errorf("trie.Process: %w", err)
	}

	encodedTrieState, err = trie.EncodeCurrentState(nil)
	if err != nil {
		return nil, nil, baselineTxNum, nil, fmt.Errorf("EncodeCurrentState: %w", err)
	}
	// mode-D residual wrong-root diagnostic (2026-08-19): log the
	// hit/miss/divergence stats accumulated by HistoryStateReader.Read
	// during trie.Process above.
	logger.Info("[dbg-hist-reader] compute post-fold stats",
		"toTxN", toTxNum,
		"stateReads", histReaderCompareCount.Load(),
		"histSeekHits", histReaderHitCount.Load(),
		"histSeekMisses", histReaderMissCount.Load(),
		"fallbackDivergences", histReaderDivergeCount.Load(),
		"missedHistSeek", histReaderMissedHistSeek.Load())

	// Detect retire/merge flipping the visible file set mid-compute.
	for _, dom := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain} {
		exitFiles := summariseFiles(dom)
		if exitFiles != visibleAtEntry[dom] {
			logger.Warn("[dbg-visible-set] file set changed mid-compute",
				"domain", dom.String(),
				"toTxN", toTxNum,
				"entry", visibleAtEntry[dom],
				"exit", exitFiles)
		}
	}

	logger.Info("[commitment-recompute-sdless] compute exit",
		"toTxNum", toTxNum,
		"baselineTxNum", baselineTxNum,
		"root", fmt.Sprintf("%x", root),
		"touchesAccounts", touchesByDomain[kv.AccountsDomain],
		"touchesStorage", touchesByDomain[kv.StorageDomain],
		"touchesCode", touchesByDomain[kv.CodeDomain],
		"touchHashAccounts", fmt.Sprintf("%016x", touchHashByDomain[kv.AccountsDomain]),
		"touchHashStorage", fmt.Sprintf("%016x", touchHashByDomain[kv.StorageDomain]),
		"touchHashCode", fmt.Sprintf("%016x", touchHashByDomain[kv.CodeDomain]),
	)
	return root, encodedTrieState, baselineTxNum, branches, nil
}
