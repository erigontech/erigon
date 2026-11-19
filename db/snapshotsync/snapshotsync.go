// Copyright 2024 The Erigon Authors
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

package snapshotsync

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/downloader/downloadergrpc"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
)

type CaplinMode int

const (

	// CaplinModeNone - no caplin mode
	NoCaplin   CaplinMode = 1
	OnlyCaplin CaplinMode = 2
	AlsoCaplin CaplinMode = 3
)

func BuildDownloadRequest(
	downloadRequest []dbservices.DownloadRequest,
	logTarget string,
) *downloaderproto.DownloadRequest {
	req := &downloaderproto.DownloadRequest{
		Items:     make([]*downloaderproto.DownloadItem, 0, len(snaptype2.BlockSnapshotTypes)),
		LogTarget: logTarget,
	}
	for _, r := range downloadRequest {
		req.Items = append(req.Items, &downloaderproto.DownloadItem{
			TorrentHash: downloadergrpc.String2Proto(r.TorrentHash),
			Path:        r.Path,
		})
	}
	return req
}

// RequestSnapshotsDownload - builds the snapshots download request and downloads them
func RequestSnapshotsDownload(
	ctx context.Context,
	downloadRequest []dbservices.DownloadRequest,
	downloaderClient dbservices.DownloaderClient,
	logTarget string,
) error {
	// start seed large .seg of large size
	req := BuildDownloadRequest(downloadRequest, logTarget)
	if err := downloaderClient.Download(ctx, req); err != nil {
		return err
	}
	return nil
}

func adjustBlockPrune(blocks, minBlocksToDownload uint64) uint64 {
	if minBlocksToDownload < snaptype.Erigon2MergeLimit {
		minBlocksToDownload = snaptype.Erigon2MergeLimit
	}
	if blocks > minBlocksToDownload {
		blocks = minBlocksToDownload
	}
	return blocks - blocks%snaptype.Erigon2MergeLimit
}

// isBlobSidecarFile matches blob sidecar segments and their index, which is named after
// the index (blocksidecars), not the segment type.
func isBlobSidecarFile(name string) bool {
	return strings.Contains(name, snaptype.BlobSidecars.Name()) || strings.Contains(name, snaptype.CaplinIndexes.BlobSidecarSlot.Name)
}

// BlobRetention bounds the blob sidecar snapshots a bounded blob archive downloads.
type BlobRetention struct {
	SnapshotDir string
	// FromSlot is the first slot the archive keeps; zero keeps every segment.
	FromSlot uint64
	// ServeWindowSlots is the spec blob-serving window plus one epoch of rounding slack. It is
	// kept whatever FromSlot says, because the backfill never fills below the frozen blob tip.
	ServeWindowSlots uint64
}

// downloadFloor returns the slot below which blob segments are skipped. The wall clock is not
// known here, but it is at or past the newest preverified blob segment, so measuring the serving
// window back from that segment's end never skips a slot the node still has to serve.
func (r BlobRetention) downloadFloor(items snapcfg.PreverifiedItems) (uint64, error) {
	if r.FromSlot == 0 {
		return 0, nil
	}
	var tip uint64
	for _, p := range items {
		if !isBlobSidecarFile(p.Name) {
			continue
		}
		if info, _, ok := snaptype.ParseFileName("", p.Name); ok {
			tip = max(tip, info.To)
		}
	}
	if tip <= r.ServeWindowSlots {
		return 0, nil
	}
	floor := min(r.FromSlot, tip-r.ServeWindowSlots)
	segments, err := snaptype.Segments(r.SnapshotDir)
	if err != nil {
		return 0, err
	}
	for i := range segments {
		if segments[i].Type.Enum() == snaptype.BlobSidecars.Enum() {
			floor = min(floor, segments[i].From)
		}
	}
	return floor, nil
}

// skipBlobDownload reports whether a preverified file is a blob segment or index the node does
// not download: every one when blobs are not archived, else those below floor.
func skipBlobDownload(name string, blobs bool, floor uint64) bool {
	return isBlobSidecarFile(name) && (!blobs || blobSegmentBelow(name, floor))
}

// blobSegmentBelow reports whether a blob sidecar segment ends at or before floor,
// so it holds nothing a bounded blob archive keeps.
func blobSegmentBelow(name string, floor uint64) bool {
	if floor == 0 {
		return false
	}
	info, _, ok := snaptype.ParseFileName("", name)
	return ok && info.To <= floor
}

func isStateSnapshot(name string) bool {
	return isStateHistory(name) || strings.HasPrefix(name, "domain")
}

func isStateHistory(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history") || strings.HasPrefix(name, "accessor")
}

func localV3CommitmentTip(dirs datadir.Dirs) (tip uint64, found bool, err error) {
	entries, err := os.ReadDir(dirs.SnapDomain)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var files []snaptype.FileInfo
	for _, entry := range entries {
		info, _, ok := snaptype.ParseFileName("", entry.Name())
		if ok && info.TypeString == kv.CommitmentDomain.String() && info.Ext == ".kv" && info.Version.Major >= 3 {
			files = append(files, info)
		}
	}
	slices.SortFunc(files, func(a, b snaptype.FileInfo) int { return cmp.Compare(a.From, b.From) })
	for i := range files {
		if files[i].From > tip {
			break
		}
		tip = max(tip, files[i].To)
	}
	return tip, len(files) > 0, nil
}

func incompatibleCommitmentFiles(items snapcfg.PreverifiedItems, localV3Tip uint64) map[string]bool {
	type stepRange struct {
		history  bool
		from, to uint64
	}
	type rangedFile struct {
		stepRange
		required bool
	}
	v3 := statecfg.ExperimentalCommitmentV3
	dependents := make(map[string]bool)
	for _, pair := range statecfg.DomainDependencies() {
		dependents[pair[0].String()] = true
	}
	ranges := make(map[string]rangedFile)
	compatible := make(map[stepRange]bool)
	incompatible := make(map[stepRange]bool)
	skipped := make(map[string]bool)
	for _, p := range items {
		info, _, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			continue
		}
		r := stepRange{history: isStateHistory(p.Name), from: info.From, to: info.To}
		if info.TypeString != kv.CommitmentDomain.String() {
			if !r.history && dependents[info.TypeString] {
				ranges[p.Name] = rangedFile{stepRange: r, required: true}
			}
			continue
		}
		if info.Ext == ".kv" || info.Ext == ".v" {
			if (info.Version.Major >= 3) == v3 {
				compatible[r] = true
			} else {
				incompatible[r] = true
				skipped[p.Name] = false
				if v3 && !r.history {
					ranges[p.Name] = rangedFile{stepRange: r, required: true}
				}
			}
		} else {
			ranges[p.Name] = rangedFile{stepRange: r}
		}
	}
	hasIncompatible := func(f rangedFile) bool {
		if !f.required {
			return incompatible[f.stepRange]
		}
		for r := range incompatible {
			if !r.history && r.from < f.to && f.from < r.to {
				return true
			}
		}
		return false
	}
	for name, f := range ranges {
		if compatible[f.stepRange] || f.required && f.to <= localV3Tip || !v3 && !hasIncompatible(f) {
			continue
		}
		skipped[name] = f.required
	}
	return skipped
}

func canSnapshotBePruned(name string) bool {
	return isStateHistory(name) || strings.Contains(name, "transactions")
}

// buildBlackListForPruning returns preverified snapshot names to skip at
// download time: state history past the History window; commitment and rcache
// history past their own windows (independent of History); tx segments past the
// Blocks window, or pre-merge under chain history-expiry. Bodies, headers and
// domain files are never blacklisted.
func buildBlackListForPruning(
	pruneMode prune.Mode,
	cc *chain.Config,
	historyStepPrune, minCommitmentHistoryStep, minReceiptsStep kv.Step, minBlockToDownload, blockPrune uint64,
	preverified snapcfg.Preverified,
) (map[string]struct{}, error) {
	blackList := make(map[string]struct{})

	historyEnabled := pruneMode.History.Enabled()
	blocksEnabled := pruneMode.Blocks.Enabled()
	commitmentHistoryEnabled := minCommitmentHistoryStep > 0
	receiptsEnabled := minReceiptsStep > 0
	applyChainHistoryExpiry := pruneMode.Blocks == prune.KeepPostMergeBlocksPruneMode && cc != nil && cc.MergeHeight != nil

	if !historyEnabled && !blocksEnabled && !commitmentHistoryEnabled && !receiptsEnabled && !applyChainHistoryExpiry {
		return blackList, nil
	}

	if blocksEnabled {
		blockPrune = adjustBlockPrune(blockPrune, minBlockToDownload)
	}

	for _, p := range preverified.Items {
		name := p.Name
		// Don't prune unprunable files
		if !canSnapshotBePruned(name) {
			continue
		}
		if isStateSnapshot(name) {
			// parse "from" (0) and "to" (64) from the name
			// parse the snapshot "kind". e.g kind of 'idx/v1.0-accounts.0-64.ef' is "idx/v1.0-accounts"
			res, _, ok := snaptype.ParseFileName("", name)
			if !ok {
				return blackList, errors.New("invalid state snapshot name")
			}
			// rcache history: pruned by its own receipts window, not History.
			if isStateHistory(name) && strings.Contains(name, kv.RCacheDomain.String()) {
				if receiptsEnabled && minReceiptsStep >= kv.Step(res.To) {
					blackList[name] = struct{}{}
				}
				continue
			}
			// commitment history: pruned by its own window, not History.
			if commitmentHistoryEnabled && isStateHistory(name) && strings.Contains(name, kv.CommitmentDomain.String()) && minCommitmentHistoryStep >= kv.Step(res.To) {
				blackList[name] = struct{}{}
				continue
			}
			if !historyEnabled {
				continue
			}
			if historyStepPrune < kv.Step(res.To) {
				continue
			}
			blackList[name] = struct{}{}
			continue
		}
		// Block segment (transactions only — canSnapshotBePruned filters bodies/headers/domains).
		// e.g 'v1.0-000000-000100-transactions.seg'
		// parse "from" (000000) and "to" (000100) from the name. 100 is 100'000 blocks
		res, _, ok := snaptype.ParseFileName("", name)
		if !ok {
			continue
		}
		switch {
		case blocksEnabled:
			if blockPrune >= res.To {
				blackList[name] = struct{}{}
			}
		case applyChainHistoryExpiry:
			if cc.IsPreMerge(res.To - 1) {
				blackList[name] = struct{}{}
			}
		}
	}

	return blackList, nil
}

type BlockReader interface {
	Snapshots() dbservices.BlockSnapshots
	IterateFrozenBodies(tx kv.Getter, _ func(blockNum uint64, baseTxNum uint64, txCount uint64) error) error
	FreezingCfg() ethconfig.BlocksFreezing
	AllTypes() []snaptype.Type
	TxnumReader() rawdbv3.TxNumsReader
}

func stepAtTxNum(txNum, stepSize uint64) kv.Step {
	if txNum < stepSize-1 {
		return 0
	}
	return kv.Step((txNum - (stepSize - 1)) / stepSize)
}

func getMinimumBlocksToDownload(
	ctx context.Context,
	blockReader BlockReader,
	tx kv.Getter,
	maxStateStep, stepSize uint64,
	stateHistoryPruneTo, commitmentHistoryPruneTo, receiptsHistoryPruneTo uint64,
) (minBlockToDownload uint64, minHistoryStep, minCommitmentHistoryStep, minReceiptsStep kv.Step, err error) {
	started := time.Now()
	var iterations int64
	defer func() {
		log.Debug("getMinimumBlocksToDownload finished",
			"timeTaken", time.Since(started),
			"iterations", iterations,
			"err", err)
	}()
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	minToDownload := uint64(math.MaxUint64)
	minHistoryStep = kv.Step(math.MaxUint32)
	minCommitmentHistoryStep = kv.Step(math.MaxUint32)
	minReceiptsStep = kv.Step(math.MaxUint32)
	resumedBlockStep := kv.Step(math.MaxUint32)
	stateTxNum := maxStateStep * stepSize
	if err := blockReader.IterateFrozenBodies(tx, func(blockNum, baseTxNum, txAmount uint64) error {
		if iterations%1e6 == 0 {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
		}
		iterations++
		if blockNum == stateHistoryPruneTo {
			minHistoryStep = stepAtTxNum(baseTxNum, stepSize)
		}
		if blockNum == commitmentHistoryPruneTo {
			minCommitmentHistoryStep = stepAtTxNum(baseTxNum, stepSize)
		}
		if blockNum == receiptsHistoryPruneTo {
			minReceiptsStep = stepAtTxNum(baseTxNum, stepSize)
		}
		if stateTxNum <= baseTxNum { // only consider the block if it
			return nil
		}
		if stateTxNum < baseTxNum+txAmount {
			resumedBlockStep = stepAtTxNum(baseTxNum, stepSize)
		}
		newMinToDownload := uint64(0)
		if frozenBlocks > blockNum {
			newMinToDownload = frozenBlocks - blockNum
		}
		if newMinToDownload < minToDownload {
			minToDownload = newMinToDownload
		}
		return nil
	}); err != nil {
		return 0, 0, 0, 0, err
	}

	// A prune-to boundary below the first frozen body is never hit above, leaving
	// its step at the MaxUint32 sentinel. Resolve it to 0 (disable the filter,
	// keep everything) so an unfound cutoff can't blacklist every matching file.
	minHistoryStep = clearUnsetStep(minHistoryStep)
	minCommitmentHistoryStep = clearUnsetStep(minCommitmentHistoryStep)
	minReceiptsStep = clearUnsetStep(minReceiptsStep)
	// Execution resumes inside the block that the state files end in and replays the block's
	// first transactions from history.
	minHistoryStep = min(minHistoryStep, resumedBlockStep)

	return frozenBlocks - minToDownload, minHistoryStep, minCommitmentHistoryStep, minReceiptsStep, nil
}

func clearUnsetStep(s kv.Step) kv.Step {
	if s == kv.Step(math.MaxUint32) {
		return 0
	}
	return s
}

func getMaxStepRangeInSnapshots(preverified snapcfg.Preverified) (uint64, error) {
	maxTo := uint64(0)
	for _, p := range preverified.Items {
		// take the "to" from "domain" snapshot
		if !strings.HasPrefix(p.Name, "domain") {
			continue
		}
		info, _, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			return 0, fmt.Errorf("invalid domain snapshot filename: %s", p.Name)
		}
		if info.To > maxTo {
			maxTo = info.To
		}
	}
	return maxTo, nil
}

func computeBlocksToPrune(blockReader BlockReader, p prune.Mode) (blocksToPrune uint64, historyToPrune uint64) {
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	return p.Blocks.PruneTo(frozenBlocks), p.History.PruneTo(frozenBlocks)
}

// downloadFilteringApplies reports whether buildBlackListForPruning would
// produce any blacklist entries for pruneMode + chain. Mirrors the function's
// own early-return predicate so the (slow) getMinimumBlocksToDownload call
// is skipped for modes where no filtering happens. Notably, an
// operator-supplied hybrid like
//
//	--prune.mode=archive --prune.distance.blocks=18446744073709551615
//
// produces {Blocks: KeepPostMergeBlocksPruneMode, History: KeepPostMergeBlocksPruneMode}
// — neither field's Enabled() is true, but the operator opted into
// chain-history-expiry for blocks. The KeepPostMergeBlocksPruneMode + MergeHeight
// branch covers that.
func downloadFilteringApplies(pruneMode prune.Mode, cc *chain.Config) bool {
	if pruneMode.History.Enabled() || pruneMode.Blocks.Enabled() || pruneMode.CommitmentHistoryAmount().Enabled() || pruneMode.ReceiptsAmount().Enabled() {
		return true
	}
	return pruneMode.Blocks == prune.KeepPostMergeBlocksPruneMode && cc != nil && cc.MergeHeight != nil
}

// blocksRetentionCutoff returns the block height below which block-data
// segments (transactions and receipt-related state) are considered expired
// under pruneMode:
//   - finite Distance (full/minimal): head - distance, the EIP-8252-style
//     window.
//   - KeepPostMergeBlocksPruneMode with a chain MergeHeight: the merge height
//     (chain history-expiry policy — pre-merge data is expired).
//   - Otherwise (KeepAllBlocksPruneMode, or KeepPostMergeBlocksPruneMode without a
//     merge height): 0, meaning "nothing is expired".
//
// Both the transaction-segment blacklist and the receipts-segment filter use
// this to pick their cutoff in a consistent way.
func blocksRetentionCutoff(pruneMode prune.Mode, cc *chain.Config, head uint64) uint64 {
	switch pruneMode.Blocks {
	case prune.KeepAllBlocksPruneMode:
		return 0
	case prune.KeepPostMergeBlocksPruneMode:
		if cc != nil && cc.MergeHeight != nil {
			return *cc.MergeHeight
		}
		return 0
	default:
		return pruneMode.Blocks.PruneTo(head)
	}
}

func historyRetentionCutoff(pruneMode prune.Mode, head uint64) uint64 {
	if pruneMode.History == nil || !pruneMode.History.Enabled() {
		return 0
	}
	return pruneMode.History.PruneTo(head)
}

// receiptsSegmentRetentionCutoff picks the download cutoff for a receipt-related
// segment, matching what historyRetireCutoffs later retires it by: rcache
// history follows state history, and so do the log indexes, which are plain
// inverted indexes and take RetireCutoffs.Default. Downloading them on the
// block window instead would fetch a keep-all node's full history only to
// delete everything outside the state-history window.
func receiptsSegmentRetentionCutoff(pruneMode prune.Mode, cc *chain.Config, head uint64, name string) uint64 {
	isLogIndex := strings.Contains(name, kv.LogAddrIdx.String()) ||
		strings.Contains(name, kv.LogTopicIdx.String())
	isRcacheHistory := pruneMode.ReceiptsFollowHistory() && strings.Contains(name, kv.RCacheDomain.String())
	if isLogIndex || isRcacheHistory {
		return historyRetentionCutoff(pruneMode, head)
	}
	return blocksRetentionCutoff(pruneMode, cc, head)
}

// isReceiptsSegmentPruned reports whether a receipt-related segment predates
// pruneHeight (0 = keep everything) and should be skipped at download time.
func isReceiptsSegmentPruned(ctx context.Context, tx kv.Tx, txNumsReader rawdbv3.TxNumsReader, p snapcfg.PreverifiedItem, stepSize, pruneHeight uint64) bool {
	if strings.Contains(p.Name, "domain") {
		return false // domain snapshots are never pruned
	}
	if pruneHeight == 0 {
		return false
	}
	s, _, ok := snaptype.ParseFileName("", p.Name)
	if !ok {
		return false
	}
	minTxNum, err := txNumsReader.Min(ctx, tx, pruneHeight)
	if err != nil {
		log.Crit("Failed to get minimum transaction number", "err", err)
		return false
	}
	minStep := minTxNum / stepSize
	return s.To <= minStep // files storing data [from, to)
}

// unblackListFilesBySubstring - removes files from the blacklist that match any of the provided substrings.
func unblackListFilesBySubstring(blackList map[string]struct{}, strs ...string) {
	for _, str := range strs {
		for k := range blackList {
			if strings.Contains(k, str) {
				delete(blackList, k)
			}
		}
	}
}

func SnapshotsToDownload(
	ctx context.Context,
	logPrefix string,
	headerchain, blobs, caplinState bool,
	blobRetention BlobRetention,
	prune prune.Mode,
	caplin CaplinMode,
	tx kv.Tx,
	blockReader BlockReader,
	cc *chain.Config,
	syncCfg ethconfig.Sync,
	stepSize uint64,
	preverifiedBlockSnapshots snapcfg.Preverified,
	toBlock uint64,
	toStep kv.Step,
) ([]snapcfg.PreverifiedItem, error) {
	txNumsReader := blockReader.TxnumReader()
	frozenBlocks := blockReader.Snapshots().SegmentsMax()
	// Corner cases:
	// - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
	// - Erigon "download once": means restart/upgrade/downgrade must not download files (and will be fast)
	// - After "download once" - Erigon will produce and seed new files

	items := make([]snapcfg.PreverifiedItem, 0, len(preverifiedBlockSnapshots.Items))

	blockPrune, historyPrune := computeBlocksToPrune(blockReader, prune)
	blackListForPruning := make(map[string]struct{})
	wantToPrune := downloadFilteringApplies(prune, cc)
	if !headerchain && wantToPrune {
		maxStateStep, err := getMaxStepRangeInSnapshots(preverifiedBlockSnapshots)
		if err != nil {
			return nil, err
		}
		commitmentHistoryPrune := prune.CommitmentHistoryAmount().PruneTo(frozenBlocks)
		receiptsPrune := prune.ReceiptsAmount().PruneTo(frozenBlocks)
		minBlockToDownload, minHistoryStep, minCommitmentHistoryStep, minReceiptsStep, err := getMinimumBlocksToDownload(
			ctx, blockReader, tx, maxStateStep, stepSize, historyPrune, commitmentHistoryPrune, receiptsPrune,
		)
		if err != nil {
			return nil, err
		}
		// Commitment-history filtering is opt-in; a zero step disables it in
		// buildBlackListForPruning, so clear it when the window is unbounded.
		if !prune.CommitmentHistoryAmount().Enabled() {
			minCommitmentHistoryStep = 0
		} else if minCommitmentHistoryStep > 0 {
			log.Info(fmt.Sprintf("[%s] Filtering old commitment-history segments", logPrefix),
				"pruneToBlock", commitmentHistoryPrune,
				"minStep", minCommitmentHistoryStep)
		}
		// Receipts filtering is opt-in (requires --prune.include-receipts); a zero
		// step disables it in buildBlackListForPruning.
		if !prune.ReceiptsAmount().Enabled() {
			minReceiptsStep = 0
		} else if minReceiptsStep > 0 {
			log.Info(fmt.Sprintf("[%s] Filtering old receipt-cache segments", logPrefix),
				"pruneToBlock", receiptsPrune,
				"minStep", minReceiptsStep)
		}

		blackListForPruning, err = buildBlackListForPruning(prune, cc, minHistoryStep, minCommitmentHistoryStep, minReceiptsStep, minBlockToDownload, blockPrune, preverifiedBlockSnapshots)
		if err != nil {
			return nil, err
		}
	}

	// If we want to get all receipts, we also need to unblack list log indexes (otherwise eth_getLogs won't work).
	if syncCfg.PersistReceiptsCacheV2 {
		unblackListFilesBySubstring(blackListForPruning, kv.LogAddrIdx.String(), kv.LogTopicIdx.String())
	}

	blobFloor := uint64(0)
	if !headerchain && blobs && caplin != NoCaplin {
		var err error
		blobFloor, err = blobRetention.downloadFloor(preverifiedBlockSnapshots.Items)
		if err != nil {
			return nil, err
		}
		if blobFloor > 0 {
			log.Info(fmt.Sprintf("[%s] Filtering old blob sidecar segments", logPrefix), "fromSlot", blobRetention.FromSlot, "floor", blobFloor)
		}
	}

	for _, p := range preverifiedBlockSnapshots.Items {
		isCaplinFile := strings.Contains(p.Name, "beaconblocks") || isBlobSidecarFile(p.Name) || strings.Contains(p.Name, "caplin")
		if caplin == NoCaplin && isCaplinFile {
			continue
		}
		if caplin == OnlyCaplin && !isCaplinFile {
			continue
		}

		if isStateSnapshot(p.Name) && blockReader.FreezingCfg().DisableDownloadE3 {
			continue
		}
		if skipBlobDownload(p.Name, blobs, blobFloor) {
			continue
		}
		if !caplinState && strings.Contains(p.Name, "caplin/") {
			continue
		}
		if headerchain &&
			!(strings.Contains(p.Name, "headers") || strings.Contains(p.Name, "bodies") || p.Name == "salt-blocks.txt" || p.Name == "erigondb.toml") {
			continue
		}
		if !syncCfg.KeepExecutionProofs && isStateHistory(p.Name) && strings.Contains(p.Name, kv.CommitmentDomain.String()) {
			continue
		}

		if !syncCfg.PersistReceiptsCacheV2 && isStateSnapshot(p.Name) && strings.Contains(p.Name, kv.RCacheDomain.String()) {
			continue
		}

		// rcache reaches this filter only in the follow-history default;
		// explicit receipts windows are handled by the blacklist above.
		isLogIndexSegment := strings.Contains(p.Name, kv.LogAddrIdx.String()) ||
			strings.Contains(p.Name, kv.LogTopicIdx.String())
		isRcacheHistorySegment := prune.ReceiptsFollowHistory() && strings.Contains(p.Name, kv.RCacheDomain.String())
		if isLogIndexSegment || isRcacheHistorySegment {
			cutoff := receiptsSegmentRetentionCutoff(prune, cc, frozenBlocks, p.Name)
			if isReceiptsSegmentPruned(ctx, tx, txNumsReader, p, stepSize, cutoff) {
				log.Debug(fmt.Sprintf("[%s] skipping expired receipt-related segment", logPrefix),
					"name", p.Name, "cutoffBlock", cutoff, "rcacheHistory", isRcacheHistorySegment)
				continue
			}
		}

		if _, ok := blackListForPruning[p.Name]; ok {
			continue
		}

		if filterToBlock(p.Name, toBlock, toStep, headerchain) {
			continue
		}

		items = append(items, p)
	}
	return items, nil
}

// SyncSnapshots - Check snapshot states, determine what needs to be requested from the downloader
// then wait for downloads to complete.
func SyncSnapshots(
	ctx context.Context,
	logPrefix, task string,
	headerchain, blobs, caplinState bool,
	blobRetention BlobRetention,
	prune prune.Mode,
	caplin CaplinMode,
	tx kv.RwTx,
	blockReader BlockReader,
	cc *chain.Config,
	snapshotDownloader dbservices.DownloaderClient,
	syncCfg ethconfig.Sync,
	stepSize uint64,
	dirs datadir.Dirs,
) error {
	if blockReader.FreezingCfg().NoDownloader || snapshotDownloader == nil {
		return nil
	}
	snapCfg := snapcfg.KnownCfgOrDevnet(cc.ChainName)
	// Skip getMinimumBlocksToDownload if we can because it's slow.
	if snapCfg.Local {
		// This belongs higher up the call chain.
		if !headerchain {
			log.Info(fmt.Sprintf("[%s] Skipping SyncSnapshots, local preverified. Use snapshots reset to resync", logPrefix))
		}
	} else {
		toBlock := syncCfg.SnapshotDownloadToBlock // exclusive [0, toBlock)
		toStep := kv.Step(math.MaxUint64)          // exclusive [0, toStep)
		if !headerchain && toBlock > 0 {
			toTxNum, err := blockReader.TxnumReader().Min(ctx, tx, syncCfg.SnapshotDownloadToBlock)
			if err != nil {
				return err
			}
			toStep = kv.Step(toTxNum / stepSize)
			log.Debug(fmt.Sprintf("[%s] filtering", logPrefix), "toBlock", toBlock, "toStep", toStep, "toTxNum", toTxNum)
			// we downloaded extra seg files during the header chain download (the ones containing the toBlock)
			// so that we can correctly calculate toTxNum above (now we should delete these)
			if err := blockReader.Snapshots().RetireFilesAbove(toBlock, func(files []string) error {
				return snapshotDownloader.Delete(ctx, files)
			}); err != nil {
				return err
			}
			// re-open headers and bodies with alignMin=false after deletes,
			// otherwise no headers/bodies will be visible since transactions are not downloaded yet
			err = blockReader.Snapshots().OpenSegments([]snaptype.Type{snaptype2.Headers, snaptype2.Bodies}, false)
			if err != nil {
				return fmt.Errorf("error opening segments after to block filter deletion: %w", err)
			}
			// RetireFilesAbove + OpenSegments changed the on-disk file set; refresh the
			// tx's pinned view so getMinimumBlocksToDownload below reads the new set.
			tx.(kv.CanReopenUnderlyingFilesTx).ForceReopenUnderlyingFilesTx()
		}

		log.Info(fmt.Sprintf("[%s] Preparing snapshots request for %s", logPrefix, task))
		var localV3Tip uint64
		var localV3 bool
		if statecfg.ExperimentalCommitmentV3 {
			var err error
			if localV3Tip, localV3, err = localV3CommitmentTip(dirs); err != nil {
				return err
			}
		}
		incompatibleCommitment := incompatibleCommitmentFiles(snapCfg.Preverified.Items, localV3Tip)
		v3Remedy := "sync with `--experimental.commitment-v3` off, then convert it with `integration commitment convert --v3` before enabling it"
		if localV3 {
			v3Remedy = "run with `--snap.skip-state-snapshot-download`, or restore with `integration commitment convert --restore`, sync with `--experimental.commitment-v3` off and convert again"
		}
		legacyRemedy := "run with `--experimental.commitment-v3` or `--snap.skip-state-snapshot-download`"
		items, err := SnapshotsToDownload(ctx, logPrefix, headerchain, blobs, caplinState, blobRetention, prune, caplin, tx, blockReader, cc, syncCfg, stepSize, snapCfg.Preverified, toBlock, toStep)
		if err != nil {
			return err
		}
		downloadRequest := make([]dbservices.DownloadRequest, 0, len(items))
		var skippedTip uint64
		for _, p := range items {
			if missing, ok := incompatibleCommitment[p.Name]; ok {
				if missing && !statecfg.ExperimentalCommitmentV3 {
					return fmt.Errorf("commitment v3 is disabled, but the snapshot manifest has only v3 commitment for %s: %s", p.Name, legacyRemedy)
				}
				if missing {
					return fmt.Errorf("commitment v3 is enabled, but the snapshot manifest has no v3 commitment for %s: %s", p.Name, v3Remedy)
				}
				if info, _, ok := snaptype.ParseFileName("", p.Name); ok && info.TypeString == kv.CommitmentDomain.String() && info.Ext == ".kv" {
					skippedTip = max(skippedTip, info.To)
				}
				continue
			}
			downloadRequest = append(downloadRequest, dbservices.DownloadRequest{
				Path:        p.Name,
				TorrentHash: p.Hash,
			})
		}

		var stateTip uint64
		commitmentTip := localV3Tip
		for _, request := range downloadRequest {
			if !strings.HasPrefix(request.Path, "domain/") {
				continue
			}
			info, _, ok := snaptype.ParseFileName("", request.Path)
			if !ok {
				continue
			}
			stateTip = max(stateTip, info.To)
			if info.TypeString == kv.CommitmentDomain.String() && info.Ext == ".kv" {
				commitmentTip = max(commitmentTip, info.To)
			}
		}
		if stateTip > commitmentTip && statecfg.ExperimentalCommitmentV3 {
			return fmt.Errorf("commitment v3 is enabled, but the snapshot manifest has no v3 commitment for steps %d-%d of its state: %s", commitmentTip, stateTip, v3Remedy)
		}
		if stateTip > commitmentTip && skippedTip > commitmentTip {
			return fmt.Errorf("commitment v3 is disabled, but the snapshot manifest has only v3 commitment for steps %d-%d of its state: %s", commitmentTip, min(stateTip, skippedTip), legacyRemedy)
		}

		// Only add the preverified hashes until the initial sync completed for the first time.

		log.Info(fmt.Sprintf("[%s] Requesting %s from downloader", logPrefix, task))
		for {
			err := RequestSnapshotsDownload(ctx, downloadRequest, snapshotDownloader, task)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return err
			}
			if !grpcutil.IsRetryLater(err) {
				return err
			}
			log.Error(fmt.Sprintf("[%s] error requesting snapshots download from downloader", logPrefix), "err", err)
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-time.After(10 * time.Second):
			}
		}
		log.Info(fmt.Sprintf("[%s] Downloader completed %s", logPrefix, task))
	}

	return nil
}

// BlockFileRetainedUnderCap reports whether a block-segment file ending at
// `to` survives a download capped at toBlock; toBlock == 0 means no cap.
// Callers deriving a block target must use this same rule, so the target and
// the downloaded byte total describe the same file set.
func BlockFileRetainedUnderCap(to, toBlock uint64) bool {
	return toBlock == 0 || to <= toBlock
}

func filterToBlock(name string, toBlock uint64, toStep kv.Step, headerchain bool) bool {
	if toBlock == 0 {
		return false // toBlock filtering is not enabled
	}
	fileInfo, stateFile, ok := snaptype.ParseFileName("", name)
	if !ok {
		return true
	}
	if strings.HasPrefix(name, "salt") {
		return false // not applicable
	}
	if strings.HasPrefix(name, "caplin/") {
		return false // not applicable, caplin files are slot-based
	}
	if stateFile {
		return kv.Step(fileInfo.To) > toStep
	}
	if headerchain {
		// if we are downloading the header chain, we want to download the seg file which contains our toBlock
		// so that we can correctly calculate its maxTxNum from the body segment files (we will later on delete this file)
		return fileInfo.From > toBlock
	}
	return !BlockFileRetainedUnderCap(fileInfo.To, toBlock)
}
