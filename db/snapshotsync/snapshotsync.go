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
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadergrpc"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
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
	downloadRequest []services.DownloadRequest,
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
	downloadRequest []services.DownloadRequest,
	downloaderClient downloader.Client,
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

func isStateSnapshot(name string) bool {
	return isStateHistory(name) || strings.HasPrefix(name, "domain")
}
func isStateHistory(name string) bool {
	return strings.HasPrefix(name, "idx") || strings.HasPrefix(name, "history") || strings.HasPrefix(name, "accessor")
}
func canSnapshotBePruned(name string) bool {
	return (isStateHistory(name) || strings.Contains(name, "transactions")) && !strings.Contains(name, "rcache")
}

// buildBlackListForPruning returns the set of preverified snapshot names that
// should be skipped at download time according to pruneMode:
//   - state history files (idx/history/accessor): blacklisted when stepPrune
//     reaches their To and pruneMode.History is enabled.
//   - transaction segments: blacklisted by distance when pruneMode.Blocks is a
//     finite Distance (res.To <= blockPrune), or by chain history-expiry when
//     pruneMode.Blocks is KeepPostMergeBlocksPruneMode and cc has MergeHeight set
//     (cc.IsPreMerge(res.From)). KeepAllBlocksPruneMode leaves tx segments alone.
//   - bodies, headers, rcache files: never blacklisted here
//     (canSnapshotBePruned filters them out).
func buildBlackListForPruning(
	pruneMode prune.Mode,
	cc *chain.Config,
	stepPrune, minBlockToDownload, blockPrune uint64,
	preverified snapcfg.Preverified,
) (map[string]struct{}, error) {

	blackList := make(map[string]struct{})

	historyEnabled := pruneMode.History.Enabled()
	blocksEnabled := pruneMode.Blocks.Enabled()
	applyChainHistoryExpiry := pruneMode.Blocks == prune.KeepPostMergeBlocksPruneMode && cc != nil && cc.MergeHeight != nil

	if !historyEnabled && !blocksEnabled && !applyChainHistoryExpiry {
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
			if !historyEnabled {
				continue
			}
			// parse "from" (0) and "to" (64) from the name
			// parse the snapshot "kind". e.g kind of 'idx/v1.0-accounts.0-64.ef' is "idx/v1.0-accounts"
			res, _, ok := snaptype.ParseFileName("", name)
			if !ok {
				return blackList, errors.New("invalid state snapshot name")
			}
			if stepPrune < res.To {
				continue
			}
			blackList[name] = struct{}{}
			continue
		}
		// Block segment (transactions only — canSnapshotBePruned filters bodies/headers/rcache).
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
			if cc.IsPreMerge(res.From) {
				blackList[name] = struct{}{}
			}
		}
	}

	return blackList, nil
}

type blockReader interface {
	Snapshots() services.BlockSnapshots
	BorSnapshots() services.BlockSnapshots
	IterateFrozenBodies(_ func(blockNum uint64, baseTxNum uint64, txCount uint64) error) error
	FreezingCfg() ethconfig.BlocksFreezing
	AllTypes() []snaptype.Type
	FrozenFiles() (list []string)
	TxnumReader() rawdbv3.TxNumsReader
}

// getMinimumBlocksToDownload - get the minimum number of blocks to download
func getMinimumBlocksToDownload(
	ctx context.Context,
	blockReader blockReader,
	maxStateStep uint64,
	historyPruneTo uint64,
	stepSize uint64,
) (minBlockToDownload uint64, minStateStepToDownload uint64, err error) {
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
	minStateStepToDownload = uint64(math.MaxUint32)
	stateTxNum := maxStateStep * stepSize
	if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
		if iterations%1e6 == 0 {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
		}
		iterations++
		if blockNum == historyPruneTo {
			minStateStepToDownload = (baseTxNum - (stepSize - 1)) / stepSize
			if baseTxNum < (stepSize - 1) {
				minStateStepToDownload = 0
			}
		}
		if stateTxNum <= baseTxNum { // only consider the block if it
			return nil
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
		return 0, 0, err
	}

	// return the minimum number of blocks to download and the minimum step.
	return frozenBlocks - minToDownload, minStateStepToDownload, nil
}

func getMaxStepRangeInSnapshots(preverified snapcfg.Preverified) (uint64, error) {
	maxTo := uint64(0)
	for _, p := range preverified.Items {
		// take the "to" from "domain" snapshot
		if !strings.HasPrefix(p.Name, "domain") {
			continue
		}
		name := strings.TrimPrefix(p.Name, "domain/")
		versionString := strings.Split(name, "-")[0]
		name = strings.TrimPrefix(name, versionString)

		rangeString := strings.Split(name, ".")[1]
		rangeNums := strings.Split(rangeString, "-")
		// convert the range to uint64
		to, err := strconv.ParseUint(rangeNums[1], 10, 64)
		if err != nil {
			return 0, err
		}
		if to > maxTo {
			maxTo = to
		}
	}
	return maxTo, nil
}

// getMaxBlockInSnapshots returns the highest block number "to" across all
// block-numbered preverified segments (headers/bodies/transactions). State
// (domain/history/idx) segments are ignored.
func getMaxBlockInSnapshots(preverified snapcfg.Preverified) uint64 {
	var maxTo uint64
	for _, p := range preverified.Items {
		info, stateFile, ok := snaptype.ParseFileName("", p.Name)
		if !ok || stateFile {
			continue
		}
		if info.To > maxTo {
			maxTo = info.To
		}
	}
	return maxTo
}

func computeBlocksToPrune(blockReader blockReader, p prune.Mode) (blocksToPrune uint64, historyToPrune uint64) {
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
	if pruneMode.History.Enabled() || pruneMode.Blocks.Enabled() {
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

// isReceiptsSegmentPruned reports whether a receipt-related preverified
// segment (rcache, logaddrs, logtopics) should be skipped at download time.
// It mirrors buildBlackListForPruning's per-mode handling for tx segments,
// but operates on block height (converted to txNum/step) because receipts
// are step-aligned in storage.
func isReceiptsSegmentPruned(ctx context.Context, tx kv.RwTx, txNumsReader rawdbv3.TxNumsReader, cc *chain.Config, pruneMode prune.Mode, head uint64, p snapcfg.PreverifiedItem, stepSize uint64) bool {
	if strings.Contains(p.Name, "domain") {
		return false // domain snapshots are never pruned
	}
	pruneHeight := blocksRetentionCutoff(pruneMode, cc, head)
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
	return s.From < minStep
}

// commitmentHistoryMinStep returns the minimum step number a commitment-history
// snapshot must end at to be downloaded, given a step distance bound. Snapshot
// files with End <= returned step are entirely older than the configured window
// and can be skipped. Returns 0 (no filtering) when distanceSteps == 0 or
// maxStateStep is too small to apply the bound.
func commitmentHistoryMinStep(maxStateStep, distanceSteps uint64) uint64 {
	if distanceSteps == 0 || maxStateStep <= distanceSteps {
		return 0
	}
	return maxStateStep - distanceSteps
}

// blocksToStepDistance converts a "last N blocks" window into a step-count
// distance using the blocks-per-step ratio observed in the preverified set:
// stepDistance = olderBlocks * maxStateStep / maxBlock. Returns 0 when the
// ratio is undefined (no blocks/no steps), which disables filtering.
func blocksToStepDistance(olderBlocks, maxStateStep, maxBlock uint64) uint64 {
	if olderBlocks == 0 || maxStateStep == 0 || maxBlock == 0 {
		return 0
	}
	return olderBlocks * maxStateStep / maxBlock
}

// shouldSkipCommitmentHistorySegment reports whether the given preverified file
// is a commitment-history segment that lies entirely below the keep-window
// implied by minStep. The check is conservative: it only applies to history /
// inverted-index files for the commitment domain (domain snapshots themselves
// are always kept).
func shouldSkipCommitmentHistorySegment(name string, minStep uint64) bool {
	if minStep == 0 {
		return false
	}
	if !isStateHistory(name) {
		return false
	}
	if !strings.Contains(name, kv.CommitmentDomain.String()) {
		return false
	}
	res, _, ok := snaptype.ParseFileName("", name)
	if !ok {
		return false
	}
	// A segment ending at `res.To` covers steps [res.From, res.To). We skip
	// segments whose end-step is at or below the boundary — those are fully
	// outside the keep window.
	return res.To <= minStep
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

// SyncSnapshots - Check snapshot states, determine what needs to be requested from the downloader
// then wait for downloads to complete.
func SyncSnapshots(
	ctx context.Context,
	logPrefix, task string,
	headerchain, blobs, caplinState bool,
	prune prune.Mode,
	caplin CaplinMode,
	tx kv.RwTx,
	blockReader blockReader,
	cc *chain.Config,
	snapshotDownloader downloader.Client,
	syncCfg ethconfig.Sync,
	stepSize uint64,
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
		toStep := uint64(math.MaxUint64)           // exclusive [0, toStep)
		if !headerchain && toBlock > 0 {
			toTxNum, err := blockReader.TxnumReader().Min(ctx, tx, syncCfg.SnapshotDownloadToBlock)
			if err != nil {
				return err
			}
			toStep = toTxNum / stepSize
			log.Debug(fmt.Sprintf("[%s] filtering", logPrefix), "toBlock", toBlock, "toStep", toStep, "toTxNum", toTxNum)
			// we downloaded extra seg files during the header chain download (the ones containing the toBlock)
			// so that we can correctly calculate toTxNum above (now we should delete these)
			var toDeleteSeg, toDeleteDownloader []string
			for _, f := range blockReader.FrozenFiles() {
				fileInfo, stateFile, ok := snaptype.ParseFileName("", f)
				if !ok || stateFile || strings.HasPrefix(fileInfo.Name(), "salt") || fileInfo.To < toBlock {
					continue
				}
				toDeleteSeg = append(toDeleteSeg, f)
				toDeleteDownloader = append(toDeleteDownloader, f, strings.Replace(f, ".seg", ".idx", 1))
			}
			log.Debug(fmt.Sprintf("[%s] deleting", logPrefix), "toDeleteSeg", toDeleteSeg, "toDeleteDownloader", toDeleteDownloader)
			err = snapshotDownloader.Delete(ctx, toDeleteDownloader)
			if err != nil {
				return err
			}
			err = blockReader.Snapshots().Delete(toDeleteSeg...)
			if err != nil {
				return err
			}
			// re-open headers and bodies with alignMin=false after deletes,
			// otherwise no headers/bodies will be visible since transactions are not downloaded yet
			err = blockReader.Snapshots().OpenSegments([]snaptype.Type{snaptype2.Headers, snaptype2.Bodies}, true, false)
			if err != nil {
				return fmt.Errorf("error opening segments after to block filter deletion: %w", err)
			}
		}

		txNumsReader := blockReader.TxnumReader()

		// This clause belongs in another function. We can take a long time here to determine what
		// requests to send to the Downloader. Need to communicate that.
		log.Info(fmt.Sprintf("[%s] Preparing snapshots request for %s", logPrefix, task))

		frozenBlocks := blockReader.Snapshots().SegmentsMax()
		//Corner cases:
		// - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
		// - Erigon "download once": means restart/upgrade/downgrade must not download files (and will be fast)
		// - After "download once" - Erigon will produce and seed new files

		// send all hashes to the Downloader service
		preverifiedBlockSnapshots := snapCfg.Preverified
		downloadRequest := make([]services.DownloadRequest, 0, len(preverifiedBlockSnapshots.Items))

		blockPrune, historyPrune := computeBlocksToPrune(blockReader, prune)
		blackListForPruning := make(map[string]struct{})
		wantToPrune := downloadFilteringApplies(prune, cc)

		// Pre-compute the minimum commitment-history step from the configured
		// block distance and the maximum state step in the preverified set, so
		// that segments below this boundary are skipped in the download loop.
		var commitmentHistoryMinStepBound uint64
		if !headerchain && syncCfg.KeepExecutionProofs && syncCfg.CommitmentHistoryOlder > 0 {
			maxStateStep, err := getMaxStepRangeInSnapshots(preverifiedBlockSnapshots)
			if err != nil {
				return err
			}
			maxBlock := getMaxBlockInSnapshots(preverifiedBlockSnapshots)
			distanceSteps := blocksToStepDistance(syncCfg.CommitmentHistoryOlder, maxStateStep, maxBlock)
			commitmentHistoryMinStepBound = commitmentHistoryMinStep(maxStateStep, distanceSteps)
			if commitmentHistoryMinStepBound > 0 {
				log.Info(fmt.Sprintf("[%s] Filtering old commitment-history segments", logPrefix),
					"maxStateStep", maxStateStep,
					"maxBlock", maxBlock,
					"olderBlocks", syncCfg.CommitmentHistoryOlder,
					"distanceSteps", distanceSteps,
					"minStep", commitmentHistoryMinStepBound)
			}
		}

		if !headerchain && wantToPrune {
			maxStateStep, err := getMaxStepRangeInSnapshots(preverifiedBlockSnapshots)
			if err != nil {
				return err
			}
			minBlockToDownload, minStepToDownload, err := getMinimumBlocksToDownload(ctx, blockReader, maxStateStep, historyPrune, stepSize)
			if err != nil {
				return err
			}

			blackListForPruning, err = buildBlackListForPruning(prune, cc, minStepToDownload, minBlockToDownload, blockPrune, preverifiedBlockSnapshots)
			if err != nil {
				return err
			}
		}

		// If we want to get all receipts, we also need to unblack list log indexes (otherwise eth_getLogs won't work).
		if syncCfg.PersistReceiptsCacheV2 {
			unblackListFilesBySubstring(blackListForPruning, kv.LogAddrIdx.String(), kv.LogTopicIdx.String())
		}

		// build all download requests
		for _, p := range preverifiedBlockSnapshots.Items {
			if caplin == NoCaplin && (strings.Contains(p.Name, "beaconblocks") || strings.Contains(p.Name, "blobsidecars") || strings.Contains(p.Name, "caplin")) {
				continue
			}
			if caplin == OnlyCaplin && !strings.Contains(p.Name, "beaconblocks") && !strings.Contains(p.Name, "blobsidecars") && !strings.Contains(p.Name, "caplin") {
				continue
			}

			if isStateSnapshot(p.Name) && blockReader.FreezingCfg().DisableDownloadE3 {
				continue
			}
			if !blobs && strings.Contains(p.Name, snaptype.BlobSidecars.Name()) {
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
			if shouldSkipCommitmentHistorySegment(p.Name, commitmentHistoryMinStepBound) {
				continue
			}

			if !syncCfg.PersistReceiptsCacheV2 && isStateSnapshot(p.Name) && strings.Contains(p.Name, kv.RCacheDomain.String()) {
				continue
			}

			isRcacheRelatedSegment := strings.Contains(p.Name, kv.RCacheDomain.String()) ||
				strings.Contains(p.Name, kv.LogAddrIdx.String()) ||
				strings.Contains(p.Name, kv.LogTopicIdx.String())

			if isRcacheRelatedSegment && isReceiptsSegmentPruned(ctx, tx, txNumsReader, cc, prune, frozenBlocks, p, stepSize) {
				continue
			}

			if _, ok := blackListForPruning[p.Name]; ok {
				continue
			}

			if filterToBlock(p.Name, toBlock, toStep, headerchain) {
				continue
			}

			downloadRequest = append(downloadRequest, services.DownloadRequest{
				Path:        p.Name,
				TorrentHash: p.Hash,
			})
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

func filterToBlock(name string, toBlock uint64, toStep uint64, headerchain bool) bool {
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
		return fileInfo.To > toStep
	}
	if headerchain {
		// if we are downloading the header chain, we want to download the seg file which contains our toBlock
		// so that we can correctly calculate its maxTxNum from the body segment files (we will later on delete this file)
		return fileInfo.From > toBlock
	}
	return fileInfo.To > toBlock
}
