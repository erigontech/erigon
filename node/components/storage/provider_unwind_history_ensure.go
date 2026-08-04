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

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

// walkDomains lists the state domains whose history the mode-B compute
// walks via HistoryKeyTxNumRange to build the touch set (see
// execution/commitment/commitmentdb/recompute_sdless.go). Non-history
// files (block seg, tracesfrom/tracesto/logaddrs/logtopics, receipt/
// rcache) are never consulted by the compute and are excluded from the
// on-demand download set.
var walkDomains = map[string]struct{}{
	"accounts": {},
	"storage":  {},
	"code":     {},
}

// ensureHistoryForUnwindWalk downloads any preverified history files
// needed for the mode-B compute walk (baselineStep, walkEndStep] that
// aren't currently present on disk, and returns a cleanup callback
// that removes those temp-downloaded files. Under
// `--prune.mode=minimal` preverified state history is deliberately not
// downloaded at bootstrap (db/snapshotsync/preverified_filter.go), so
// the compute walking from a step-aligned baseline needs the missing
// files pulled in on demand for the duration of the compute.
//
// Cleanup MUST be deferred by the caller — leaving temp files across
// unwinds produces non-deterministic starting state for subsequent
// iters and reproduces the "works sometimes" flakiness the whole
// investigation traced back to. See
// memory/mode-b-temp-history-cleanup-decision.md.
//
// No-op paths (return a no-op cleanup):
//   - Aggregator nil (tools/tests without state).
//   - downloaderClient nil (`--no-downloader` or offline tools).
//   - ChainConfig nil.
//   - StepSize() == 0.
//   - baselineStep couldn't be determined (no local commitment .kv
//     with endStep ≤ walkEndStep — unusual, means fresh bootstrap
//     hasn't completed).
//   - No preverified items intersect the walk range.
//   - Every intersecting item is already on disk.
func (p *Provider) ensureHistoryForUnwindWalk(ctx context.Context, opts UnwindOpts, toBlock uint64) (func(), error) {
	noop := func() {}

	if p.Aggregator == nil || p.downloaderClient == nil || p.ChainConfig == nil {
		return noop, nil
	}
	stepSize := p.Aggregator.StepSize()
	if stepSize == 0 {
		return noop, nil
	}

	toBlockLastTxNum, err := rawdbv3.TxNums.Max(ctx, opts.Tx, toBlock)
	if err != nil {
		return noop, fmt.Errorf("TxNums.Max(%d): %w", toBlock, err)
	}
	// walkEndStep = (toBlockLastTxNum + 1) / stepSize mirrors the compute's
	// stepBoundary derivation in execution/commitment/commitmentdb/
	// recompute_sdless.go. This is the maxStep the compute passes to
	// getLatestFromFilesUpToStep for its baseline lookup.
	walkEndStep := (toBlockLastTxNum + 1) / stepSize
	if walkEndStep == 0 {
		return noop, nil
	}

	// baselineStep is the endStep of the widest local commitment .kv file
	// with endStep ≤ walkEndStep — the same file the compute picks up as
	// its baseline. History for txN < baselineStep*stepSize is already
	// captured in the trie state encoded in that baseline, so downloading
	// pre-baseline history files (e.g. .0-256.v/.ef/.efi/.vi ≈ hundreds of
	// GB across all domains) would be pure waste.
	baselineStep, ok := localCommitmentBaselineStep(p.snapDir, walkEndStep, stepSize)
	if !ok {
		return noop, nil
	}
	if baselineStep >= walkEndStep {
		// Baseline is already at/past the walk end — the compute's
		// touch range is empty and it will just re-encode the baseline
		// trie state. No history walk needed.
		return noop, nil
	}

	chainName := p.ChainConfig.ChainName
	cfg := snapcfg.KnownCfgOrDevnet(chainName)
	if cfg == nil {
		return noop, nil
	}

	needed := neededPreverifiedHistoryForWalk(cfg.Preverified.Items, baselineStep, walkEndStep, stepSize)
	if len(needed) == 0 {
		return noop, nil
	}

	missing, downloadedPaths, downloadedNames := filterMissingOnDisk(needed, p.snapDir)
	if len(missing) == 0 {
		return noop, nil
	}

	if p.logger != nil {
		p.logger.Info("[storage] Provider.Unwind: downloading missing preverified history for compute walk",
			"toBlock", toBlock, "baselineStep", baselineStep, "walkEndStep", walkEndStep, "files", len(missing))
	}

	if err := snapshotsync.RequestSnapshotsDownload(ctx, missing, p.downloaderClient, "mode-b-history-ensure"); err != nil {
		return noop, fmt.Errorf("RequestSnapshotsDownload: %w", err)
	}

	if err := p.Aggregator.OpenFolder(); err != nil {
		p.discardDownloadedHistory(ctx, downloadedPaths, downloadedNames)
		return noop, fmt.Errorf("OpenFolder after history download: %w", err)
	}
	// OpenFolder publishes a new visible-file generation carrying the just-
	// downloaded history files, but opts.Tx still pins the generation captured
	// at BeginTemporalRw time. Repin it so the compute walk (which reads
	// through opts.Tx) sees the new files. Safe under the retire+refcount
	// mechanism — the old generation stays live until every reader releases.
	reopener, ok := opts.Tx.(kv.CanReopenUnderlyingFilesTx)
	if !ok {
		p.discardDownloadedHistory(ctx, downloadedPaths, downloadedNames)
		return noop, fmt.Errorf("opts.Tx (%T) does not implement kv.CanReopenUnderlyingFilesTx", opts.Tx)
	}
	reopener.ForceReopenUnderlyingFilesTx()

	return func() {
		p.discardDownloadedHistory(ctx, downloadedPaths, downloadedNames)
		if err := p.Aggregator.OpenFolder(); err != nil && p.logger != nil {
			p.logger.Warn("[storage] Provider.Unwind: OpenFolder after temp-history cleanup failed", "err", err)
		}
	}, nil
}

// discardDownloadedHistory removes the given files, their .torrent
// sidecars, AND drops the corresponding torrents from the downloader
// client's internal state. The Delete step is load-bearing: without
// it, the torrent client keeps the torrents marked as "complete" from
// the previous download, and the next mode-B ensure call reports
// 100%/instant success even though the files were unlinked — the
// compute then walks over an empty visible file set and produces the
// baseline root instead of the target root. Errors are logged and
// swallowed — cleanup is best-effort. Callers own the aggregator
// refresh (this function does not call OpenFolder so it can be reused
// on the error path where the caller hasn't yet done the initial
// OpenFolder either).
func (p *Provider) discardDownloadedHistory(ctx context.Context, paths []string, names []string) {
	if p.downloaderClient != nil && len(names) > 0 {
		if err := p.downloaderClient.Delete(ctx, names); err != nil && p.logger != nil {
			p.logger.Warn("[storage] Provider.Unwind: downloader.Delete temp history failed", "count", len(names), "err", err)
		}
	}
	for _, path := range paths {
		if err := dir.RemoveFile(path); err != nil && !errors.Is(err, os.ErrNotExist) && p.logger != nil {
			p.logger.Warn("[storage] Provider.Unwind: remove temp history file failed", "path", path, "err", err)
		}
		torrentPath := path + ".torrent"
		if err := dir.RemoveFile(torrentPath); err != nil && !errors.Is(err, os.ErrNotExist) && p.logger != nil {
			p.logger.Debug("[storage] Provider.Unwind: remove temp history torrent failed", "path", torrentPath, "err", err)
		}
	}
}

// localCommitmentBaselineStep scans snapDir/domain for `*-commitment.*.kv`
// files, returning the largest endStep with endStep ≤ walkEndStep. That
// endStep is the compute's baseline: getLatestFromFilesUpToStep picks
// the same file. Returns (0, false) when no eligible commitment file is
// on disk — the mode-B compute would then fail to find a baseline
// anyway; the ensure step just no-ops so we don't paper over the
// upstream problem.
func localCommitmentBaselineStep(snapDir string, walkEndStep, stepSize uint64) (uint64, bool) {
	domainDir := filepath.Join(snapDir, "domain")
	entries, err := os.ReadDir(domainDir)
	if err != nil {
		return 0, false
	}
	var best uint64
	found := false
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".kv") {
			continue
		}
		if !strings.Contains(name, "-commitment.") {
			continue
		}
		_, toStep, ok := parseStateFileStepRange(name, stepSize)
		if !ok {
			continue
		}
		if toStep > walkEndStep {
			continue
		}
		if !found || toStep > best {
			best = toStep
			found = true
		}
	}
	return best, found
}

// neededPreverifiedHistoryForWalk returns preverified items that
// (a) live under history/, idx/, or accessor/ AND belong to a
// compute-walked domain (accounts/storage/code), and (b) have a step
// range overlapping (baselineStep, walkEndStep]. Extracted so unit
// tests can drive it with a synthetic PreverifiedItems slice without a
// full Provider.
func neededPreverifiedHistoryForWalk(items snapcfg.PreverifiedItems, baselineStep, walkEndStep, stepSize uint64) []snapcfg.PreverifiedItem {
	if baselineStep >= walkEndStep {
		return nil
	}
	out := make([]snapcfg.PreverifiedItem, 0, 16)
	for _, item := range items {
		if !isHistoryOrIdxOrAccessor(item.Name) {
			continue
		}
		if !isWalkDomain(item.Name) {
			continue
		}
		fromStep, toStep, ok := parseStateFileStepRange(item.Name, stepSize)
		if !ok {
			continue
		}
		if fromStep >= toStep {
			continue
		}
		// Overlap with (baselineStep, walkEndStep]: file [fromStep, toStep)
		// intersects the range iff toStep > baselineStep AND fromStep <= walkEndStep.
		if toStep <= baselineStep {
			continue
		}
		if fromStep > walkEndStep {
			continue
		}
		out = append(out, item)
	}
	return out
}

// filterMissingOnDisk returns the subset of `needed` whose destination
// path under snapDir does not yet exist, plus the parallel list of
// absolute destination paths and snap-dir-relative names (both used by
// discardDownloadedHistory — the paths for FS unlink, the names for
// downloader.Delete so the torrent client drops the torrent from its
// internal state; without the Delete step the next mode-B ensure call
// sees the torrent as "have complete" from the prior download and
// returns immediately even though the actual files were unlinked).
func filterMissingOnDisk(needed []snapcfg.PreverifiedItem, snapDir string) ([]dbservices.DownloadRequest, []string, []string) {
	missing := make([]dbservices.DownloadRequest, 0, len(needed))
	paths := make([]string, 0, len(needed))
	names := make([]string, 0, len(needed))
	for _, item := range needed {
		absPath := filepath.Join(snapDir, item.Name)
		if _, err := os.Stat(absPath); err == nil {
			continue
		}
		missing = append(missing, dbservices.DownloadRequest{Path: item.Name, TorrentHash: item.Hash})
		paths = append(paths, absPath)
		names = append(names, item.Name)
	}
	return missing, paths, names
}

func isHistoryOrIdxOrAccessor(name string) bool {
	return strings.HasPrefix(name, "history/") ||
		strings.HasPrefix(name, "idx/") ||
		strings.HasPrefix(name, "accessor/")
}

// isWalkDomain reports whether the state file's domain is one the
// mode-B compute walks — accounts, storage, or code. Names shape:
// "<subdir>/vX.Y-<domain>.<from>-<to>.<ext>".
func isWalkDomain(name string) bool {
	base := filepath.Base(name)
	_, after, ok0 := strings.Cut(base, "-")
	if !ok0 {
		return false
	}
	afterVersion := after
	dot := strings.IndexByte(afterVersion, '.')
	if dot <= 0 {
		return false
	}
	_, ok := walkDomains[afterVersion[:dot]]
	return ok
}

// parseStateFileStepRange returns the file's [fromStep, toStep) step range.
// State files' From/To in the parsed FileInfo are step indices for legacy
// (pre-v4.0) naming and raw txNums for v4.0+; this helper converts either
// to steps using stepSize when needed.
func parseStateFileStepRange(name string, stepSize uint64) (fromStep, toStep uint64, ok bool) {
	base := filepath.Base(name)
	info, _, parsed := snaptype.ParseFileName("", base)
	if !parsed || info.From >= info.To {
		return 0, 0, false
	}
	if info.Version.Cmp(version.TxNumNamingPivot) < 0 {
		return info.From, info.To, true
	}
	if stepSize == 0 {
		return 0, 0, false
	}
	return info.From / stepSize, info.To / stepSize, true
}
