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

package stagedsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/tiering"
)

var (
	mxTieringColdBoundaryBlock   = metrics.GetOrCreateGauge("tiering_cold_boundary_block")
	mxTieringDemoteBoundaryBlock = metrics.GetOrCreateGauge("tiering_demote_boundary_block")
	mxTieringLagFiles            = metrics.GetOrCreateGaugeVec("tiering_lag_files", []string{"domain", "reason"})
	mxTieringLagBytes            = metrics.GetOrCreateGaugeVec("tiering_lag_bytes", []string{"domain", "reason"})
	mxTieringLagTxs              = metrics.GetOrCreateGaugeVec("tiering_lag_txs", []string{"domain", "reason"})
	mxTieringLagBlocks           = metrics.GetOrCreateGaugeVec("tiering_lag_blocks", []string{"domain", "reason"})
	mxTieringColdResidentFiles   = metrics.GetOrCreateGaugeVec("tiering_cold_resident_files", []string{"domain"})
	mxTieringColdResidentBytes   = metrics.GetOrCreateGaugeVec("tiering_cold_resident_bytes", []string{"domain"})
	mxTieringHDDEndBlock         = metrics.GetOrCreateGaugeVec("tiering_hdd_end_block", []string{"domain"})
	mxTieringSSDStartBlock       = metrics.GetOrCreateGaugeVec("tiering_ssd_start_block", []string{"domain"})
	mxTieringCommitmentBytes     = metrics.GetOrCreateGaugeVec("tiering_commitment_bytes", []string{"tier", "kind"})
	mxTieringDuplicateBytes      = metrics.GetOrCreateGaugeVec("tiering_duplicate_bytes", []string{"reason"})

	probesPerBlockBuckets          = []float64{0, 1, 10, 100, 1e3, 1e4, 3e4, 1e5, 3e5, 1e6, 3e6, 1e7}
	mxTieringSSDProbesPerBlock     = metrics.NewHistogram("tiering_ssd_probes_per_block", probesPerBlockBuckets)
	mxTieringHDDProbesPerBlock     = metrics.NewHistogram("tiering_hdd_probes_per_block", probesPerBlockBuckets)
	mxTieringHDDSlowProbesPerBlock = metrics.NewHistogram("tiering_hdd_slow_probes_per_block", probesPerBlockBuckets)

	tieringGaugesAt atomic.Int64
	tieringLoggedAt atomic.Int64
)

// tieringGaugesInterval keeps the per-file scan (a stat per state file) off the per-block path at the chain tip.
const tieringGaugesInterval = 10 * time.Second

const tieringTickInterval = 30 * time.Second

func reportTiering(ctx context.Context, cfg ExecuteBlockCfg, stepSize uint64, tx kv.TemporalTx, head uint64, logger log.Logger) error {
	lookback := cfg.syncCfg.TieringLookbackBlocks
	if lookback == 0 {
		return nil
	}
	now := time.Now()
	gaugesDue := now.Sub(time.Unix(0, tieringGaugesAt.Load())) >= tieringGaugesInterval
	if !gaugesDue && !cfg.syncCfg.TieringDemote {
		return nil
	}
	if cfg.syncCfg.TieringDemote {
		demoteBoundary := tiering.DemoteBoundary(head, rawdb.ReadForkchoiceFinalizedNum(tx), lookback)
		demoteTxNum, err := cfg.blockReader.TxnumReader().Min(ctx, tx, demoteBoundary)
		if err != nil {
			return err
		}
		mxTieringDemoteBoundaryBlock.SetUint64(demoteBoundary)
		cfg.db.Debug().DemoteInBackground(demoteTxNum)
	}
	if !gaugesDue {
		return nil
	}
	tieringGaugesAt.Store(now.UnixNano())
	state.PublishTierProbeCounts()

	boundary := tiering.ColdBoundary(head, lookback)
	boundaryTxNum, err := cfg.blockReader.TxnumReader().Min(ctx, tx, boundary)
	if err != nil {
		return err
	}

	mxTieringColdBoundaryBlock.SetUint64(boundary)
	coldDir := cfg.syncCfg.TieringColdDir
	files, err := tieringFiles(tiering.KVFiles(tx.Debug().DomainFiles(kv.CommitmentDomain)))
	if err != nil {
		return err
	}
	copyingBytes, err := tiering.StatCopying(coldDir, files)
	if err != nil {
		return err
	}
	commitmentLag := tiering.ComputeLag(files, boundaryTxNum, coldDir)
	name := kv.CommitmentDomain.String()
	for reason, u := range map[string]tiering.Usage{"straddling_file": commitmentLag.Straddling, "queued": commitmentLag.Queued, "copying": commitmentLag.Copying} {
		mxTieringLagFiles.WithLabelValues(name, reason).SetInt(u.Files)
		mxTieringLagBytes.WithLabelValues(name, reason).SetUint64(u.Bytes)
		mxTieringLagTxs.WithLabelValues(name, reason).SetUint64(u.Txs)
	}
	mxTieringColdResidentFiles.WithLabelValues(name).SetInt(commitmentLag.ColdResident.Files)
	mxTieringColdResidentBytes.WithLabelValues(name).SetUint64(commitmentLag.ColdResident.Bytes)
	hddEnd, ssdStart, err := tierEdgeBlocks(commitmentLag.ColdEnd, commitmentLag.HotStart, func(txNum uint64) (uint64, error) {
		return blockOfTxNum(ctx, cfg, tx, txNum, head)
	})
	if err != nil {
		return err
	}
	mxTieringHDDEndBlock.WithLabelValues(name).SetUint64(hddEnd)
	mxTieringSSDStartBlock.WithLabelValues(name).SetUint64(ssdStart)
	mxTieringDuplicateBytes.WithLabelValues("copying").SetUint64(copyingBytes)
	if err := reportCommitmentTiers(ctx, cfg, tx, files, commitmentLag, boundary, boundaryTxNum); err != nil {
		return err
	}

	if now.Sub(time.Unix(0, tieringLoggedAt.Load())) < 30*time.Second {
		return nil
	}
	tieringLoggedAt.Store(now.UnixNano())
	cold := tiering.ColdFiles(tiering.KVFiles(tx.Debug().DomainFiles(kv.CommitmentDomain)), boundaryTxNum)
	names := make([]string, len(cold))
	for i, f := range cold {
		names[i] = filepath.Base(f.Fullpath())
	}
	logger.Info("[tiering] cold boundary", "head", head, "lookback", lookback, "boundaryBlock", boundary,
		"boundaryStep", boundaryTxNum/stepSize, "coldCommitmentFiles", names,
		"commitmentQueued", usageString(commitmentLag.Queued), "commitmentCopying", usageString(commitmentLag.Copying), "commitmentStraddling", usageString(commitmentLag.Straddling),
		"commitmentColdResident", usageString(commitmentLag.ColdResident))
	return nil
}

// startTieringTicker repeats reportTiering on its own read-only tx while a long exec cycle runs; head stays the
// cycle's start block.
func startTieringTicker(ctx context.Context, cfg ExecuteBlockCfg, stepSize, head uint64, logger log.Logger) (stop func()) {
	if cfg.syncCfg.TieringLookbackBlocks == 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(tieringTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			err := cfg.db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
				return reportTiering(ctx, cfg, stepSize, tx, head, logger)
			})
			if err != nil && ctx.Err() == nil {
				logger.Warn("[tiering] periodic report", "err", err)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func reportCommitmentTiers(ctx context.Context, cfg ExecuteBlockCfg, tx kv.TemporalTx, files []tiering.File, lag tiering.Lag, boundary, boundaryTxNum uint64) error {
	coldDir := cfg.syncCfg.TieringColdDir
	var hot []tiering.File
	for _, f := range files {
		if !tiering.IsCold(f.Path, coldDir) {
			hot = append(hot, f)
		}
	}
	accessors, err := tiering.AccessorBytes(cfg.dirs.SnapDomain, files)
	if err != nil {
		return err
	}
	duplicate, err := tiering.DuplicateBytes(coldDir, hot)
	if err != nil {
		return err
	}
	mxTieringCommitmentBytes.WithLabelValues("hdd", "kv").SetUint64(lag.ColdResident.Bytes)
	mxTieringCommitmentBytes.WithLabelValues("ssd", "kv").SetUint64(lag.Hot.Bytes)
	mxTieringCommitmentBytes.WithLabelValues("ssd", "accessor").SetUint64(accessors)
	mxTieringDuplicateBytes.WithLabelValues("swap_pending").SetUint64(duplicate)

	straddlingStart := boundaryTxNum - lag.Straddling.Txs
	straddlingStartBlock, err := blockOfTxNum(ctx, cfg, tx, straddlingStart, boundary)
	if err != nil {
		return err
	}
	copyPendingStartBlock, err := blockOfTxNum(ctx, cfg, tx, straddlingStart-lag.Queued.Txs-lag.Copying.Txs, straddlingStartBlock)
	if err != nil {
		return err
	}
	name := kv.CommitmentDomain.String()
	mxTieringLagBlocks.WithLabelValues(name, "straddling_file").SetUint64(boundary - straddlingStartBlock)
	mxTieringLagBlocks.WithLabelValues(name, "copy_pending").SetUint64(straddlingStartBlock - copyPendingStartBlock)
	return nil
}

// blockOfTxNum returns the block that holds txNum; fallback when the txnum index does not reach it.
func blockOfTxNum(ctx context.Context, cfg ExecuteBlockCfg, tx kv.TemporalTx, txNum, fallback uint64) (uint64, error) {
	blockNum, ok, err := cfg.blockReader.TxnumReader().FindBlockNum(ctx, tx, txNum)
	if err != nil || !ok {
		return fallback, err
	}
	return min(blockNum, fallback), nil
}

func tieringFiles(visible kv.VisibleFiles) ([]tiering.File, error) {
	files := make([]tiering.File, 0, len(visible))
	for _, f := range visible {
		path, err := filepath.EvalSymlinks(f.Fullpath())
		if err != nil {
			return nil, err
		}
		fi, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		files = append(files, tiering.File{Start: f.StartRootNum(), End: f.EndRootNum(), Size: uint64(fi.Size()), Path: path})
	}
	return files, nil
}

func usageString(u tiering.Usage) string {
	return fmt.Sprintf("%d/%s", u.Files, common.ByteCount(u.Bytes))
}

// tierEdgeBlocks returns the block of the last cold tx (0 when nothing is cold) and the block of the first hot tx.
func tierEdgeBlocks(coldEnd, hotStart uint64, blockOf func(txNum uint64) (uint64, error)) (hddEnd, ssdStart uint64, err error) {
	if coldEnd > 0 {
		if hddEnd, err = blockOf(coldEnd - 1); err != nil {
			return 0, 0, err
		}
	}
	ssdStart, err = blockOf(hotStart)
	return hddEnd, ssdStart, err
}
