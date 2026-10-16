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
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/tiering"
)

var (
	mxTieringColdBoundaryBlock = metrics.GetOrCreateGauge("tiering_cold_boundary_block")
	mxTieringLagFiles          = metrics.GetOrCreateGaugeVec("tiering_lag_files", []string{"domain", "reason"})
	mxTieringLagBytes          = metrics.GetOrCreateGaugeVec("tiering_lag_bytes", []string{"domain", "reason"})
	mxTieringLagTxs            = metrics.GetOrCreateGaugeVec("tiering_lag_txs", []string{"domain", "reason"})
	mxTieringLagBlocks         = metrics.GetOrCreateGaugeVec("tiering_lag_blocks", []string{"domain", "reason"})
	mxTieringColdResidentFiles = metrics.GetOrCreateGaugeVec("tiering_cold_resident_files", []string{"domain"})
	mxTieringColdResidentBytes = metrics.GetOrCreateGaugeVec("tiering_cold_resident_bytes", []string{"domain"})
	mxTieringHDDEndStep        = metrics.GetOrCreateGaugeVec("tiering_hdd_end_step", []string{"domain"})
	mxTieringSSDStartStep      = metrics.GetOrCreateGaugeVec("tiering_ssd_start_step", []string{"domain"})
	mxTieringCommitmentBytes   = metrics.GetOrCreateGaugeVec("tiering_commitment_bytes", []string{"tier", "kind"})
	mxTieringDuplicateBytes    = metrics.GetOrCreateGaugeVec("tiering_duplicate_bytes", []string{"reason"})

	probesPerBlockBuckets          = []float64{0, 1, 10, 100, 1e3, 1e4, 3e4, 1e5, 3e5, 1e6, 3e6, 1e7}
	mxTieringSSDProbesPerBlock     = metrics.NewHistogram("tiering_ssd_probes_per_block", probesPerBlockBuckets)
	mxTieringHDDProbesPerBlock     = metrics.NewHistogram("tiering_hdd_probes_per_block", probesPerBlockBuckets)
	mxTieringHDDSlowProbesPerBlock = metrics.NewHistogram("tiering_hdd_slow_probes_per_block", probesPerBlockBuckets)

	tieringGaugesAt atomic.Int64
	tieringLoggedAt atomic.Int64
)

// tieringGaugesInterval keeps the per-file scan (a stat per state file) off the per-block path at the chain tip.
const tieringGaugesInterval = 10 * time.Second

func reportTiering(ctx context.Context, cfg ExecuteBlockCfg, doms *execctx.SharedDomains, tx kv.TemporalTx, head uint64, logger log.Logger) error {
	lookback := cfg.syncCfg.TieringLookbackBlocks
	if lookback == 0 {
		return nil
	}
	now := time.Now()
	if now.Sub(time.Unix(0, tieringGaugesAt.Load())) < tieringGaugesInterval {
		return nil
	}
	tieringGaugesAt.Store(now.UnixNano())
	state.PublishTierProbeCounts()

	boundary := tiering.ColdBoundary(head, lookback)
	txNumsReader := cfg.blockReader.TxnumReader()
	boundaryTxNum, err := txNumsReader.Min(ctx, tx, boundary)
	if err != nil {
		return err
	}
	mxTieringColdBoundaryBlock.SetUint64(boundary)
	coldDir, stepSize := cfg.syncCfg.TieringColdDir, doms.StepSize()
	var commitmentLag tiering.Lag
	for _, d := range []kv.Domain{kv.CommitmentDomain, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		files, err := tieringFiles(tiering.KVFiles(tx.Debug().DomainFiles(d)))
		if err != nil {
			return err
		}
		lag := tiering.ComputeLag(files, boundaryTxNum, coldDir)
		name := d.String()
		for reason, u := range map[string]tiering.Usage{"straddling_file": lag.Straddling, "copy_pending": lag.CopyPending} {
			mxTieringLagFiles.WithLabelValues(name, reason).SetInt(u.Files)
			mxTieringLagBytes.WithLabelValues(name, reason).SetUint64(u.Bytes)
			mxTieringLagTxs.WithLabelValues(name, reason).SetUint64(u.Txs)
		}
		mxTieringColdResidentFiles.WithLabelValues(name).SetInt(lag.ColdResident.Files)
		mxTieringColdResidentBytes.WithLabelValues(name).SetUint64(lag.ColdResident.Bytes)
		mxTieringHDDEndStep.WithLabelValues(name).SetUint64(lag.ColdEnd / stepSize)
		mxTieringSSDStartStep.WithLabelValues(name).SetUint64(lag.HotStart / stepSize)
		if d == kv.CommitmentDomain {
			commitmentLag = lag
			if err := reportCommitmentTiers(ctx, cfg, tx, files, lag, boundary, boundaryTxNum); err != nil {
				return err
			}
		}
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
		"commitmentCopyPending", usageString(commitmentLag.CopyPending), "commitmentStraddling", usageString(commitmentLag.Straddling),
		"commitmentColdResident", usageString(commitmentLag.ColdResident))
	return nil
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
	copyPendingStartBlock, err := blockOfTxNum(ctx, cfg, tx, straddlingStart-lag.CopyPending.Txs, straddlingStartBlock)
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
