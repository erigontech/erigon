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
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/tiering"
)

var (
	mxTieringColdBoundaryBlock = metrics.GetOrCreateGauge("tiering_cold_boundary_block")
	mxTieringLagFiles          = metrics.GetOrCreateGaugeVec("tiering_lag_files", []string{"domain", "reason"})
	mxTieringLagBytes          = metrics.GetOrCreateGaugeVec("tiering_lag_bytes", []string{"domain", "reason"})
	mxTieringColdResidentFiles = metrics.GetOrCreateGaugeVec("tiering_cold_resident_files", []string{"domain"})
	mxTieringColdResidentBytes = metrics.GetOrCreateGaugeVec("tiering_cold_resident_bytes", []string{"domain"})

	tieringLoggedAt atomic.Int64
)

func reportTiering(ctx context.Context, cfg ExecuteBlockCfg, doms *execctx.SharedDomains, tx kv.TemporalTx, head uint64, logger log.Logger) error {
	lookback := cfg.syncCfg.TieringLookbackBlocks
	if lookback == 0 {
		return nil
	}
	boundary := tiering.ColdBoundary(head, lookback)
	boundaryTxNum, err := cfg.blockReader.TxnumReader().Min(ctx, tx, boundary)
	if err != nil {
		return err
	}
	mxTieringColdBoundaryBlock.SetUint64(boundary)
	var commitmentLag tiering.Lag
	for _, d := range []kv.Domain{kv.CommitmentDomain, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		files, err := tieringFiles(tiering.KVFiles(tx.Debug().DomainFiles(d)))
		if err != nil {
			return err
		}
		lag := tiering.ComputeLag(files, boundaryTxNum, cfg.syncCfg.TieringColdDir)
		name := d.String()
		mxTieringLagFiles.WithLabelValues(name, "straddling_file").SetInt(lag.Straddling.Files)
		mxTieringLagBytes.WithLabelValues(name, "straddling_file").SetUint64(lag.Straddling.Bytes)
		mxTieringLagFiles.WithLabelValues(name, "copy_pending").SetInt(lag.CopyPending.Files)
		mxTieringLagBytes.WithLabelValues(name, "copy_pending").SetUint64(lag.CopyPending.Bytes)
		mxTieringColdResidentFiles.WithLabelValues(name).SetInt(lag.ColdResident.Files)
		mxTieringColdResidentBytes.WithLabelValues(name).SetUint64(lag.ColdResident.Bytes)
		if d == kv.CommitmentDomain {
			commitmentLag = lag
		}
	}

	now := time.Now()
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
		"boundaryStep", boundaryTxNum/doms.StepSize(), "coldCommitmentFiles", names,
		"commitmentCopyPending", usageString(commitmentLag.CopyPending), "commitmentStraddling", usageString(commitmentLag.Straddling),
		"commitmentColdResident", usageString(commitmentLag.ColdResident))
	return nil
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
