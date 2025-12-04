// Copyright 2025 The Erigon Authors
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

package backtester

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
)

type Backtester struct {
	logger      log.Logger
	db          kv.TemporalRoDB
	blockReader services.FullBlockReader
	outputDir   string
}

func New(logger log.Logger, db kv.TemporalRoDB, br services.FullBlockReader, outputDir string) Backtester {
	return Backtester{
		logger:      logger,
		db:          db,
		blockReader: br,
		outputDir:   outputDir,
	}
}

func (b Backtester) Run(ctx context.Context, fromBlock uint64, toBlock uint64) error {
	start := time.Now()
	b.logger.Info("starting commitment backtest", "fromBlock", fromBlock, "toBlock", toBlock)
	if fromBlock > toBlock || fromBlock == 0 {
		return fmt.Errorf("invalid block range for backtest: fromBlock=%d, toBlock=%d", fromBlock, toBlock)
	}
	tx, err := b.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tnr := b.blockReader.TxnumReader(ctx)
	err = checkHistoryAvailable(tx, fromBlock, tnr)
	if err != nil {
		return err
	}
	runId := fmt.Sprintf("%d_%d_%d", fromBlock, toBlock, start.Unix())
	runOutputDir := path.Join(b.outputDir, runId)
	err = os.MkdirAll(runOutputDir, 0755)
	if err != nil {
		return err
	}
	for block := fromBlock; block <= toBlock; block++ {
		blockOutputDir := path.Join(runOutputDir, fmt.Sprintf("block_%d", block))
		err = os.MkdirAll(blockOutputDir, 0755)
		if err != nil {
			return err
		}
		err = b.backtestBlock(ctx, tx, block, tnr, blockOutputDir)
		if err != nil {
			return err
		}
	}
	b.logger.Info("finished commitment backtest", "blocks", toBlock-fromBlock+1, "in", time.Since(start), "output", runOutputDir)
	return nil
}

func (b Backtester) backtestBlock(ctx context.Context, tx kv.TemporalTx, block uint64, tnr rawdbv3.TxNumsReader, blockOutputDir string) error {
	start := time.Now()
	b.logger.Info("backtesting block commitment", "block", block)
	fromTxNum, err := tnr.Min(tx, block)
	if err != nil {
		return err
	}
	maxTxNum, err := tnr.Max(tx, block)
	if err != nil {
		return err
	}
	toTxNum := maxTxNum + 1
	b.logger.Info("backtesting block commitment", "fromTxNum", fromTxNum, "toTxNum", toTxNum)
	sd, err := execctx.NewSharedDomains(ctx, tx, b.logger)
	if err != nil {
		return err
	}
	defer sd.Close()
	sd.GetCommitmentCtx().SetStateReader(newBacktestStateReader(tx, fromTxNum, toTxNum))
	sd.GetCommitmentCtx().SetTrace(b.logger.Enabled(ctx, log.LvlTrace))
	sd.GetCommitmentCtx().EnableCsvMetrics(path.Join(blockOutputDir, "commitment_metrics"))
	err = sd.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}
	if expected := block - 1; sd.BlockNum() != expected {
		return fmt.Errorf("unexpected sd block number: %d != %d", sd.BlockNum(), expected)
	}
	if expected := fromTxNum - 1; sd.TxNum() != expected {
		return fmt.Errorf("unexpected sd tx number: %d != %d", sd.TxNum(), maxTxNum)
	}
	err = b.replayChanges(tx, kv.AccountsDomain, sd, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	err = b.replayChanges(tx, kv.StorageDomain, sd, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	b.logger.Info("computing commitment", "block", block)
	commitmentStart := time.Now()
	root, err := sd.ComputeCommitment(ctx, tx, false /*saveState*/, block, maxTxNum, "commitment-backtester", nil /*progress*/)
	if err != nil {
		return err
	}
	b.logger.Info("computed commitment", "block", block, "in", time.Since(commitmentStart))
	canonicalHeader, err := b.blockReader.HeaderByNumber(ctx, tx, block)
	if err != nil {
		return err
	}
	if canonicalHeader == nil {
		return fmt.Errorf("canonical header not found for block %d", block)
	}
	if common.Hash(root) != canonicalHeader.Root {
		return fmt.Errorf("computed commitment %x does not match canonical header root %x", root, canonicalHeader.Root)
	}
	b.logger.Info("computed commitment matches canonical header root", "block", block, "root", canonicalHeader.Root)
	b.logger.Info("backtested block commitment", "block", block, "in", time.Since(start))
	return nil
}

func (b Backtester) replayChanges(tx kv.TemporalTx, d kv.Domain, sd *execctx.SharedDomains, fromTxNum uint64, toTxNum uint64) error {
	starTime := time.Now()
	changes := 0
	defer func() {
		b.logger.Info("replayed changes", "domain", d, "changes", changes, "in", time.Since(starTime))
	}()
	b.logger.Info("replaying changes", "domain", d, "fromTxNum", fromTxNum, "toTxNum", toTxNum)
	it, err := tx.HistoryRange(d, int(fromTxNum), int(toTxNum), order.Asc, -1)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return err
		}
		sd.GetCommitmentContext().TouchKey(d, string(k), nil)
		changes++
	}
	return nil
}

func checkHistoryAvailable(tx kv.TemporalTx, fromBlock uint64, tnr rawdbv3.TxNumsReader) error {
	fromTxNum, err := tnr.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	historyAvailabilityTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
	if fromTxNum < historyAvailabilityTxNum {
		return fmt.Errorf("history not available for given start: %d < %d", fromTxNum, historyAvailabilityTxNum)
	}
	return nil
}
