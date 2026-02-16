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
	"container/heap"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/felixge/fgprof"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

type Opt func(bt *Backtester)

func WithParaTrie(paraTrie bool) Opt {
	return func(bt *Backtester) {
		bt.paraTrie = paraTrie
	}
}

func WithTrieWarmup(trieWarmup bool) Opt {
	return func(bt *Backtester) {
		bt.trieWarmup = trieWarmup
	}
}

func WithChartsTopN(n uint64) Opt {
	return func(bt *Backtester) {
		bt.metricsTopN = n
	}
}

func WithChartsPageSize(n uint64) Opt {
	return func(bt *Backtester) {
		bt.metricsPageSize = n
	}
}

func New(logger log.Logger, db kv.TemporalRoDB, br services.FullBlockReader, outputDir string, opts ...Opt) Backtester {
	bt := Backtester{
		logger:          logger,
		db:              db,
		blockReader:     br,
		outputDir:       outputDir,
		metricsTopN:     10,
		metricsPageSize: 1024,
	}
	for _, opt := range opts {
		opt(&bt)
	}
	return bt
}

type Backtester struct {
	logger          log.Logger
	db              kv.TemporalRoDB
	blockReader     services.FullBlockReader
	outputDir       string
	paraTrie        bool
	trieWarmup      bool
	metricsTopN     uint64
	metricsPageSize uint64
}

func (bt Backtester) RunTMinusN(ctx context.Context, n uint64) error {
	tx, err := bt.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tnr := bt.blockReader.TxnumReader()
	toBlockNum, _, err := tnr.Last(tx)
	if err != nil {
		return err
	}
	var fromBlockNum uint64
	if toBlockNum <= n {
		fromBlockNum = 1
	} else {
		fromBlockNum = toBlockNum - n
	}
	return bt.run(ctx, tx, fromBlockNum, toBlockNum)
}

func (bt Backtester) Run(ctx context.Context, fromBlock uint64, toBlock uint64) error {
	tx, err := bt.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return bt.run(ctx, tx, fromBlock, toBlock)
}

func (bt Backtester) run(ctx context.Context, tx kv.TemporalTx, fromBlock uint64, toBlock uint64) error {
	start := time.Now()
	bt.logger.Info("starting commitment backtest", "fromBlock", fromBlock, "toBlock", toBlock)
	if fromBlock > toBlock || fromBlock == 0 {
		return fmt.Errorf("invalid block range for backtest: fromBlock=%d, toBlock=%d", fromBlock, toBlock)
	}
	tnr := bt.blockReader.TxnumReader()
	if toBlock == math.MaxUint64 {
		var err error
		toBlock, _, err = tnr.Last(tx)
		if err != nil {
			return err
		}
	}
	err := checkDataAvailable(ctx, tx, fromBlock, toBlock, tnr)
	if err != nil {
		return err
	}
	ri := runId{
		paraTrie:   bt.paraTrie,
		trieWarmup: bt.trieWarmup,
		fromBlock:  fromBlock,
		toBlock:    toBlock,
		start:      start,
	}
	runOutputDir := path.Join(bt.outputDir, ri.String())
	err = os.MkdirAll(runOutputDir, 0755)
	if err != nil {
		return err
	}
	for block := fromBlock; block <= toBlock; block++ {
		err = bt.backtestBlock(ctx, tx, block, tnr, runOutputDir)
		if err != nil {
			return err
		}
	}
	resultsFilePath, err := bt.processResults(fromBlock, toBlock, runOutputDir)
	if err != nil {
		return err
	}
	bt.logger.Info(
		"finished commitment backtest",
		"blocks", toBlock-fromBlock+1,
		"in", time.Since(start),
		"results", runOutputDir,
	)
	bt.logger.Info("metrics", "at", fmt.Sprintf("file://%s", resultsFilePath))
	return nil
}

func (bt Backtester) backtestBlock(ctx context.Context, tx kv.TemporalTx, block uint64, tnr rawdbv3.TxNumsReader, runOutputDir string) error {
	start := time.Now()
	bt.logger.Info("backtesting block commitment", "block", block)
	blockOutputDir := deriveBlockOutputDir(runOutputDir, block)
	err := os.MkdirAll(blockOutputDir, 0755)
	if err != nil {
		return err
	}
	fromTxNum, err := tnr.Min(ctx, tx, block)
	if err != nil {
		return err
	}
	maxTxNum, err := tnr.Max(ctx, tx, block)
	if err != nil {
		return err
	}
	toTxNum := maxTxNum + 1
	bt.logger.Info("backtesting block commitment", "fromTxNum", fromTxNum, "toTxNum", toTxNum, "paraTrie", bt.paraTrie)
	if bt.paraTrie {
		statecfg.ExperimentalConcurrentCommitment = true
	}
	sd, err := execctx.NewSharedDomains(ctx, tx, bt.logger)
	if err != nil {
		return err
	}
	defer sd.Close()
	if bt.trieWarmup {
		sd.EnableParaTrieDB(bt.db)
		sd.EnableTrieWarmup(true)
	}
	if bt.paraTrie {
		sd.EnableParaTrieDB(bt.db)
	}
	sd.GetCommitmentCtx().SetStateReader(newBacktestStateReader(tx, fromTxNum, toTxNum))
	sd.GetCommitmentCtx().SetTrace(bt.logger.Enabled(ctx, log.LvlTrace))
	sd.GetCommitmentCtx().EnableCsvMetrics(deriveBlockMetricsFilePrefix(blockOutputDir))
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
	err = bt.replayChanges(tx, kv.AccountsDomain, sd, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	err = bt.replayChanges(tx, kv.StorageDomain, sd, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	bt.logger.Info("computing commitment", "block", block)
	cpuProfilePath := path.Join(blockOutputDir, "cpu.prof")
	cpuProfile, err := os.Create(cpuProfilePath)
	if err != nil {
		return fmt.Errorf("failed to create cpu profile: %w", err)
	}
	defer func() {
		err := cpuProfile.Close()
		if err != nil {
			bt.logger.Error("failed to close cpu profile", "f", cpuProfilePath, "err", err)
		}
	}()
	fgprofProfilePath := path.Join(blockOutputDir, "cpu.fgprof")
	fgprofProfile, err := os.Create(fgprofProfilePath)
	if err != nil {
		return fmt.Errorf("failed to create fgprof profile: %w", err)
	}
	defer func() {
		err := fgprofProfile.Close()
		if err != nil {
			bt.logger.Error("failed to close fgprof profile", "f", fgprofProfilePath, "err", err)
		}
	}()
	stopFgprof := fgprof.Start(fgprofProfile, fgprof.FormatPprof)
	err = pprof.StartCPUProfile(cpuProfile)
	if err != nil {
		return fmt.Errorf("failed to start cpu profile: %s: %w", cpuProfilePath, err)
	}
	commitmentStart := time.Now()
	root, err := sd.ComputeCommitment(ctx, tx, false /*saveState*/, block, maxTxNum, "commitment-backtester", nil /*progress*/)
	if err != nil {
		return err
	}
	bt.logger.Info("computed commitment", "block", block, "in", time.Since(commitmentStart))
	pprof.StopCPUProfile()
	err = stopFgprof()
	if err != nil {
		return fmt.Errorf("failed to stop fgprof: %w", err)
	}
	canonicalHeader, err := bt.blockReader.HeaderByNumber(ctx, tx, block)
	if err != nil {
		return err
	}
	if canonicalHeader == nil {
		return fmt.Errorf("canonical header not found for block %d", block)
	}
	if common.Hash(root) != canonicalHeader.Root {
		return fmt.Errorf("computed commitment %x does not match canonical header root %x", root, canonicalHeader.Root)
	}
	bt.logger.Info("computed commitment matches canonical header root", "block", block, "root", canonicalHeader.Root)
	bt.logger.Info("backtested block commitment", "block", block, "in", time.Since(start))
	return nil
}

func (bt Backtester) replayChanges(tx kv.TemporalTx, d kv.Domain, sd *execctx.SharedDomains, fromTxNum uint64, toTxNum uint64) error {
	starTime := time.Now()
	changes := 0
	defer func() {
		bt.logger.Info("replayed changes", "domain", d, "changes", changes, "in", time.Since(starTime))
	}()
	bt.logger.Info("replaying changes", "domain", d, "fromTxNum", fromTxNum, "toTxNum", toTxNum)
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

func (bt Backtester) processResults(fromBlock uint64, toBlock uint64, runOutputDir string) (string, error) {
	bt.logger.Info("processing results", "fromBlock", fromBlock, "toBlock", toBlock, "runOutputDir", runOutputDir)
	var chartsPageFilePaths []string
	var topNSlowest slowestBatchesHeap
	var branchJumpdestCounts [128][16]uint64
	var branchKeyLenCounts [128]uint64
	pageMetrics := make([]MetricValues, 0, bt.metricsPageSize)
	for pageBlockFrom := fromBlock; pageBlockFrom <= toBlock; pageBlockFrom += bt.metricsPageSize {
		pageBlockTo := min(pageBlockFrom+bt.metricsPageSize-1, toBlock)
		pageMetrics = pageMetrics[:0]
		for block := pageBlockFrom; block <= pageBlockTo; block++ {
			blockOutputDir := deriveBlockOutputDir(runOutputDir, block)
			commitmentMetricsFilePrefix := deriveBlockMetricsFilePrefix(blockOutputDir)
			mVals, err := commitment.UnmarshallMetricValuesCsv(commitmentMetricsFilePrefix)
			if err != nil {
				return "", err
			}
			if len(mVals) != 1 {
				return "", fmt.Errorf("expected metrics for 1 batch: got=%d, block=%d", len(mVals), block)
			}
			mv := MetricValues{
				BatchId:      block,
				MetricValues: mVals[0],
			}
			if uint64(topNSlowest.Len()) < bt.metricsTopN {
				heap.Push(&topNSlowest, mv)
			} else if mv.SpentProcessing > topNSlowest[0].SpentProcessing {
				heap.Pop(&topNSlowest)
				heap.Push(&topNSlowest, mv)
			}
			for branchKey, branchStats := range mVals[0].Branches {
				nibbles, err := hex.DecodeString(branchKey)
				if err != nil {
					return "", err
				}
				if commitment.HasTerm(nibbles) {
					nibbles = nibbles[:len(nibbles)-1]
				}
				lastNibble := nibbles[len(nibbles)-1]
				depth := len(nibbles) - 1
				branchJumpdestCounts[depth][lastNibble] += branchStats.LoadBranch
				branchKeyLenCounts[depth]++
			}
			pageMetrics = append(pageMetrics, mv)
		}
		chartsPageFilePath, err := renderDetailedPage(pageMetrics, runOutputDir)
		if err != nil {
			return "", err
		}
		chartsPageFilePaths = append(chartsPageFilePaths, chartsPageFilePath)
	}
	agg := crossPageAggMetrics{
		top:                  &topNSlowest,
		branchJumpdestCounts: &branchJumpdestCounts,
		branchKeyLenCounts:   &branchKeyLenCounts,
	}
	return renderOverviewPage(agg, chartsPageFilePaths, runOutputDir)
}

func checkDataAvailable(ctx context.Context, tx kv.TemporalTx, fromBlock uint64, toBlock uint64, tnr rawdbv3.TxNumsReader) error {
	firstBlockNum, _, err := tnr.First(tx)
	if err != nil {
		return err
	}
	if fromBlock < firstBlockNum {
		return fmt.Errorf("block not available for given start: %d < %d", fromBlock, firstBlockNum)
	}
	lastBlockNum, _, err := tnr.Last(tx)
	if err != nil {
		return err
	}
	if toBlock > lastBlockNum {
		return fmt.Errorf("block not available for given end: %d > %d", toBlock, lastBlockNum)
	}
	fromTxNum, err := tnr.Min(ctx, tx, fromBlock)
	if err != nil {
		return err
	}
	historyAvailableFromTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
	if fromTxNum < historyAvailableFromTxNum {
		return fmt.Errorf("history not available for given start: %d < %d", fromTxNum, historyAvailableFromTxNum)
	}
	toTxNum, err := tnr.Max(ctx, tx, toBlock)
	if err != nil {
		return err
	}
	historyAvailableToTxNum := tx.Debug().DomainProgress(kv.CommitmentDomain)
	if toTxNum > historyAvailableToTxNum {
		return fmt.Errorf("history not available for given end: %d > %d", toTxNum, historyAvailableToTxNum)
	}
	return nil
}

type runId struct {
	paraTrie   bool
	trieWarmup bool
	fromBlock  uint64
	toBlock    uint64
	start      time.Time
}

func (ri runId) String() string {
	var sb strings.Builder
	if ri.paraTrie {
		sb.WriteString("para")
	} else {
		sb.WriteString("hph")
	}
	if ri.trieWarmup {
		sb.WriteString("_warm")
	} else {
		sb.WriteString("_nowarm")
	}
	return fmt.Sprintf("%s_%d_%d_%d", sb.String(), ri.fromBlock, ri.toBlock, ri.start.Unix())
}

func extractRunId(s string) runId {
	parts := strings.Split(s, "_")
	paraTrie := parts[0] == "para"
	trieWarmup := parts[1] == "warm"
	fromBlock, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		panic(fmt.Errorf("extractRunId failed to parse fromBlock: %w", err))
	}
	toBlock, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		panic(fmt.Errorf("extractRunId failed to parse toBlock: %w", err))
	}
	startUnix, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		panic(fmt.Errorf("extractRunId failed to parse start: %w", err))
	}
	return runId{
		paraTrie:   paraTrie,
		trieWarmup: trieWarmup,
		fromBlock:  fromBlock,
		toBlock:    toBlock,
		start:      time.Unix(startUnix, 0),
	}
}

func deriveBlockOutputDir(runOutputDir string, block uint64) string {
	return path.Join(runOutputDir, fmt.Sprintf("block_%d", block))
}

func deriveBlockMetricsFilePrefix(blockOutputDir string) string {
	return path.Join(blockOutputDir, "commitment_metrics")
}
