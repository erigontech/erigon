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
	"math"
	"os"
	"path"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/felixge/fgprof"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
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

func New(logger log.Logger, db kv.TemporalRoDB, br dbservices.FullBlockReader, outputDir string, opts ...Opt) Backtester {
	bt := Backtester{
		logger:      logger,
		db:          db,
		blockReader: br,
		outputDir:   outputDir,
	}
	for _, opt := range opts {
		opt(&bt)
	}
	return bt
}

type Backtester struct {
	logger      log.Logger
	db          kv.TemporalRoDB
	blockReader dbservices.FullBlockReader
	outputDir   string
	paraTrie    bool
	trieWarmup  bool
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
	idx, err := integrity.NewChangedKeysPerBlockIdx(ctx, tx, bt.blockReader, fromBlock, toBlock+1, bt.logger)
	if err != nil {
		return err
	}
	for block := fromBlock; block <= toBlock; block++ {
		err = bt.backtestBlock(ctx, tx, block, tnr, runOutputDir, idx)
		if err != nil {
			return err
		}
	}
	bt.logger.Info(
		"finished commitment backtest",
		"blocks", toBlock-fromBlock+1,
		"in", time.Since(start),
		"results", runOutputDir,
	)
	return nil
}

func (bt Backtester) backtestBlock(ctx context.Context, tx kv.TemporalTx, block uint64, tnr rawdbv3.TxNumsReader, runOutputDir string, idx *integrity.ChangedKeysPerBlockIdx) error {
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
	cfg := commitment.DefaultTrieConfig()
	if bt.paraTrie {
		cfg.Variant = commitment.VariantParallelHexPatricia
	}
	cfg.EnableTrieWarmup = bt.trieWarmup
	sd, err := execctx.NewSharedDomains(ctx, tx, bt.logger, execctx.WithTrieConfig(cfg))
	if err != nil {
		return err
	}
	defer sd.Close()
	if bt.trieWarmup {
		sd.EnableParaTrieDB(bt.db)
	}
	if bt.paraTrie {
		sd.EnableParaTrieDB(bt.db)
	}
	// A history reader that reads:
	//   - commitment data as-of the beginning of the block
	//   - account/storage/code data as-of the end of the block
	sd.GetCommitmentCtx().SetStateReader(commitmentdb.NewSplitHistoryReader(tx, fromTxNum, toTxNum /* withHistory */, false))
	if bt.logger.Enabled(ctx, log.LvlTrace) {
		sd.GetCommitmentCtx().SetTraceWriter(os.Stderr)
	} else {
		sd.GetCommitmentCtx().SetTraceWriter(nil)
	}
	latestTxNum, latestBlockNum, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}
	if expected := block - 1; latestBlockNum != expected {
		return fmt.Errorf("unexpected sd block number: %d != %d", latestBlockNum, expected)
	}
	if expected := fromTxNum - 1; latestTxNum != expected {
		return fmt.Errorf("unexpected sd tx number: %d != %d", latestTxNum, expected)
	}
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		domainIdx := idx[d]
		offsets := domainIdx.Offsets(block)
		for _, off := range offsets {
			sd.GetCommitmentCtx().TouchKey(d, domainIdx.Key(off), nil)
		}
		bt.logger.Info("replayed changes", "domain", d, "changes", len(offsets))
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

func deriveBlockOutputDir(runOutputDir string, block uint64) string {
	return path.Join(runOutputDir, fmt.Sprintf("block_%d", block))
}
