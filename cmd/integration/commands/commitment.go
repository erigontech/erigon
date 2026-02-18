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

package commands

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
)

var branchPrefixFlag string

// visualize command flags
var (
	visualizeOutputDir   string
	visualizeConcurrency int
	visualizeTrieVariant string
	visualizeCompression string
	visualizePrintState  bool
	visualizeDepth       int
)

// bench-lookup command flags
var (
	benchSampleSize int
	benchSeed       int64
	benchUseGetAsOf bool
)

// bench-history-lookup command flags
var (
	benchHistoryPrefix    string
	benchHistorySamplePct float64
	benchHistorySeed      int64
)

func init() {
	// commitment branch
	withChain(commitmentBranchCmd)
	withDataDir(commitmentBranchCmd)
	withConfig(commitmentBranchCmd)
	commitmentBranchCmd.Flags().StringVar(&branchPrefixFlag, "prefix", "", "hex prefix to read (e.g., 'aa', '0a1b')")
	commitmentCmd.AddCommand(commitmentBranchCmd)

	// commitment rebuild
	withChain(cmdCommitmentRebuild)
	withDataDir(cmdCommitmentRebuild)
	withConfig(cmdCommitmentRebuild)
	withReset(cmdCommitmentRebuild)
	withSqueeze(cmdCommitmentRebuild)
	withBlock(cmdCommitmentRebuild)
	withConcurrentCommitment(cmdCommitmentRebuild)
	withUnwind(cmdCommitmentRebuild)
	withPruneTo(cmdCommitmentRebuild)
	withIntegrityChecks(cmdCommitmentRebuild)
	withHeimdall(cmdCommitmentRebuild)
	withChaosMonkey(cmdCommitmentRebuild)
	withClearCommitment(cmdCommitmentRebuild)
	commitmentCmd.AddCommand(cmdCommitmentRebuild)

	// commitment print
	withChain(cmdCommitmentPrint)
	withDataDir(cmdCommitmentPrint)
	withConfig(cmdCommitmentPrint)
	commitmentCmd.AddCommand(cmdCommitmentPrint)

	// commitment visualize
	cmdCommitmentVisualize.Flags().StringVar(&visualizeOutputDir, "output", "", "existing directory to store output HTML. By default, same as commitment files")
	cmdCommitmentVisualize.Flags().IntVarP(&visualizeConcurrency, "concurrency", "j", 4, "amount of concurrently processed files")
	cmdCommitmentVisualize.Flags().StringVar(&visualizeTrieVariant, "trie", "hex", "commitment trie variant (values are hex and bin)")
	cmdCommitmentVisualize.Flags().StringVar(&visualizeCompression, "compression", "none", "compression type (none, k, v, kv)")
	cmdCommitmentVisualize.Flags().BoolVar(&visualizePrintState, "state", false, "print state of file")
	cmdCommitmentVisualize.Flags().IntVar(&visualizeDepth, "depth", 0, "depth of the prefixes to analyze")
	commitmentCmd.AddCommand(cmdCommitmentVisualize)

	// commitment bench-lookup
	withChain(cmdCommitmentBenchLookup)
	withDataDir(cmdCommitmentBenchLookup)
	withConfig(cmdCommitmentBenchLookup)
	cmdCommitmentBenchLookup.Flags().IntVar(&benchSampleSize, "sample-size", 10000000, "number of random keys to sample via reservoir sampling")
	cmdCommitmentBenchLookup.Flags().Int64Var(&benchSeed, "seed", 0, "random seed for sampling (0 = use current time)")
	cmdCommitmentBenchLookup.Flags().BoolVar(&benchUseGetAsOf, "use-get-as-of", false, "use GetAsOf(math.MaxUint64) instead of GetLatest() for lookups")
	commitmentCmd.AddCommand(cmdCommitmentBenchLookup)

	// commitment bench-history-lookup
	withChain(cmdCommitmentBenchHistoryLookup)
	withDataDir(cmdCommitmentBenchHistoryLookup)
	withConfig(cmdCommitmentBenchHistoryLookup)
	cmdCommitmentBenchHistoryLookup.Flags().StringVar(&benchHistoryPrefix, "prefix", "", "hex-encoded key prefix to look up in commitment domain (empty = root lookup)")
	cmdCommitmentBenchHistoryLookup.Flags().Float64Var(&benchHistorySamplePct, "sample-percentage", 10.0, "percentage of txnums to sample from each history file's range or from MDBX (0-100)")
	cmdCommitmentBenchHistoryLookup.Flags().Int64Var(&benchHistorySeed, "seed", 0, "random seed for sampling (0 = use current time)")
	commitmentCmd.AddCommand(cmdCommitmentBenchHistoryLookup)

	rootCmd.AddCommand(commitmentCmd)
}

var commitmentCmd = &cobra.Command{
	Use:   "commitment",
	Short: "Commitment domain commands",
}

// integration commitment branch
var commitmentBranchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Read branch data from the commitment domain",
	Long: `Opens the commitment domain from a given datadir and reads the branch data
for the specified prefix. The prefix should be provided as hex nibbles.

Examples:
  integration commitment branch --chain=mainnet --datadir ~/data/eth-mainnet --prefix aa
  integration commitment branch --datadir /path/to/datadir --prefix 0a1b
  integration commitment branch --datadir /path/to/datadir  # reads root (empty prefix)`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		prefix, err := commitment.PrefixStringToNibbles(branchPrefixFlag)
		if err != nil {
			logger.Error("Failed to parse prefix", "error", err)
			return
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		// Start a read-only temporal transaction
		tx, err := chainDb.BeginTemporalRo(ctx)
		if err != nil {
			logger.Error("Failed to begin temporal tx", "error", err)
			return
		}
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(ctx, tx, logger)
		if err != nil {
			logger.Error("Failed to create shared domains", "error", err)
			return
		}
		defer sd.Close()
		// Use LatestStateReader to read from the commitment domain.
		// This is the same approach used by commitmentdb.TrieContext.Branch internally:
		// TrieContext.Branch -> TrieContext.readDomain -> StateReader.Read
		commitmentReader := commitmentdb.NewLatestStateReader(tx, sd)

		if err := readBranch(commitmentReader, prefix, logger); err != nil {
			logger.Error("Failed to read branch", "error", err)
			return
		}
	},
}

func readBranch(stateReader *commitmentdb.LatestStateReader, prefix []byte, logger interface {
	Info(msg string, ctx ...any)
}) error {
	compactKey := commitment.HexNibblesToCompactBytes(prefix)
	val, step, err := stateReader.Read(kv.CommitmentDomain, compactKey, config3.DefaultStepSize)
	if err != nil {
		return fmt.Errorf("failed to get branch for prefix %x: %w", prefix, err)
	}

	fmt.Printf("Prefix: 0x%s\n", hex.EncodeToString(prefix))
	fmt.Printf("Step: %d\n", step)

	if len(val) == 0 {
		fmt.Println("Branch data: <empty>")
		return nil
	}

	fmt.Printf("Branch data (hex): %s\n", hex.EncodeToString(val))
	fmt.Printf("Branch data length: %d bytes\n", len(val))

	// Parse and display the branch data in human-readable format
	branchData := commitment.BranchData(val)
	fmt.Printf("\nParsed branch data:\n%s\n", branchData.String())

	return nil
}

// integration commitment rebuild
var cmdCommitmentRebuild = &cobra.Command{
	Use:   "rebuild",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(dbcfg.ChainDB, chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := commitmentRebuild(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func commitmentRebuild(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if reset {
		return rawdbreset.Reset(ctx, db, stages.Execution)
	}

	br, _ := blocksIO(db, logger)
	cfg := stagedsync.StageTrieCfg(db, true, true, dirs.Tmp, br)

	rwTx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()

	if !clearCommitment {
		domainProgress := rwTx.Debug().DomainProgress(kv.CommitmentDomain)
		ok, err := br.TxnumReader().IsMaxTxNumPopulated(ctx, rwTx, domainProgress)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("max tx num is not populated; run: integration stage_headers --reset --datadir=<dir> --chain=<chain>")
		}
	}

	commitmentHistoryEnabled, _, err := rawdb.ReadDBCommitmentHistoryEnabled(rwTx)
	if err != nil {
		return err
	}

	withHistory := false
	if commitmentHistoryEnabled && clearCommitment {
		withHistory = true
	} else if commitmentHistoryEnabled {
		fmt.Print("commitment history is enabled. Rebuild with history? (y/n): ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		resp := strings.ToLower(strings.TrimSpace(scanner.Text()))
		withHistory = resp == "y" || resp == "yes"
	}

	// remove all existing state commitment snapshots
	// when not rebuilding with history, only delete domain files (preserve existing history/index)
	if err := app.DeleteStateSnapshots(dirs, false, true, false, "0-999999", !withHistory, kv.CommitmentDomain.String()); err != nil {
		return err
	}

	log.Info("Clearing commitment-related DB tables to rebuild on clean data...")
	sconf := statecfg.Schema.CommitmentDomain
	for _, tn := range sconf.Tables() {
		log.Info("Clearing", "table", tn)
		if err := rwTx.ClearTable(tn); err != nil {
			return fmt.Errorf("failed to clear table %s: %w", tn, err)
		}
	}
	if err := rwTx.Commit(); err != nil {
		return err
	}

	if clearCommitment {
		log.Info("Commitment data removed from DB and state files deleted")
		return nil
	}

	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if err = agg.OpenFolder(); err != nil { // reopen after snapshot file deletions
		return fmt.Errorf("failed to re-open aggregator: %w", err)
	}

	blockSnapBuildSema := semaphore.NewWeighted(int64(runtime.NumCPU()))
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetCollateAndBuildWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetMergeWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	agg.PeriodicalyPrintProcessSet(ctx)

	if withHistory {
		if _, err := stagedsync.RebuildPatriciaTrieWithHistory(ctx, cfg, squeeze); err != nil {
			return err
		}
	} else {
		if _, err := stagedsync.RebuildPatriciaTrieBasedOnFiles(ctx, cfg, squeeze); err != nil {
			return err
		}
	}
	return nil
}

// integration commitment print
var cmdCommitmentPrint = &cobra.Command{
	Use:   "print",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(dbcfg.ChainDB, chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := printCommitment(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func printCommitment(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	blockSnapBuildSema := semaphore.NewWeighted(int64(runtime.NumCPU()))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetCollateAndBuildWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetMergeWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	agg.PeriodicalyPrintProcessSet(ctx)

	// disable hard alignment; allowing commitment and storage/account to have
	// different visibleFiles
	agg.DisableAllDependencies()

	acRo := agg.BeginFilesRo() // this tx is used to read existing domain files and closed in the end
	defer acRo.Close()
	defer acRo.MadvNormal().DisableReadAhead()

	commitmentFiles := acRo.Files(kv.CommitmentDomain)
	fmt.Printf("Commitment files: %d\n", len(commitmentFiles))
	for _, f := range commitmentFiles {
		name := filepath.Base(f.Fullpath())
		count := acRo.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())
		rootNodePrefix := []byte("state")
		rootNode, _, _, _, err := acRo.DebugGetLatestFromFiles(kv.CommitmentDomain, rootNodePrefix, f.EndRootNum()-1)
		if err != nil {
			return fmt.Errorf("failed to get root node from files: %w", err)
		}
		rootString, err := commitment.HexTrieStateToShortString(rootNode)
		if err != nil {
			return fmt.Errorf("failed to extract state root from root node: %w", err)
		}
		fmt.Printf("%28s: prefixes %8s %s\n", name, common.PrettyCounter(count), rootString)
	}

	str, err := dbstate.CheckCommitmentForPrint(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to check commitment: %w", err)
	}
	fmt.Printf("\n%s", str)

	return nil
}

// integration commitment visualize
var cmdCommitmentVisualize = &cobra.Command{
	Use:   "visualize [files...]",
	Short: "Visualize commitment .kv files and generate HTML analysis",
	Long: `Analyzes commitment .kv files and generates HTML visualization with charts
showing prefix length distribution, cell counters, medians, and file contents.

Examples:
  integration commitment visualize /path/to/commitment.kv
  integration commitment visualize --output /tmp/analysis /path/to/*.kv
  integration commitment visualize --state /path/to/commitment.kv  # print state only`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		_ = debug.SetupCobra(cmd, "integration")
		visualizeCommitmentFiles(args)
	},
}

// integration commitment bench-lookup
var cmdCommitmentBenchLookup = &cobra.Command{
	Use:   "bench-lookup",
	Short: "Benchmark commitment domain lookup performance",
	Long: `Benchmarks trie node lookup times in the commitment domain.
Uses reservoir sampling to select random keys and measures lookup latencies.

Examples:
  integration commitment bench-lookup --chain=mainnet --datadir ~/data/eth-mainnet
  integration commitment bench-lookup --datadir /path/to/datadir --sample-size 100000
  integration commitment bench-lookup --datadir /path/to/datadir --seed 12345`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		if err := benchLookup(ctx, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

// integration commitment bench-history-lookup
var cmdCommitmentBenchHistoryLookup = &cobra.Command{
	Use:   "bench-history-lookup",
	Short: "Benchmark commitment history lookup performance across different files",
	Long: `Benchmarks GetAsOf() lookup times for a specific key across commitment history files.
Samples txnums from each history file's range and measures lookup latencies per file.

Examples:
  integration commitment bench-history-lookup --chain=mainnet --datadir ~/data/eth-mainnet --prefix aa
  integration commitment bench-history-lookup --datadir /path/to/datadir --prefix 0a1b2c --sample-percentage 5
  integration commitment bench-history-lookup --datadir /path/to/datadir --prefix aa --seed 12345`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		if err := benchHistoryLookup(ctx, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

type BenchStats struct {
	Count      int
	TotalTime  time.Duration
	Min        time.Duration
	Max        time.Duration
	Mean       time.Duration
	Median     time.Duration
	P50        time.Duration
	P90        time.Duration
	P95        time.Duration
	P99        time.Duration
	P999       time.Duration
	StdDev     time.Duration
	Throughput float64 // ops/sec
}

func benchLookup(ctx context.Context, logger log.Logger) error {
	if benchSampleSize <= 0 {
		return fmt.Errorf("sample-size must be positive, got %d", benchSampleSize)
	}

	seed := benchSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	dirs := datadir.New(datadirCli)
	chainDb, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
	if err != nil {
		return fmt.Errorf("opening DB: %w", err)
	}
	defer chainDb.Close()

	agg := chainDb.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	agg.DisableAllDependencies()

	acRo := agg.BeginFilesRo()
	defer acRo.Close()

	// Sample keys from commtiment domain using reservoir sampling
	logger.Info("Sampling keys from commitment domain files...", "sampleSize", benchSampleSize)

	keys, totalCount, err := sampleCommitmentKeysFromFiles(ctx, acRo, benchSampleSize, rng, logger)
	if err != nil {
		return fmt.Errorf("failed to sample keys: %w", err)
	}

	if len(keys) == 0 {
		logger.Warn("No keys found in commitment domain")
		return nil
	}

	logger.Info("Sampled keys",
		"sampledKeys", len(keys),
		"totalKeysInFiles", totalCount,
		"sampleRate", fmt.Sprintf("%.4f%%", float64(len(keys))/float64(totalCount)*100))

	// Benchmark lookups
	logger.Info("Benchmarking lookups...", "keyCount", len(keys))

	// Shuffle keys to randomize access pattern
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	tx, err := chainDb.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin temporal tx: %w", err)
	}
	defer tx.Rollback()

	var commitmentReader commitmentdb.StateReader
	if benchUseGetAsOf {
		logger.Info("Using GetAsOf(math.MaxUint64) for lookups")
		commitmentReader = commitmentdb.NewHistoryStateReader(tx, math.MaxUint64)
	} else {
		logger.Info("Using GetLatest() for lookups")
		sd, err := execctx.NewSharedDomains(ctx, tx, logger)
		if err != nil {
			return fmt.Errorf("failed to create shared domains: %w", err)
		}
		defer sd.Close()
		commitmentReader = commitmentdb.NewLatestStateReader(tx, sd)
	}
	durations := make([]time.Duration, len(keys))
	var totalSize int64

	startTime := time.Now()
	for i, key := range keys {
		lookupStart := time.Now()
		val, _, err := commitmentReader.Read(kv.CommitmentDomain, key, config3.DefaultStepSize)
		durations[i] = time.Since(lookupStart)

		if err != nil {
			logger.Warn("Lookup failed", "key", fmt.Sprintf("%x", key), "error", err)
			continue
		}
		totalSize += int64(len(val))

		// Progress logging
		if (i+1)%10000 == 0 {
			elapsed := time.Since(startTime)
			opsPerSec := float64(i+1) / elapsed.Seconds()
			logger.Info("Progress", "completed", i+1, "total", len(keys), "ops/sec", fmt.Sprintf("%.0f", opsPerSec))
		}
	}
	totalBenchTime := time.Since(startTime)

	// Calculate statistics
	stats := calculateBenchStats(durations)
	stats.Throughput = float64(len(keys)) / totalBenchTime.Seconds()

	// Print results
	printBenchResults("Commitment Domain Lookups", stats, totalSize, len(keys), totalCount)

	return nil
}

type HistoryBenchStats struct {
	Name        string
	StartTxNum  uint64
	EndTxNum    uint64
	SampleCount int
	TotalTime   time.Duration
	Stats       BenchStats
}

func benchHistoryLookup(ctx context.Context, logger log.Logger) error {
	if benchHistorySamplePct <= 0 || benchHistorySamplePct > 100 {
		return fmt.Errorf("--sample-percentage must be between 0 and 100, got %.2f", benchHistorySamplePct)
	}

	// Parse prefix from hex (empty string = root lookup)
	var prefix []byte
	var err error
	if benchHistoryPrefix != "" {
		prefix, err = hex.DecodeString(benchHistoryPrefix)
		if err != nil {
			return fmt.Errorf("invalid hex prefix %q: %w", benchHistoryPrefix, err)
		}
	}

	// Convert hex nibbles to compact bytes for commitment lookup
	compactKey := commitment.HexNibblesToCompactBytes(prefix)

	seed := benchHistorySeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	dirs := datadir.New(datadirCli)
	chainDb, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
	if err != nil {
		return fmt.Errorf("opening DB: %w", err)
	}
	defer chainDb.Close()

	agg := chainDb.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	agg.DisableAllDependencies()

	acRo := agg.BeginFilesRo()
	defer acRo.Close()

	// Get commitment files and filter for history .v files
	commitmentFiles := acRo.Files(kv.CommitmentDomain)

	var historyFiles []dbstate.VisibleFile
	for _, f := range commitmentFiles {
		if strings.HasSuffix(f.Fullpath(), ".v") {
			historyFiles = append(historyFiles, f)
		}
	}

	// Sort by StartRootNum for consistent ordering
	if len(historyFiles) > 0 {
		slices.SortFunc(historyFiles, func(a, b dbstate.VisibleFile) int {
			if a.StartRootNum() < b.StartRootNum() {
				return -1
			}
			if a.StartRootNum() > b.StartRootNum() {
				return 1
			}
			return 0
		})
	}

	// Find the max txnum covered by history files
	var maxFileTxNum uint64
	for _, f := range historyFiles {
		if f.EndRootNum() > maxFileTxNum {
			maxFileTxNum = f.EndRootNum()
		}
	}

	logger.Info("Found commitment history files",
		"historyFiles", len(historyFiles),
		"totalFiles", len(commitmentFiles),
		"maxFileTxNum", maxFileTxNum,
		"prefix", benchHistoryPrefix,
		"samplePercentage", benchHistorySamplePct)

	tx, err := chainDb.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin temporal tx: %w", err)
	}
	defer tx.Rollback()

	// Benchmark lookup in snapshot history files
	allFileStats, err := benchSnapshotsHistoryLookup(ctx, tx, historyFiles, compactKey, benchHistorySamplePct, rng, logger)
	if err != nil {
		return err
	}

	// Sample and benchmark keys from MDBX
	mdbxStats, err := benchMdbxHistoryLookup(ctx, tx, compactKey, maxFileTxNum, benchHistorySamplePct, rng, logger)
	if err != nil {
		logger.Warn("Failed to benchmark MDBX history lookups", "error", err)
	}
	allStats := allFileStats
	if mdbxStats != nil {
		allStats = append(allStats, *mdbxStats)
	}

	// Print results table
	printHistoryBenchResultsTable(prefix, compactKey, allStats)

	return nil
}

// benchSnapshotsHistoryLookup benchmarks history lookups across snapshot files
func benchSnapshotsHistoryLookup(ctx context.Context, tx kv.TemporalTx, historyFiles []dbstate.VisibleFile, compactKey []byte, samplePct float64, rng *rand.Rand, logger log.Logger) ([]HistoryBenchStats, error) {
	allFileStats := make([]HistoryBenchStats, 0, len(historyFiles))

	for _, f := range historyFiles {
		fpath := f.Fullpath()
		fname := filepath.Base(fpath)
		startTxNum := f.StartRootNum()
		endTxNum := f.EndRootNum()
		txnumRange := endTxNum - startTxNum

		if txnumRange == 0 {
			logger.Warn("Skipping file with empty range", "file", fname)
			continue
		}

		// Calculate number of samples based on percentage
		sampleCount := int(float64(txnumRange) * samplePct / 100.0)

		logger.Info("Benchmarking file...",
			"file", fname,
			"startTxNum", startTxNum,
			"endTxNum", endTxNum,
			"range", txnumRange,
			"sampleCount", sampleCount)

		// Generate random sample of txnums within this file's range
		sampledTxNums := make([]uint64, sampleCount)
		for i := 0; i < sampleCount; i++ {
			// Generate random txnum in [startTxNum, endTxNum)
			sampledTxNums[i] = startTxNum + uint64(rng.Int63n(int64(txnumRange)))
		}

		// Shuffle for random access pattern
		rng.Shuffle(len(sampledTxNums), func(i, j int) {
			sampledTxNums[i], sampledTxNums[j] = sampledTxNums[j], sampledTxNums[i]
		})

		// Benchmark lookups for this file using HistoryStateReader
		durations := make([]time.Duration, 0, sampleCount)

		startTime := time.Now()
		for _, txNum := range sampledTxNums {
			// Create a HistoryStateReader for this txNum
			reader := commitmentdb.NewHistoryStateReader(tx, txNum)

			lookupStart := time.Now()
			_, _, err := reader.Read(kv.CommitmentDomain, compactKey, config3.DefaultStepSize)
			elapsed := time.Since(lookupStart)

			if err != nil {
				logger.Warn("Lookup failed", "txNum", txNum, "error", err)
				continue
			}

			durations = append(durations, elapsed)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		totalTime := time.Since(startTime)

		// Calculate statistics for this file
		stats := calculateBenchStats(durations)
		if len(durations) > 0 {
			stats.Throughput = float64(len(durations)) / totalTime.Seconds()
		}

		fileStats := HistoryBenchStats{
			Name:        fname,
			StartTxNum:  startTxNum,
			EndTxNum:    endTxNum,
			SampleCount: len(durations),
			TotalTime:   totalTime,
			Stats:       stats,
		}
		allFileStats = append(allFileStats, fileStats)

		logger.Info("File complete",
			"file", fname,
			"samples", len(durations),
			"throughput", fmt.Sprintf("%.0f ops/sec", stats.Throughput),
			"p50", stats.P50,
			"p99", stats.P99)
	}

	return allFileStats, nil
}

// benchMdbxHistoryLookup benchmarks history lookups for txnums in the MDBX range
func benchMdbxHistoryLookup(ctx context.Context, tx kv.TemporalTx, compactKey []byte, minTxNum uint64, samplePct float64, rng *rand.Rand, logger log.Logger) (*HistoryBenchStats, error) {
	// Get the max txnum in MDBX via DomainProgress API
	aggTx := dbstate.AggTx(tx)
	maxTxNum := aggTx.DomainProgress(kv.CommitmentDomain, tx)

	logger.Info("Finding MDBX txnum range...",
		"minTxNum", minTxNum,
		"maxTxNum", maxTxNum)

	txnumRange := maxTxNum - minTxNum
	if txnumRange <= 0 {
		logger.Info("No MDBX history entries found for txnums > minTxNum",
			"minTxNum", minTxNum,
			"maxTxNum", maxTxNum)
		return nil, nil
	}

	// Calculate number of samples based on sampling percentage
	sampleCount := int(float64(txnumRange) * samplePct / 100.0)

	logger.Info("Benchmarking MDBX history lookups...",
		"minTxNum", minTxNum,
		"maxTxNum", maxTxNum,
		"range", txnumRange,
		"sampleCount", sampleCount)

	// Generate random txnums in [minTxNum, maxTxNum)
	sampledTxNums := make([]uint64, sampleCount)
	for i := 0; i < sampleCount; i++ {
		sampledTxNums[i] = minTxNum + uint64(rng.Int63n(int64(txnumRange)))
	}

	// Shuffle for random access pattern
	rng.Shuffle(len(sampledTxNums), func(i, j int) {
		sampledTxNums[i], sampledTxNums[j] = sampledTxNums[j], sampledTxNums[i]
	})

	// Benchmark lookups using HistoryStateReader - same API as file benchmarks
	// The reader has logic to determine whether to look in snapshots or MDBX
	durations := make([]time.Duration, 0, sampleCount)

	benchStart := time.Now()
	for _, txNum := range sampledTxNums {
		// Create a HistoryStateReader for this txNum
		reader := commitmentdb.NewHistoryStateReader(tx, txNum)

		lookupStart := time.Now()
		_, _, err := reader.Read(kv.CommitmentDomain, compactKey, config3.DefaultStepSize)
		elapsed := time.Since(lookupStart)

		if err != nil {
			logger.Warn("MDBX lookup failed", "txNum", txNum, "error", err)
			continue
		}

		durations = append(durations, elapsed)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	totalTime := time.Since(benchStart)

	// Calculate statistics
	stats := calculateBenchStats(durations)
	if len(durations) > 0 {
		stats.Throughput = float64(len(durations)) / totalTime.Seconds()
	}

	mdbxStats := &HistoryBenchStats{
		Name:        "MDBX",
		StartTxNum:  minTxNum,
		EndTxNum:    maxTxNum,
		SampleCount: len(durations),
		TotalTime:   totalTime,
		Stats:       stats,
	}

	logger.Info("MDBX benchmark complete",
		"samples", len(durations),
		"throughput", fmt.Sprintf("%.0f ops/sec", stats.Throughput),
		"p50", stats.P50,
		"p99", stats.P99)

	return mdbxStats, nil
}

func printHistoryBenchResultsTable(prefix []byte, compactKey []byte, fileStats []HistoryBenchStats) {
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Printf("  HISTORY LOOKUP BENCHMARK RESULTS\n")
	if len(prefix) == 0 {
		fmt.Printf("  Prefix: (empty - root lookup)\n")
	} else {
		fmt.Printf("  Prefix: %x\n", prefix)
	}
	fmt.Printf("  Compact Key: %x\n", compactKey)
	fmt.Println("================================================================================")
	fmt.Println()

	// Print header
	fmt.Printf("%-45s %12s %12s %8s %10s %10s %10s %10s\n",
		"File", "StartTxNum", "EndTxNum", "Samples", "Mean", "P50", "P95", "P99")
	fmt.Println(strings.Repeat("-", 127))

	// Print each file's stats
	var totalSamples int
	var totalDuration time.Duration

	for _, fs := range fileStats {
		fmt.Printf("%-45s %12d %12d %8d %10v %10v %10v %10v\n",
			fs.Name,
			fs.StartTxNum,
			fs.EndTxNum,
			fs.SampleCount,
			fs.Stats.Mean,
			fs.Stats.P50,
			fs.Stats.P95,
			fs.Stats.P99)

		totalSamples += fs.SampleCount
		totalDuration += fs.TotalTime
	}

	fmt.Println(strings.Repeat("-", 127))

	fmt.Println()
	fmt.Printf("  Total Files:    %d\n", len(fileStats))
	fmt.Printf("  Total Samples:  %d\n", totalSamples)
	fmt.Printf("  Total Duration: %v\n", totalDuration)
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println()
}

// sampleCommitmentKeysFromFiles samples keys from all commitment domain .kv files using reservoir sampling.
// This iterates over ALL key-value pairs in ALL .kv files
func sampleCommitmentKeysFromFiles(ctx context.Context, acRo *dbstate.AggregatorRoTx, sampleSize int, rng *rand.Rand, logger log.Logger) ([][]byte, int, error) {
	keys := make([][]byte, 0, sampleSize)
	globalCount := 0
	lastLog := time.Now()

	commitmentFiles := acRo.Files(kv.CommitmentDomain)

	// consider .kv files only
	var commitmentKVFiles []dbstate.VisibleFile
	for _, f := range commitmentFiles {
		if strings.HasSuffix(f.Fullpath(), ".kv") {
			commitmentKVFiles = append(commitmentKVFiles, f)
		}
	}
	logger.Info("Found commitment .kv files", "kvFiles", len(commitmentKVFiles), "totalFiles", len(commitmentFiles))

	for fileIdx, f := range commitmentKVFiles {
		fpath := f.Fullpath()
		logger.Info("Scanning file...", "file", filepath.Base(fpath), "fileIdx", fileIdx+1, "totalFiles", len(commitmentKVFiles))

		dec, err := seg.NewDecompressor(fpath)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to create decompressor for %s: %w", fpath, err)
		}
		defer dec.Close()

		fc := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
		getter := seg.NewReader(dec.MakeGetter(), fc)

		fileKeyCount := 0
		for getter.HasNext() {
			key, _ := getter.Next(nil)
			if !getter.HasNext() {
				return nil, 0, fmt.Errorf("invalid key/value pair in %s", fpath)
			}
			getter.Skip() // skip value

			// Skip the "state" key
			if bytes.Equal(key, []byte("state")) {
				continue
			}

			globalCount++
			fileKeyCount++

			if len(keys) < sampleSize {
				// Reservoir not full yet - always add
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				keys = append(keys, keyCopy)
			} else {
				// Reservoir full - replace with probability sampleSize/globalCount
				j := rng.Intn(globalCount)
				if j < sampleSize {
					if len(keys[j]) != len(key) {
						keys[j] = make([]byte, len(key))
					}
					copy(keys[j], key)
				}
			}

			if time.Since(lastLog) > 10*time.Second {
				logger.Info("Sampling...",
					"file", filepath.Base(fpath),
					"fileKeys", fileKeyCount,
					"globalScanned", globalCount,
					"reservoir", len(keys))
				lastLog = time.Now()
			}

			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			default:
			}
		}
		logger.Info("File complete", "file", filepath.Base(fpath), "keysInFile", fileKeyCount)
	}

	return keys, globalCount, nil
}

func calculateBenchStats(durations []time.Duration) BenchStats {
	if len(durations) == 0 {
		return BenchStats{}
	}

	// Sort for percentile calculations
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	slices.Sort(sorted)

	// Calculate basic stats
	var total time.Duration
	minD := sorted[0]
	maxD := sorted[len(sorted)-1]

	for _, d := range sorted {
		total += d
	}

	mean := total / time.Duration(len(sorted))

	// Calculate standard deviation
	var sumSquaredDiff float64
	meanFloat := float64(mean)
	for _, d := range sorted {
		diff := float64(d) - meanFloat
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(len(sorted))
	stdDev := time.Duration(math.Sqrt(variance))

	return BenchStats{
		Count:     len(durations),
		TotalTime: total,
		Min:       minD,
		Max:       maxD,
		Mean:      mean,
		Median:    benchPercentile(sorted, 0.50),
		P50:       benchPercentile(sorted, 0.50),
		P90:       benchPercentile(sorted, 0.90),
		P95:       benchPercentile(sorted, 0.95),
		P99:       benchPercentile(sorted, 0.99),
		P999:      benchPercentile(sorted, 0.999),
		StdDev:    stdDev,
	}
}

func benchPercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printBenchResults(name string, stats BenchStats, totalSize int64, keyCount int, totalKeysInFiles int) {
	avgValueSize := float64(totalSize) / float64(keyCount)

	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Printf("  BENCHMARK RESULTS: %s\n", name)
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Printf("  Total Keys in Files: %d\n", totalKeysInFiles)
	fmt.Printf("  Sampled Keys:        %d\n", keyCount)
	fmt.Printf("  Sample Rate:         %.4f%%\n", float64(keyCount)/float64(totalKeysInFiles)*100)
	fmt.Println()
	fmt.Printf("  Total Lookups:       %d\n", stats.Count)
	fmt.Printf("  Total Time:          %v\n", stats.TotalTime)
	fmt.Printf("  Throughput:          %.2f ops/sec\n", stats.Throughput)
	fmt.Printf("  Avg Value Size:      %.2f bytes\n", avgValueSize)
	fmt.Println()
	fmt.Println("  Latency Statistics:")
	fmt.Println("  -------------------")
	fmt.Printf("  Min:                 %v\n", stats.Min)
	fmt.Printf("  Max:                 %v\n", stats.Max)
	fmt.Printf("  Mean:                %v\n", stats.Mean)
	fmt.Printf("  Std Dev:             %v\n", stats.StdDev)
	fmt.Println()
	fmt.Println("  Percentiles:")
	fmt.Println("  ------------")
	fmt.Printf("  P50 (Median):        %v\n", stats.P50)
	fmt.Printf("  P90:                 %v\n", stats.P90)
	fmt.Printf("  P95:                 %v\n", stats.P95)
	fmt.Printf("  P99:                 %v\n", stats.P99)
	fmt.Printf("  P99.9:               %v\n", stats.P999)
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println()
}

func visualizeCommitmentFiles(files []string) {
	sema := make(chan struct{}, visualizeConcurrency)
	for i := 0; i < cap(sema); i++ {
		sema <- struct{}{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	page := components.NewPage()
	page.SetLayout(components.PageFlexLayout)
	page.PageTitle = "Commitment Analysis"

	for i, fp := range files {
		fpath, pos := fp, i
		_ = pos
		<-sema

		fmt.Printf("[%d/%d] - %s..", pos+1, len(files), path.Base(fpath))

		wg.Add(1)
		go func(wg *sync.WaitGroup, mu *sync.Mutex) {
			defer wg.Done()
			defer func() { sema <- struct{}{} }()

			stat, err := processCommitmentFile(fpath)
			if err != nil {
				fmt.Printf("processing failed: %v", err)
				return
			}

			mu.Lock()
			page.AddCharts(
				prefixLenCountChart(fpath, stat),
				countersChart(fpath, stat),
				mediansChart(fpath, stat),
				fileContentsMapChart(fpath, stat),
			)
			mu.Unlock()
		}(&wg, &mu)
	}
	wg.Wait()
	fmt.Println()
	if visualizePrintState {
		return
	}

	dir := filepath.Dir(files[0])
	if visualizeOutputDir != "" {
		dir = visualizeOutputDir
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	outPath := path.Join(dir, "analysis.html")
	fmt.Printf("rendering total graph to %s\n", outPath)

	f, err := os.Create(outPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	defer f.Sync()

	if err := page.Render(io.MultiWriter(f)); err != nil {
		panic(err)
	}
}

type visualizeOverallStat struct {
	branches   *commitment.BranchStat
	roots      *commitment.BranchStat
	prefixes   map[uint64]*commitment.BranchStat
	prefCount  map[uint64]uint64
	rootsCount uint64
}

func newVisualizeOverallStat() *visualizeOverallStat {
	return &visualizeOverallStat{
		branches:  new(commitment.BranchStat),
		roots:     new(commitment.BranchStat),
		prefixes:  make(map[uint64]*commitment.BranchStat),
		prefCount: make(map[uint64]uint64),
	}
}

func extractKVPairFromCompressed(filename string, keysSink chan commitment.BranchStat) error {
	defer close(keysSink)
	dec, err := seg.NewDecompressor(filename)
	if err != nil {
		return fmt.Errorf("failed to create decompressor: %w", err)
	}
	defer dec.Close()
	tv := commitment.ParseTrieVariant(visualizeTrieVariant)

	fc, err := seg.ParseFileCompression(visualizeCompression)
	if err != nil {
		return err
	}
	size := dec.Size()
	pairs := dec.Count() / 2
	cpair := 0
	depth := visualizeDepth
	var afterValPos uint64
	var key, val []byte
	getter := seg.NewReader(dec.MakeGetter(), fc)
	for getter.HasNext() {
		key, _ = getter.Next(key[:0])
		if !getter.HasNext() {
			return errors.New("invalid key/value pair during decompression")
		}
		if visualizePrintState && !bytes.Equal(key, []byte("state")) {
			getter.Skip()
			continue
		}

		val, afterValPos = getter.Next(val[:0])
		cpair++
		if bytes.Equal(key, []byte("state")) {
			str, err := commitment.HexTrieStateToString(val)
			if err != nil {
				fmt.Printf("[ERR] failed to decode state: %v", err)
			}
			fmt.Printf("\n%s: %s\n", dec.FileName(), str)
			continue
		}

		if cpair%100000 == 0 {
			fmt.Printf("\r%s pair %d/%d %s/%s", filename, cpair, pairs,
				datasize.ByteSize(afterValPos).HumanReadable(), datasize.ByteSize(size).HumanReadable())
		}

		if depth > len(key) {
			continue
		}
		stat := commitment.DecodeBranchAndCollectStat(key, val, tv)
		if stat == nil {
			err := fmt.Errorf("failed to decode branch: %x %x", key, val)
			panic(err)
		}
		keysSink <- *stat
	}
	return nil
}

func processCommitmentFile(fpath string) (*visualizeOverallStat, error) {
	stats := make(chan commitment.BranchStat, 8)
	errch := make(chan error)
	go func() {
		err := extractKVPairFromCompressed(fpath, stats)
		if err != nil {
			errch <- err
		}
		close(errch)
	}()

	totals := newVisualizeOverallStat()
	for s := range stats {
		if s.IsRoot {
			totals.rootsCount++
			totals.roots.Collect(&s)
		} else {
			totals.branches.Collect(&s)
		}
		totals.prefCount[s.KeySize]++

		ps, ok := totals.prefixes[s.KeySize]
		if !ok {
			ps = new(commitment.BranchStat)
		}
		ps.Collect(&s)
		totals.prefixes[s.KeySize] = ps
	}

	select {
	case err := <-errch:
		if err != nil {
			return nil, err
		}
	default:
	}
	return totals, nil
}

func prefixLenCountChart(fname string, data *visualizeOverallStat) *charts.Pie {
	items := make([]opts.PieData, 0, len(data.prefCount))
	for prefSize, count := range data.prefCount {
		items = append(items, opts.PieData{Name: strconv.FormatUint(prefSize, 10), Value: count})
	}

	pie := charts.NewPie()
	pie.SetGlobalOptions(
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
		charts.WithTitleOpts(opts.Title{Subtitle: filepath.Base(fname), Title: "key prefix length distribution (bytes)", Top: "25"}),
	)

	pie.AddSeries("prefixLen/count", items)
	return pie
}

func fileContentsMapChart(fileName string, data *visualizeOverallStat) *charts.TreeMap {
	TreeMap := []opts.TreeMapNode{
		{Name: "prefixes"},
		{Name: "values"},
	}

	keysIndex := 0
	TreeMap[keysIndex].Children = make([]opts.TreeMapNode, 0)
	for prefSize, stat := range data.prefixes {
		TreeMap[keysIndex].Children = append(TreeMap[keysIndex].Children, opts.TreeMapNode{
			Name:  strconv.FormatUint(prefSize, 10),
			Value: int(stat.KeySize),
		})
	}

	valsIndex := 1
	TreeMap[valsIndex].Children = []opts.TreeMapNode{
		{
			Name:  "hashes",
			Value: int(data.branches.HashSize),
		},
		{
			Name:  "extensions",
			Value: int(data.branches.ExtSize),
		},
		{
			Name:  "accountKey",
			Value: int(data.branches.APKSize),
		},
		{
			Name:  "storageKey",
			Value: int(data.branches.SPKSize),
		},
		{
			Name:  "leafHashes",
			Value: int(data.branches.LeafHashSize),
		},
	}

	graph := charts.NewTreeMap()
	graph.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeMacarons}),
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:      opts.Bool(true),
			Formatter: opts.FuncOpts(visualizeToolTipFormatter),
		}),
	)

	// Add initialized data to graph.
	graph.AddSeries(filepath.Base(fileName), TreeMap).
		SetSeriesOptions(
			charts.WithTreeMapOpts(
				opts.TreeMapChart{
					UpperLabel: &opts.UpperLabel{Show: opts.Bool(true), Color: "#fff"},
					Levels: &[]opts.TreeMapLevel{
						{ // Series
							ItemStyle: &opts.ItemStyle{
								BorderColor: "#777",
								BorderWidth: 1,
								GapWidth:    1,
							},
							UpperLabel: &opts.UpperLabel{Show: opts.Bool(true)},
						},
						{ // Level
							ItemStyle: &opts.ItemStyle{
								BorderColor: "#666",
								BorderWidth: 1,
								GapWidth:    1,
							},
							Emphasis: &opts.Emphasis{
								ItemStyle: &opts.ItemStyle{BorderColor: "#555"},
							},
						},
						{ // Node
							ColorSaturation: []float32{0.35, 0.5},
							ItemStyle: &opts.ItemStyle{
								GapWidth:              1,
								BorderWidth:           0,
								BorderColorSaturation: 0.6,
							},
						},
					},
				},
			),
			charts.WithItemStyleOpts(opts.ItemStyle{BorderColor: "#fff"}),
			charts.WithLabelOpts(opts.Label{Show: opts.Bool(true), Position: "inside", Color: "White"}),
		)
	return graph
}

var visualizeToolTipFormatter = `
function (info) {
    var bytes = Number(info.value);
    const KB = 1024;
    const MB = 1024 * KB;
    const GB = 1024 * MB;

    let result;
    if (bytes >= GB) {
        result = (bytes / GB).toFixed(2) + ' GB';
    } else if (bytes >= MB) {
        result = (bytes / MB).toFixed(2) + ' MB';
    } else if (bytes >= KB) {
        result = (bytes / KB).toFixed(2) + ' KB';
    } else {
        result = bytes + ' bytes';
    }

    var formatUtil = echarts.format;
    var treePathInfo = info.treePathInfo;
    var treePath = [];
    for (var i = 1; i < treePathInfo.length; i++) {
        treePath.push(treePathInfo[i].name);
    }

    return [
        '<div class="tooltip-title" style="color: white;">' + formatUtil.encodeHTML(treePath.join('/')) + '</div>',
				'<span style="color: white;">Disk Usage: ' + result + '</span>',
    ].join('');
}
`

func countersChart(fname string, data *visualizeOverallStat) *charts.Sankey {
	sankey := charts.NewSankey()
	sankey.SetGlobalOptions(
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
	)

	nodes := []opts.SankeyNode{
		{Name: "Cells"},
		{Name: "APK"},
		{Name: "SPK"},
		{Name: "Hashes"},
		{Name: "Extensions"},
		{Name: "LeafHashes"},
	}
	sankeyLink := []opts.SankeyLink{
		{Source: nodes[0].Name, Target: nodes[1].Name, Value: float32(data.branches.APKCount)},
		{Source: nodes[0].Name, Target: nodes[2].Name, Value: float32(data.branches.SPKCount)},
		{Source: nodes[0].Name, Target: nodes[3].Name, Value: float32(data.branches.HashCount)},
		{Source: nodes[0].Name, Target: nodes[4].Name, Value: float32(data.branches.ExtCount)},
		{Source: nodes[0].Name, Target: nodes[5].Name, Value: float32(data.branches.LeafHashCount)},
	}

	sankey.AddSeries("Counts "+filepath.Base(fname), nodes, sankeyLink).
		SetSeriesOptions(
			charts.WithLineStyleOpts(opts.LineStyle{
				Color:     "source",
				Curveness: 0.5,
			}),
			charts.WithLabelOpts(opts.Label{
				Show: opts.Bool(true),
			}),
		)
	return sankey
}

func mediansChart(fname string, data *visualizeOverallStat) *charts.Sankey {
	sankey := charts.NewSankey()
	sankey.SetGlobalOptions(
		charts.WithLegendOpts(opts.Legend{Show: opts.Bool(true)}),
		charts.WithTooltipOpts(opts.Tooltip{Show: opts.Bool(true)}),
	)

	nodes := []opts.SankeyNode{
		{Name: "Cells"},
		{Name: "Addr"},
		{Name: "Addr+Storage"},
		{Name: "Hashes"},
		{Name: "Extensions"},
		{Name: "LeafHashes"},
	}
	sankeyLink := []opts.SankeyLink{
		{Source: nodes[0].Name, Target: nodes[1].Name, Value: float32(data.branches.MedianAPK)},
		{Source: nodes[0].Name, Target: nodes[2].Name, Value: float32(data.branches.MedianSPK)},
		{Source: nodes[0].Name, Target: nodes[3].Name, Value: float32(data.branches.MedianHash)},
		{Source: nodes[0].Name, Target: nodes[4].Name, Value: float32(data.branches.MedianExt)},
		{Source: nodes[0].Name, Target: nodes[5].Name, Value: float32(data.branches.MedianLH)},
	}

	sankey.AddSeries("Medians "+filepath.Base(fname), nodes, sankeyLink).
		SetSeriesOptions(
			charts.WithLineStyleOpts(opts.LineStyle{
				Color:     "source",
				Curveness: 0.5,
			}),
			charts.WithLabelOpts(opts.Label{
				Show: opts.Bool(true),
			}),
		)
	return sankey
}
