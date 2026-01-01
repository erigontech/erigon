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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

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
	"github.com/erigontech/erigon/db/seg"
	dbstate "github.com/erigontech/erigon/db/state"
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

		// Use LatestStateReader to read from the commitment domain.
		// This is the same approach used by commitmentdb.TrieContext.Branch internally:
		// TrieContext.Branch -> TrieContext.readDomain -> StateReader.Read
		commitmentReader := commitmentdb.NewLatestStateReader(tx)

		if err := readBranch(commitmentReader, prefix, logger); err != nil {
			logger.Error("Failed to read branch", "error", err)
			return
		}
	},
}

func readBranch(stateReader *commitmentdb.LatestStateReader, prefix []byte, logger interface {
	Info(msg string, ctx ...interface{})
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

	rwTx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()

	// remove all existing state commitment snapshots
	if err := app.DeleteStateSnapshots(dirs, false, true, false, "0-999999", kv.CommitmentDomain.String()); err != nil {
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

	if _, err := stagedsync.RebuildPatriciaTrieBasedOnFiles(ctx, cfg, squeeze); err != nil {
		return err
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
	items := make([]opts.PieData, 0)
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
	var TreeMap = []opts.TreeMapNode{
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
								GapWidth:    1},
							UpperLabel: &opts.UpperLabel{Show: opts.Bool(true)},
						},
						{ // Level
							ItemStyle: &opts.ItemStyle{
								BorderColor: "#666",
								BorderWidth: 1,
								GapWidth:    1},
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
