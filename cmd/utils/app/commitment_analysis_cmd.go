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

package app

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
)

func doCommitmentAnalysis(cliCtx *cli.Context) error {
	logger := log.Root()
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	ctx := cliCtx.Context

	outputDir := cliCtx.String("output")
	concurrency := cliCtx.Int("concurrency")
	trieVariant := cliCtx.String("trie")
	compression := cliCtx.String("compression")
	printState := cliCtx.Bool("state")
	depth := cliCtx.Int("depth")

	if concurrency < 1 {
		concurrency = 4
	}

	// Open only the aggregator — lighter than openSnaps since we only need commitment domain .kv files.
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	agg := openAgg(ctx, dirs, chainDB, logger)
	defer agg.Close()

	// Collect commitment domain .kv file paths through the aggregator.
	ac := agg.BeginFilesRo()
	files := ac.Files(kv.CommitmentDomain)
	ac.Close()

	var kvFiles []string
	for _, f := range files {
		if strings.HasSuffix(f.Fullpath(), ".kv") {
			kvFiles = append(kvFiles, f.Fullpath())
		}
	}

	if len(kvFiles) == 0 {
		return errors.New("no commitment domain .kv files found in datadir")
	}

	logger.Info("commitment-analysis", "files", len(kvFiles))

	tv := commitment.ParseTrieVariant(trieVariant)
	fc, err := seg.ParseFileCompression(compression)
	if err != nil {
		return fmt.Errorf("parse compression: %w", err)
	}

	// Process files concurrently.
	sema := make(chan struct{}, concurrency)
	for i := 0; i < cap(sema); i++ {
		sema <- struct{}{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	page := components.NewPage()
	page.SetLayout(components.PageFlexLayout)
	page.PageTitle = "Commitment Analysis"

	for i, fp := range kvFiles {
		fpath, pos := fp, i
		<-sema

		fmt.Printf("[%d/%d] - %s..", pos+1, len(kvFiles), filepath.Base(fpath))

		wg.Add(1)
		go func(wg *sync.WaitGroup, mu *sync.Mutex) {
			defer wg.Done()
			defer func() { sema <- struct{}{} }()

			stat, err := processCommitmentKVFile(fpath, tv, fc, printState, depth)
			if err != nil {
				fmt.Printf("processing failed: %v", err)
				return
			}

			mu.Lock()
			page.AddCharts(
				commitmentPrefixLenCountChart(fpath, stat),
				commitmentCountersChart(fpath, stat),
				commitmentMediansChart(fpath, stat),
				commitmentFileContentsMapChart(fpath, stat),
			)
			mu.Unlock()
		}(&wg, &mu)
	}
	wg.Wait()
	fmt.Println()

	if printState {
		return nil
	}

	// Determine output directory.
	dir := filepath.Dir(kvFiles[0])
	if outputDir != "" {
		dir = outputDir
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
	}
	outPath := filepath.Join(dir, "analysis.html")
	fmt.Printf("rendering total graph to %s\n", outPath)

	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()
	defer f.Sync()

	if err := page.Render(io.MultiWriter(f)); err != nil {
		return fmt.Errorf("render page: %w", err)
	}

	return nil
}

type commitmentOverallStat struct {
	branches   *commitment.BranchStat
	roots      *commitment.BranchStat
	prefixes   map[uint64]*commitment.BranchStat
	prefCount  map[uint64]uint64
	rootsCount uint64
}

func newCommitmentOverallStat() *commitmentOverallStat {
	return &commitmentOverallStat{
		branches:  new(commitment.BranchStat),
		roots:     new(commitment.BranchStat),
		prefixes:  make(map[uint64]*commitment.BranchStat),
		prefCount: make(map[uint64]uint64),
	}
}

func extractCommitmentKVPairs(filename string, keysSink chan commitment.BranchStat, tv commitment.TrieVariant, fc seg.FileCompression, printState bool, depth int) error {
	defer close(keysSink)
	dec, err := seg.NewDecompressor(filename)
	if err != nil {
		return fmt.Errorf("failed to create decompressor: %w", err)
	}
	defer dec.Close()

	size := dec.Size()
	pairs := dec.Count() / 2
	cpair := 0
	var afterValPos uint64
	var key, val []byte
	getter := seg.NewReader(dec.MakeGetter(), fc)
	for getter.HasNext() {
		key, _ = getter.Next(key[:0])
		if !getter.HasNext() {
			return errors.New("invalid key/value pair during decompression")
		}
		if printState && !bytes.Equal(key, []byte("state")) {
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
			return fmt.Errorf("failed to decode branch: %x %x", key, val)
		}
		keysSink <- *stat
	}
	return nil
}

func processCommitmentKVFile(fpath string, tv commitment.TrieVariant, fc seg.FileCompression, printState bool, depth int) (*commitmentOverallStat, error) {
	stats := make(chan commitment.BranchStat, 8)
	errch := make(chan error)
	go func() {
		err := extractCommitmentKVPairs(fpath, stats, tv, fc, printState, depth)
		if err != nil {
			errch <- err
		}
		close(errch)
	}()

	totals := newCommitmentOverallStat()
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

func commitmentPrefixLenCountChart(fname string, data *commitmentOverallStat) *charts.Pie {
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

func commitmentFileContentsMapChart(fileName string, data *commitmentOverallStat) *charts.TreeMap {
	var treeMap = []opts.TreeMapNode{
		{Name: "prefixes"},
		{Name: "values"},
	}

	keysIndex := 0
	treeMap[keysIndex].Children = make([]opts.TreeMapNode, 0)
	for prefSize, stat := range data.prefixes {
		treeMap[keysIndex].Children = append(treeMap[keysIndex].Children, opts.TreeMapNode{
			Name:  strconv.FormatUint(prefSize, 10),
			Value: int(stat.KeySize),
		})
	}

	valsIndex := 1
	treeMap[valsIndex].Children = []opts.TreeMapNode{
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
			Formatter: opts.FuncOpts(commitmentToolTipFormatter),
		}),
	)

	graph.AddSeries(filepath.Base(fileName), treeMap).
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

var commitmentToolTipFormatter = `
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

func commitmentCountersChart(fname string, data *commitmentOverallStat) *charts.Sankey {
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

func commitmentMediansChart(fname string, data *commitmentOverallStat) *charts.Sankey {
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
