package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/state"
)

var (
	flagOutputDirectory = flag.String("output", "", "existing directory to store output images. By default, same as commitment files")
	flagConcurrency     = flag.Int("j", 4, "amount of concurrently proceeded files")
	flagTrieVariant     = flag.String("trie", "hex", "commitment trie variant (values are hex and bin)")
	flagCompression     = flag.String("compression", "none", "compression type (none, k, v, kv)")
)

func main() {
	flag.Parse()
	if len(os.Args) == 1 {
		fmt.Printf("no .kv file path provided")
		return
	}
	files := flag.Args()

	sema := make(chan struct{}, *flagConcurrency)
	for i := 0; i < cap(sema); i++ {
		sema <- struct{}{}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	total := NewOverallStat()

	for i, fp := range files {
		fpath, pos := fp, i
		<-sema

		fmt.Printf("\r[%d/%d] - %s..", pos+1, len(files), path.Base(fpath))

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
			//total.Collect(&stat)
			total = stat
			mu.Unlock()
		}(&wg, &mu)
	}
	wg.Wait()
	fmt.Println()

	dir := filepath.Dir(files[0])
	if *flagOutputDirectory != "" {
		dir = *flagOutputDirectory
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	outPath := path.Join(dir, "analysis.html")
	fmt.Printf("rendering total graph to %s\n", outPath)

	page := components.NewPage()
	page.AddCharts(
		renderKeyPrefixBar(total),
		treeMapBase(files[0], total),
	)

	f, err := os.Create(outPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	page.Render(io.MultiWriter(f))
}

func extractKVPairFromCompressed(filename string, keysSink chan commitment.BranchStat) error {
	defer close(keysSink)
	dec, err := compress.NewDecompressor(filename)
	if err != nil {
		return fmt.Errorf("failed to create decompressor: %w", err)
	}
	defer dec.Close()
	tv := commitment.ParseTrieVariant(*flagTrieVariant)

	fc, err := state.ParseFileCompression(*flagCompression)
	if err != nil {
		return err
	}

	getter := state.NewArchiveGetter(dec.MakeGetter(), fc)
	for getter.HasNext() {
		key, _ := getter.Next(nil)
		if !getter.HasNext() {
			return fmt.Errorf("invalid key/value pair during decompression")
		}
		val, _ := getter.Next(nil)

		stat := commitment.DecodeBranchAndCollectStat(key, val, tv)
		if stat == nil {
			fmt.Printf("failed to decode branch: %x %x\n", key, val)
		}
		keysSink <- *stat
	}
	return nil
}

type overallStat struct {
	branches   commitment.BranchStat
	roots      commitment.BranchStat
	prefixes   map[uint64]commitment.BranchStat
	prefCount  map[uint64]uint64
	rootsCount uint64
}

func NewOverallStat() *overallStat {
	return &overallStat{
		prefixes:  make(map[uint64]commitment.BranchStat),
		prefCount: make(map[uint64]uint64),
	}
}

func (s *overallStat) Collect(other *overallStat) {
	if other == nil {
		return
	}
	s.branches.Collect(other.branches)
	//if other.roots != nil {
	s.roots.Collect(other.roots)
	//}
	if other.prefCount != nil {
		for k, v := range other.prefCount {
			s.prefCount[k] += v
		}
	}
	if other.prefixes != nil {
		for k, v := range other.prefixes {
			ps, _ := s.prefixes[k]
			ps.Collect(v)
			s.prefixes[k] = ps
		}
	}
}

func processCommitmentFile(fpath string) (*overallStat, error) {
	stats := make(chan commitment.BranchStat, 1)
	errch := make(chan error)
	go func() {
		err := extractKVPairFromCompressed(fpath, stats)
		if err != nil {
			errch <- err
		}
		close(errch)
	}()

	totals := NewOverallStat()
	for s := range stats {
		if s.IsRoot {
			totals.rootsCount++
			//if totals.roots == nil {
			//	totals.roots = &s
			//} else {
			totals.roots.Collect(s)
			//}
		} else {
			//if totals.branches == nil {
			//	totals.branches = &s
			//} else {
			totals.branches.Collect(s)
			//}
		}
		totals.prefCount[s.KeySize]++

		ps, _ := totals.prefixes[s.KeySize]
		ps.Collect(s)
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

func renderKeyPrefixBar(data *overallStat) *charts.Pie {
	items := make([]opts.PieData, 0)
	for prefSize, count := range data.prefCount {
		items = append(items, opts.PieData{Name: fmt.Sprintf("%d", prefSize), Value: count})
	}

	pie := charts.NewPie()
	pie.SetGlobalOptions(
		charts.WithTooltipOpts(opts.Tooltip{Show: true}),
		charts.WithTitleOpts(opts.Title{Title: "key prefix length distribution (bytes)", Top: "25"}),
	)

	pie.AddSeries("prefixLen/count", items)
	return pie
}

func treeMapBase(fileName string, data *overallStat) *charts.TreeMap {
	var TreeMap = []opts.TreeMapNode{
		{Name: "prefixes"},
		{Name: "values"},
	}

	graph := charts.NewTreeMap()
	graph.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeMacarons}),
		charts.WithTitleOpts(opts.Title{
			Title: fileName,
			Left:  "center",
			Top:   "15",
		}),
		charts.WithLegendOpts(opts.Legend{Show: false}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:      true,
			Formatter: opts.FuncOpts(ToolTipFormatter),
			//TextStyle: &opts.TextStyle{
			//	Color: "#ff0000", // Set the text color
			//},
		}),
		//charts.WithToolboxOpts(opts.Toolbox{
		//	Orient: "horizontal",
		//	Left:   "right",
		//	Feature: &opts.ToolBoxFeature{
		//		SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{
		//			Title: "Save as image"},
		//		Restore: &opts.ToolBoxFeatureRestore{
		//			Title: "Reset"},
		//	}}),
	)

	keysIndex := 0
	TreeMap[keysIndex].Children = make([]opts.TreeMapNode, 0)
	for prefSize, stat := range data.prefixes {
		TreeMap[keysIndex].Children = append(TreeMap[keysIndex].Children, opts.TreeMapNode{
			Name:  fmt.Sprintf("%d", prefSize),
			Value: int(stat.KeySize),
		})
	}

	valsIndex := 1
	TreeMap[valsIndex].Children = make([]opts.TreeMapNode, 0)
	TreeMap[valsIndex].Children = append(TreeMap[valsIndex].Children, opts.TreeMapNode{
		Name:  "hashes",
		Value: int(data.branches.HashSize),
	})
	TreeMap[valsIndex].Children = append(TreeMap[valsIndex].Children, opts.TreeMapNode{
		Name:  "extensions",
		Value: int(data.branches.ExtSize),
	})
	TreeMap[valsIndex].Children = append(TreeMap[valsIndex].Children, opts.TreeMapNode{
		Name:  "apk",
		Value: int(data.branches.APKSize),
	})
	TreeMap[valsIndex].Children = append(TreeMap[valsIndex].Children, opts.TreeMapNode{
		Name:  "spk",
		Value: int(data.branches.SPKSize),
	})

	// Add initialized data to graph.
	graph.AddSeries(fileName, TreeMap).
		SetSeriesOptions(
			charts.WithTreeMapOpts(
				opts.TreeMapChart{
					Animation:  true,
					Roam:       true,
					UpperLabel: &opts.UpperLabel{Show: true, Color: "#fff"},
					Levels: &[]opts.TreeMapLevel{
						{ // Series
							ItemStyle: &opts.ItemStyle{
								BorderColor: "#777",
								BorderWidth: 1,
								GapWidth:    1},
							UpperLabel: &opts.UpperLabel{Show: true},
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
			charts.WithLabelOpts(opts.Label{Show: true, Position: "inside", Color: "White"}),
		)
	return graph
}

var ToolTipFormatter = `
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
