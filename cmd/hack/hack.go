package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/mgr"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/util"
)

var emptyCodeHash = crypto.Keccak256(nil)

var action = flag.String("action", "", "action to execute")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var reset = flag.Int("reset", -1, "reset to given block number")
var rewind = flag.Int("rewind", 1, "rewind to given number of blocks")
var block = flag.Int("block", 1, "specifies a block number for operation")
var account = flag.String("account", "0x", "specifies account to investigate")
var name = flag.String("name", "", "name to add to the file names")
var chaindata = flag.String("chaindata", "chaindata", "path to the chaindata database file")
var hash = flag.String("hash", "0x00", "image for preimage or state root for testBlockHashes action")
var preImage = flag.String("preimage", "0x00", "preimage")

func bucketList(db *bolt.DB) [][]byte {
	bucketList := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if len(name) == 20 || bytes.Equal(name, dbutils.CurrentStateBucket) {
				n := make([]byte, len(name))
				copy(n, name)
				bucketList = append(bucketList, n)
			}
			return nil
		})
		return err
	})
	if err != nil {
		panic(fmt.Sprintf("Could view db: %s", err))
	}
	return bucketList
}

// prefixLen returns the length of the common prefix of a and b.
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func parseFloat64(str string) float64 {
	v, _ := strconv.ParseFloat(str, 64)
	return v
}

func readData(filename string) (blocks []float64, hours []float64, dbsize []float64, trienodes []float64, heap []float64) {
	err := util.File.ReadByLines(filename, func(line string) error {
		parts := strings.Split(line, ",")
		blocks = append(blocks, parseFloat64(strings.Trim(parts[0], " ")))
		hours = append(hours, parseFloat64(strings.Trim(parts[1], " ")))
		dbsize = append(dbsize, parseFloat64(strings.Trim(parts[2], " ")))
		trienodes = append(trienodes, parseFloat64(strings.Trim(parts[3], " ")))
		heap = append(heap, parseFloat64(strings.Trim(parts[4], " ")))
		return nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	return
}

func notables() []chart.GridLine {
	return []chart.GridLine{
		{Value: 1.0},
		{Value: 2.0},
		{Value: 3.0},
		{Value: 4.0},
		{Value: 5.0},
		{Value: 6.0},
	}
}

func days() []chart.GridLine {
	return []chart.GridLine{
		{Value: 24.0},
		{Value: 48.0},
		{Value: 72.0},
		{Value: 96.0},
		{Value: 120.0},
		{Value: 144.0},
		{Value: 168.0},
		{Value: 192.0},
		{Value: 216.0},
		{Value: 240.0},
		{Value: 264.0},
		{Value: 288.0},
	}
}

func mychart() {
	blocks, hours, dbsize, trienodes, heap := readData("bolt.csv")
	blocks0, hours0, dbsize0, _, _ := readData("badger.csv")
	mainSeries := &chart.ContinuousSeries{
		Name: "Cumulative sync time (bolt)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: blocks,
		YValues: hours,
	}
	badgerSeries := &chart.ContinuousSeries{
		Name: "Cumulative sync time (badger)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
			FillColor:   chart.ColorRed.WithAlpha(100),
		},
		XValues: blocks0,
		YValues: hours0,
	}
	dbsizeSeries := &chart.ContinuousSeries{
		Name: "Database size (bolt)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: blocks,
		YValues: dbsize,
	}
	dbsizeSeries0 := &chart.ContinuousSeries{
		Name: "Database size (badger)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorOrange,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: blocks,
		YValues: dbsize0,
	}

	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "Elapsed time",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d h", int(v.(float64)))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: days(),
		},
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d G", int(v.(float64)))
			},
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			GridLines: notables(),
		},
		Series: []chart.Series{
			mainSeries,
			badgerSeries,
			dbsizeSeries,
			dbsizeSeries0,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err := graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart1.png", buffer.Bytes(), 0644)
	check(err)

	heapSeries := &chart.ContinuousSeries{
		Name: "Allocated heap",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorYellow,
			FillColor:   chart.ColorYellow.WithAlpha(100),
		},
		XValues: blocks,
		YValues: heap,
	}
	trienodesSeries := &chart.ContinuousSeries{
		Name: "Trie nodes",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorGreen,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: blocks,
		YValues: trienodes,
	}
	graph2 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "Allocated heap",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f G", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorYellow,
				StrokeWidth: 1.0,
			},
			GridLines: days(),
		},
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f m", v.(float64))
			},
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			GridLines: notables(),
		},
		Series: []chart.Series{
			heapSeries,
			trienodesSeries,
		},
	}

	graph2.Elements = []chart.Renderable{chart.LegendThin(&graph2)}
	buffer.Reset()
	err = graph2.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart2.png", buffer.Bytes(), 0644)
	check(err)
}

func accountSavings(db *bolt.DB) (int, int) {
	emptyRoots := 0
	emptyCodes := 0
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(k) != 32 {
				continue
			}
			if bytes.Contains(v, trie.EmptyRoot.Bytes()) {
				emptyRoots++
			}
			if bytes.Contains(v, emptyCodeHash) {
				emptyCodes++
			}
		}
		return nil
	})
	return emptyRoots, emptyCodes
}

func allBuckets(db *bolt.DB) [][]byte {
	bucketList := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			n := make([]byte, len(name))
			copy(n, name)
			bucketList = append(bucketList, n)
			return nil
		})
		return err
	})
	if err != nil {
		panic(fmt.Sprintf("Could view db: %s", err))
	}
	return bucketList
}

func printBuckets(db *bolt.DB) {
	bucketList := allBuckets(db)
	for _, bucket := range bucketList {
		fmt.Printf("%s\n", bucket)
	}
}

func bucketStats(chaindata string) {
	db, err := bolt.Open(chaindata, 0600, &bolt.Options{ReadOnly: true})
	check(err)
	bucketList := allBuckets(db)
	fmt.Printf(",BranchPageN,BranchOverflowN,LeafPageN,LeafOverflowN,KeyN,Depth,BranchAlloc,BranchInuse,LeafAlloc,LeafInuse,BucketN,InlineBucketN,InlineBucketInuse\n")
	db.View(func(tx *bolt.Tx) error {
		for _, bucket := range bucketList {
			b := tx.Bucket(bucket)
			bs := b.Stats()
			fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", string(bucket),
				bs.BranchPageN, bs.BranchOverflowN, bs.LeafPageN, bs.LeafOverflowN, bs.KeyN, bs.Depth, bs.BranchAlloc, bs.BranchInuse,
				bs.LeafAlloc, bs.LeafInuse, bs.BucketN, bs.InlineBucketN, bs.InlineBucketInuse)
		}
		return nil
	})
}

func readTrieLog() ([]float64, map[int][]float64, []float64) {
	data, err := ioutil.ReadFile("dust/hack.log")
	check(err)
	thresholds := []float64{}
	counts := map[int][]float64{}
	for i := 2; i <= 16; i++ {
		counts[i] = []float64{}
	}
	shorts := []float64{}
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		if bytes.HasPrefix(line, []byte("Threshold:")) {
			tokens := bytes.Split(line, []byte(" "))
			if len(tokens) == 23 {
				wei := parseFloat64(string(tokens[1]))
				thresholds = append(thresholds, wei)
				for i := 2; i <= 16; i++ {
					pair := bytes.Split(tokens[i+3], []byte(":"))
					counts[i] = append(counts[i], parseFloat64(string(pair[1])))
				}
				pair := bytes.Split(tokens[21], []byte(":"))
				shorts = append(shorts, parseFloat64(string(pair[1])))
			}
		}
	}
	return thresholds, counts, shorts
}

func ts() []chart.GridLine {
	return []chart.GridLine{
		{Value: 420.0},
	}
}

func trieChart() {
	thresholds, counts, shorts := readTrieLog()
	fmt.Printf("%d %d %d\n", len(thresholds), len(counts), len(shorts))
	shortsSeries := &chart.ContinuousSeries{
		Name: "Short nodes",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: thresholds,
		YValues: shorts,
	}
	countSeries := make(map[int]*chart.ContinuousSeries)
	for i := 2; i <= 16; i++ {
		countSeries[i] = &chart.ContinuousSeries{
			Name: fmt.Sprintf("%d-nodes", i),
			Style: chart.Style{
				Show:        true,
				StrokeColor: chart.GetAlternateColor(i),
			},
			XValues: thresholds,
			YValues: counts[i],
		}
	}
	xaxis := &chart.XAxis{
		Name: "Dust theshold",
		Style: chart.Style{
			Show: true,
		},
		ValueFormatter: func(v interface{}) string {
			return fmt.Sprintf("%d wei", int(v.(float64)))
		},
		GridMajorStyle: chart.Style{
			Show:        true,
			StrokeColor: chart.DefaultStrokeColor,
			StrokeWidth: 1.0,
		},
		Range: &chart.ContinuousRange{
			Min: thresholds[0],
			Max: thresholds[len(thresholds)-1],
		},
		Ticks: []chart.Tick{
			{Value: 0.0, Label: "0"},
			{Value: 1.0, Label: "wei"},
			{Value: 10.0, Label: "10"},
			{Value: 100.0, Label: "100"},
			{Value: 1e3, Label: "1e3"},
			{Value: 1e4, Label: "1e4"},
			{Value: 1e5, Label: "1e5"},
			{Value: 1e6, Label: "1e6"},
			{Value: 1e7, Label: "1e7"},
			{Value: 1e8, Label: "1e8"},
			{Value: 1e9, Label: "1e9"},
			{Value: 1e10, Label: "1e10"},
			//{1e15, "finney"},
			//{1e18, "ether"},
		},
	}

	graph3 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: *xaxis,
		YAxis: chart.YAxis{
			Name:      "Node count",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%dm", int(v.(float64)/1e6))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.DefaultStrokeColor,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			shortsSeries,
		},
	}
	graph3.Elements = []chart.Renderable{chart.LegendThin(&graph3)}
	buffer := bytes.NewBuffer([]byte{})
	err := graph3.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart3.png", buffer.Bytes(), 0644)
	check(err)
	graph4 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: *xaxis,
		YAxis: chart.YAxis{
			Name:      "Node count",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fm", v.(float64)/1e6)
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.DefaultStrokeColor,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			countSeries[2],
			countSeries[3],
		},
	}
	graph4.Elements = []chart.Renderable{chart.LegendThin(&graph4)}
	buffer = bytes.NewBuffer([]byte{})
	err = graph4.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart4.png", buffer.Bytes(), 0644)
	check(err)
	graph5 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: *xaxis,
		YAxis: chart.YAxis{
			Name:      "Node count",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fk", v.(float64)/1e3)
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.DefaultStrokeColor,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			countSeries[4],
			countSeries[5],
			countSeries[6],
			countSeries[7],
			countSeries[8],
			countSeries[9],
			countSeries[10],
			countSeries[11],
			countSeries[12],
			countSeries[13],
			countSeries[14],
			countSeries[15],
			countSeries[16],
		},
	}
	graph5.Elements = []chart.Renderable{chart.LegendThin(&graph5)}
	buffer = bytes.NewBuffer([]byte{})
	err = graph5.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart5.png", buffer.Bytes(), 0644)
	check(err)
}

func mgrSchedule(chaindata string, block uint64) {
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)

	//bc, err := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	//check(err)
	//defer db.Close()
	//tds, err := bc.GetTrieDbState()
	//check(err)
	//currentBlock := bc.CurrentBlock()
	//currentBlockNr := currentBlock.NumberU64()

	loader := trie.NewSubTrieLoader(0)
	tr := trie.New(common.Hash{})
	rs := trie.NewRetainList(0)
	rs.AddHex([]byte{})
	subTries, err := loader.LoadSubTries(db, 0, rs, [][]byte{nil}, []int{0}, false)
	check(err)

	err = tr.HookSubTries(subTries, [][]byte{nil}) // hook up to the root
	check(err)

	stateSize := tr.EstimateWitnessSize([]byte{})
	schedule := mgr.NewStateSchedule(stateSize, block, block+mgr.BlocksPerCycle+100)

	var buf bytes.Buffer
	var witnessSizeAccumulator uint64
	var witnessCount int64
	var witnessEstimatedSizeAccumulator uint64
	//fmt.Printf("%s\n", schedule)
	//fmt.Printf("stateSize: %d\n", stateSize)
	for i := range schedule.Ticks {
		tick := schedule.Ticks[i]
		for j := range tick.StateSizeSlices {
			ss := tick.StateSizeSlices[j]
			stateSlice, err2 := mgr.StateSizeSlice2StateSlice(db, tr, ss)
			if err2 != nil {
				panic(err2)
			}
			retain := trie.NewRetainRange(common.CopyBytes(stateSlice.From), common.CopyBytes(stateSlice.To))
			//if tick.IsLastInCycle() {
			//	fmt.Printf("\nretain: %s\n", retain)
			//}
			witness, err2 := tr.ExtractWitness(false, retain)
			if err2 != nil {
				panic(err2)
			}

			buf.Reset()
			_, err = witness.WriteTo(&buf)
			if err != nil {
				panic(err)
			}

			witnessCount++
			witnessSizeAccumulator += uint64(buf.Len())
		}
		witnessEstimatedSizeAccumulator += tick.ToSize - tick.FromSize
	}

	fmt.Printf("witnessCount: %s\n", humanize.Comma(witnessCount))
	fmt.Printf("witnessSizeAccumulator: %s\n", humanize.Bytes(witnessSizeAccumulator))
	fmt.Printf("witnessEstimatedSizeAccumulator: %s\n", humanize.Bytes(witnessEstimatedSizeAccumulator))
}

func execToBlock(chaindata string, block uint64, fromScratch bool) {
	state.MaxTrieCacheSize = 32
	blockDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	bcb, err := core.NewBlockChain(blockDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	defer blockDb.Close()
	if fromScratch {
		os.Remove("statedb")
	}
	stateDb, err := ethdb.NewBoltDatabase("statedb")
	check(err)
	defer stateDb.Close()

	//_, _, _, err = core.SetupGenesisBlock(stateDb, core.DefaultGenesisBlock())
	_, _, _, err = core.SetupGenesisBlock(stateDb, nil, false /* history */)
	check(err)
	bc, err := core.NewBlockChain(stateDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	tds, err := bc.GetTrieDbState()
	check(err)

	importedBn := tds.GetBlockNr()
	if importedBn == 0 {
		importedBn = 1
	}

	//bc.SetNoHistory(true)
	blocks := types.Blocks{}
	var lastBlock *types.Block

	now := time.Now()

	for i := importedBn; i <= block; i++ {
		lastBlock = bcb.GetBlockByNumber(i)
		blocks = append(blocks, lastBlock)
		if len(blocks) >= 1000 || i == block {
			_, err = bc.InsertChain(context.Background(), blocks)
			if err != nil {
				log.Error("Could not insert blocks (group)", "number", len(blocks), "error", err)
				// Try to insert blocks one by one to keep the latest state
				for j := 0; j < len(blocks); j++ {
					if _, err1 := bc.InsertChain(context.Background(), blocks[j:j+1]); err1 != nil {
						log.Error("Could not insert block", "error", err1)
						break
					}
				}
				break
			}
			blocks = types.Blocks{}
		}
		if i%1000 == 0 {
			fmt.Printf("Inserted %dK, %s \n", i/1000, time.Since(now))
		}
	}

	root := tds.LastRoot()
	fmt.Printf("Root hash: %x\n", root)
	fmt.Printf("Last block root hash: %x\n", lastBlock.Root())
	filename := fmt.Sprintf("right_%d.txt", lastBlock.NumberU64())
	fmt.Printf("Generating deep snapshot of the right tries... %s\n", filename)
	f, err := os.Create(filename)
	if err == nil {
		defer f.Close()
		tds.PrintTrie(f)
	}
}

func extractTrie(block int) {
	stateDb, err := ethdb.NewBoltDatabase("statedb")
	check(err)
	defer stateDb.Close()
	bc, err := core.NewBlockChain(stateDb, nil, params.RopstenChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	baseBlock := bc.GetBlockByNumber(uint64(block))
	tds := state.NewTrieDbState(baseBlock.Root(), stateDb, baseBlock.NumberU64())
	rebuiltRoot := tds.LastRoot()
	fmt.Printf("Rebuit root hash: %x\n", rebuiltRoot)
	filename := fmt.Sprintf("right_%d.txt", baseBlock.NumberU64())
	fmt.Printf("Generating deep snapshot of the right tries... %s\n", filename)
	f, err := os.Create(filename)
	if err == nil {
		defer f.Close()
		tds.PrintTrie(f)
	}
}

func testRewind(chaindata string, block, rewind int) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer ethDb.Close()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	currentBlock := bc.CurrentBlock()
	currentBlockNr := currentBlock.NumberU64()
	if block == 1 {
		block = int(currentBlockNr)
	}
	baseBlock := bc.GetBlockByNumber(uint64(block))
	baseBlockNr := baseBlock.NumberU64()
	fmt.Printf("Base block number: %d\n", baseBlockNr)
	fmt.Printf("Base block root hash: %x\n", baseBlock.Root())
	db := ethDb.NewBatch()
	defer db.Rollback()
	tds := state.NewTrieDbState(baseBlock.Root(), db, baseBlockNr)
	tds.SetHistorical(baseBlockNr != currentBlockNr)
	rebuiltRoot := tds.LastRoot()
	fmt.Printf("Rebuit root hash: %x\n", rebuiltRoot)
	startTime := time.Now()
	rewindLen := uint64(rewind)

	err = tds.UnwindTo(baseBlockNr - rewindLen)
	fmt.Printf("Unwind done in %v\n", time.Since(startTime))
	check(err)
	rewoundBlock1 := bc.GetBlockByNumber(baseBlockNr - rewindLen + 1)
	fmt.Printf("Rewound+1 block number: %d\n", rewoundBlock1.NumberU64())
	fmt.Printf("Rewound+1 block hash: %x\n", rewoundBlock1.Hash())
	fmt.Printf("Rewound+1 block root hash: %x\n", rewoundBlock1.Root())
	fmt.Printf("Rewound+1 block parent hash: %x\n", rewoundBlock1.ParentHash())

	rewoundBlock := bc.GetBlockByNumber(baseBlockNr - rewindLen)
	fmt.Printf("Rewound block number: %d\n", rewoundBlock.NumberU64())
	fmt.Printf("Rewound block hash: %x\n", rewoundBlock.Hash())
	fmt.Printf("Rewound block root hash: %x\n", rewoundBlock.Root())
	fmt.Printf("Rewound block parent hash: %x\n", rewoundBlock.ParentHash())
	rewoundRoot := tds.LastRoot()
	fmt.Printf("Calculated rewound root hash: %x\n", rewoundRoot)
	/*
		filename := fmt.Sprintf("root_%d.txt", rewoundBlock.NumberU64())
		fmt.Printf("Generating deep snapshot of the wront tries... %s\n", filename)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			tds.PrintTrie(f)
		}

		{
			tds, err = state.NewTrieDbState(rewoundBlock.Root(), db, rewoundBlock.NumberU64())
			tds.SetHistorical(true)
			check(err)
			rebuiltRoot, err := tds.TrieRoot()
			fmt.Printf("Rebuilt root: %x\n", rebuiltRoot)
			check(err)
		}
	*/
}

func testStartup() {
	startTime := time.Now()
	//ethDb, err := ethdb.NewBoltDatabase(node.DefaultDataDir() + "/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	currentBlock := bc.CurrentBlock()
	currentBlockNr := currentBlock.NumberU64()
	fmt.Printf("Current block number: %d\n", currentBlockNr)
	fmt.Printf("Current block root hash: %x\n", currentBlock.Root())
	l := trie.NewSubTrieLoader(currentBlockNr)
	rl := trie.NewRetainList(0)
	subTries, err1 := l.LoadSubTries(ethDb, currentBlockNr, rl, [][]byte{nil}, []int{0}, false)
	if err1 != nil {
		fmt.Printf("%v\n", err1)
	}
	if subTries.Hashes[0] != currentBlock.Root() {
		fmt.Printf("Hash mismatch, got %x, expected %x\n", subTries.Hashes[0], currentBlock.Root())
	}
	fmt.Printf("Took %v\n", time.Since(startTime))
}

func dbSlice(chaindata string, prefix []byte) {
	db, err := bolt.Open(chaindata, 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		st := tx.Bucket(dbutils.SyncStageProgress)
		c := st.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			fmt.Printf("db.Put(dbutils.SyncStageProgress, common.FromHex(\"%x\"), common.FromHex(\"%x\"))\n", k, v)
		}
		return nil
	})
	check(err)
}

func testResolve(chaindata string) {
	startTime := time.Now()
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer ethDb.Close()
	//bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	//check(err)
	/*
		currentBlock := bc.CurrentBlock()
		currentBlockNr := currentBlock.NumberU64()
		fmt.Printf("Current block number: %d\n", currentBlockNr)
		fmt.Printf("Current block root hash: %x\n", currentBlock.Root())
		prevBlock := bc.GetBlockByNumber(currentBlockNr - 2)
		fmt.Printf("Prev block root hash: %x\n", prevBlock.Root())
	*/
	currentBlockNr := uint64(286798)
	//var contract []byte
	//contract = common.FromHex("8416044c93d8fdf2d06a5bddbea65234695a3d4d278d5c824776c8b31702505dfffffffffffffffe")
	resolveHash := common.HexToHash("321131c74d582ebe29075d573023accd809234e4dbdee29e814bacedd3467279")
	l := trie.NewSubTrieLoader(currentBlockNr)
	var key []byte
	key = common.FromHex("0a080d05070c0604040302030508050100020105040e05080c0a0f030d0d050f08070a050b0c08090b02040e0e0200030f0c0b0f0704060a0d0703050009010f")
	rl := trie.NewRetainList(0)
	rl.AddHex(key[:3])
	subTries, err1 := l.LoadSubTries(ethDb, currentBlockNr, rl, [][]byte{{0xa8, 0xd0}}, []int{12}, true)
	if err1 != nil {
		fmt.Printf("Resolve error: %v\n", err1)
	}
	if subTries.Hashes[0] != resolveHash {
		fmt.Printf("Has mismatch, got %x, expected %x\n", subTries.Hashes[0], resolveHash)
	}
	/*
		var filename string
		if err == nil {
			filename = fmt.Sprintf("right_%d.txt", currentBlockNr)
		} else {
			filename = fmt.Sprintf("root_%d.txt", currentBlockNr)
		}
		fmt.Printf("Generating deep snapshot of the tries... %s\n", filename)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			t.Print(f)
		}
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	*/
	fmt.Printf("Took %v\n", time.Since(startTime))
}

func hashFile() {
	f, err := os.Open("/Users/alexeyakhunov/mygit/go-ethereum/geth.log")
	check(err)
	defer f.Close()
	w, err := os.Create("/Users/alexeyakhunov/mygit/go-ethereum/geth_read.log")
	check(err)
	defer w.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "ResolveWithDb") || strings.HasPrefix(line, "Error") ||
			strings.HasPrefix(line, "0000000000000000000000000000000000000000000000000000000000000000") ||
			strings.HasPrefix(line, "ERROR") || strings.HasPrefix(line, "tc{") {
			fmt.Printf("%d %s\n", count, line)
			count++
		} else if count == 66 {
			w.WriteString(line)
			w.WriteString("\n")
		}
	}
	fmt.Printf("%d lines scanned\n", count)
}

func rlpIndices() {
	keybuf := new(bytes.Buffer)
	for i := 0; i < 512; i++ {
		keybuf.Reset()
		rlp.Encode(keybuf, uint(i))
		fmt.Printf("Encoding of %d is %x\n", i, keybuf.Bytes())
	}
}

func printFullNodeRLPs() {
	trie.FullNode1()
	trie.FullNode2()
	trie.FullNode3()
	trie.FullNode4()
	trie.ShortNode1()
	trie.ShortNode2()
	trie.Hash1()
	trie.Hash2()
	trie.Hash3()
	trie.Hash4()
	trie.Hash5()
	trie.Hash6()
	trie.Hash7()
}

func testDifficulty() {
	genesisBlock, _, _, err := core.DefaultGenesisBlock().ToBlock(nil, false /* history */)
	check(err)
	d1 := ethash.CalcDifficulty(params.MainnetChainConfig, 100000, genesisBlock.Header())
	fmt.Printf("Block 1 difficulty: %d\n", d1)
}

// Searches 1000 blocks from the given one to try to find the one with the given state root hash
func testBlockHashes(chaindata string, block int, stateRoot common.Hash) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	blocksToSearch := 10000000
	for i := uint64(block); i < uint64(block+blocksToSearch); i++ {
		hash := rawdb.ReadCanonicalHash(ethDb, i)
		header := rawdb.ReadHeader(ethDb, hash, i)
		if header.Root == stateRoot || stateRoot == (common.Hash{}) {
			fmt.Printf("\n===============\nCanonical hash for %d: %x\n", i, hash)
			fmt.Printf("Header.Root: %x\n", header.Root)
			fmt.Printf("Header.TxHash: %x\n", header.TxHash)
			fmt.Printf("Header.UncleHash: %x\n", header.UncleHash)
		}
	}
}

func printCurrentBlockNumber(chaindata string) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer ethDb.Close()
	hash := rawdb.ReadHeadBlockHash(ethDb)
	number := rawdb.ReadHeaderNumber(ethDb, hash)
	fmt.Printf("Block number: %d\n", *number)
}

func printTxHashes() {
	ethDb, err := ethdb.NewBoltDatabase(node.DefaultDataDir() + "/geth/chaindata")
	check(err)
	defer ethDb.Close()
	for b := uint64(0); b < uint64(100000); b++ {
		hash := rawdb.ReadCanonicalHash(ethDb, b)
		block := rawdb.ReadBlock(ethDb, hash, b)
		if block == nil {
			break
		}
		for _, tx := range block.Transactions() {
			fmt.Printf("%x\n", tx.Hash())
		}
	}
}

func relayoutKeys() {
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open(node.DefaultDataDir()+"/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	var accountChangeSetCount, storageChangeSetCount int
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.AccountChangeSetBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			accountChangeSetCount++
		}
		return nil
	})
	check(err)
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageChangeSetBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			storageChangeSetCount++
		}
		return nil
	})
	check(err)
	fmt.Printf("Account changeset: %d\n", accountChangeSetCount)
	fmt.Printf("Storage changeset: %d\n", storageChangeSetCount)
	fmt.Printf("Total: %d\n", accountChangeSetCount+storageChangeSetCount)
}

func upgradeBlocks() {
	//ethDb, err := ethdb.NewBoltDatabase(node.DefaultDataDir() + "/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	start := []byte{}
	var keys [][]byte
	if err := ethDb.Walk([]byte("b"), start, 0, func(k, v []byte) (bool, error) {
		if len(keys)%1000 == 0 {
			fmt.Printf("Collected keys: %d\n", len(keys))
		}
		keys = append(keys, common.CopyBytes(k))
		return true, nil
	}); err != nil {
		panic(err)
	}
	for i, key := range keys {
		v, err := ethDb.Get([]byte("b"), key)
		if err != nil {
			panic(err)
		}
		smallBody := new(types.SmallBody) // To be changed to SmallBody
		if err := rlp.Decode(bytes.NewReader(v), smallBody); err != nil {
			panic(err)
		}
		body := new(types.Body)
		blockNum := binary.BigEndian.Uint64(key[:8])
		signer := types.MakeSigner(params.MainnetChainConfig, big.NewInt(int64(blockNum)))
		body.Senders = make([]common.Address, len(smallBody.Transactions))
		for j, tx := range smallBody.Transactions {
			addr, err := signer.Sender(tx)
			if err != nil {
				panic(err)
			}
			body.Senders[j] = addr
		}
		body.Transactions = smallBody.Transactions
		body.Uncles = smallBody.Uncles
		newV, err := rlp.EncodeToBytes(body)
		if err != nil {
			panic(err)
		}
		ethDb.Put([]byte("b"), key, newV)
		if i%1000 == 0 {
			fmt.Printf("Upgraded keys: %d\n", i)
		}
	}
	check(ethDb.DeleteBucket([]byte("r")))
}

func readTrie(filename string) *trie.Trie {
	f, err := os.Open(filename)
	check(err)
	defer f.Close()
	t, err := trie.Load(f)
	check(err)
	return t
}

func invTree(wrong, right, diff string, name string) {
	fmt.Printf("Reading trie...\n")
	t1 := readTrie(fmt.Sprintf("%s_%s.txt", wrong, name))
	fmt.Printf("Root hash: %x\n", t1.Hash())
	fmt.Printf("Reading trie 2...\n")
	t2 := readTrie(fmt.Sprintf("%s_%s.txt", right, name))
	fmt.Printf("Root hash: %x\n", t2.Hash())
	c, err := os.Create(fmt.Sprintf("%s_%s.txt", diff, name))
	check(err)
	defer c.Close()
	t1.PrintDiff(t2, c)
}

func preimage(chaindata string, image common.Hash) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer ethDb.Close()
	p, err := ethDb.Get(dbutils.PreimagePrefix, image[:])
	check(err)
	fmt.Printf("%x\n", p)
}

func addPreimage(chaindata string, image common.Hash, preimage []byte) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer ethDb.Close()
	err = ethDb.Put(dbutils.PreimagePrefix, image[:], preimage)
	check(err)
}

func loadAccount() {
	ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase(node.DefaultDataDir() + "/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth/geth/chaindata")
	check(err)
	defer ethDb.Close()
	blockNr := uint64(*block)
	blockSuffix := dbutils.EncodeTimestamp(blockNr)
	accountBytes := common.FromHex(*account)
	secKey := crypto.Keccak256(accountBytes)
	accountData, err := ethDb.GetAsOf(dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, secKey, blockNr+1)
	check(err)
	fmt.Printf("Account data: %x\n", accountData)
	startkey := make([]byte, len(accountBytes)+32)
	copy(startkey, accountBytes)
	t := trie.New(common.Hash{})
	count := 0
	if err := ethDb.WalkAsOf(dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startkey, len(accountBytes)*8, blockNr, func(k, v []byte) (bool, error) {
		key := k[len(accountBytes):]
		//fmt.Printf("%x: %x\n", key, v)
		t.Update(key, v)
		count++
		return true, nil
	}); err != nil {
		panic(err)
	}
	fmt.Printf("After %d updates, reconstructed storage root: %x\n", count, t.Hash())
	var keys [][]byte
	if err := ethDb.Walk(dbutils.StorageHistoryBucket, accountBytes, len(accountBytes)*8, func(k, v []byte) (bool, error) {
		if !bytes.HasSuffix(k, blockSuffix) {
			return true, nil
		}
		key := k[:len(k)-len(blockSuffix)]
		keys = append(keys, common.CopyBytes(key))
		return true, nil
	}); err != nil {
		panic(err)
	}
	fmt.Printf("%d keys updated\n", len(keys))
	for _, k := range keys {
		v, err := ethDb.GetAsOf(dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, k, blockNr+1)
		if err != nil {
			fmt.Printf("for key %x err %v\n", k, err)
		}
		vOrig, err := ethDb.GetAsOf(dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, k, blockNr)
		if err != nil {
			fmt.Printf("for key %x err %v\n", k, err)
		}
		key := ([]byte(k))[len(accountBytes):]
		if len(v) > 0 {
			fmt.Printf("Updated %x: %x from %x\n", key, v, vOrig)
			t.Update(key, v)
			check(err)
		} else {
			fmt.Printf("Deleted %x from %x\n", key, vOrig)
			t.Delete(key)
		}
	}
	fmt.Printf("Updated storage root: %x\n", t.Hash())
}

func printBranches(block uint64) {
	ethDb, err := ethdb.NewBoltDatabase(node.DefaultDataDir() + "/testnet/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	fmt.Printf("All headers at the same height %d\n", block)
	{
		var hashes []common.Hash
		numberEnc := make([]byte, 8)
		binary.BigEndian.PutUint64(numberEnc, block)
		if err := ethDb.Walk([]byte("h"), numberEnc, 8*8, func(k, v []byte) (bool, error) {
			if len(k) == 8+32 {
				hashes = append(hashes, common.BytesToHash(k[8:]))
			}
			return true, nil
		}); err != nil {
			panic(err)
		}
		for _, hash := range hashes {
			h := rawdb.ReadHeader(ethDb, hash, block)
			fmt.Printf("block hash: %x, root hash: %x\n", h.Hash(), h.Root)
		}
	}
}

func readPlainAccount(chaindata string, address common.Address, block uint64, rewind uint64) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	var acc accounts.Account
	enc, err := ethDb.Get(dbutils.PlainStateBucket, address[:])
	if err != nil {
		panic(err)
	} else if enc == nil {
		panic("acc not found")
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		panic(err)
	}
	fmt.Printf("%x\n%x\n%x\n%d\n", address, acc.Root, acc.CodeHash, acc.Incarnation)
}

func readAccount(chaindata string, account common.Address, block uint64, rewind uint64) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	secKey := crypto.Keccak256(account[:])
	var a accounts.Account
	ok, err := rawdb.ReadAccount(ethDb, common.BytesToHash(secKey), &a)
	if err != nil {
		panic(err)
	} else if !ok {
		panic("acc not found")
	}
	fmt.Printf("%x\n%x\n%x\n%d\n", secKey, a.Root, a.CodeHash, a.Incarnation)
	//var addrHash common.Hash
	//copy(addrHash[:], secKey)
	//codeHash, err := ethDb.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash, a.Incarnation))
	//check(err)
	//fmt.Printf("codeHash: %x\n", codeHash)
	timestamp := block
	for i := uint64(0); i < rewind; i++ {
		var printed bool
		encodedTS := dbutils.EncodeTimestamp(timestamp)
		var v []byte
		v, _ = ethDb.Get(dbutils.StorageChangeSetBucket, encodedTS)
		if v != nil {
			err = changeset.StorageChangeSetBytes(v).Walk(func(key, value []byte) error {
				if bytes.HasPrefix(key, secKey) {
					incarnation := ^binary.BigEndian.Uint64(key[common.HashLength : common.HashLength+common.IncarnationLength])
					if !printed {
						fmt.Printf("Changes for block %d\n", timestamp)
						printed = true
					}
					fmt.Printf("%d %x %x\n", incarnation, key[common.HashLength+common.IncarnationLength:], value)
				}
				return nil
			})
			check(err)
		}
		timestamp--
	}
}

func fixAccount(chaindata string, addrHash common.Hash, storageRoot common.Hash) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	var a accounts.Account
	if ok, err := rawdb.ReadAccount(ethDb, addrHash, &a); err != nil {
		panic(err)
	} else if !ok {
		panic("acc not found")
	}
	a.Root = storageRoot
	if err := rawdb.WriteAccount(ethDb, addrHash, a); err != nil {
		panic(err)
	}
}

func nextIncarnation(chaindata string, addrHash common.Hash) {
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	var found bool
	var incarnationBytes [common.IncarnationLength]byte
	startkey := make([]byte, common.HashLength+common.IncarnationLength+common.HashLength)
	var fixedbits int = 8 * common.HashLength
	copy(startkey, addrHash[:])
	if err := ethDb.Walk(dbutils.CurrentStateBucket, startkey, fixedbits, func(k, v []byte) (bool, error) {
		copy(incarnationBytes[:], k[common.HashLength:])
		found = true
		return false, nil
	}); err != nil {
		fmt.Printf("Incarnation(z): %d\n", 0)
		return
	}
	if found {
		fmt.Printf("Incarnation: %d\n", (^binary.BigEndian.Uint64(incarnationBytes[:]))+1)
		return
	}
	fmt.Printf("Incarnation(f): %d\n", state.FirstContractIncarnation)
}

func repairCurrent() {
	historyDb, err := bolt.Open("/Volumes/tb4/turbo-geth/ropsten/geth/chaindata", 0600, &bolt.Options{})
	check(err)
	defer historyDb.Close()
	currentDb, err := bolt.Open("statedb", 0600, &bolt.Options{})
	check(err)
	defer currentDb.Close()
	check(historyDb.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(dbutils.CurrentStateBucket); err != nil {
			return err
		}
		newB, err := tx.CreateBucket(dbutils.CurrentStateBucket, true)
		if err != nil {
			return err
		}
		count := 0
		if err := currentDb.View(func(ctx *bolt.Tx) error {
			b := ctx.Bucket(dbutils.CurrentStateBucket)
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if len(k) == 32 {
					continue
				}
				newB.Put(k, v)
				count++
				if count == 10000 {
					fmt.Printf("Copied %d storage items\n", count)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}))
}

func dumpStorage() {
	db, err := bolt.Open(node.DefaultDataDir()+"/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	err = db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(dbutils.StorageHistoryBucket)
		if sb == nil {
			fmt.Printf("Storage bucket not found\n")
			return nil
		}
		return sb.ForEach(func(k, v []byte) error {
			fmt.Printf("%x %x\n", k, v)
			return nil
		})
	})
	check(err)
	db.Close()
}

func testMemBolt() {
	db, err := bolt.Open("membolt", 0600, &bolt.Options{MemOnly: true})
	check(err)
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("B"), false)
		if err != nil {
			return fmt.Errorf("Bucket creation: %v", err)
		}
		for i := 0; i < 1000; i++ {
			err = bucket.Put(append([]byte("gjdfigjkdfljgdlfkjg"), []byte(fmt.Sprintf("%d", i))...), []byte("kljklgjfdkljkdjd"))
			if err != nil {
				return fmt.Errorf("Put: %v", err)
			}
		}
		return nil
	})
	check(err)
}

func printBucket(chaindata string) {
	db, err := bolt.Open(chaindata, 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	f, err := os.Create("bucket.txt")
	check(err)
	defer f.Close()
	fb := bufio.NewWriter(f)
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageHistoryBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Fprintf(fb, "%x %x\n", k, v)
		}
		return nil
	})
	check(err)
	fb.Flush()
}

func GenerateTxLookups(chaindata string) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	db.DeleteBucket(dbutils.TxLookupPrefix)
	log.Info("Open databased and deleted tx lookup bucket", "duration", time.Since(startTime))
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	var finished bool
	for !finished {
		blockNum, finished = generateLoop(db, blockNum, interruptCh)
	}
	log.Info("All done", "duration", time.Since(startTime))
}

func generateLoop(db ethdb.Database, startBlock uint64, interruptCh chan bool) (uint64, bool) {
	startTime := time.Now()
	var lookups []uint64
	var entry [8]byte
	var blockNum = startBlock
	var interrupt bool
	var finished = true
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		if body == nil {
			break
		}
		for txIndex, tx := range body.Transactions {
			copy(entry[:2], tx.Hash().Bytes()[:2])
			binary.BigEndian.PutUint32(entry[2:6], uint32(blockNum))
			binary.BigEndian.PutUint16(entry[6:8], uint16(txIndex))
			lookups = append(lookups, binary.BigEndian.Uint64(entry[:]))
		}
		blockNum++
		if blockNum%100000 == 0 {
			log.Info("Processed", "blocks", blockNum, "tx count", len(lookups))
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Memory", "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		if len(lookups) >= 100000000 {
			log.Info("Reached specified number of transactions")
			finished = false
			break
		}
	}
	log.Info("Processed", "blocks", blockNum, "tx count", len(lookups))
	log.Info("Filling up lookup array done", "duration", time.Since(startTime))
	startTime = time.Now()
	sort.Slice(lookups, func(i, j int) bool {
		return lookups[i] < lookups[j]
	})
	log.Info("Sorting lookup array done", "duration", time.Since(startTime))
	if len(lookups) == 0 {
		return blockNum, true
	}
	startTime = time.Now()
	var rangeStartIdx int
	var range2Bytes uint64
	for i, lookup := range lookups {
		// Find the range where lookups entries share the same first two bytes
		if i == 0 {
			rangeStartIdx = 0
			range2Bytes = lookup & 0xffff000000000000
			continue
		}
		twoBytes := lookup & 0xffff000000000000
		if range2Bytes != twoBytes {
			// Range finished
			fillSortRange(db, lookups, entry[:], rangeStartIdx, i)
			rangeStartIdx = i
			range2Bytes = twoBytes
		}
		if i%1000000 == 0 {
			log.Info("Processed", "transactions", i)
		}
	}
	fillSortRange(db, lookups, entry[:], rangeStartIdx, len(lookups))
	log.Info("Second roung of sorting done", "duration", time.Since(startTime))
	startTime = time.Now()
	batch := db.NewBatch()
	var n big.Int
	for i, lookup := range lookups {
		binary.BigEndian.PutUint64(entry[:], lookup)
		blockNumber := uint64(binary.BigEndian.Uint32(entry[2:6]))
		txIndex := int(binary.BigEndian.Uint16(entry[6:8]))
		blockHash := rawdb.ReadCanonicalHash(db, blockNumber)
		body := rawdb.ReadBody(db, blockHash, blockNumber)
		tx := body.Transactions[txIndex]
		n.SetInt64(int64(blockNumber))
		err := batch.Put(dbutils.TxLookupPrefix, tx.Hash().Bytes(), common.CopyBytes(n.Bytes()))
		check(err)
		if i != 0 && i%1000000 == 0 {
			_, err = batch.Commit()
			check(err)
			log.Info("Commited", "transactions", i)
		}
	}
	_, err := batch.Commit()
	check(err)
	log.Info("Commited", "transactions", len(lookups))
	log.Info("Tx committing done", "duration", time.Since(startTime))
	return blockNum, finished
}

func generateLoop1(db ethdb.Database, startBlock uint64, interruptCh chan bool) (uint64, bool) {
	f, _ := os.OpenFile(".lookups.tmp",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	entry := make([]byte, 36)
	var blockNum = startBlock
	var interrupt bool
	var finished = true
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		if body == nil {
			break
		}
		for _, tx := range body.Transactions {
			copy(entry[:32], tx.Hash().Bytes())
			binary.BigEndian.PutUint32(entry[32:], uint32(blockNum))
			_, _ = f.Write(append(entry, '\n'))
		}
		blockNum++
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
	}
	return blockNum, finished
}

func fillSortRange(db rawdb.DatabaseReader, lookups []uint64, entry []byte, start, end int) {
	for j := start; j < end; j++ {
		binary.BigEndian.PutUint64(entry[:], lookups[j])
		blockNum := uint64(binary.BigEndian.Uint32(entry[2:6]))
		txIndex := int(binary.BigEndian.Uint16(entry[6:8]))
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		tx := body.Transactions[txIndex]
		copy(entry[:2], tx.Hash().Bytes()[2:4])
		lookups[j] = binary.BigEndian.Uint64(entry[:])
	}
	sort.Slice(lookups[start:end], func(i, j int) bool {
		return lookups[i] < lookups[j]
	})
}

func GenerateTxLookups1(chaindata string, block int) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	db.DeleteBucket(dbutils.TxLookupPrefix)
	log.Info("Open databased and deleted tx lookup bucket", "duration", time.Since(startTime))
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	var interrupt bool
	var txcount int
	var n big.Int
	batch := db.NewBatch()
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)
		if body == nil {
			break
		}
		for _, tx := range body.Transactions {
			txcount++
			n.SetInt64(int64(blockNum))
			err = batch.Put(dbutils.TxLookupPrefix, tx.Hash().Bytes(), common.CopyBytes(n.Bytes()))
			check(err)
			if txcount%100000 == 0 {
				_, err = batch.Commit()
				check(err)
			}
			if txcount%1000000 == 0 {
				log.Info("Commited", "transactions", txcount)
			}
		}
		blockNum++
		if blockNum%100000 == 0 {
			log.Info("Processed", "blocks", blockNum)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Memory", "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		if block != 1 && int(blockNum) > block {
			log.Info("Reached specified block count")
			break
		}
	}
	_, err = batch.Commit()
	check(err)
	log.Info("Commited", "transactions", txcount)
	log.Info("Processed", "blocks", blockNum)
	log.Info("Tx committing done", "duration", time.Since(startTime))
}

func GenerateTxLookups2(chaindata string) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	db.DeleteBucket(dbutils.TxLookupPrefix)
	log.Info("Open databased and deleted tx lookup bucket", "duration", time.Since(startTime))
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	generateTxLookups2(db, blockNum, interruptCh)
	log.Info("All done", "duration", time.Since(startTime))
}

type LookupFile struct {
	reader io.Reader
	file   *os.File
	buffer []byte
	pos    uint64
}

type Entries []byte

type HeapElem struct {
	val   []byte
	index int
}

type Heap []HeapElem

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	return bytes.Compare(h[i].val, h[j].val) < 0
}
func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(HeapElem))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (a Entries) Len() int {
	return len(a) / 35
}

func (a Entries) Less(i, j int) bool {
	return bytes.Compare(a[35*i:35*i+35], a[35*j:35*j+35]) < 0
}

func (a Entries) Swap(i, j int) {
	tmp := common.CopyBytes(a[35*i : 35*i+35])
	copy(a[35*i:35*i+35], a[35*j:35*j+35])
	copy(a[35*j:35*j+35], tmp)
}

func insertInFileForLookups2(file *os.File, entries Entries, it uint64) {
	sorted := entries[:35*it]
	sort.Sort(sorted)
	_, err := file.Write(sorted)
	check(err)
	log.Info("File Insertion Occured")
}

func generateTxLookups2(db *ethdb.BoltDatabase, startBlock uint64, interruptCh chan bool) {
	var bufferLen int = 143360 // 35 * 4096
	var count uint64 = 5000000
	var entries Entries = make([]byte, count*35)
	bn := make([]byte, 3)
	var lookups []LookupFile
	var iterations uint64
	var blockNum = startBlock
	var interrupt bool
	filename := fmt.Sprintf(".lookups_%d.tmp", len(lookups))
	fileTmp, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)

		if body == nil {
			log.Info("Now Inserting to file")
			insertInFileForLookups2(fileTmp, entries, iterations)
			lookups = append(lookups, LookupFile{nil, nil, nil, 35})
			iterations = 0
			entries = nil
			fileTmp.Close()
			break
		}

		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		bn[0] = byte(blockNum >> 16)
		bn[1] = byte(blockNum >> 8)
		bn[2] = byte(blockNum)

		for _, tx := range body.Transactions {
			copy(entries[35*iterations:], tx.Hash().Bytes())
			copy(entries[35*iterations+32:], bn)
			iterations++
			if iterations == count {
				log.Info("Now Inserting to file")
				insertInFileForLookups2(fileTmp, entries, iterations)
				lookups = append(lookups, LookupFile{nil, nil, nil, 35})
				iterations = 0
				fileTmp.Close()
				filename = fmt.Sprintf(".lookups_%d.tmp", len(lookups))
				fileTmp, _ = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
			}
		}
		blockNum++
		if blockNum%100000 == 0 {
			log.Info("Processed", "blocks", blockNum, "iterations", iterations)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Memory", "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		}
	}
	batch := db.NewBatch()
	h := &Heap{}
	heap.Init(h)
	for i := range lookups {
		file, err := os.Open(fmt.Sprintf(".lookups_%d.tmp", i))
		check(err)
		lookups[i].file = file
		lookups[i].reader = bufio.NewReader(file)
		lookups[i].buffer = make([]byte, bufferLen)
		check(err)
		n, err := lookups[i].file.Read(lookups[i].buffer)
		if n != bufferLen {
			lookups[i].buffer = lookups[i].buffer[:n]
		}
		heap.Push(h, HeapElem{lookups[i].buffer[:35], i})
	}

	for !interrupt && len(*h) != 0 {
		val := (heap.Pop(h)).(HeapElem)
		if lookups[val.index].pos == uint64(bufferLen) {
			if val.val[32] != 0 {
				err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[32:]))
				check(err)
			} else {
				err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[33:]))
				check(err)
			}
			n, _ := lookups[val.index].reader.Read(lookups[val.index].buffer)
			iterations++
			if n == 0 {
				err := lookups[val.index].file.Close()
				check(err)
				os.Remove(fmt.Sprintf(".lookups_%d.tmp", val.index))
			} else {
				if n != bufferLen {
					lookups[val.index].buffer = lookups[val.index].buffer[:n]
				}
				lookups[val.index].pos = 35
				heap.Push(h, HeapElem{lookups[val.index].buffer[:35], val.index})
			}
			continue
		}

		heap.Push(h, HeapElem{lookups[val.index].buffer[lookups[val.index].pos : lookups[val.index].pos+35], val.index})
		lookups[val.index].pos += 35
		iterations++
		if val.val[32] != 0 {
			err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[32:]))
			check(err)
		} else {
			err := batch.Put(dbutils.TxLookupPrefix, val.val[:32], common.CopyBytes(val.val[33:]))
			check(err)
		}

		if iterations%1000000 == 0 {
			batch.Commit()
			log.Info("Commit Occured", "progress", iterations)
		}
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
	}
	batch.Commit()
	batch.Close()
}

func ValidateTxLookups2(chaindata string) {
	startTime := time.Now()
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	//nolint: errcheck
	startTime = time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		interruptCh <- true
	}()
	var blockNum uint64 = 1
	validateTxLookups2(db, blockNum, interruptCh)
	log.Info("All done", "duration", time.Since(startTime))
}

func validateTxLookups2(db *ethdb.BoltDatabase, startBlock uint64, interruptCh chan bool) {
	blockNum := startBlock
	iterations := 0
	var interrupt bool
	// Validation Process
	blockBytes := big.NewInt(0)
	for !interrupt {
		blockHash := rawdb.ReadCanonicalHash(db, blockNum)
		body := rawdb.ReadBody(db, blockHash, blockNum)

		if body == nil {
			break
		}

		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup...")
		default:
		}
		blockBytes.SetUint64(blockNum)
		bn := blockBytes.Bytes()

		for _, tx := range body.Transactions {
			val, err := db.Get(dbutils.TxLookupPrefix, tx.Hash().Bytes())
			iterations++
			if iterations%100000 == 0 {
				log.Info("Validated", "entries", iterations, "number", blockNum)
			}
			if bytes.Compare(val, bn) != 0 {
				check(err)
				panic(fmt.Sprintf("Validation process failed(%d). Expected %b, got %b", iterations, bn, val))
			}
		}
		blockNum++
	}
}

func indexSize(chaindata string) {
	//db, err := bolt.Open(chaindata, 0600, &bolt.Options{ReadOnly: true})
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer db.Close()
	fStorage, err := os.Create("index_sizes_storage.csv")
	check(err)
	defer fStorage.Close()
	fAcc, err := os.Create("index_sizes_acc.csv")
	check(err)
	defer fAcc.Close()
	csvAcc := csv.NewWriter(fAcc)
	defer csvAcc.Flush()
	err = csvAcc.Write([]string{"key", "ln"})
	check(err)
	csvStorage := csv.NewWriter(fStorage)
	defer csvStorage.Flush()
	err = csvStorage.Write([]string{"key", "ln"})

	i := 0
	maxLenAcc := 0
	accountsOver4096 := 0
	if err := db.Walk(dbutils.AccountsHistoryBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		i++
		if i%10_000_000 == 0 {
			fmt.Println(i/10_000_000, maxLenAcc)
		}
		if len(v) > maxLenAcc {
			maxLenAcc = len(v)
		}
		if len(v) > 4096 {
			accountsOver4096++
		}
		if err := csvAcc.Write([]string{common.Bytes2Hex(k), strconv.Itoa(len(v))}); err != nil {
			panic(err)
		}

		return true, nil
	}); err != nil {
		check(err)
	}

	i = 0
	maxLenSt := 0
	storageOver4096 := 0
	if err := db.Walk(dbutils.StorageHistoryBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		i++
		if i%10_000_000 == 0 {
			fmt.Println(i/10_000_000, maxLenSt)
		}

		if len(v) > maxLenSt {
			maxLenSt = len(v)
		}
		if len(v) > 4096 {
			storageOver4096++
		}
		if err := csvStorage.Write([]string{common.Bytes2Hex(k), strconv.Itoa(len(v))}); err != nil {
			panic(err)
		}

		return true, nil
	}); err != nil {
		check(err)
	}

	fmt.Println("Results:")
	fmt.Println("maxLenAcc:", maxLenAcc)
	fmt.Println("maxLenSt:", maxLenSt)
	fmt.Println("account over 4096 index:", accountsOver4096)
	fmt.Println("storage over 4096 index:", storageOver4096)
}

func getModifiedAccounts(chaindata string) {
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	addrs, err := ethdb.GetModifiedAccounts(db, 49300, 49400)
	check(err)
	fmt.Printf("Len(addrs)=%d\n", len(addrs))
}

func walkOverStorage(chaindata string) {
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	var startkey [32 + 8 + 32]byte
	h, err := common.HashData(common.FromHex("0x109c4f2ccc82c4d77bde15f306707320294aea3f"))
	check(err)
	copy(startkey[:], h[:])
	binary.BigEndian.PutUint64(startkey[32:], ^uint64(1))
	fmt.Printf("startkey %x\n", startkey)
	err = db.WalkAsOf(dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startkey[:], 8*(32+8), 50796, func(k []byte, v []byte) (bool, error) {
		fmt.Printf("%x: %x\n", k, v)
		return true, nil
	})
	check(err)
	fmt.Printf("Success\n")
}

func resetState(chaindata string) {
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer db.Close()
	//nolint:errcheck
	db.DeleteBucket(dbutils.CurrentStateBucket)
	//nolint:errcheck
	db.DeleteBucket(dbutils.AccountChangeSetBucket)
	//nolint:errcheck
	db.DeleteBucket(dbutils.StorageChangeSetBucket)
	//nolint:errcheck
	db.DeleteBucket(dbutils.PlainStateBucket)
	//nolint:errcheck
	db.DeleteBucket(dbutils.PlainAccountChangeSetBucket)
	//nolint:errcheck
	db.DeleteBucket(dbutils.PlainStorageChangeSetBucket)
	_, _, err = core.DefaultGenesisBlock().CommitGenesisState(db, false)
	check(err)
	core.UsePlainStateExecution = true
	_, _, err = core.DefaultGenesisBlock().CommitGenesisState(db, false)
	check(err)
	err = downloader.SaveStageProgress(db, downloader.Execution, 0)
	check(err)
	fmt.Printf("Reset state done\n")
}

type Receiver struct {
	defaultReceiver *trie.DefaultReceiver
	accountMap      map[string]*accounts.Account
	storageMap      map[string][]byte
	unfurlList      []string
	currentIdx      int
}

func (r *Receiver) Receive(
	itemType trie.StreamItem,
	accountKey []byte,
	storageKeyPart1 []byte,
	storageKeyPart2 []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	cutoff int,
	witnessLen uint64,
) error {
	for r.currentIdx < len(r.unfurlList) {
		ks := r.unfurlList[r.currentIdx]
		k := []byte(ks)
		var c int
		switch itemType {
		case trie.StorageStreamItem, trie.SHashStreamItem:
			if len(k) > common.HashLength {
				c = bytes.Compare(k[:common.HashLength], storageKeyPart1)
				if c == 0 {
					c = bytes.Compare(k[common.HashLength+common.IncarnationLength:], storageKeyPart2)
				}
			} else {
				c = bytes.Compare(k, storageKeyPart1)
			}
		case trie.AccountStreamItem, trie.AHashStreamItem:
			c = bytes.Compare(k, accountKey)
		case trie.CutoffStreamItem:
			c = -1
		}
		if c > 0 {
			return r.defaultReceiver.Receive(itemType, accountKey, storageKeyPart1, storageKeyPart2, accountValue, storageValue, hash, cutoff, witnessLen)
		}
		if len(k) > common.HashLength {
			v := r.storageMap[ks]
			if c <= 0 && len(v) > 0 {
				if err := r.defaultReceiver.Receive(trie.StorageStreamItem, nil, k[:32], k[40:], nil, v, nil, 0, 0); err != nil {
					return err
				}
			}
		} else {
			v := r.accountMap[ks]
			if c <= 0 && v != nil {
				if err := r.defaultReceiver.Receive(trie.AccountStreamItem, k, nil, nil, v, nil, nil, 0, 0); err != nil {
					return err
				}
			}
		}
		r.currentIdx++
		if c == 0 {
			return nil
		}
	}
	// We ran out of modifications, simply pass through
	return r.defaultReceiver.Receive(itemType, accountKey, storageKeyPart1, storageKeyPart2, accountValue, storageValue, hash, cutoff, witnessLen)
}

func (r *Receiver) Result() trie.SubTries {
	return r.defaultReceiver.Result()
}

func testGetProof(chaindata string, block uint64, account common.Address) {
	fmt.Printf("testGetProof %s, %d, %x\n", chaindata, block, account)
	// Find all changesets larger than given blocks
	db, err := ethdb.NewBoltDatabase(chaindata)
	check(err)
	defer db.Close()
	ts := dbutils.EncodeTimestamp(block)
	accountCs := 0
	accountMap := make(map[string]*accounts.Account)
	if err = db.Walk(dbutils.AccountChangeSetBucket, ts, 0, func(k, v []byte) (bool, error) {
		if changeset.Len(v) > 0 {
			walker := func(kk, vv []byte) error {
				if _, ok := accountMap[string(kk)]; !ok {
					if len(vv) > 0 {
						var a accounts.Account
						check(a.DecodeForStorage(vv))
						accountMap[string(kk)] = &a
					} else {
						accountMap[string(kk)] = nil
					}
				}
				return nil
			}
			v = common.CopyBytes(v) // Making copy because otherwise it will be invalid after the transaction
			if innerErr := changeset.AccountChangeSetBytes(v).Walk(walker); innerErr != nil {
				return false, innerErr
			}
		}
		accountCs++
		return true, nil
	}); err != nil {
		panic(err)
	}
	storageCs := 0
	storageMap := make(map[string][]byte)
	if err = db.Walk(dbutils.StorageChangeSetBucket, ts, 0, func(k, v []byte) (bool, error) {
		if changeset.Len(v) > 0 {
			walker := func(kk, vv []byte) error {
				if _, ok := storageMap[string(kk)]; !ok {
					storageMap[string(kk)] = vv
				}
				return nil
			}
			v = common.CopyBytes(v) // Making copy because otherwise it will be invalid after the transaction
			if innerErr := changeset.StorageChangeSetBytes(v).Walk(walker); innerErr != nil {
				return false, innerErr
			}
		}
		storageCs++
		return true, nil
	}); err != nil {
		panic(err)
	}
	var unfurlList = make([]string, len(accountMap)+len(storageMap))
	unfurl := trie.NewRetainList(0)
	i := 0
	for ks, acc := range accountMap {
		unfurlList[i] = ks
		i++
		unfurl.AddKey([]byte(ks))
		if acc != nil {
			// Fill the code hashes
			if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
				if codeHash, err1 := db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix([]byte(ks), acc.Incarnation)); err1 == nil {
					copy(acc.CodeHash[:], codeHash)
				}
			}
		}
	}
	for ks := range storageMap {
		unfurlList[i] = ks
		i++
		var sk [64]byte
		copy(sk[:], []byte(ks)[:common.HashLength])
		copy(sk[common.HashLength:], []byte(ks)[common.HashLength+common.IncarnationLength:])
		unfurl.AddKey(sk[:])
	}
	sort.Strings(unfurlList)
	fmt.Printf("Account changesets: %d, storage changesets: %d, unfurlList: %d\n", accountCs, storageCs, len(unfurlList))
	loader := trie.NewFlatDbSubTrieLoader()
	if err = loader.Reset(db, unfurl, [][]byte{nil}, []int{0}, false); err != nil {
		panic(err)
	}
	r := &Receiver{defaultReceiver: trie.NewDefaultReceiver(), unfurlList: unfurlList, accountMap: accountMap, storageMap: storageMap}
	rl := trie.NewRetainList(0)
	r.defaultReceiver.Reset(rl, false)
	loader.SetStreamReceiver(r)
	subTries, err := loader.LoadSubTries()
	if err != nil {
		panic(err)
	}
	headHash := rawdb.ReadHeadBlockHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	headHeader := rawdb.ReadHeader(db, headHash, *headNumber)
	fmt.Printf("Head block number: %d, root hash: %x\n", *headNumber, headHeader.Root)
	fmt.Printf("Root hash: %x\n", subTries.Hashes[0])
	hash := rawdb.ReadCanonicalHash(db, block-1)
	header := rawdb.ReadHeader(db, hash, block-1)
	fmt.Printf("Block state root: %x\n", header.Root)
}

func testSeek(chaindata string) {
	db, err := bolt.Open(chaindata, 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	if err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		c := b.Cursor()
		//kk := common.FromHex("0x434751")
		kk := common.FromHex("0x434750c1bd61c93ad930ba5b31d2181636e06fcad87ea82ac78c3ad9515d099f")
		c.Seek(kk)
		/*
			for k, _ := c.Seek(common.FromHex("4347500d81465512b2397af46c12792c44f2a89e3601af951cf9c7735385b0cb")); k != nil; k, _ = c.Next() {
				if bytes.Compare(k, kk) > 0 {
					fmt.Printf("Found nexgt key: %x\n", k)
					break
				}
			}
		*/
		return nil
	}); err != nil {
		panic(err)
	}
}

func main() {
	var (
		ostream log.Handler
		glogger *log.GlogHandler
	)

	usecolor := (isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
	output := io.Writer(os.Stderr)
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	ostream = log.StreamHandler(output, log.TerminalFormat(usecolor))
	glogger = log.NewGlogHandler(ostream)
	log.Root().SetHandler(glogger)
	glogger.Verbosity(log.LvlInfo)

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return
		}
		defer pprof.StopCPUProfile()
	}
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open(node.DefaultDataDir() + "/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//check(err)
	//defer db.Close()
	if *action == "bucketStats" {
		bucketStats(*chaindata)
	}
	if *action == "syncChart" {
		mychart()
	}
	//testRebuild()
	if *action == "testRewind" {
		testRewind(*chaindata, *block, *rewind)
	}
	//hashFile()
	//buildHashFromFile()
	if *action == "testResolve" {
		testResolve(*chaindata)
	}
	//rlpIndices()
	//printFullNodeRLPs()
	//testStartup()
	//testDifficulty()
	//testRewindTests()
	//if *reset != -1 {
	//	testReset(uint64(*reset))
	//}
	if *action == "testBlockHashes" {
		testBlockHashes(*chaindata, *block, common.HexToHash(*hash))
	}
	//printBuckets(db)
	//printTxHashes()
	//relayoutKeys()
	//testRedis()
	//upgradeBlocks()
	//compareTries()
	if *action == "invTree" {
		invTree("root", "right", "diff", *name)
	}
	//invTree("iw", "ir", "id", *block, true)
	//loadAccount()
	if *action == "preimage" {
		preimage(*chaindata, common.HexToHash(*hash))
	}
	if *action == "addPreimage" {
		addPreimage(*chaindata, common.HexToHash(*hash), common.FromHex(*preImage))
	}
	//printBranches(uint64(*block))
	if *action == "execToBlock" {
		execToBlock(*chaindata, uint64(*block), true)
	}
	//extractTrie(*block)
	//repair()
	if *action == "readAccount" {
		readAccount(*chaindata, common.HexToAddress(*account), uint64(*block), uint64(*rewind))
	}
	if *action == "readPlainAccount" {
		readPlainAccount(*chaindata, common.HexToAddress(*account), uint64(*block), uint64(*rewind))
	}
	if *action == "fixAccount" {
		fixAccount(*chaindata, common.HexToHash(*account), common.HexToHash(*hash))
	}
	if *action == "nextIncarnation" {
		nextIncarnation(*chaindata, common.HexToHash(*account))
	}
	//repairCurrent()
	//testMemBolt()
	//fmt.Printf("\u00b3\n")
	if *action == "dumpStorage" {
		dumpStorage()
	}
	if *action == "current" {
		printCurrentBlockNumber(*chaindata)
	}
	if *action == "bucket" {
		printBucket(*chaindata)
	}
	if *action == "gen-tx-lookup" {
		GenerateTxLookups(*chaindata)
	}
	if *action == "gen-tx-lookup-1" {
		GenerateTxLookups1(*chaindata, *block)
	}

	if *action == "gen-tx-lookup-2" {
		GenerateTxLookups2(*chaindata)
	}

	if *action == "val-tx-lookup-2" {
		ValidateTxLookups2(*chaindata)
	}
	if *action == "indexSize" {
		indexSize(*chaindata)
	}
	if *action == "modiAccounts" {
		getModifiedAccounts(*chaindata)
	}
	if *action == "storage" {
		walkOverStorage(*chaindata)
	}
	if *action == "slice" {
		dbSlice(*chaindata, common.FromHex(*hash))
	}
	if *action == "mgrSchedule" {
		mgrSchedule(*chaindata, uint64(*block))
	}
	if *action == "resetState" {
		resetState(*chaindata)
	}
	if *action == "getProof" {
		testGetProof(*chaindata, uint64(*block), common.HexToAddress(*account))
	}
	if *action == "seek" {
		testSeek(*chaindata)
	}
}
