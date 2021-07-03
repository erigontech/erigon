package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	kv2 "github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/util"

	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/hack/db"
	"github.com/ledgerwatch/erigon/cmd/hack/flow"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

var (
	verbosity  = flag.Uint("verbosity", 3, "Logging verbosity: 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (default 3)")
	action     = flag.String("action", "", "action to execute")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
	rewind     = flag.Int("rewind", 1, "rewind to given number of blocks")
	block      = flag.Int("block", 1, "specifies a block number for operation")
	blockTotal = flag.Int("blocktotal", 1, "specifies a total amount of blocks to process")
	account    = flag.String("account", "0x", "specifies account to investigate")
	name       = flag.String("name", "", "name to add to the file names")
	chaindata  = flag.String("chaindata", "chaindata", "path to the chaindata database file")
	bucket     = flag.String("bucket", "", "bucket in the database")
	hash       = flag.String("hash", "0x00", "image for preimage or state root for testBlockHashes action")
)

func readData(filename string) (blocks []float64, hours []float64, dbsize []float64, trienodes []float64, heap []float64) {
	err := util.File.ReadByLines(filename, func(line string) error {
		parts := strings.Split(line, ",")
		blocks = append(blocks, tool.ParseFloat64(strings.Trim(parts[0], " ")))
		hours = append(hours, tool.ParseFloat64(strings.Trim(parts[1], " ")))
		dbsize = append(dbsize, tool.ParseFloat64(strings.Trim(parts[2], " ")))
		trienodes = append(trienodes, tool.ParseFloat64(strings.Trim(parts[3], " ")))
		heap = append(heap, tool.ParseFloat64(strings.Trim(parts[4], " ")))
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
	tool.Check(err)
	err = ioutil.WriteFile("chart1.png", buffer.Bytes(), 0644)
	tool.Check(err)

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
	tool.Check(err)
	err = ioutil.WriteFile("chart2.png", buffer.Bytes(), 0644)
	tool.Check(err)
}

func bucketStats(chaindata string) error {
	ethDb := kv2.MustOpenKV(chaindata)
	defer ethDb.Close()

	var bucketList []string
	if err1 := ethDb.View(context.Background(), func(txa ethdb.Tx) error {
		if bl, err := txa.(ethdb.BucketMigrator).ExistingBuckets(); err == nil {
			bucketList = bl
		} else {
			return err
		}
		return nil
	}); err1 != nil {
		ethDb.Close()
		return err1
	}

	fmt.Printf(",BranchPageN,LeafPageN,OverflowN,Entries\n")
	switch kv := ethDb.(type) {
	case *kv2.MdbxKV:
		type MdbxStat interface {
			BucketStat(name string) (*mdbx.Stat, error)
		}

		if err := kv.View(context.Background(), func(tx ethdb.Tx) error {
			for _, bucket := range bucketList {
				bs, statErr := tx.(MdbxStat).BucketStat(bucket)
				tool.Check(statErr)
				fmt.Printf("%s,%d,%d,%d,%d\n", bucket,
					bs.BranchPages, bs.LeafPages, bs.OverflowPages, bs.Entries)
			}
			bs, statErr := tx.(MdbxStat).BucketStat("freelist")
			tool.Check(statErr)
			fmt.Printf("%s,%d,%d,%d,%d\n", "freelist", bs.BranchPages, bs.LeafPages, bs.OverflowPages, bs.Entries)
			return nil
		}); err != nil {
			panic(err)
		}
	}
	return nil
}

func readTrieLog() ([]float64, map[int][]float64, []float64) {
	data, err := ioutil.ReadFile("dust/hack.log")
	tool.Check(err)
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
				wei := tool.ParseFloat64(string(tokens[1]))
				thresholds = append(thresholds, wei)
				for i := 2; i <= 16; i++ {
					pair := bytes.Split(tokens[i+3], []byte(":"))
					counts[i] = append(counts[i], tool.ParseFloat64(string(pair[1])))
				}
				pair := bytes.Split(tokens[21], []byte(":"))
				shorts = append(shorts, tool.ParseFloat64(string(pair[1])))
			}
		}
	}
	return thresholds, counts, shorts
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
	tool.Check(err)
	err = ioutil.WriteFile("chart3.png", buffer.Bytes(), 0644)
	tool.Check(err)
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
	tool.Check(err)
	err = ioutil.WriteFile("chart4.png", buffer.Bytes(), 0644)
	tool.Check(err)
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
	tool.Check(err)
	err = ioutil.WriteFile("chart5.png", buffer.Bytes(), 0644)
	tool.Check(err)
}

func dbSlice(chaindata string, bucket string, prefix []byte) {
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(bucket)
		if err != nil {
			return err
		}
		for k, v, err := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v, err = c.Next() {
			if err != nil {
				return err
			}
			fmt.Printf("db.Put([]byte(\"%s\"), common.FromHex(\"%x\"), common.FromHex(\"%x\"))\n", bucket, k, v)
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func hashFile() {
	f, err := os.Open("/Users/alexeyakhunov/mygit/go-ethereum/geth.log")
	tool.Check(err)
	defer f.Close()
	w, err := os.Create("/Users/alexeyakhunov/mygit/go-ethereum/geth_read.log")
	tool.Check(err)
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

// Searches 1000 blocks from the given one to try to find the one with the given state root hash
func testBlockHashes(chaindata string, block int, stateRoot common.Hash) {
	ethDb := kv2.MustOpen(chaindata)
	defer ethDb.Close()
	blocksToSearch := 10000000
	for i := uint64(block); i < uint64(block+blocksToSearch); i++ {
		hash, err := rawdb.ReadCanonicalHash(ethDb, i)
		if err != nil {
			panic(err)
		}
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
	ethDb := kv2.MustOpenKV(chaindata)
	defer ethDb.Close()
	ethDb.View(context.Background(), func(tx ethdb.Tx) error {
		hash := rawdb.ReadHeadBlockHash(tx)
		number := rawdb.ReadHeaderNumber(tx, hash)
		fmt.Printf("Block number: %d\n", *number)
		return nil
	})
}

func printTxHashes() {
	db := kv2.MustOpen(paths.DefaultDataDir() + "/geth/chaindata").RwKV()
	defer db.Close()
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		for b := uint64(0); b < uint64(100000); b++ {
			hash, err := rawdb.ReadCanonicalHash(tx, b)
			tool.Check(err)
			block := rawdb.ReadBlock(tx, hash, b)
			if block == nil {
				break
			}
			for _, tx := range block.Transactions() {
				fmt.Printf("%x\n", tx.Hash())
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func readTrie(filename string) *trie.Trie {
	f, err := os.Open(filename)
	tool.Check(err)
	defer f.Close()
	t, err := trie.Load(f)
	tool.Check(err)
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
	tool.Check(err)
	defer c.Close()
	t1.PrintDiff(t2, c)
}

func printBranches(block uint64) {
	//ethDb := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	ethDb := kv2.MustOpen(paths.DefaultDataDir() + "/testnet/geth/chaindata")
	defer ethDb.Close()
	fmt.Printf("All headers at the same height %d\n", block)
	{
		var hashes []common.Hash
		numberEnc := make([]byte, 8)
		binary.BigEndian.PutUint64(numberEnc, block)
		if err := ethDb.Walk("h", numberEnc, 8*8, func(k, v []byte) (bool, error) {
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

func readAccount(chaindata string, account common.Address) error {
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()

	tx, txErr := db.BeginRo(context.Background())
	if txErr != nil {
		return txErr
	}
	defer tx.Rollback()

	a, err := state.NewPlainStateReader(tx).ReadAccountData(account)
	if err != nil {
		return err
	} else if a == nil {
		return fmt.Errorf("acc not found")
	}
	fmt.Printf("CodeHash:%x\nIncarnation:%d\n", a.CodeHash, a.Incarnation)

	c, err := tx.Cursor(dbutils.PlainStateBucket)
	if err != nil {
		return err
	}
	for k, v, e := c.Seek(account.Bytes()); k != nil && e == nil; k, v, e = c.Next() {
		if e != nil {
			return e
		}
		if !bytes.HasPrefix(k, account.Bytes()) {
			break
		}
		fmt.Printf("%x => %x\n", k, v)
	}
	return nil
}

func nextIncarnation(chaindata string, addrHash common.Hash) {
	ethDb := kv2.MustOpen(chaindata)
	defer ethDb.Close()
	var found bool
	var incarnationBytes [common.IncarnationLength]byte
	startkey := make([]byte, common.HashLength+common.IncarnationLength+common.HashLength)
	var fixedbits = 8 * common.HashLength
	copy(startkey, addrHash[:])
	if err := ethDb.Walk(dbutils.HashedStorageBucket, startkey, fixedbits, func(k, v []byte) (bool, error) {
		copy(incarnationBytes[:], k[common.HashLength:])
		found = true
		return false, nil
	}); err != nil {
		fmt.Printf("Incarnation(z): %d\n", 0)
		return
	}
	if found {
		fmt.Printf("Incarnation: %d\n", (binary.BigEndian.Uint64(incarnationBytes[:]))+1)
		return
	}
	fmt.Printf("Incarnation(f): %d\n", state.FirstContractIncarnation)
}

func repairCurrent() {
	historyDb := kv2.MustOpen("/Volumes/tb4/erigon/ropsten/geth/chaindata")
	defer historyDb.Close()
	currentDb := kv2.MustOpen("statedb")
	defer currentDb.Close()
	tool.Check(historyDb.ClearBuckets(dbutils.HashedStorageBucket))
	tool.Check(historyDb.RwKV().Update(context.Background(), func(tx ethdb.RwTx) error {
		newB, err := tx.RwCursor(dbutils.HashedStorageBucket)
		if err != nil {
			return err
		}
		count := 0
		if err := currentDb.RwKV().View(context.Background(), func(ctx ethdb.Tx) error {
			c, err := ctx.Cursor(dbutils.HashedStorageBucket)
			if err != nil {
				return err
			}
			for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				tool.Check(newB.Put(k, v))
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
	db := kv2.MustOpen(paths.DefaultDataDir() + "/geth/chaindata")
	defer db.Close()
	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.StorageHistoryBucket)
		if err != nil {
			return err
		}
		return ethdb.ForEach(c, func(k, v []byte) (bool, error) {
			fmt.Printf("%x %x\n", k, v)
			return true, nil
		})
	}); err != nil {
		panic(err)
	}
}

func printBucket(chaindata string) {
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	f, err := os.Create("bucket.txt")
	tool.Check(err)
	defer f.Close()
	fb := bufio.NewWriter(f)
	defer fb.Flush()
	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.StorageHistoryBucket)
		if err != nil {
			return err
		}
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			fmt.Fprintf(fb, "%x %x\n", k, v)
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func ValidateTxLookups2(chaindata string) {
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	startTime := time.Now()
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

func validateTxLookups2(db ethdb.Database, startBlock uint64, interruptCh chan bool) {
	tx, err := db.(ethdb.HasRwKV).RwKV().BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	blockNum := startBlock
	iterations := 0
	var interrupt bool
	// Validation Process
	blockBytes := big.NewInt(0)
	for !interrupt {
		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		tool.Check(err)
		body := rawdb.ReadBody(tx, blockHash, blockNum)

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

		for _, txn := range body.Transactions {
			val, err := tx.GetOne(dbutils.TxLookupPrefix, txn.Hash().Bytes())
			iterations++
			if iterations%100000 == 0 {
				log.Info("Validated", "entries", iterations, "number", blockNum)
			}
			if !bytes.Equal(val, bn) {
				tool.Check(err)
				panic(fmt.Sprintf("Validation process failed(%d). Expected %b, got %b", iterations, bn, val))
			}
		}
		blockNum++
	}
}

func getModifiedAccounts(chaindata string) {
	// TODO(tjayrush): The call to GetModifiedAccounts needs a database tx
	fmt.Println("hack - getModiiedAccounts is temporarily disabled.")
	// db := ethdb.MustOpen(chaindata)
	// defer db.Close()
	// addrs, err := ethdb.GetModifiedAccounts(db, 49300, 49400)
	// check(err)
	// fmt.Printf("Len(addrs)=%d\n", len(addrs))
}

type Receiver struct {
	defaultReceiver *trie.RootHashAggregator
	accountMap      map[string]*accounts.Account
	storageMap      map[string][]byte
	unfurlList      []string
	currentIdx      int
}

func (r *Receiver) Root() common.Hash { panic("don't call me") }
func (r *Receiver) Receive(
	itemType trie.StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	hasTree bool,
	cutoff int,
) error {
	for r.currentIdx < len(r.unfurlList) {
		ks := r.unfurlList[r.currentIdx]
		k := []byte(ks)
		var c int
		switch itemType {
		case trie.StorageStreamItem, trie.SHashStreamItem:
			c = bytes.Compare(k, storageKey)
		case trie.AccountStreamItem, trie.AHashStreamItem:
			c = bytes.Compare(k, accountKey)
		case trie.CutoffStreamItem:
			c = -1
		}
		if c > 0 {
			return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, hasTree, cutoff)
		}
		if len(k) > common.HashLength {
			v := r.storageMap[ks]
			if len(v) > 0 {
				if err := r.defaultReceiver.Receive(trie.StorageStreamItem, nil, k, nil, v, nil, hasTree, 0); err != nil {
					return err
				}
			}
		} else {
			v := r.accountMap[ks]
			if v != nil {
				if err := r.defaultReceiver.Receive(trie.AccountStreamItem, k, nil, v, nil, nil, hasTree, 0); err != nil {
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
	return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, hasTree, cutoff)
}

func (r *Receiver) Result() trie.SubTries {
	return r.defaultReceiver.Result()
}

func regenerate(chaindata string) error {
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tool.Check(stagedsync.ResetIH(tx))
	to, err := stages.GetStageProgress(tx, stages.HashState)
	if err != nil {
		return err
	}
	hash, err := rawdb.ReadCanonicalHash(tx, to)
	if err != nil {
		return err
	}
	syncHeadHeader := rawdb.ReadHeader(tx, hash, to)
	expectedRootHash := syncHeadHeader.Root
	_, err = stagedsync.RegenerateIntermediateHashes("", tx, stagedsync.StageTrieCfg(db, true, true, ""), expectedRootHash, nil)
	tool.Check(err)
	log.Info("Regeneration ended")
	return tx.Commit()
}

func testGetProof(chaindata string, address common.Address, rewind int, regen bool) error {
	if regen {
		if err := regenerate(chaindata); err != nil {
			return err
		}
	}
	storageKeys := []string{}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()
	tx, err1 := db.BeginRo(context.Background())
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()

	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	block := *headNumber - uint64(rewind)
	log.Info("GetProof", "address", address, "storage keys", len(storageKeys), "head", *headNumber, "block", block,
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))

	ts := dbutils.EncodeBlockNumber(block + 1)
	accountMap := make(map[string]*accounts.Account)

	if err := changeset.Walk(tx.(ethdb.HasTx).Tx(), dbutils.AccountChangeSetBucket, ts, 0, func(blockN uint64, address, v []byte) (bool, error) {
		if blockN > *headNumber {
			return false, nil
		}

		var addrHash, err = common.HashData(address)
		if err != nil {
			return false, err
		}
		k := addrHash[:]

		if _, ok := accountMap[string(k)]; !ok {
			if len(v) > 0 {
				var a accounts.Account
				if innerErr := a.DecodeForStorage(v); innerErr != nil {
					return false, innerErr
				}
				accountMap[string(k)] = &a
			} else {
				accountMap[string(k)] = nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	runtime.ReadMemStats(&m)
	log.Info("Constructed account map", "size", len(accountMap),
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	storageMap := make(map[string][]byte)
	if err := changeset.Walk(tx.(ethdb.HasTx).Tx(), dbutils.StorageChangeSetBucket, ts, 0, func(blockN uint64, address, v []byte) (bool, error) {
		if blockN > *headNumber {
			return false, nil
		}
		var addrHash, err = common.HashData(address)
		if err != nil {
			return false, err
		}
		k := addrHash[:]
		if _, ok := storageMap[string(k)]; !ok {
			storageMap[string(k)] = v
		}
		return true, nil
	}); err != nil {
		return err
	}
	runtime.ReadMemStats(&m)
	log.Info("Constructed storage map", "size", len(storageMap),
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
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
				if codeHash, err1 := tx.GetOne(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix([]byte(ks), acc.Incarnation)); err1 == nil {
					copy(acc.CodeHash[:], codeHash)
				} else {
					return err1
				}
			}
		}
	}
	for ks := range storageMap {
		unfurlList[i] = ks
		i++
		unfurl.AddKey([]byte(ks))
	}
	rl := trie.NewRetainList(0)
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	rl.AddKey(addrHash[:])
	unfurl.AddKey(addrHash[:])
	for _, key := range storageKeys {
		keyAsHash := common.HexToHash(key)
		if keyHash, err1 := common.HashData(keyAsHash[:]); err1 == nil {
			//TODO Add incarnation in the middle of this
			trieKey := append(addrHash[:], keyHash[:]...)
			rl.AddKey(trieKey)
			unfurl.AddKey(trieKey)
		} else {
			return err1
		}
	}
	sort.Strings(unfurlList)
	runtime.ReadMemStats(&m)
	log.Info("Constructed account unfurl lists",
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))

	loader := trie.NewFlatDBTrieLoader("checkRoots")
	if err = loader.Reset(unfurl, nil, nil, false); err != nil {
		panic(err)
	}
	_, err = loader.CalcTrieRoot(tx.(ethdb.HasTx).Tx(), nil, nil)
	if err != nil {
		return err
	}
	r := &Receiver{defaultReceiver: trie.NewRootHashAggregator(), unfurlList: unfurlList, accountMap: accountMap, storageMap: storageMap}
	r.defaultReceiver.Reset(nil, nil /* HashCollector */, false)
	loader.SetStreamReceiver(r)
	root, err := loader.CalcTrieRoot(tx.(ethdb.HasTx).Tx(), nil, nil)
	if err != nil {
		return err
	}
	runtime.ReadMemStats(&m)
	log.Info("Loaded subtries",
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	hash, err := rawdb.ReadCanonicalHash(tx, block)
	tool.Check(err)
	header := rawdb.ReadHeader(tx, hash, block)
	runtime.ReadMemStats(&m)
	log.Info("Constructed trie",
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	fmt.Printf("Resulting root: %x, expected root: %x\n", root, header.Root)
	return nil
}

func dumpState(chaindata string) error {
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	f, err := os.Create("statedump")
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	stAccounts := 0
	stStorage := 0
	var varintBuf [10]byte // Buffer for varint number
	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.PlainStateBucket)
		if err != nil {
			return err
		}
		k, v, e := c.First()
		for ; k != nil && e == nil; k, v, e = c.Next() {
			keyLen := binary.PutUvarint(varintBuf[:], uint64(len(k)))
			if _, err = w.Write(varintBuf[:keyLen]); err != nil {
				return err
			}
			if _, err = w.Write([]byte(k)); err != nil {
				return err
			}
			valLen := binary.PutUvarint(varintBuf[:], uint64(len(v)))
			if _, err = w.Write(varintBuf[:valLen]); err != nil {
				return err
			}
			if len(v) > 0 {
				if _, err = w.Write(v); err != nil {
					return err
				}
			}
			if len(k) > 28 {
				stStorage++
			} else {
				stAccounts++
			}
			if (stStorage+stAccounts)%100000 == 0 {
				fmt.Printf("State records: %d\n", stStorage+stAccounts)
			}
		}
		return e
	}); err != nil {
		return err
	}
	fmt.Printf("stAccounts = %d, stStorage = %d\n", stAccounts, stStorage)
	return nil
}

func changeSetStats(chaindata string, block1, block2 uint64) error {
	db := kv2.MustOpen(chaindata)
	defer db.Close()

	fmt.Printf("State stats\n")
	stAccounts := 0
	stStorage := 0
	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.PlainStateBucket)
		if err != nil {
			return err
		}
		k, _, e := c.First()
		for ; k != nil && e == nil; k, _, e = c.Next() {
			if len(k) > 28 {
				stStorage++
			} else {
				stAccounts++
			}
			if (stStorage+stAccounts)%100000 == 0 {
				fmt.Printf("State records: %d\n", stStorage+stAccounts)
			}
		}
		return e
	}); err != nil {
		return err
	}
	fmt.Printf("stAccounts = %d, stStorage = %d\n", stAccounts, stStorage)
	fmt.Printf("Changeset stats from %d to %d\n", block1, block2)
	accounts := make(map[string]struct{})
	tx, err1 := db.Begin(context.Background(), ethdb.RW)
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()
	if err := changeset.Walk(tx.(ethdb.HasTx).Tx(), dbutils.AccountChangeSetBucket, dbutils.EncodeBlockNumber(block1), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if blockN >= block2 {
			return false, nil
		}
		if (blockN-block1)%100000 == 0 {
			fmt.Printf("at the block %d for accounts, booster size: %d\n", blockN, len(accounts))
		}
		accounts[string(common.CopyBytes(k))] = struct{}{}
		return true, nil
	}); err != nil {
		return err
	}

	storage := make(map[string]struct{})
	if err := changeset.Walk(tx.(ethdb.HasTx).Tx(), dbutils.StorageChangeSetBucket, dbutils.EncodeBlockNumber(block1), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if blockN >= block2 {
			return false, nil
		}
		if (blockN-block1)%100000 == 0 {
			fmt.Printf("at the block %d for accounts, booster size: %d\n", blockN, len(accounts))
		}
		storage[string(common.CopyBytes(k))] = struct{}{}
		return true, nil
	}); err != nil {
		return err
	}

	fmt.Printf("accounts changed: %d, storage changed: %d\n", len(accounts), len(storage))
	return nil
}

func searchChangeSet(chaindata string, key []byte, block uint64) error {
	fmt.Printf("Searching changesets\n")
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	tx, err1 := db.Begin(context.Background(), ethdb.RW)
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()

	if err := changeset.Walk(tx.(ethdb.HasTx).Tx(), dbutils.AccountChangeSetBucket, dbutils.EncodeBlockNumber(block), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if bytes.Equal(k, key) {
			fmt.Printf("Found in block %d with value %x\n", blockN, v)
		}
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func searchStorageChangeSet(chaindata string, key []byte, block uint64) error {
	fmt.Printf("Searching storage changesets\n")
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	tx, err1 := db.Begin(context.Background(), ethdb.RW)
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()
	if err := changeset.Walk(tx.(ethdb.HasTx).Tx(), dbutils.StorageChangeSetBucket, dbutils.EncodeBlockNumber(block), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if bytes.Equal(k, key) {
			fmt.Printf("Found in block %d with value %x\n", blockN, v)
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

func supply(chaindata string) error {
	startTime := time.Now()
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	count := 0
	supply := uint256.NewInt(0)
	var a accounts.Account
	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.PlainStateBucket)
		if err != nil {
			return err
		}
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) != 20 {
				continue
			}
			if err1 := a.DecodeForStorage(v); err1 != nil {
				return err1
			}
			count++
			supply.Add(supply, &a.Balance)
			if count%100000 == 0 {
				fmt.Printf("Processed %dK account records\n", count/1000)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	fmt.Printf("Total accounts: %d, supply: %d, took: %s\n", count, supply, time.Since(startTime))
	return nil
}

func extractCode(chaindata string) error {
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	var contractCount int
	if err1 := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.CodeBucket)
		if err != nil {
			return err
		}
		// This is a mapping of CodeHash => Byte code
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			fmt.Printf("%x,%x", k, v)
			contractCount++
		}
		return nil
	}); err1 != nil {
		return err1
	}
	fmt.Fprintf(os.Stderr, "contractCount: %d\n", contractCount)
	return nil
}

func iterateOverCode(chaindata string) error {
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	var contractCount int
	var contractKeyTotalLength int
	var contractValTotalLength int
	var codeHashTotalLength int
	var codeTotalLength int // Total length of all byte code (just to illustrate iterating)
	if err1 := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.PlainContractCodeBucket)
		if err != nil {
			return err
		}
		// This is a mapping of contractAddress + incarnation => CodeHash
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			contractKeyTotalLength += len(k)
			contractValTotalLength += len(v)
		}
		c, err = tx.Cursor(dbutils.CodeBucket)
		if err != nil {
			return err
		}
		// This is a mapping of CodeHash => Byte code
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			codeHashTotalLength += len(k)
			codeTotalLength += len(v)
			contractCount++
		}
		return nil
	}); err1 != nil {
		return err1
	}
	fmt.Printf("contractCount: %d,contractKeyTotalLength: %d, contractValTotalLength: %d, codeHashTotalLength: %d, codeTotalLength: %d\n",
		contractCount, contractKeyTotalLength, contractValTotalLength, codeHashTotalLength, codeTotalLength)
	return nil
}

func mint(chaindata string, block uint64) error {
	f, err := os.Create("mint.csv")
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	db := kv2.MustOpen(chaindata).RwKV()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	//chiTokenAddr = common.HexToAddress("0x0000000000004946c0e9F43F4Dee607b0eF1fA1c")
	//mintFuncPrefix = common.FromHex("0xa0712d68")
	var gwei uint256.Int
	gwei.SetUint64(1000000000)
	blockEncoded := dbutils.EncodeBlockNumber(block)
	canonical := make(map[common.Hash]struct{})
	c, err := tx.Cursor(dbutils.HeaderCanonicalBucket)
	if err != nil {
		return err
	}

	// This is a mapping of contractAddress + incarnation => CodeHash
	for k, v, err := c.Seek(blockEncoded); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		// Skip non relevant records
		canonical[common.BytesToHash(v)] = struct{}{}
		if len(canonical)%100_000 == 0 {
			log.Info("Read canonical hashes", "count", len(canonical))
		}
	}
	log.Info("Read canonical hashes", "count", len(canonical))
	c, err = tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	var prevBlock uint64
	var burntGas uint64
	for k, _, err := c.Seek(blockEncoded); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		if _, isCanonical := canonical[blockHash]; !isCanonical {
			continue
		}
		if blockNumber != prevBlock && blockNumber != prevBlock+1 {
			fmt.Printf("Gap [%d-%d]\n", prevBlock, blockNumber-1)
		}
		prevBlock = blockNumber
		body := rawdb.ReadBody(tx, blockHash, blockNumber)
		header := rawdb.ReadHeader(tx, blockHash, blockNumber)
		senders, errSenders := rawdb.ReadSenders(tx, blockHash, blockNumber)
		if errSenders != nil {
			return errSenders
		}
		var ethSpent uint256.Int
		var ethSpentTotal uint256.Int
		var totalGas uint256.Int
		count := 0
		for i, tx := range body.Transactions {
			ethSpent.SetUint64(tx.GetGas())
			totalGas.Add(&totalGas, &ethSpent)
			if senders[i] == header.Coinbase {
				continue // Mining pool sending payout potentially with abnormally low fee, skip
			}
			ethSpent.Mul(&ethSpent, tx.GetPrice())
			ethSpentTotal.Add(&ethSpentTotal, &ethSpent)
			count++
		}
		if count > 0 {
			ethSpentTotal.Div(&ethSpentTotal, &totalGas)
			ethSpentTotal.Div(&ethSpentTotal, &gwei)
			gasPrice := ethSpentTotal.Uint64()
			burntGas += header.GasUsed
			fmt.Fprintf(w, "%d, %d\n", burntGas, gasPrice)
		}
		if blockNumber%100_000 == 0 {
			log.Info("Processed", "blocks", blockNumber)
		}
	}
	return tx.Commit()
}

func extractHashes(chaindata string, blockStep uint64, blockTotal uint64, name string) error {
	db := kv2.MustOpen(chaindata)
	defer db.Close()

	f, err := os.Create(fmt.Sprintf("preverified_hashes_%s.go", name))
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "package headerdownload\n\n")
	fmt.Fprintf(w, "var %sPreverifiedHashes = []string{\n", name)

	b := uint64(0)
	for b <= blockTotal {
		hash, err := rawdb.ReadCanonicalHash(db, b)
		if err != nil {
			return err
		}

		if hash == (common.Hash{}) {
			break
		}

		fmt.Fprintf(w, "	\"%x\",\n", hash)
		b += blockStep
	}
	b -= blockStep
	fmt.Fprintf(w, "}\n\n")
	fmt.Fprintf(w, "const %sPreverifiedHeight uint64 = %d\n", name, b)
	fmt.Printf("Last block is %d\n", b)
	return nil
}

func extractHeaders(chaindata string, block uint64) error {
	db := kv2.MustOpen(chaindata).RwKV()
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(dbutils.HeadersBucket)
	if err != nil {
		return err
	}
	defer c.Close()
	blockEncoded := dbutils.EncodeBlockNumber(block)
	for k, v, err := c.Seek(blockEncoded); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		var header types.Header
		if err = rlp.DecodeBytes(v, &header); err != nil {
			return fmt.Errorf("decoding header from %x: %v", v, err)
		}
		fmt.Printf("Header %d %x: stateRoot %x, parentHash %x, diff %d\n", blockNumber, blockHash, header.Root, header.ParentHash, header.Difficulty)
	}
	return nil
}

func extractBodies(chaindata string, block uint64) error {
	db := kv2.MustOpen(chaindata).RwKV()
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	defer c.Close()
	blockEncoded := dbutils.EncodeBlockNumber(block)
	for k, _, err := c.Seek(blockEncoded); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		_, baseTxId, txAmount := rawdb.ReadBodyWithoutTransactions(tx, blockHash, blockNumber)
		fmt.Printf("Body %d %x: baseTxId %d, txAmount %d\n", blockNumber, blockHash, baseTxId, txAmount)
	}
	return nil
}

func fixUnwind(chaindata string) error {
	contractAddr := common.HexToAddress("0x577a32aa9c40cf4266e49fc1e44c749c356309bd")
	db := kv2.MustOpen(chaindata)
	defer db.Close()
	i, err := db.GetOne(dbutils.IncarnationMapBucket, contractAddr[:])
	if err != nil {
		return err
	} else if i == nil {
		fmt.Print("Not found\n")
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], 1)
		if err = db.Put(dbutils.IncarnationMapBucket, contractAddr[:], b[:]); err != nil {
			return err
		}
	} else {
		fmt.Printf("Inc: %x\n", i)
	}
	return nil
}

func snapSizes(chaindata string) error {
	db := kv2.MustOpen(chaindata)
	defer db.Close()

	dbtx, err := db.Begin(context.Background(), ethdb.RO)
	if err != nil {
		return err
	}
	defer dbtx.Rollback()
	tx := dbtx.(ethdb.HasTx).Tx()

	c, _ := tx.Cursor(dbutils.CliqueSeparateBucket)
	defer c.Close()

	sizes := make(map[int]int)
	differentValues := make(map[string]struct{})

	var (
		total uint64
		k, v  []byte
	)

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		sizes[len(v)]++
		differentValues[string(v)] = struct{}{}
		total += uint64(len(v) + len(k))
	}

	var lens = make([]int, len(sizes))

	i := 0
	for l := range sizes {
		lens[i] = l
		i++
	}
	sort.Ints(lens)

	for _, l := range lens {
		fmt.Printf("%6d - %d\n", l, sizes[l])
	}

	fmt.Printf("Different keys %d\n", len(differentValues))
	fmt.Printf("Total size: %d bytes\n", total)

	return nil
}

func readCallTraces(chaindata string, block uint64) error {
	kv := kv2.MustOpenKV(chaindata)
	defer kv.Close()
	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	traceCursor, err1 := tx.RwCursorDupSort(dbutils.CallTraceSet)
	if err1 != nil {
		return err1
	}
	defer traceCursor.Close()
	var k []byte
	var v []byte
	count := 0
	for k, v, err = traceCursor.First(); k != nil && err == nil; k, v, err = traceCursor.Next() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum == block {
			fmt.Printf("%x\n", v)
		}
		count++
	}
	if err != nil {
		return err
	}
	fmt.Printf("Found %d records\n", count)
	idxCursor, err2 := tx.Cursor(dbutils.CallToIndex)
	if err2 != nil {
		return err2
	}
	var acc common.Address = common.HexToAddress("0x511bc4556d823ae99630ae8de28b9b80df90ea2e")
	for k, v, err = idxCursor.Seek(acc[:]); k != nil && err == nil && bytes.HasPrefix(k, acc[:]); k, v, err = idxCursor.Next() {
		bm := roaring64.New()
		_, err = bm.ReadFrom(bytes.NewReader(v))
		if err != nil {
			return err
		}
		//fmt.Printf("%x: %d\n", k, bm.ToArray())
	}
	if err != nil {
		return err
	}
	return nil
}

func fixTd(chaindata string) error {
	kv := kv2.MustOpenKV(chaindata)
	defer kv.Close()
	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err1 := tx.RwCursor(dbutils.HeadersBucket)
	if err1 != nil {
		return err1
	}
	defer c.Close()
	var k, v []byte
	for k, v, err = c.First(); err == nil && k != nil; k, v, err = c.Next() {
		hv, herr := tx.GetOne(dbutils.HeaderTDBucket, k)
		if herr != nil {
			return herr
		}
		if hv == nil {
			fmt.Printf("Missing TD record for %x, fixing\n", k)
			var header types.Header
			if err = rlp.DecodeBytes(v, &header); err != nil {
				return fmt.Errorf("decoding header from %x: %v", v, err)
			}
			if header.Number.Uint64() == 0 {
				continue
			}
			var parentK [40]byte
			binary.BigEndian.PutUint64(parentK[:], header.Number.Uint64()-1)
			copy(parentK[8:], header.ParentHash[:])
			var parentTdRec []byte
			if parentTdRec, err = tx.GetOne(dbutils.HeaderTDBucket, parentK[:]); err != nil {
				return fmt.Errorf("reading parentTd Rec for %d: %v", header.Number.Uint64(), err)
			}
			var parentTd big.Int
			if err = rlp.DecodeBytes(parentTdRec, &parentTd); err != nil {
				return fmt.Errorf("decoding parent Td record for block %d, from %x: %v", header.Number.Uint64(), parentTdRec, err)
			}
			var td big.Int
			td.Add(&parentTd, header.Difficulty)
			var newHv []byte
			if newHv, err = rlp.EncodeToBytes(&td); err != nil {
				return fmt.Errorf("encoding td record for block %d: %v", header.Number.Uint64(), err)
			}
			if err = tx.Put(dbutils.HeaderTDBucket, k, newHv); err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func advanceExec(chaindata string) error {
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stageExec, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("Stage exec", "progress", stageExec)
	if err = stages.SaveStageProgress(tx, stages.Execution, stageExec+1); err != nil {
		return err
	}
	stageExec, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("Stage exec", "changed to", stageExec)
	if err = stages.SaveStageUnwind(tx, stages.Execution, 0); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func backExec(chaindata string) error {
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stageExec, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("Stage exec", "progress", stageExec)
	if err = stages.SaveStageProgress(tx, stages.Execution, stageExec-1); err != nil {
		return err
	}
	stageExec, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("Stage exec", "changed to", stageExec)
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func unwind(chaindata string, block uint64) error {
	db := kv2.MustOpenKV(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	log.Info("Unwinding to", "block", block)
	if err = stages.SaveStageUnwind(tx, stages.Headers, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.BlockHashes, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.Bodies, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.Senders, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.Execution, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.HashState, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.IntermediateHashes, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.CallTraces, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.AccountHistoryIndex, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.StorageHistoryIndex, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.LogIndex, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.TxLookup, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.Finish, block); err != nil {
		return err
	}
	if err = stages.SaveStageUnwind(tx, stages.TxPool, block); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func fixState(chaindata string) error {
	kv := kv2.MustOpenKV(chaindata)
	defer kv.Close()
	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err1 := tx.RwCursor(dbutils.HeaderCanonicalBucket)
	if err1 != nil {
		return err1
	}
	defer c.Close()
	var prevHeaderKey [40]byte
	var k, v []byte
	for k, v, err = c.First(); err == nil && k != nil; k, v, err = c.Next() {
		var headerKey [40]byte
		copy(headerKey[:], k)
		copy(headerKey[8:], v)
		hv, herr := tx.GetOne(dbutils.HeadersBucket, headerKey[:])
		if herr != nil {
			return herr
		}
		if hv == nil {
			return fmt.Errorf("missing header record for %x", headerKey)
		}
		var header types.Header
		if err = rlp.DecodeBytes(hv, &header); err != nil {
			return fmt.Errorf("decoding header from %x: %v", v, err)
		}
		if header.Number.Uint64() > 1 {
			var parentK [40]byte
			binary.BigEndian.PutUint64(parentK[:], header.Number.Uint64()-1)
			copy(parentK[8:], header.ParentHash[:])
			if !bytes.Equal(parentK[:], prevHeaderKey[:]) {
				fmt.Printf("broken ancestry from %d %x (parent hash %x): prevKey %x\n", header.Number.Uint64(), v, header.ParentHash, prevHeaderKey)
			}
		}
		copy(prevHeaderKey[:], headerKey[:])
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func trimTxs(chaindata string) error {
	db := kv2.MustOpen(chaindata).RwKV()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	lastTxId, err := tx.ReadSequence(dbutils.EthTx)
	if err != nil {
		return err
	}
	txs, err1 := tx.RwCursor(dbutils.EthTx)
	if err1 != nil {
		return err1
	}
	defer txs.Close()
	bodies, err2 := tx.Cursor(dbutils.BlockBodyPrefix)
	if err2 != nil {
		return err
	}
	defer bodies.Close()
	toDelete := roaring64.New()
	toDelete.AddRange(0, lastTxId)
	// Exclude transaction that are used, from the range
	for k, v, err := bodies.First(); k != nil; k, v, err = bodies.Next() {
		if err != nil {
			return err
		}
		var body types.BodyForStorage
		if err = rlp.DecodeBytes(v, &body); err != nil {
			return err
		}
		// Remove from the map
		toDelete.RemoveRange(body.BaseTxId, body.BaseTxId+uint64(body.TxAmount))
	}
	fmt.Printf("Number of tx records to delete: %d\n", toDelete.GetCardinality())
	// Takes 20min to iterate 1.4b
	toDelete2 := roaring64.New()
	var iterated int
	for k, _, err := txs.First(); k != nil; k, _, err = txs.Next() {
		if err != nil {
			return err
		}
		toDelete2.Add(binary.BigEndian.Uint64(k))
		iterated++
		if iterated%100_000_000 == 0 {
			fmt.Printf("Iterated %d\n", iterated)
		}
	}
	fmt.Printf("Number of tx records: %d\n", toDelete2.GetCardinality())
	toDelete.And(toDelete2)
	fmt.Printf("Number of tx records to delete: %d\n", toDelete.GetCardinality())
	fmt.Printf("Roaring size: %d\n", toDelete.GetSizeInBytes())

	iter := toDelete.Iterator()
	for {
		var deleted int
		for iter.HasNext() {
			txId := iter.Next()
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], txId)
			if err = txs.Delete(key[:], nil); err != nil {
				return err
			}
			deleted++
			if deleted >= 10_000_000 {
				break
			}
		}
		if deleted == 0 {
			fmt.Printf("Nothing more to delete\n")
			break
		}
		fmt.Printf("Committing after deleting %d records\n", deleted)
		if err = tx.Commit(); err != nil {
			return err
		}
		txs.Close()
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		txs, err = tx.RwCursor(dbutils.EthTx)
		if err != nil {
			return err
		}
		defer txs.Close()
	}
	return nil
}

func scanTxs(chaindata string) error {
	db := kv2.MustOpen(chaindata).RwKV()
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(dbutils.EthTx)
	if err != nil {
		return err
	}
	defer c.Close()
	trTypes := make(map[byte]int)
	trTypesAl := make(map[byte]int)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		var tr types.Transaction
		if tr, err = types.DecodeTransaction(rlp.NewStream(bytes.NewReader(v), 0)); err != nil {
			return err
		}
		if _, ok := trTypes[tr.Type()]; !ok {
			fmt.Printf("Example for type %d:\n%x\n", tr.Type(), v)
		}
		trTypes[tr.Type()]++
		if tr.GetAccessList().StorageKeys() > 0 {
			if _, ok := trTypesAl[tr.Type()]; !ok {
				fmt.Printf("Example for type %d with AL:\n%x\n", tr.Type(), v)
			}
			trTypesAl[tr.Type()]++
		}
	}
	fmt.Printf("Transaction types: %v\n", trTypes)
	return nil
}

func scanReceipts(chaindata string, block uint64) error {
	dbdb := kv2.MustOpen(chaindata).RwKV()
	defer dbdb.Close()
	txtx, err := dbdb.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer txtx.Rollback()
	var db ethdb.Database = kv2.WrapIntoTxDB(txtx)
	var tx ethdb.Tx
	if hasTx, ok := db.(ethdb.HasTx); ok {
		tx = hasTx.Tx()
	} else {
		return fmt.Errorf("no transaction")
	}
	var key [8]byte
	var v []byte
	binary.BigEndian.PutUint64(key[:], block)
	if v, err = tx.GetOne(dbutils.BlockReceiptsPrefix, key[:]); err != nil {
		return err
	}
	fmt.Printf("blockNum = %d, receipt %x\n", block, v)
	return nil
}

var txParseTests = []struct {
	payloadStr  string
	senderStr   string
	idHashStr   string
	signHashStr string
}{
	// Legacy unprotected
	{payloadStr: "f86a808459682f0082520894fe3b557e8fb62b89f4916b721be55ceb828dbd73872386f26fc10000801ca0d22fc3eed9b9b9dbef9eec230aa3fb849eff60356c6b34e86155dca5c03554c7a05e3903d7375337f103cb9583d97a59dcca7472908c31614ae240c6a8311b02d6",
		senderStr: "fe3b557e8fb62b89f4916b721be55ceb828dbd73", idHashStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		signHashStr: "e2b043ecdbcfed773fe7b5ffc2e23ec238081c77137134a06d71eedf9cdd81d3"},
	// Legacy protected (EIP-155) from calveras, with chainId 123
	{payloadStr: "f86d808459682f0082520894e80d2a018c813577f33f9e69387dc621206fb3a48856bc75e2d63100008082011aa04ae3cae463329a32573f4fbf1bd9b011f93aecf80e4185add4682a03ba4a4919a02b8f05f3f4858b0da24c93c2a65e51b2fbbecf5ffdf97c1f8cc1801f307dc107",
		senderStr: "1041afbcb359d5a8dc58c15b2ff51354ff8a217d", idHashStr: "f4a91979624effdb45d2ba012a7995c2652b62ebbeb08cdcab00f4923807aa8a",
		signHashStr: "ff44cf01ee9b831f09910309a689e8da83d19aa60bad325ee9154b7c25cf4de8"},
	{payloadStr: "b86d02f86a7b80843b9aca00843b9aca0082520894e80d2a018c813577f33f9e69387dc621206fb3a48080c001a02c73a04cd144e5a84ceb6da942f83763c2682896b51f7922e2e2f9a524dd90b7a0235adda5f87a1d098e2739e40e83129ff82837c9042e6ad61d0481334dcb6f1a",
		senderStr: "e80d2a018c813577f33f9e69387dc621206fb3a4", idHashStr: "1247438da30b5919f1401eff4422fd11added646eff41278cd5276a5d3df802e",
		signHashStr: "34ef1790ebd860a84c73ba27576ae96621ec21e96f70935c94e8e24dc1b62f2b"},
	{payloadStr: "b86e01f86b7b018203e882520894236ff1e97419ae93ad80cafbaa21220c5d78fb7d880de0b6b3a764000080c080a0987e3d8d0dcd86107b041e1dca2e0583118ff466ad71ad36a8465dd2a166ca2da02361c5018e63beea520321b290097cd749febc2f437c7cb41fdd085816742060",
		senderStr: "4774e55994fce67b26c94716612c7048dcbf2dcd", idHashStr: "dec28fbfd19eb82ba91437922ea91d550d2861efb8cc7a4040b0f5efd3658284",
		signHashStr: "1ee032826e5aa14bc7353bf9f3af8683fd9f657a779879ff562ddcab0ecda30a"},
	{payloadStr: "f86780862d79883d2000825208945df9b87991262f6ba471f09758cde1c0fc1de734827a69801ca088ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0a045e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
		senderStr: "a1e4380a3b1f749673e270229993ee55f35663b4", idHashStr: "5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
		signHashStr: "19b1e28c14f33e74b96b88eba97d4a4fc8a97638d72e972310025b7e1189b049"},
	{payloadStr: "b903a301f9039f018218bf85105e34df0083048a949410a0847c2d170008ddca7c3a688124f49363003280b902e4c11695480000000000000000000000004b274e4a9af31c20ed4151769a88ffe63d9439960000000000000000000000008510211a852f0c5994051dd85eaef73112a82eb5000000000000000000000000000000000000000000000000000000000000012000000000000000000000000000bad4de000000000000000000000000607816a600000000000000000000000000000000000000000000000000000000000002200000000000000000000000000000000000000000000000000000001146aa2600000000000000000000000000000000000000000000000000000000000001bc9b000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee000000000000000000000000482579f93dc13e6b434e38b5a0447ca543d88a4600000000000000000000000000000000000000000000000000000000000000c42df546f40000000000000000000000004b274e4a9af31c20ed4151769a88ffe63d943996000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0000000000000000000000007d93f93d41604572119e4be7757a7a4a43705f080000000000000000000000000000000000000000000000003782dace9d90000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000082b5a61569b5898ac347c82a594c86699f1981aa88ca46a6a00b8e4f27b3d17bdf3714e7c0ca6a8023b37cca556602fce7dc7daac3fcee1ab04bbb3b94c10dec301cc57266db6567aa073efaa1fa6669bdc6f0877b0aeab4e33d18cb08b8877f08931abf427f11bade042177db48ca956feb114d6f5d56d1f5889047189562ec545e1c000000000000000000000000000000000000000000000000000000000000f84ff7946856ccf24beb7ed77f1f24eee5742f7ece5557e2e1a00000000000000000000000000000000000000000000000000000000000000001d694b1dd690cc9af7bb1a906a9b5a94f94191cc553cec080a0d52f3dbcad3530e73fcef6f4a75329b569a8903bf6d8109a960901f251a37af3a00ecf570e0c0ffa6efdc6e6e49be764b6a1a77e47de7bb99e167544ffbbcd65bc",
		senderStr: "1ced2cef30d40bb3617f8d455071b69f3b12d06f", idHashStr: "851bad0415758075a1eb86776749c829b866d43179c57c3e4a4b9359a0358231",
		signHashStr: "894d999ea27537def37534b3d55df3fed4e1492b31e9f640774432d21cf4512c"},
	{payloadStr: "b8d202f8cf7b038502540be40085174876e8008301869f94e77162b7d2ceb3625a4993bab557403a7b706f18865af3107a400080f85bf85994de0b295669a9fd93d5f28d9ec85e40f4cb697baef842a00000000000000000000000000000000000000000000000000000000000000003a0000000000000000000000000000000000000000000000000000000000000000780a0f73da48f3f5c9f324dfd28d106dcf911b53f33c92ae068cf6135352300e7291aa06ee83d0f59275d90000ac8cf912c6eb47261d244c9db19ffefc49e52869ff197",
		senderStr: "0961ca10d49b9b8e371aa0bcf77fe5730b18f2e4", idHashStr: "27db095399b22dc311aaab4d9ed45195873fdff1288fdc7c4e6dc1bfb17c061a",
		signHashStr: "35fbc0cd33a181e62b7432338f172106886a1396e1e3647ddf1e756740d81ae1"},
}

func testTxPool() error {
	ctx := txpool.NewTxParseContext()
	for _, tt := range txParseTests {
		var payload []byte
		var err error
		if payload, err = hex.DecodeString(tt.payloadStr); err != nil {
			return err
		}
		if _, _, err = ctx.ParseTransaction(payload, 0); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()

	log.SetupDefaultTerminalLogger(log.Lvl(*verbosity), "", "")

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

	var err error
	switch *action {
	case "cfg":
		flow.TestGenCfg()

	case "bucketStats":
		err = bucketStats(*chaindata)

	case "syncChart":
		mychart()

	case "testBlockHashes":
		testBlockHashes(*chaindata, *block, common.HexToHash(*hash))

	case "invTree":
		invTree("root", "right", "diff", *name)

	case "readAccount":
		if err := readAccount(*chaindata, common.HexToAddress(*account)); err != nil {
			fmt.Printf("Error: %v\n", err)
		}

	case "nextIncarnation":
		nextIncarnation(*chaindata, common.HexToHash(*account))

	case "dumpStorage":
		dumpStorage()

	case "current":
		printCurrentBlockNumber(*chaindata)

	case "bucket":
		printBucket(*chaindata)

	case "val-tx-lookup-2":
		ValidateTxLookups2(*chaindata)

	case "modiAccounts":
		getModifiedAccounts(*chaindata)

	case "slice":
		dbSlice(*chaindata, *bucket, common.FromHex(*hash))

	case "getProof":
		err = testGetProof(*chaindata, common.HexToAddress(*account), *rewind, false)

	case "regenerateIH":
		err = regenerate(*chaindata)

	case "searchChangeSet":
		err = searchChangeSet(*chaindata, common.FromHex(*hash), uint64(*block))

	case "searchStorageChangeSet":
		err = searchStorageChangeSet(*chaindata, common.FromHex(*hash), uint64(*block))

	case "changeSetStats":
		err = changeSetStats(*chaindata, uint64(*block), uint64(*block)+uint64(*rewind))

	case "supply":
		err = supply(*chaindata)

	case "extractCode":
		err = extractCode(*chaindata)

	case "iterateOverCode":
		err = iterateOverCode(*chaindata)

	case "mint":
		err = mint(*chaindata, uint64(*block))

	case "extractHeaders":
		err = extractHeaders(*chaindata, uint64(*block))

	case "extractHashes":
		err = extractHashes(*chaindata, uint64(*block), uint64(*blockTotal), *name)

	case "defrag":
		err = db.Defrag()

	case "textInfo":
		err = db.TextInfo(*chaindata, &strings.Builder{})

	case "extractBodies":
		err = extractBodies(*chaindata, uint64(*block))

	case "fixUnwind":
		err = fixUnwind(*chaindata)

	case "repairCurrent":
		repairCurrent()

	case "printBranches":
		printBranches(uint64(*block))

	case "printFullNodeRLPs":
		printFullNodeRLPs()

	case "rlpIndices":
		rlpIndices()

	case "hashFile":
		hashFile()

	case "trieChart":
		trieChart()

	case "printTxHashes":
		printTxHashes()

	case "snapSizes":
		err = snapSizes(*chaindata)

	case "dumpState":
		err = dumpState(*chaindata)

	case "readCallTraces":
		err = readCallTraces(*chaindata, uint64(*block))

	case "fixTd":
		err = fixTd(*chaindata)

	case "advanceExec":
		err = advanceExec(*chaindata)

	case "backExec":
		err = backExec(*chaindata)

	case "fixState":
		err = fixState(*chaindata)

	case "unwind":
		err = unwind(*chaindata, uint64(*block))

	case "trimTxs":
		err = trimTxs(*chaindata)

	case "scanTxs":
		err = scanTxs(*chaindata)

	case "scanReceipts":
		err = scanReceipts(*chaindata, uint64(*block))

	case "testTxPool":
		err = testTxPool()
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
