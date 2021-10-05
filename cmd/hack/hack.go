package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/patricia"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/params"
	"github.com/wcharczuk/go-chart/v2"

	"github.com/flanglet/kanzi-go/transform"

	hackdb "github.com/ledgerwatch/erigon/cmd/hack/db"
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
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
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
	err := chart.ReadLines(filename, func(line string) error {
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
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: blocks,
		YValues: hours,
	}
	badgerSeries := &chart.ContinuousSeries{
		Name: "Cumulative sync time (badger)",
		Style: chart.Style{
			StrokeColor: chart.ColorRed,
			FillColor:   chart.ColorRed.WithAlpha(100),
		},
		XValues: blocks0,
		YValues: hours0,
	}
	dbsizeSeries := &chart.ContinuousSeries{
		Name: "Database size (bolt)",
		Style: chart.Style{

			StrokeColor: chart.ColorBlack,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: blocks,
		YValues: dbsize,
	}
	dbsizeSeries0 := &chart.ContinuousSeries{
		Name: "Database size (badger)",
		Style: chart.Style{

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
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d h", int(v.(float64)))
			},
			GridMajorStyle: chart.Style{

				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: days(),
		},
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d G", int(v.(float64)))
			},
		},
		XAxis: chart.XAxis{
			Name:  "Blocks, million",
			Style: chart.Style{},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{

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

			StrokeColor: chart.ColorYellow,
			FillColor:   chart.ColorYellow.WithAlpha(100),
		},
		XValues: blocks,
		YValues: heap,
	}
	trienodesSeries := &chart.ContinuousSeries{
		Name: "Trie nodes",
		Style: chart.Style{

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
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f G", v.(float64))
			},
			GridMajorStyle: chart.Style{

				StrokeColor: chart.ColorYellow,
				StrokeWidth: 1.0,
			},
			GridLines: days(),
		},
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f m", v.(float64))
			},
		},
		XAxis: chart.XAxis{
			Name:  "Blocks, million",
			Style: chart.Style{},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{

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
	/*
		ethDb := mdbx.MustOpen(chaindata)
		defer ethDb.Close()

		var bucketList []string
		if err1 := ethDb.View(context.Background(), func(txa kv.Tx) error {
			if bl, err := txa.(kv.BucketMigrator).ListBuckets(); err == nil {
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
		switch db := ethDb.(type) {
		case *mdbx.MdbxKV:
			type MdbxStat interface {
				BucketStat(name string) (*mdbx.Stat, error)
			}

			if err := db.View(context.Background(), func(tx kv.Tx) error {
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
	*/
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

				StrokeColor: chart.GetAlternateColor(i),
			},
			XValues: thresholds,
			YValues: counts[i],
		}
	}
	xaxis := &chart.XAxis{
		Name:  "Dust theshold",
		Style: chart.Style{},
		ValueFormatter: func(v interface{}) string {
			return fmt.Sprintf("%d wei", int(v.(float64)))
		},
		GridMajorStyle: chart.Style{

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
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%dm", int(v.(float64)/1e6))
			},
			GridMajorStyle: chart.Style{

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
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fm", v.(float64)/1e6)
			},
			GridMajorStyle: chart.Style{

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
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fk", v.(float64)/1e3)
			},
			GridMajorStyle: chart.Style{

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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	if err := db.View(context.Background(), func(tx kv.Tx) error {
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
	ethDb := mdbx.MustOpen(chaindata)
	defer ethDb.Close()
	tool.Check(ethDb.View(context.Background(), func(tx kv.Tx) error {
		blocksToSearch := 10000000
		for i := uint64(block); i < uint64(block+blocksToSearch); i++ {
			hash, err := rawdb.ReadCanonicalHash(tx, i)
			if err != nil {
				panic(err)
			}
			header := rawdb.ReadHeader(tx, hash, i)
			if header.Root == stateRoot || stateRoot == (common.Hash{}) {
				fmt.Printf("\n===============\nCanonical hash for %d: %x\n", i, hash)
				fmt.Printf("Header.Root: %x\n", header.Root)
				fmt.Printf("Header.TxHash: %x\n", header.TxHash)
				fmt.Printf("Header.UncleHash: %x\n", header.UncleHash)
			}
		}
		return nil
	}))
}

func printCurrentBlockNumber(chaindata string) {
	ethDb := mdbx.MustOpen(chaindata)
	defer ethDb.Close()
	ethDb.View(context.Background(), func(tx kv.Tx) error {
		hash := rawdb.ReadHeadBlockHash(tx)
		number := rawdb.ReadHeaderNumber(tx, hash)
		fmt.Printf("Block number: %d\n", *number)
		return nil
	})
}

func printTxHashes() {
	db := mdbx.MustOpen(paths.DefaultDataDir() + "/geth/chaindata")
	defer db.Close()
	if err := db.View(context.Background(), func(tx kv.Tx) error {
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

func readAccount(chaindata string, account common.Address) error {
	db := mdbx.MustOpen(chaindata)
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

	c, err := tx.Cursor(kv.PlainState)
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
	ethDb := mdbx.MustOpen(chaindata)
	defer ethDb.Close()
	var found bool
	var incarnationBytes [common.IncarnationLength]byte
	startkey := make([]byte, common.HashLength+common.IncarnationLength+common.HashLength)
	var fixedbits = 8 * common.HashLength
	copy(startkey, addrHash[:])
	tool.Check(ethDb.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.HashedStorage)
		if err != nil {
			return err
		}
		defer c.Close()
		return ethdb.Walk(c, startkey, fixedbits, func(k, v []byte) (bool, error) {
			fmt.Printf("Incarnation(z): %d\n", 0)
			copy(incarnationBytes[:], k[common.HashLength:])
			found = true
			return false, nil
		})
	}))
	if found {
		fmt.Printf("Incarnation: %d\n", (binary.BigEndian.Uint64(incarnationBytes[:]))+1)
		return
	}
	fmt.Printf("Incarnation(f): %d\n", state.FirstContractIncarnation)
}

func repairCurrent() {
	historyDb := mdbx.MustOpen("/Volumes/tb4/erigon/ropsten/geth/chaindata")
	defer historyDb.Close()
	currentDb := mdbx.MustOpen("statedb")
	defer currentDb.Close()
	tool.Check(historyDb.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.ClearBucket(kv.HashedStorage)
	}))
	tool.Check(historyDb.Update(context.Background(), func(tx kv.RwTx) error {
		newB, err := tx.RwCursor(kv.HashedStorage)
		if err != nil {
			return err
		}
		count := 0
		if err := currentDb.View(context.Background(), func(ctx kv.Tx) error {
			c, err := ctx.Cursor(kv.HashedStorage)
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
	db := mdbx.MustOpen(paths.DefaultDataDir() + "/geth/chaindata")
	defer db.Close()
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.StorageHistory, nil, func(k, v []byte) error {
			fmt.Printf("%x %x\n", k, v)
			return nil
		})
	}); err != nil {
		panic(err)
	}
}

func printBucket(chaindata string) {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	f, err := os.Create("bucket.txt")
	tool.Check(err)
	defer f.Close()
	fb := bufio.NewWriter(f)
	defer fb.Flush()
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.StorageHistory)
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
	db := mdbx.MustOpen(chaindata)
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

func validateTxLookups2(db kv.RwDB, startBlock uint64, interruptCh chan bool) {
	tx, err := db.BeginRo(context.Background())
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
		body := rawdb.ReadBodyWithTransactions(tx, blockHash, blockNum)

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
			val, err := tx.GetOne(kv.TxLookup, txn.Hash().Bytes())
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
	db := mdbx.MustOpen(chaindata)
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
	db := mdbx.MustOpen(chaindata)
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

	accountMap := make(map[string]*accounts.Account)

	if err := changeset.ForRange(tx, kv.AccountChangeSet, block+1, *headNumber+1, func(blockN uint64, address, v []byte) error {
		var addrHash, err = common.HashData(address)
		if err != nil {
			return err
		}
		k := addrHash[:]

		if _, ok := accountMap[string(k)]; !ok {
			if len(v) > 0 {
				var a accounts.Account
				if innerErr := a.DecodeForStorage(v); innerErr != nil {
					return innerErr
				}
				accountMap[string(k)] = &a
			} else {
				accountMap[string(k)] = nil
			}
		}
		return nil
	}); err != nil {
		return err
	}
	runtime.ReadMemStats(&m)
	log.Info("Constructed account map", "size", len(accountMap),
		"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	storageMap := make(map[string][]byte)
	if err := changeset.ForRange(tx, kv.StorageChangeSet, block+1, *headNumber+1, func(blockN uint64, address, v []byte) error {
		var addrHash, err = common.HashData(address)
		if err != nil {
			return err
		}
		k := addrHash[:]
		if _, ok := storageMap[string(k)]; !ok {
			storageMap[string(k)] = v
		}
		return nil
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
				if codeHash, err1 := tx.GetOne(kv.ContractCode, dbutils.GenerateStoragePrefix([]byte(ks), acc.Incarnation)); err1 == nil {
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
	_, err = loader.CalcTrieRoot(tx, nil, nil)
	if err != nil {
		return err
	}
	r := &Receiver{defaultReceiver: trie.NewRootHashAggregator(), unfurlList: unfurlList, accountMap: accountMap, storageMap: storageMap}
	r.defaultReceiver.Reset(nil, nil /* HashCollector */, false)
	loader.SetStreamReceiver(r)
	root, err := loader.CalcTrieRoot(tx, nil, nil)
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

// dumpState writes the content of current state into a file with given name
func dumpState(chaindata string, statefile string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	f, err := os.Create(statefile)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	i := 0
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.PlainState)
		if err != nil {
			return err
		}
		var count uint64
		if count, err = c.Count(); err != nil {
			return err
		}
		// Write out number of key/value pairs first
		var countBytes [8]byte
		binary.BigEndian.PutUint64(countBytes[:], count)
		if _, err = w.Write(countBytes[:]); err != nil {
			return err
		}
		k, v, e := c.First()
		for ; k != nil && e == nil; k, v, e = c.Next() {
			if err = w.WriteByte(byte(len(k))); err != nil {
				return err
			}
			if _, err = w.Write(k); err != nil {
				return err
			}
			if err = w.WriteByte(byte(len(v))); err != nil {
				return err
			}
			if len(v) > 0 {
				if _, err = w.Write(v); err != nil {
					return err
				}
			}
			i++
			if i%1_000_000 == 0 {
				log.Info("Written into file", "key/value pairs", i)
			}
		}
		if e != nil {
			return e
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func mphf(chaindata string, block uint64) error {
	// Create a file to compress if it does not exist already
	statefile := "statedump.dat"
	if _, err := os.Stat(statefile); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("not sure if statedump.dat exists: %v", err)
		}
		if err = dumpState(chaindata, statefile); err != nil {
			return err
		}
	}
	var rs *recsplit.RecSplit
	f, err := os.Open(statefile)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	defer f.Close()
	var countBuf [8]byte
	if _, err = io.ReadFull(r, countBuf[:]); err != nil {
		return err
	}
	count := binary.BigEndian.Uint64(countBuf[:])
	if block > 1 {
		count = block
	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   int(count),
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     "",
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
	}); err != nil {
		return err
	}
	var buf [256]byte
	l, e := r.ReadByte()
	i := 0
	for ; e == nil; l, e = r.ReadByte() {
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if i%1 == 0 {
			// It is key, we skip the values here
			if err := rs.AddKey(buf[:l]); err != nil {
				return err
			}
		}
		i++
		if i == int(count*2) {
			break
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	start := time.Now()
	log.Info("Building recsplit...")
	if err = rs.Build(); err != nil {
		return err
	}
	s1, s2 := rs.Stats()
	log.Info("Done", "time", time.Since(start), "s1", s1, "s2", s2)
	log.Info("Testing bijection")
	bitCount := (count + 63) / 64
	bits := make([]uint64, bitCount)
	if _, err = f.Seek(8, 0); err != nil {
		return err
	}
	r = bufio.NewReader(f)
	l, e = r.ReadByte()
	i = 0
	var lookupTime time.Duration
	for ; e == nil; l, e = r.ReadByte() {
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if i%1 == 0 {
			// It is key, we skip the values here
			start := time.Now()
			idx := rs.Lookup(buf[:l], false /* trace */)
			lookupTime += time.Since(start)
			if idx >= int(count) {
				return fmt.Errorf("idx %d >= count %d", idx, count)
			}
			mask := uint64(1) << (idx & 63)
			if bits[idx>>6]&mask != 0 {
				return fmt.Errorf("no bijection key idx=%d, lookup up idx = %d", i, idx)
			}
			bits[idx>>6] |= mask
		}
		i++
		if i == int(count*2) {
			break
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	log.Info("Average lookup time", "per key", time.Duration(uint64(lookupTime)/count))
	return nil
}

// genstate generates statedump.dat file for testing
func genstate() error {
	f, err := os.Create("statedump.dat")
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	var count uint64 = 25
	var countBuf [8]byte
	binary.BigEndian.PutUint64(countBuf[:], count)
	if _, err = w.Write(countBuf[:]); err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			key := fmt.Sprintf("addr%dxlocation%d", i, j)
			val := "value"
			if err = w.WriteByte(byte(len(key))); err != nil {
				return err
			}
			if _, err = w.Write([]byte(key)); err != nil {
				return err
			}
			if err = w.WriteByte(byte(len(val))); err != nil {
				return err
			}
			if _, err = w.Write([]byte(val)); err != nil {
				return err
			}
		}
	}
	return nil
}

// processSuperstring is the worker that processes one superstring and puts results
// into the collector, using lock to mutual exclusion. At the end (when the input channel is closed),
// it notifies the waitgroup before exiting, so that the caller known when all work is done
// No error channels for now
func processSuperstring(superstringCh chan []byte, dictCollector *etl.Collector, completion *sync.WaitGroup) {
	for superstring := range superstringCh {
		//log.Info("Superstring", "len", len(superstring))
		sa := make([]int32, len(superstring))
		divsufsort, err := transform.NewDivSufSort()
		if err != nil {
			log.Error("processSuperstring", "create divsufsoet", err)
		}
		//start := time.Now()
		divsufsort.ComputeSuffixArray(superstring, sa)
		//log.Info("Suffix array built", "in", time.Since(start))
		// filter out suffixes that start with odd positions
		n := len(sa) / 2
		filtered := make([]int, n)
		var j int
		for i := 0; i < len(sa); i++ {
			if sa[i]&1 == 0 {
				filtered[j] = int(sa[i] >> 1)
				j++
			}
		}
		//log.Info("Suffix array filtered")
		// invert suffixes
		inv := make([]int, n)
		for i := 0; i < n; i++ {
			inv[filtered[i]] = i
		}
		//log.Info("Inverted array done")
		lcp := make([]byte, n)
		var k int
		// Process all suffixes one by one starting from
		// first suffix in txt[]
		for i := 0; i < n; i++ {
			/* If the current suffix is at n-1, then we don’t
			   have next substring to consider. So lcp is not
			   defined for this substring, we put zero. */
			if inv[i] == n-1 {
				k = 0
				continue
			}

			/* j contains index of the next substring to
			   be considered  to compare with the present
			   substring, i.e., next string in suffix array */
			j := int(filtered[inv[i]+1])

			// Directly start matching from k'th index as
			// at-least k-1 characters will match
			for i+k < n && j+k < n && superstring[(i+k)*2] != 0 && superstring[(j+k)*2] != 0 && superstring[(i+k)*2+1] == superstring[(j+k)*2+1] {
				k++
			}

			lcp[inv[i]] = byte(k) // lcp for the present suffix.

			// Deleting the starting character from the string.
			if k > 0 {
				k--
			}
		}
		//log.Info("Kasai algorithm finished")
		// Checking LCP array
		/*
			for i := 0; i < n-1; i++ {
				var prefixLen int
				p1 := int(filtered[i])
				p2 := int(filtered[i+1])
				for p1+prefixLen < n && p2+prefixLen < n && superstring[(p1+prefixLen)*2] != 0 && superstring[(p2+prefixLen)*2] != 0 && superstring[(p1+prefixLen)*2+1] == superstring[(p2+prefixLen)*2+1] {
					prefixLen++
				}
				if prefixLen != int(lcp[i]) {
					log.Error("Mismatch", "prefixLen", prefixLen, "lcp[i]", lcp[i])
				}
				l := int(lcp[i]) // Length of potential dictionary word
				if l < 2 {
					continue
				}
				dictKey := make([]byte, l)
				for s := 0; s < l; s++ {
					dictKey[s] = superstring[(filtered[i]+s)*2+1]
				}
				fmt.Printf("%d %d %s\n", filtered[i], lcp[i], dictKey)
			}
		*/
		//log.Info("LCP array checked")
		b := make([]int, 1000) // Sorting buffer
		// Walk over LCP array and compute the scores of the strings
		for i := 0; i < n-1; i++ {
			// Only when there is a drop in LCP value
			if lcp[i+1] >= lcp[i] {
				continue
			}
			j := i
			for l := int(lcp[i]); l > int(lcp[i+1]); l-- {
				if l < 5 {
					continue
				}
				// Go back
				var new bool
				for j > 0 && int(lcp[j-1]) >= l {
					j--
					new = true
				}
				if !new {
					break
				}
				window := i - j + 2
				for len(b) < window {
					b = append(b, 0)
				}
				copy(b, filtered[j:i+2])
				sort.Ints(b[:window])
				repeats := 1
				lastK := 0
				for k := 1; k < window; k++ {
					if b[k] >= b[lastK]+l {
						repeats++
						lastK = k
					}
				}
				if repeats > 128 {
					score := uint64(repeats * int(l-4))
					// Dictionary key is the concatenation of the score and the dictionary word (to later aggregate the scores from multiple chunks)
					dictKey := make([]byte, l)
					for s := 0; s < l; s++ {
						dictKey[s] = superstring[(filtered[i]+s)*2+1]
					}
					var dictVal [8]byte
					binary.BigEndian.PutUint64(dictVal[:], score)
					if err = dictCollector.Collect(dictKey, dictVal[:]); err != nil {
						log.Error("processSuperstring", "collect", err)
					}
				}
			}
		}
	}
	completion.Done()
}

type DictionaryItem struct {
	word  []byte
	score uint64
}

type DictionaryBuilder struct {
	limit         int
	lastWord      []byte
	lastWordScore uint64
	items         []DictionaryItem
}

func (db DictionaryBuilder) Len() int {
	return len(db.items)
}

func (db DictionaryBuilder) Less(i, j int) bool {
	if db.items[i].score == db.items[j].score {
		return bytes.Compare(db.items[i].word, db.items[j].word) < 0
	}
	return db.items[i].score < db.items[j].score
}

func (db *DictionaryBuilder) Swap(i, j int) {
	db.items[i], db.items[j] = db.items[j], db.items[i]
}

func (db *DictionaryBuilder) Push(x interface{}) {
	db.items = append(db.items, x.(DictionaryItem))
}

func (db *DictionaryBuilder) Pop() interface{} {
	old := db.items
	n := len(old)
	x := old[n-1]
	db.items = old[0 : n-1]
	return x
}

func (db *DictionaryBuilder) processWord(word []byte, score uint64) {
	heap.Push(db, DictionaryItem{word: word, score: score})
	if db.Len() > db.limit {
		// Remove the element with smallest score
		heap.Pop(db)
	}
}

func (db *DictionaryBuilder) compressLoadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	score := binary.BigEndian.Uint64(v)
	if bytes.Equal(k, db.lastWord) {
		db.lastWordScore += score
	} else {
		if db.lastWord != nil {
			db.processWord(db.lastWord, db.lastWordScore)
		}
		db.lastWord = common.CopyBytes(k)
		db.lastWordScore = score
	}
	return nil
}

func (db *DictionaryBuilder) finish() {
	if db.lastWord != nil {
		db.processWord(db.lastWord, db.lastWordScore)
	}
}

type DictAggregator struct {
	lastWord      []byte
	lastWordScore uint64
	collector     *etl.Collector
}

func (da *DictAggregator) processWord(word []byte, score uint64) error {
	var scoreBuf [8]byte
	binary.BigEndian.PutUint64(scoreBuf[:], score)
	return da.collector.Collect(word, scoreBuf[:])
}

func (da *DictAggregator) aggLoadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	score := binary.BigEndian.Uint64(v)
	if bytes.Equal(k, da.lastWord) {
		da.lastWordScore += score
	} else {
		if da.lastWord != nil {
			if err := da.processWord(da.lastWord, da.lastWordScore); err != nil {
				return err
			}
		}
		da.lastWord = common.CopyBytes(k)
		da.lastWordScore = score
	}
	return nil
}

func (da *DictAggregator) finish() error {
	if da.lastWord != nil {
		return da.processWord(da.lastWord, da.lastWordScore)
	}
	return nil
}

const CompressLogPrefix = "compress"

func compress(chaindata string, block uint64) error {
	// Create a file to compress if it does not exist already
	statefile := "statedump.dat"
	if _, err := os.Stat(statefile); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("not sure if statedump.dat exists: %v", err)
		}
		if err = dumpState(chaindata, statefile); err != nil {
			return err
		}
	}
	var superstring []byte
	const superstringLimit = 128 * 1024 * 1024
	// Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
	// We only consider values with length > 2, because smaller values are not compressible without going into bits
	f, err := os.Open(statefile)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	defer f.Close()
	// Collector for dictionary words (sorted by their score)
	tmpDir := ""
	// Read number of keys
	var countBuf [8]byte
	if _, err = io.ReadFull(r, countBuf[:]); err != nil {
		return err
	}
	count := binary.BigEndian.Uint64(countBuf[:])
	var stride int
	if block > 1 {
		stride = int(count) / int(block)
	}
	ch := make(chan []byte, runtime.NumCPU())
	var wg sync.WaitGroup
	wg.Add(runtime.NumCPU())
	collectors := make([]*etl.Collector, runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		collector := etl.NewCollector(tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		collectors[i] = collector
		go processSuperstring(ch, collector, &wg)
	}
	var buf [256]byte
	l, e := r.ReadByte()
	i := 0
	p := 0
	for ; e == nil; l, e = r.ReadByte() {
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if (stride == 0 || (i/2)%stride == 0) && l > 4 {
			if len(superstring)+2*int(l)+2 > superstringLimit {
				ch <- superstring
				superstring = nil
			}
			for _, a := range buf[:l] {
				superstring = append(superstring, 1, a)
			}
			superstring = append(superstring, 0, 0)
			p++
			if p%2_000_000 == 0 {
				log.Info("Dictionary preprocessing", "key/value pairs", p/2)
			}
		}
		i++
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if len(superstring) > 0 {
		ch <- superstring
	}
	close(ch)
	wg.Wait()
	dictCollector := etl.NewCollector(tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	dictAggregator := &DictAggregator{collector: dictCollector}
	for _, collector := range collectors {
		if err = collector.Load(CompressLogPrefix, nil /* db */, "" /* toBucket */, dictAggregator.aggLoadFunc, etl.TransformArgs{}); err != nil {
			return err
		}
		collector.Close(CompressLogPrefix)
	}
	if err = dictAggregator.finish(); err != nil {
		return err
	}
	// Aggregate the dictionary and build Aho-Corassick matcher
	defer dictCollector.Close(CompressLogPrefix)
	db := &DictionaryBuilder{limit: 16_000_000} // Only collect 16m words with highest scores
	if err = dictCollector.Load(CompressLogPrefix, nil /* db */, "" /* toBucket */, db.compressLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	db.finish()
	var df *os.File
	df, err = os.Create("dictionary.txt")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(df)
	// Sort dictionary builder
	sort.Sort(db)
	for _, item := range db.items {
		fmt.Fprintf(w, "%d %x\n", item.score, item.word)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	df.Close()
	return nil
}

type DictEntry struct {
	score uint64
	code  uint64
}

// DynamicCell represents result of dynamic programming for certain starting position
type DynamicCell struct {
	optimStart  int
	coverStart  int
	patterns    []int
	compression int
	score       uint64
}

type Ring struct {
	cells             []DynamicCell
	head, tail, count int
}

func NewRing() *Ring {
	return &Ring{
		cells: make([]DynamicCell, 16),
		head:  0,
		tail:  0,
		count: 0,
	}
}

func (r *Ring) Reset() {
	r.count = 0
	r.head = 0
	r.tail = 0
}

func (r *Ring) ensureSize() {
	if r.count < len(r.cells) {
		return
	}
	newcells := make([]DynamicCell, r.count*2)
	if r.tail > r.head {
		copy(newcells, r.cells[r.head:r.tail])
	} else {
		n := copy(newcells, r.cells[r.head:])
		copy(newcells[n:], r.cells[:r.tail])
	}
	r.head = 0
	r.tail = r.count
	r.cells = newcells
}

func (r *Ring) PushFront() *DynamicCell {
	r.ensureSize()
	if r.head == 0 {
		r.head = len(r.cells)
	}
	r.head--
	r.count++
	return &r.cells[r.head]
}

func (r *Ring) PushBack() *DynamicCell {
	r.ensureSize()
	if r.tail == len(r.cells) {
		r.tail = 0
	}
	result := &r.cells[r.tail]
	r.tail++
	r.count++
	return result
}

func (r Ring) Len() int {
	return r.count
}

func (r *Ring) Get(i int) *DynamicCell {
	if i < 0 || i >= r.count {
		return nil
	}
	return &r.cells[(r.head+i)&(len(r.cells)-1)]
}

// Truncate removes all items starting from i
func (r *Ring) Truncate(i int) {
	r.count = i
	r.tail = (r.head + i) & (len(r.cells) - 1)
}

func optimiseCluster(trace bool, numBuf []byte, input []byte, inputLen int, output []byte, matches []*patricia.Match, cover []bool, cellRing *Ring, dict map[string]DictEntry, usedDict map[string]int) []byte {
	if trace {
		fmt.Printf("Cluster | input = %x\n", input)
		for _, match := range matches {
			fmt.Printf(" [%x %d-%d]", match.Slice, match.Pos, match.Pos+len(match.Slice))
		}
	}
	cellRing.Reset()
	lastF := matches[len(matches)-1]
	for j := lastF.Pos; j < lastF.Pos+len(lastF.Slice); j++ {
		d := cellRing.PushBack()
		d.optimStart = j + 1
		d.coverStart = inputLen
		d.compression = 0
		d.patterns = nil
		d.score = 0
	}
	// Starting from the last match
	for i := len(matches); i > 0; i-- {
		f := matches[i-1]
		firstCell := cellRing.Get(0)
		if firstCell == nil {
			fmt.Printf("cellRing.Len() = %d\n", cellRing.Len())
		}
		maxCompression := firstCell.compression
		maxScore := firstCell.score
		maxCell := *firstCell
		var maxInclude bool
		for e := 0; e < cellRing.Len(); e++ {
			cell := cellRing.Get(e)
			comp := cell.compression - 4
			if cell.coverStart >= f.Pos+len(f.Slice) {
				comp += len(f.Slice)
			} else {
				comp += cell.coverStart - f.Pos
			}
			score := cell.score + binary.BigEndian.Uint64(f.Vals[0])
			if comp > maxCompression || (comp == maxCompression && score > maxScore) {
				maxCompression = comp
				maxScore = score
				maxInclude = true
				maxCell = *cell
			}
			if cell.optimStart > f.Pos+len(f.Slice) {
				cellRing.Truncate(e)
				break
			}
		}
		if maxInclude {
			if trace {
				fmt.Printf("[include] cell for %d: with patterns", f.Pos)
				fmt.Printf(" [%x %d-%d]", f.Slice, f.Pos, f.Pos+len(f.Slice))
				for _, pattern := range maxCell.patterns {
					fmt.Printf(" [%x %d-%d]", matches[pattern].Slice, matches[pattern].Pos, matches[pattern].Pos+len(matches[pattern].Slice))
				}
				fmt.Printf("\n\n")
			}
			d := cellRing.PushFront()
			d.optimStart = f.Pos
			d.coverStart = f.Pos
			d.patterns = append([]int{i - 1}, maxCell.patterns...)
			d.compression = maxCompression
			d.score = maxScore
		} else {
			if trace {
				fmt.Printf("cell for %d: with patterns", f.Pos)
				for _, pattern := range maxCell.patterns {
					fmt.Printf(" [%x %d-%d]", matches[pattern].Slice, matches[pattern].Pos, matches[pattern].Pos+len(matches[pattern].Slice))
				}
				fmt.Printf("\n\n")
			}
			d := cellRing.PushFront()
			d.optimStart = f.Pos
			d.coverStart = maxCell.coverStart
			d.patterns = maxCell.patterns
			d.compression = maxCompression
			d.score = maxScore
		}
	}
	optimCell := cellRing.Get(0)
	if trace {
		fmt.Printf("optimal =")
	}
	p := binary.PutUvarint(numBuf, uint64(len(optimCell.patterns)))
	output = append(output, numBuf[:p]...)
	for _, pattern := range optimCell.patterns {
		if trace {
			fmt.Printf(" [%x %d-%d]", matches[pattern].Slice, matches[pattern].Pos, matches[pattern].Pos+len(matches[pattern].Slice))
		}
		for j := matches[pattern].Pos; j < matches[pattern].Pos+len(matches[pattern].Slice); j++ {
			cover[j] = true
		}
		// Starting position
		p := binary.PutUvarint(numBuf, uint64(matches[pattern].Pos))
		output = append(output, numBuf[:p]...)
		// Code
		p = binary.PutUvarint(numBuf, uint64(dict[string(matches[pattern].Slice)].code))
		output = append(output, numBuf[:p]...)
		usedDict[string(matches[pattern].Slice)]++
	}
	if trace {
		fmt.Printf("\n\n")
	}
	return output
}

func reduceDictWorker(numBuf []byte, input []byte, output []byte, cover []bool, cellRing *Ring, trie patricia.PatriciaTree, mf *patricia.MatchFinder, dict map[string]DictEntry, usedDict map[string]int) ([]byte, []bool) {
	// Write length of the string
	p := binary.PutUvarint(numBuf, uint64(len(input)))
	output = append(output, numBuf[:p]...)
	matches := mf.FindLongestMatches(trie, input)
	if len(matches) > 0 {
		for i := 0; i < len(input); i++ {
			if i == len(cover) {
				cover = append(cover, false)
			} else {
				cover[i] = false
			}
		}
		output = optimiseCluster(false, numBuf, input, len(input), output, matches, cover, cellRing, dict, usedDict)
		// Add uncoded input
		for i := 0; i < len(input); i++ {
			if !cover[i] {
				output = append(output, input[i])
			}
		}
	} else {
		if len(input) > 0 {
			p := binary.PutUvarint(numBuf, 0)
			output = append(output, numBuf[:p]...)
			output = append(output, input...)
		}
	}
	return output, cover
}

func decode(output []byte, invDict map[uint64][]byte) ([]byte, error) {
	r := bytes.NewReader(output)
	var l uint64
	var err error
	if l, err = binary.ReadUvarint(r); err != nil {
		return nil, err
	}
	input := make([]byte, l)
	if l == 0 {
		return input, nil
	}
	cover := make([]bool, l) // Which characters are covered by the pattens
	var p uint64             // Number of patterns
	if p, err = binary.ReadUvarint(r); err != nil {
		return nil, err
	}
	// Now reading patterns one by one
	for i := 0; i < int(p); i++ {
		var pos uint64 // Starting position for pattern
		if pos, err = binary.ReadUvarint(r); err != nil {
			return nil, err
		}
		var code uint64 // Code of the pattern
		if code, err = binary.ReadUvarint(r); err != nil {
			return nil, err
		}
		word := invDict[code]
		for j := 0; j < len(word); j++ {
			input[int(pos)+j] = word[j]
			cover[int(pos)+j] = true
		}
	}
	// Read uncovered characters
	for i := 0; i < int(l); i++ {
		if !cover[i] {
			if input[i], err = r.ReadByte(); err != nil {
				return nil, err
			}
		}
	}
	return input, nil
}

// reduceDict reduces the dictionary by trying the substitutions and counting frequency for each word
func reducedict(block int) error {
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Error("Failure in running pprof server", "err", err)
		}
	}()
	// Read up the dictionary
	df, err := os.Open("reduced_dictionary.txt")
	if err != nil {
		return err
	}
	defer df.Close()
	// DictonaryBuilder is for sorting words by their freuency (to assign codes)
	var pt patricia.PatriciaTree
	dict := make(map[string]DictEntry)
	invDict := make(map[uint64][]byte)
	ds := bufio.NewScanner(df)
	for ds.Scan() {
		tokens := strings.Split(ds.Text(), " ")
		var score int64
		if score, err = strconv.ParseInt(tokens[0], 10, 64); err != nil {
			return err
		}
		var word []byte
		if word, err = hex.DecodeString(tokens[1]); err != nil {
			return err
		}
		var scoreBuf [8]byte
		binary.BigEndian.PutUint64(scoreBuf[:], uint64(score))
		pt.Insert(word, scoreBuf[:])
		code := uint64(uint64(len(dict)))
		dict[string(word)] = DictEntry{score: uint64(score), code: code}
		invDict[code] = word
	}
	df.Close()
	log.Info("dictionary file parsed", "entries", len(dict))
	statefile := "statedump.dat"
	f, err := os.Open(statefile)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Seek(8, 0); err != nil {
		return err
	}

	usedDict := make(map[string]int)
	var inputSize int
	var outputSize int
	r := bufio.NewReader(f)
	var buf [256]byte
	var output []byte = make([]byte, 0, 256)
	var cover []bool = make([]bool, 256)
	cellRing := NewRing()
	numBuf := make([]byte, binary.MaxVarintLen64)
	l, e := r.ReadByte()
	i := 0
	var mf patricia.MatchFinder
	for ; e == nil; l, e = r.ReadByte() {
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		output, cover = reduceDictWorker(numBuf, buf[:l], output[:0], cover, cellRing, pt, &mf, dict, usedDict)
		/*
			var input []byte
			if input, err = decode(output, invDict); err != nil {
				return fmt.Errorf("decoding compression output %x for input %x: %v", output, buf[:l], err)
			}
			if !bytes.Equal(input, buf[:l]) {
				return fmt.Errorf("decoded %x, expected %x", input, buf[:l])
			}
		*/
		inputSize += 1 + int(l)
		outputSize += len(output)
		i++
		if block > 1 && i == block {
			break
		}
		if i%2_000_000 == 0 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Replacement preprocessing", "key/value pairs", i/2, "output", common.StorageSize(outputSize), "input", common.StorageSize(inputSize), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Done", "output", common.StorageSize(outputSize), "input", common.StorageSize(inputSize), "effective dict size", len(usedDict), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	var rf *os.File
	rf, err = os.Create("reduced_dictionary.txt")
	if err != nil {
		return err
	}
	rw := bufio.NewWriter(rf)
	// Optimise the dictionary
	var db DictionaryBuilder
	for word, used := range usedDict {
		db.items = append(db.items, DictionaryItem{word: []byte(word), score: uint64(used)})
	}
	sort.Sort(&db)
	for i := len(db.items); i > 0; i-- {
		if db.items[i-1].score < 128 {
			break
		}
		fmt.Fprintf(rw, "%d %x\n", db.items[i-1].score, db.items[i-1].word)
	}
	if err = rw.Flush(); err != nil {
		return err
	}
	rf.Close()

	return nil
}

func changeSetStats(chaindata string, block1, block2 uint64) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()

	fmt.Printf("State stats\n")
	stAccounts := 0
	stStorage := 0
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.PlainState)
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
	tx, err1 := db.BeginRw(context.Background())
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()
	if err := changeset.ForRange(tx, kv.AccountChangeSet, block1, block2, func(blockN uint64, k, v []byte) error {
		if (blockN-block1)%100000 == 0 {
			fmt.Printf("at the block %d for accounts, booster size: %d\n", blockN, len(accounts))
		}
		accounts[string(common.CopyBytes(k))] = struct{}{}
		return nil
	}); err != nil {
		return err
	}

	storage := make(map[string]struct{})
	if err := changeset.ForRange(tx, kv.StorageChangeSet, block1, block2, func(blockN uint64, k, v []byte) error {
		if (blockN-block1)%100000 == 0 {
			fmt.Printf("at the block %d for accounts, booster size: %d\n", blockN, len(accounts))
		}
		storage[string(common.CopyBytes(k))] = struct{}{}
		return nil
	}); err != nil {
		return err
	}

	fmt.Printf("accounts changed: %d, storage changed: %d\n", len(accounts), len(storage))
	return nil
}

func searchChangeSet(chaindata string, key []byte, block uint64) error {
	fmt.Printf("Searching changesets\n")
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err1 := db.BeginRw(context.Background())
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()

	if err := changeset.ForEach(tx, kv.AccountChangeSet, dbutils.EncodeBlockNumber(block), func(blockN uint64, k, v []byte) error {
		if bytes.Equal(k, key) {
			fmt.Printf("Found in block %d with value %x\n", blockN, v)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func searchStorageChangeSet(chaindata string, key []byte, block uint64) error {
	fmt.Printf("Searching storage changesets\n")
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err1 := db.BeginRw(context.Background())
	if err1 != nil {
		return err1
	}
	defer tx.Rollback()
	if err := changeset.ForEach(tx, kv.StorageChangeSet, dbutils.EncodeBlockNumber(block), func(blockN uint64, k, v []byte) error {
		if bytes.Equal(k, key) {
			fmt.Printf("Found in block %d with value %x\n", blockN, v)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func supply(chaindata string) error {
	startTime := time.Now()
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	count := 0
	supply := uint256.NewInt(0)
	var a accounts.Account
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.PlainState)
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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	var contractCount int
	if err1 := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Code)
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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	hashes := make(map[common.Hash][]byte)
	if err1 := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Code)
		if err != nil {
			return err
		}
		// This is a mapping of CodeHash => Byte code
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if len(v) > 0 && v[0] == 0xef {
				fmt.Printf("Found code with hash %x: %x\n", k, v)
				hashes[common.BytesToHash(k)] = common.CopyBytes(v)
			}
		}
		c, err = tx.Cursor(kv.PlainContractCode)
		if err != nil {
			return err
		}
		// This is a mapping of contractAddress + incarnation => CodeHash
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			hash := common.BytesToHash(v)
			if code, ok := hashes[hash]; ok {
				fmt.Printf("address: %x: %x\n", k[:20], code)
			}
		}
		return nil
	}); err1 != nil {
		return err1
	}
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
	db := mdbx.MustOpen(chaindata)
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
	c, err := tx.Cursor(kv.HeaderCanonical)
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
	c, err = tx.Cursor(kv.BlockBody)
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
		body := rawdb.ReadBodyWithTransactions(tx, blockHash, blockNumber)
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
	db := mdbx.MustOpen(chaindata)
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
	tool.Check(db.View(context.Background(), func(tx kv.Tx) error {
		for b <= blockTotal {
			hash, err := rawdb.ReadCanonicalHash(tx, b)
			if err != nil {
				return err
			}

			if hash == (common.Hash{}) {
				break
			}

			fmt.Fprintf(w, "	\"%x\",\n", hash)
			b += blockStep
		}
		return nil
	}))

	b -= blockStep
	fmt.Fprintf(w, "}\n\n")
	fmt.Fprintf(w, "const %sPreverifiedHeight uint64 = %d\n", name, b)
	fmt.Printf("Last block is %d\n", b)
	return nil
}

func extractHeaders(chaindata string, block uint64) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(kv.Headers)
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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(kv.BlockBody)
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
		_, baseTxId, txAmount := rawdb.ReadBody(tx, blockHash, blockNumber)
		fmt.Printf("Body %d %x: baseTxId %d, txAmount %d\n", blockNumber, blockHash, baseTxId, txAmount)
	}
	return nil
}

func fixUnwind(chaindata string) error {
	contractAddr := common.HexToAddress("0x577a32aa9c40cf4266e49fc1e44c749c356309bd")
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tool.Check(db.Update(context.Background(), func(tx kv.RwTx) error {
		i, err := tx.GetOne(kv.IncarnationMap, contractAddr[:])
		if err != nil {
			return err
		} else if i == nil {
			fmt.Print("Not found\n")
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], 1)
			if err = tx.Put(kv.IncarnationMap, contractAddr[:], b[:]); err != nil {
				return err
			}
		} else {
			fmt.Printf("Inc: %x\n", i)
		}
		return nil
	}))
	return nil
}

func snapSizes(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c, _ := tx.Cursor(kv.CliqueSeparate)
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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	traceCursor, err1 := tx.RwCursorDupSort(kv.CallTraceSet)
	if err1 != nil {
		return err1
	}
	defer traceCursor.Close()
	var k []byte
	var v []byte
	count := 0
	for k, v, err = traceCursor.First(); k != nil; k, v, err = traceCursor.Next() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum == block {
			fmt.Printf("%x\n", v)
		}
		count++
	}
	fmt.Printf("Found %d records\n", count)
	idxCursor, err2 := tx.Cursor(kv.CallToIndex)
	if err2 != nil {
		return err2
	}
	var acc = common.HexToAddress("0x511bc4556d823ae99630ae8de28b9b80df90ea2e")
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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err1 := tx.RwCursor(kv.Headers)
	if err1 != nil {
		return err1
	}
	defer c.Close()
	var k, v []byte
	for k, v, err = c.First(); err == nil && k != nil; k, v, err = c.Next() {
		hv, herr := tx.GetOne(kv.HeaderTD, k)
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
			if parentTdRec, err = tx.GetOne(kv.HeaderTD, parentK[:]); err != nil {
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
			if err = tx.Put(kv.HeaderTD, k, newHv); err != nil {
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
	db := mdbx.MustOpen(chaindata)
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
	log.Info("ID exec", "progress", stageExec)
	if err = stages.SaveStageProgress(tx, stages.Execution, stageExec+1); err != nil {
		return err
	}
	stageExec, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("ID exec", "changed to", stageExec)
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func backExec(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
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
	log.Info("ID exec", "progress", stageExec)
	if err = stages.SaveStageProgress(tx, stages.Execution, stageExec-1); err != nil {
		return err
	}
	stageExec, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("ID exec", "changed to", stageExec)
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func fixState(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err1 := tx.RwCursor(kv.HeaderCanonical)
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
		hv, herr := tx.GetOne(kv.Headers, headerKey[:])
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
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	lastTxId, err := tx.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	txs, err1 := tx.RwCursor(kv.EthTx)
	if err1 != nil {
		return err1
	}
	defer txs.Close()
	bodies, err2 := tx.Cursor(kv.BlockBody)
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
		txs, err = tx.RwCursor(kv.EthTx)
		if err != nil {
			return err
		}
		defer txs.Close()
	}
	return nil
}

func scanTxs(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(kv.EthTx)
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

func scanReceipts3(chaindata string, block uint64) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var key [8]byte
	var v []byte
	binary.BigEndian.PutUint64(key[:], block)
	if v, err = tx.GetOne(kv.Receipts, key[:]); err != nil {
		return err
	}
	fmt.Printf("%x\n", v)
	return nil
}

func scanReceipts2(chaindata string) error {
	f, err := os.Create("receipts.txt")
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	dbdb := mdbx.MustOpen(chaindata)
	defer dbdb.Close()
	tx, err := dbdb.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	blockNum, err := changeset.AvailableFrom(tx)
	if err != nil {
		return err
	}
	fixedCount := 0
	logInterval := 30 * time.Second
	logEvery := time.NewTicker(logInterval)
	var key [8]byte
	var v []byte
	for ; true; blockNum++ {
		select {
		default:
		case <-logEvery.C:
			log.Info("Scanned", "block", blockNum, "fixed", fixedCount)
		}
		var hash common.Hash
		if hash, err = rawdb.ReadCanonicalHash(tx, blockNum); err != nil {
			return err
		}
		if hash == (common.Hash{}) {
			break
		}
		binary.BigEndian.PutUint64(key[:], blockNum)
		if v, err = tx.GetOne(kv.Receipts, key[:]); err != nil {
			return err
		}
		var receipts types.Receipts
		if err = cbor.Unmarshal(&receipts, bytes.NewReader(v)); err == nil {
			broken := false
			for _, receipt := range receipts {
				if receipt.CumulativeGasUsed < 10000 {
					broken = true
					break
				}
			}
			if !broken {
				continue
			}
		}
		fmt.Fprintf(w, "%d %x\n", blockNum, v)
		fixedCount++
		if fixedCount > 100 {
			break
		}

	}
	tx.Rollback()
	return nil
}

func scanReceipts(chaindata string, block uint64) error {
	f, err := os.Create("fixed.txt")
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	blockNum, err := changeset.AvailableFrom(tx)
	if err != nil {
		return err
	}
	if block > blockNum {
		blockNum = block
	}

	genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
	if err != nil {
		return err
	}
	chainConfig, cerr := rawdb.ReadChainConfig(tx, genesisBlock.Hash())
	if cerr != nil {
		return cerr
	}
	vmConfig := vm.Config{}
	noOpWriter := state.NewNoopWriter()
	var buf bytes.Buffer
	fixedCount := 0
	logInterval := 30 * time.Second
	logEvery := time.NewTicker(logInterval)
	var key [8]byte
	var v []byte
	for ; true; blockNum++ {
		select {
		default:
		case <-logEvery.C:
			log.Info("Commit", "block", blockNum, "fixed", fixedCount)
			tx.Commit()
			if tx, err = db.BeginRw(context.Background()); err != nil {
				return err
			}
		}
		var hash common.Hash
		if hash, err = rawdb.ReadCanonicalHash(tx, blockNum); err != nil {
			return err
		}
		if hash == (common.Hash{}) {
			break
		}
		binary.BigEndian.PutUint64(key[:], blockNum)
		if v, err = tx.GetOne(kv.Receipts, key[:]); err != nil {
			return err
		}
		var receipts types.Receipts
		if err = cbor.Unmarshal(&receipts, bytes.NewReader(v)); err == nil {
			broken := false
			for _, receipt := range receipts {
				if receipt.CumulativeGasUsed < 10000 {
					broken = true
					break
				}
			}
			if !broken {
				continue
			}
		} else {
			// Receipt is using old CBOR encoding
			var oldReceipts migrations.OldReceipts
			if err = cbor.Unmarshal(&oldReceipts, bytes.NewReader(v)); err != nil {
				return err
			}
			var body *types.Body
			if chainConfig.IsBerlin(blockNum) {
				body = rawdb.ReadBodyWithTransactions(tx, hash, blockNum)
			}
			receipts = make(types.Receipts, len(oldReceipts))
			for i, oldReceipt := range oldReceipts {
				receipts[i] = new(types.Receipt)
				receipts[i].PostState = oldReceipt.PostState
				receipts[i].Status = oldReceipt.Status
				receipts[i].CumulativeGasUsed = oldReceipt.CumulativeGasUsed
				if body != nil {
					receipts[i].Type = body.Transactions[i].Type()
				}
			}
			buf.Reset()
			if err = cbor.Marshal(&buf, receipts); err != nil {
				return err
			}
			if err = tx.Put(kv.Receipts, common.CopyBytes(key[:]), common.CopyBytes(buf.Bytes())); err != nil {
				return err
			}
			fixedCount++
			continue
		}
		var block *types.Block
		if block, _, err = rawdb.ReadBlockWithSenders(tx, hash, blockNum); err != nil {
			return err
		}

		dbstate := state.NewPlainState(tx, block.NumberU64()-1)
		intraBlockState := state.New(dbstate)

		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
		contractHasTEVM := ethdb.GetHasTEVM(tx)
		receipts1, err1 := runBlock(intraBlockState, noOpWriter, noOpWriter, chainConfig, getHeader, contractHasTEVM, block, vmConfig)
		if err1 != nil {
			return err1
		}
		fix := true
		if chainConfig.IsByzantium(blockNum) {
			receiptSha := types.DeriveSha(receipts1)
			if receiptSha != block.Header().ReceiptHash {
				fmt.Printf("(retrace) mismatched receipt headers for block %d: %x, %x\n", block.NumberU64(), receiptSha, block.Header().ReceiptHash)
				fix = false
			}
		}
		if fix {
			// All good, we can fix receipt record
			buf.Reset()
			err := cbor.Marshal(&buf, receipts1)
			if err != nil {
				return fmt.Errorf("encode block receipts for block %d: %v", blockNum, err)
			}
			if err = tx.Put(kv.Receipts, key[:], buf.Bytes()); err != nil {
				return fmt.Errorf("writing receipts for block %d: %v", blockNum, err)
			}
			if _, err = w.Write([]byte(fmt.Sprintf("%d\n", blockNum))); err != nil {
				return err
			}
			fixedCount++
		}
	}
	return tx.Commit()
}

func runBlock(ibs *state.IntraBlockState, txnWriter state.StateWriter, blockWriter state.StateWriter,
	chainConfig *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, contractHasTEVM func(common.Hash) (bool, error), block *types.Block, vmConfig vm.Config) (types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	rules := chainConfig.Rules(block.NumberU64())
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, txnWriter, header, tx, usedGas, vmConfig, contractHasTEVM)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%x] failed: %v", i, tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
		//fmt.Printf("%d, cumulative gas: %d\n", i, receipt.CumulativeGasUsed)
	}

	if !vmConfig.ReadOnly {
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		if _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, nil, nil, nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %v", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(rules, blockWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
		}
	}

	return receipts, nil
}

func devTx(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := rawdb.ReadBlockByNumber(tx, 0)
	tool.Check(err)
	cc, err := rawdb.ReadChainConfig(tx, b.Hash())
	tool.Check(err)
	txn := types.NewTransaction(2, common.Address{}, uint256.NewInt(100), 100_000, uint256.NewInt(1), []byte{1})
	signedTx, err := types.SignTx(txn, *types.LatestSigner(cc), core.DevnetSignPrivateKey)
	tool.Check(err)
	buf := bytes.NewBuffer(nil)
	err = signedTx.MarshalBinary(buf)
	tool.Check(err)
	fmt.Printf("%x\n", buf.Bytes())
	return nil
}
func main() {
	flag.Parse()

	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(*verbosity), log.StderrHandler))

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
		err = hackdb.Defrag()

	case "textInfo":
		err = hackdb.TextInfo(*chaindata, &strings.Builder{})

	case "extractBodies":
		err = extractBodies(*chaindata, uint64(*block))

	case "fixUnwind":
		err = fixUnwind(*chaindata)

	case "repairCurrent":
		repairCurrent()

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

	case "mphf":
		err = mphf(*chaindata, uint64(*block))

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

	case "trimTxs":
		err = trimTxs(*chaindata)

	case "scanTxs":
		err = scanTxs(*chaindata)

	case "scanReceipts":
		err = scanReceipts(*chaindata, uint64(*block))

	case "scanReceipts2":
		err = scanReceipts2(*chaindata)

	case "scanReceipts3":
		err = scanReceipts3(*chaindata, uint64(*block))

	case "devTx":
		err = devTx(*chaindata)
	case "compress":
		err = compress(*chaindata, uint64(*block))
	case "reducedict":
		err = reducedict(*block)
	case "genstate":
		err = genstate()
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
