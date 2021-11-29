package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"io"
	"io/fs"
	"io/ioutil"
	"math/big"
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/flanglet/kanzi-go/transform"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/patricia"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	hackdb "github.com/ledgerwatch/erigon/cmd/hack/db"
	"github.com/ledgerwatch/erigon/cmd/hack/flow"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
	"github.com/wcharczuk/go-chart/v2"
	atomic2 "go.uber.org/atomic"
)

const ASSERT = false

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
func dumpState(chaindata string, block int, name string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	fa, err := os.Create(name + ".accounts.dat")
	if err != nil {
		return err
	}
	defer fa.Close()
	wa := bufio.NewWriterSize(fa, etl.BufIOSize)
	// Write out number of key/value pairs first
	var countBytes [8]byte
	binary.BigEndian.PutUint64(countBytes[:], 0) // TODO: Write correct number or remove
	if _, err = wa.Write(countBytes[:]); err != nil {
		return err
	}
	defer wa.Flush()
	var fs, fc *os.File
	if fs, err = os.Create(name + ".storage.dat"); err != nil {
		return err
	}
	defer fs.Close()
	ws := bufio.NewWriterSize(fs, etl.BufIOSize)
	binary.BigEndian.PutUint64(countBytes[:], 0) // TODO: Write correct number or remove
	if _, err = ws.Write(countBytes[:]); err != nil {
		return err
	}
	defer ws.Flush()
	if fc, err = os.Create(name + ".code.dat"); err != nil {
		return err
	}
	defer fc.Close()
	wc := bufio.NewWriterSize(fc, etl.BufIOSize)
	binary.BigEndian.PutUint64(countBytes[:], 0) // TODO: Write correct number or remove
	if _, err = wc.Write(countBytes[:]); err != nil {
		return err
	}
	defer wc.Flush()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var sc kv.Cursor
	if sc, err = tx.Cursor(kv.PlainState); err != nil {
		return err
	}
	defer sc.Close()
	var cc kv.Cursor
	if cc, err = tx.Cursor(kv.PlainContractCode); err != nil {
		return err
	}
	defer cc.Close()
	i := 0
	numBuf := make([]byte, binary.MaxVarintLen64)
	k, v, e := sc.First()
	var a accounts.Account
	var addr common.Address
	var ks [20 + 32]byte
	for ; k != nil && e == nil; k, v, e = sc.Next() {
		if len(k) == 20 {
			n := binary.PutUvarint(numBuf, uint64(len(k)))
			if _, err = wa.Write(numBuf[:n]); err != nil {
				return err
			}
			if _, err = wa.Write(k); err != nil {
				return err
			}
			n = binary.PutUvarint(numBuf, uint64(len(v)))
			if _, err = wa.Write(numBuf[:n]); err != nil {
				return err
			}
			if len(v) > 0 {
				if _, err = wa.Write(v); err != nil {
					return err
				}
			}
			if err = a.DecodeForStorage(v); err != nil {
				return err
			}
			if a.CodeHash != trie.EmptyCodeHash {
				code, err := tx.GetOne(kv.Code, a.CodeHash[:])
				if err != nil {
					return err
				}
				if len(code) != 0 {
					n = binary.PutUvarint(numBuf, uint64(len(k)))
					if _, err = wc.Write(numBuf[:n]); err != nil {
						return err
					}
					if _, err = wc.Write(k); err != nil {
						return err
					}
					n = binary.PutUvarint(numBuf, uint64(len(code)))
					if _, err = wc.Write(numBuf[:n]); err != nil {
						return err
					}
					if len(code) > 0 {
						if _, err = wc.Write(code); err != nil {
							return err
						}
					}
					i += 2
					if i%10_000_000 == 0 {
						log.Info("Written into file", "millions", i/1_000_000)
					}
				}
			}
			copy(addr[:], k)
			i += 2
			if i%10_000_000 == 0 {
				log.Info("Written into file", "millions", i/1_000_000)
			}
		}
		if len(k) == 60 {
			inc := binary.BigEndian.Uint64(k[20:])
			if bytes.Equal(k[:20], addr[:]) && inc == a.Incarnation {
				copy(ks[:], k[:20])
				copy(ks[20:], k[20+8:])
				n := binary.PutUvarint(numBuf, uint64(len(ks)))
				if _, err = ws.Write(numBuf[:n]); err != nil {
					return err
				}
				if _, err = ws.Write(ks[:]); err != nil {
					return err
				}
				n = binary.PutUvarint(numBuf, uint64(len(v)))
				if _, err = ws.Write(numBuf[:n]); err != nil {
					return err
				}
				if len(v) > 0 {
					if _, err = ws.Write(v); err != nil {
						return err
					}
				}
				i += 2
				if i%10_000_000 == 0 {
					log.Info("Written into file", "millions", i/1_000_000)
				}
			}
		}
	}
	if e != nil {
		return e
	}
	return nil
}

func mphf(chaindata string, block int) error {
	// Create a file to compress if it does not exist already
	statefile := "statedump.dat"
	if _, err := os.Stat(statefile); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("not sure if statedump.dat exists: %w", err)
		}
		if err = dumpState(chaindata, int(block), "statefile"); err != nil {
			return err
		}
	}
	var rs *recsplit.RecSplit
	f, err := os.Open(statefile)
	if err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, etl.BufIOSize)
	defer f.Close()
	var countBuf [8]byte
	if _, err = io.ReadFull(r, countBuf[:]); err != nil {
		return err
	}
	count := binary.BigEndian.Uint64(countBuf[:])
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   int(count),
		BucketSize: 2000,
		Salt:       1,
		LeafSize:   8,
		TmpDir:     "",
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: "state.idx",
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
		if i%2 == 0 {
			// It is key, we skip the values here
			if err := rs.AddKey(buf[:l], uint64(i/2)); err != nil {
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
	idx := recsplit.MustOpen("state.idx")
	defer idx.Close()
	log.Info("Testing bijection")
	bitCount := (count + 63) / 64
	bits := make([]uint64, bitCount)
	if _, err = f.Seek(8, 0); err != nil {
		return err
	}
	r = bufio.NewReaderSize(f, etl.BufIOSize)
	l, e = r.ReadByte()
	i = 0
	var lookupTime time.Duration
	for ; e == nil; l, e = r.ReadByte() {
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if i%2 == 0 {
			// It is key, we skip the values here
			start := time.Now()
			offset := idx.Lookup(buf[:l])
			lookupTime += time.Since(start)
			if offset >= count {
				return fmt.Errorf("idx %d >= count %d", offset, count)
			}
			mask := uint64(1) << (offset & 63)
			if bits[offset>>6]&mask != 0 {
				return fmt.Errorf("no bijection key idx=%d, lookup up idx = %d", i, offset)
			}
			bits[offset>>6] |= mask
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
	w := bufio.NewWriterSize(f, etl.BufIOSize)
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
		lcp := make([]int32, n)
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
			lcp[inv[i]] = int32(k) // lcp for the present suffix.

			// Deleting the starting character from the string.
			if k > 0 {
				k--
			}
		}
		//log.Info("Kasai algorithm finished")
		// Checking LCP array

		if ASSERT {
			for i := 0; i < n-1; i++ {
				var prefixLen int
				p1 := int(filtered[i])
				p2 := int(filtered[i+1])
				for p1+prefixLen < n &&
					p2+prefixLen < n &&
					superstring[(p1+prefixLen)*2] != 0 &&
					superstring[(p2+prefixLen)*2] != 0 &&
					superstring[(p1+prefixLen)*2+1] == superstring[(p2+prefixLen)*2+1] {
					prefixLen++
				}
				if prefixLen != int(lcp[i]) {
					log.Error("Mismatch", "prefixLen", prefixLen, "lcp[i]", lcp[i], "i", i)
					break
				}
				l := int(lcp[i]) // Length of potential dictionary word
				if l < 2 {
					continue
				}
				dictKey := make([]byte, l)
				for s := 0; s < l; s++ {
					dictKey[s] = superstring[(filtered[i]+s)*2+1]
				}
				//fmt.Printf("%d %d %s\n", filtered[i], lcp[i], dictKey)
			}
		}
		//log.Info("LCP array checked")
		// Walk over LCP array and compute the scores of the strings
		b := inv
		j = 0
		for i := 0; i < n-1; i++ {
			// Only when there is a drop in LCP value
			if lcp[i+1] >= lcp[i] {
				j = i
				continue
			}
			for l := int(lcp[i]); l > int(lcp[i+1]); l-- {
				if l < minPatternLen || l > maxPatternLen {
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
				score := uint64(repeats * int(l-4))
				if score > minPatternScore {
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

const CompressLogPrefix = "compress"

// superstringLimit limits how large can one "superstring" get before it is processed
// Compressor allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
const superstringLimit = 16 * 1024 * 1024

// minPatternLen is minimum length of pattern we consider to be included into the dictionary
const minPatternLen = 5
const maxPatternLen = 64

// minPatternScore is minimum score (per superstring) required to consider including pattern into the dictionary
const minPatternScore = 1024

func compress1(chaindata string, fileName, segmentFileName string) error {
	database := mdbx.MustOpen(chaindata)
	defer database.Close()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	// Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
	// We only consider values with length > 2, because smaller values are not compressible without going into bits
	var superstring []byte

	workers := runtime.NumCPU() / 2
	// Collector for dictionary words (sorted by their score)
	tmpDir := ""
	ch := make(chan []byte, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	collectors := make([]*etl.Collector, workers)
	for i := 0; i < workers; i++ {
		collector := etl.NewCollector(CompressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		collectors[i] = collector
		go processSuperstring(ch, collector, &wg)
	}
	i := 0
	if err := snapshotsync.ReadSimpleFile(fileName+".dat", func(v []byte) error {
		if len(superstring)+2*len(v)+2 > superstringLimit {
			ch <- superstring
			superstring = nil
		}
		for _, a := range v {
			superstring = append(superstring, 1, a)
		}
		superstring = append(superstring, 0, 0)
		i++
		select {
		default:
		case <-logEvery.C:
			log.Info("Dictionary preprocessing", "processed", fmt.Sprintf("%dK", i/1_000))
		}
		return nil
	}); err != nil {
		return err
	}
	if len(superstring) > 0 {
		ch <- superstring
	}
	close(ch)
	wg.Wait()

	db, err := compress.DictionaryBuilderFromCollectors(context.Background(), CompressLogPrefix, tmpDir, collectors)
	if err != nil {
		panic(err)
	}
	if err := compress.PersistDictrionary(fileName+".dictionary.txt", db); err != nil {
		return err
	}

	if err := reducedict(fileName, segmentFileName); err != nil {
		return err
	}
	return nil
}

type DynamicCell struct {
	optimStart  int
	coverStart  int
	compression int
	score       uint64
	patternIdx  int // offset of the last element in the pattern slice
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

func optimiseCluster(trace bool, numBuf []byte, input []byte, trie *patricia.PatriciaTree, mf *patricia.MatchFinder, output []byte, uncovered []int, patterns []int, cellRing *Ring, posMap map[uint64]uint64) ([]byte, []int, []int) {
	matches := mf.FindLongestMatches(trie, input)
	if len(matches) == 0 {
		n := binary.PutUvarint(numBuf, 0)
		output = append(output, numBuf[:n]...)
		output = append(output, input...)
		return output, patterns, uncovered
	}
	if trace {
		fmt.Printf("Cluster | input = %x\n", input)
		for _, match := range matches {
			fmt.Printf(" [%x %d-%d]", input[match.Start:match.End], match.Start, match.End)
		}
	}
	cellRing.Reset()
	patterns = append(patterns[:0], 0, 0) // Sentinel entry - no meaning
	lastF := matches[len(matches)-1]
	for j := lastF.Start; j < lastF.End; j++ {
		d := cellRing.PushBack()
		d.optimStart = j + 1
		d.coverStart = len(input)
		d.compression = 0
		d.patternIdx = 0
		d.score = 0
	}
	// Starting from the last match
	for i := len(matches); i > 0; i-- {
		f := matches[i-1]
		p := f.Val.(*Pattern)
		firstCell := cellRing.Get(0)
		if firstCell == nil {
			fmt.Printf("cellRing.Len() = %d\n", cellRing.Len())
		}
		maxCompression := firstCell.compression
		maxScore := firstCell.score
		maxCell := firstCell
		var maxInclude bool
		for e := 0; e < cellRing.Len(); e++ {
			cell := cellRing.Get(e)
			comp := cell.compression - 4
			if cell.coverStart >= f.End {
				comp += f.End - f.Start
			} else {
				comp += cell.coverStart - f.Start
			}
			score := cell.score + p.score
			if comp > maxCompression || (comp == maxCompression && score > maxScore) {
				maxCompression = comp
				maxScore = score
				maxInclude = true
				maxCell = cell
			}
			if cell.optimStart > f.End {
				cellRing.Truncate(e)
				break
			}
		}
		d := cellRing.PushFront()
		d.optimStart = f.Start
		d.score = maxScore
		d.compression = maxCompression
		if maxInclude {
			if trace {
				fmt.Printf("[include] cell for %d: with patterns", f.Start)
				fmt.Printf(" [%x %d-%d]", input[f.Start:f.End], f.Start, f.End)
				patternIdx := maxCell.patternIdx
				for patternIdx != 0 {
					pattern := patterns[patternIdx]
					fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
					patternIdx = patterns[patternIdx+1]
				}
				fmt.Printf("\n\n")
			}
			d.coverStart = f.Start
			d.patternIdx = len(patterns)
			patterns = append(patterns, i-1, maxCell.patternIdx)
		} else {
			if trace {
				fmt.Printf("cell for %d: with patterns", f.Start)
				patternIdx := maxCell.patternIdx
				for patternIdx != 0 {
					pattern := patterns[patternIdx]
					fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
					patternIdx = patterns[patternIdx+1]
				}
				fmt.Printf("\n\n")
			}
			d.coverStart = maxCell.coverStart
			d.patternIdx = maxCell.patternIdx
		}
	}
	optimCell := cellRing.Get(0)
	if trace {
		fmt.Printf("optimal =")
	}
	// Count number of patterns
	var patternCount uint64
	patternIdx := optimCell.patternIdx
	for patternIdx != 0 {
		patternCount++
		patternIdx = patterns[patternIdx+1]
	}
	p := binary.PutUvarint(numBuf, patternCount)
	output = append(output, numBuf[:p]...)
	patternIdx = optimCell.patternIdx
	lastStart := 0
	var lastUncovered int
	uncovered = uncovered[:0]
	for patternIdx != 0 {
		pattern := patterns[patternIdx]
		p := matches[pattern].Val.(*Pattern)
		if trace {
			fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
		}
		if matches[pattern].Start > lastUncovered {
			uncovered = append(uncovered, lastUncovered, matches[pattern].Start)
		}
		lastUncovered = matches[pattern].End
		// Starting position
		posMap[uint64(matches[pattern].Start-lastStart+1)]++
		lastStart = matches[pattern].Start
		n := binary.PutUvarint(numBuf, uint64(matches[pattern].Start))
		output = append(output, numBuf[:n]...)
		// Code
		n = binary.PutUvarint(numBuf, p.code)
		output = append(output, numBuf[:n]...)
		atomic.AddUint64(&p.uses, 1)
		patternIdx = patterns[patternIdx+1]
	}
	if len(input) > lastUncovered {
		uncovered = append(uncovered, lastUncovered, len(input))
	}
	if trace {
		fmt.Printf("\n\n")
	}
	// Add uncoded input
	for i := 0; i < len(uncovered); i += 2 {
		output = append(output, input[uncovered[i]:uncovered[i+1]]...)
	}
	return output, patterns, uncovered
}

func reduceDictWorker(inputCh chan []byte, completion *sync.WaitGroup, trie *patricia.PatriciaTree, collector *etl.Collector, inputSize, outputSize *atomic2.Uint64, posMap map[uint64]uint64) {
	defer completion.Done()
	var output = make([]byte, 0, 256)
	var uncovered = make([]int, 256)
	var patterns = make([]int, 0, 256)
	cellRing := NewRing()
	var mf patricia.MatchFinder
	numBuf := make([]byte, binary.MaxVarintLen64)
	for input := range inputCh {
		// First 8 bytes are idx
		n := binary.PutUvarint(numBuf, uint64(len(input)-8))
		output = append(output[:0], numBuf[:n]...)
		if len(input) > 8 {
			output, patterns, uncovered = optimiseCluster(false, numBuf, input[8:], trie, &mf, output, uncovered, patterns, cellRing, posMap)
			if err := collector.Collect(input[:8], output); err != nil {
				log.Error("Could not collect", "error", err)
				return
			}
		}
		inputSize.Add(1 + uint64(len(input)-8))
		outputSize.Add(uint64(len(output)))
		posMap[uint64(len(input)-8+1)]++
		posMap[0]++
	}
}

type Pattern struct {
	score    uint64
	uses     uint64
	code     uint64 // Allocated numerical code
	codeBits int    // Number of bits in the code
	w        []byte
	offset   uint64 // Offset of this patten in the dictionary representation
}

// PatternList is a sorted list of pattern for the purpose of
// building Huffman tree to determine efficient coding.
// Patterns with least usage come first, we use numerical code
// as a tie breaker to make sure the resulting Huffman code is canonical
type PatternList []*Pattern

func (pl PatternList) Len() int {
	return len(pl)
}

func (pl PatternList) Less(i, j int) bool {
	if pl[i].uses == pl[j].uses {
		return pl[i].code < pl[j].code
	}
	return pl[i].uses < pl[j].uses
}

func (pl *PatternList) Swap(i, j int) {
	(*pl)[i], (*pl)[j] = (*pl)[j], (*pl)[i]
}

type PatternHuff struct {
	uses       uint64
	tieBreaker uint64
	p0, p1     *Pattern
	h0, h1     *PatternHuff
	offset     uint64 // Offset of this huffman tree node in the dictionary representation
}

func (h *PatternHuff) AddZero() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.codeBits++
	} else {
		h.h0.AddZero()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.codeBits++
	} else {
		h.h1.AddZero()
	}
}

func (h *PatternHuff) AddOne() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.code++
		h.p0.codeBits++
	} else {
		h.h0.AddOne()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.code++
		h.p1.codeBits++
	} else {
		h.h1.AddOne()
	}
}

// PatternHeap is priority queue of pattern for the purpose of building
// Huffman tree to determine efficient coding. Patterns with least usage
// have highest priority. We use a tie-breaker to make sure
// the resulting Huffman code is canonical
type PatternHeap []*PatternHuff

func (ph PatternHeap) Len() int {
	return len(ph)
}

func (ph PatternHeap) Less(i, j int) bool {
	if ph[i].uses == ph[j].uses {
		return ph[i].tieBreaker < ph[j].tieBreaker
	}
	return ph[i].uses < ph[j].uses
}

func (ph *PatternHeap) Swap(i, j int) {
	(*ph)[i], (*ph)[j] = (*ph)[j], (*ph)[i]
}

func (ph *PatternHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*PatternHuff))
}

func (ph *PatternHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

type Position struct {
	pos      uint64
	uses     uint64
	code     uint64
	codeBits int
	offset   uint64
}

type PositionHuff struct {
	uses       uint64
	tieBreaker uint64
	p0, p1     *Position
	h0, h1     *PositionHuff
	offset     uint64
}

func (h *PositionHuff) AddZero() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.codeBits++
	} else {
		h.h0.AddZero()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.codeBits++
	} else {
		h.h1.AddZero()
	}
}

func (h *PositionHuff) AddOne() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.code++
		h.p0.codeBits++
	} else {
		h.h0.AddOne()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.code++
		h.p1.codeBits++
	} else {
		h.h1.AddOne()
	}
}

type PositionList []*Position

func (pl PositionList) Len() int {
	return len(pl)
}

func (pl PositionList) Less(i, j int) bool {
	if pl[i].uses == pl[j].uses {
		return pl[i].pos < pl[j].pos
	}
	return pl[i].uses < pl[j].uses
}

func (pl *PositionList) Swap(i, j int) {
	(*pl)[i], (*pl)[j] = (*pl)[j], (*pl)[i]
}

type PositionHeap []*PositionHuff

func (ph PositionHeap) Len() int {
	return len(ph)
}

func (ph PositionHeap) Less(i, j int) bool {
	if ph[i].uses == ph[j].uses {
		return ph[i].tieBreaker < ph[j].tieBreaker
	}
	return ph[i].uses < ph[j].uses
}

func (ph *PositionHeap) Swap(i, j int) {
	(*ph)[i], (*ph)[j] = (*ph)[j], (*ph)[i]
}

func (ph *PositionHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*PositionHuff))
}

func (ph *PositionHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

type HuffmanCoder struct {
	w          *bufio.Writer
	outputBits int
	outputByte byte
}

func (hf *HuffmanCoder) encode(code uint64, codeBits int) error {
	for codeBits > 0 {
		var bitsUsed int
		if hf.outputBits+codeBits > 8 {
			bitsUsed = 8 - hf.outputBits
		} else {
			bitsUsed = codeBits
		}
		mask := (uint64(1) << bitsUsed) - 1
		hf.outputByte |= byte((code & mask) << hf.outputBits)
		code >>= bitsUsed
		codeBits -= bitsUsed
		hf.outputBits += bitsUsed
		if hf.outputBits == 8 {
			if e := hf.w.WriteByte(hf.outputByte); e != nil {
				return e
			}
			hf.outputBits = 0
			hf.outputByte = 0
		}
	}
	return nil
}

func (hf *HuffmanCoder) flush() error {
	if hf.outputBits > 0 {
		if e := hf.w.WriteByte(hf.outputByte); e != nil {
			return e
		}
		hf.outputBits = 0
		hf.outputByte = 0
	}
	return nil
}

// reduceDict reduces the dictionary by trying the substitutions and counting frequency for each word
func reducedict(name string, segmentFileName string) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	// DictionaryBuilder is for sorting words by their freuency (to assign codes)
	var pt patricia.PatriciaTree
	code2pattern := make([]*Pattern, 0, 256)
	if err := compress.ReadDictrionary(name+".dictionary.txt", func(score uint64, word []byte) error {
		p := &Pattern{
			score:    score,
			uses:     0,
			code:     uint64(len(code2pattern)),
			codeBits: 0,
			w:        word,
		}
		pt.Insert(word, p)
		code2pattern = append(code2pattern, p)
		return nil
	}); err != nil {
		return err
	}
	log.Info("dictionary file parsed", "entries", len(code2pattern))
	tmpDir := ""
	ch := make(chan []byte, 10000)
	inputSize, outputSize := atomic2.NewUint64(0), atomic2.NewUint64(0)
	var wg sync.WaitGroup
	workers := runtime.NumCPU() / 2
	var collectors []*etl.Collector
	var posMaps []map[uint64]uint64
	for i := 0; i < workers; i++ {
		collector := etl.NewCollector(CompressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		collectors = append(collectors, collector)
		posMap := make(map[uint64]uint64)
		posMaps = append(posMaps, posMap)
		wg.Add(1)
		go reduceDictWorker(ch, &wg, &pt, collector, inputSize, outputSize, posMap)
	}
	i := 0
	if err := snapshotsync.ReadSimpleFile(name+".dat", func(v []byte) error {
		input := make([]byte, 8+int(len(v)))
		binary.BigEndian.PutUint64(input, uint64(i))
		copy(input[8:], v)
		ch <- input
		i++
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Replacement preprocessing", "processed", fmt.Sprintf("%dK", i/1_000), "input", common.StorageSize(inputSize.Load()), "output", common.StorageSize(outputSize.Load()), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		}
		return nil
	}); err != nil {
		return err
	}
	close(ch)
	wg.Wait()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Done", "input", common.StorageSize(inputSize.Load()), "output", common.StorageSize(outputSize.Load()), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	posMap := make(map[uint64]uint64)
	for _, m := range posMaps {
		for l, c := range m {
			posMap[l] += c
		}
	}
	//fmt.Printf("posMap = %v\n", posMap)
	var patternList PatternList
	for _, p := range code2pattern {
		if p.uses > 0 {
			patternList = append(patternList, p)
		}
	}
	sort.Sort(&patternList)
	if len(patternList) == 0 {
		log.Warn("dictionary is empty")
		//err := fmt.Errorf("dictionary is empty")
		//panic(err)
		//return err
	}
	// Calculate offsets of the dictionary patterns and total size
	var offset uint64
	numBuf := make([]byte, binary.MaxVarintLen64)
	for _, p := range patternList {
		p.offset = offset
		n := binary.PutUvarint(numBuf, uint64(len(p.w)))
		offset += uint64(n + len(p.w))
	}
	patternCutoff := offset // All offsets below this will be considered patterns
	i = 0
	log.Info("Effective dictionary", "size", patternList.Len())
	// Build Huffman tree for codes
	var codeHeap PatternHeap
	heap.Init(&codeHeap)
	tieBreaker := uint64(0)
	var huffs []*PatternHuff // To be used to output dictionary
	for codeHeap.Len()+(patternList.Len()-i) > 1 {
		// New node
		h := &PatternHuff{
			tieBreaker: tieBreaker,
			offset:     offset,
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
			n := binary.PutUvarint(numBuf, h.h0.offset)
			offset += uint64(n)
		} else {
			// Take p0 from the list
			h.p0 = patternList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			n := binary.PutUvarint(numBuf, h.p0.offset)
			offset += uint64(n)
			i++
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
			n := binary.PutUvarint(numBuf, h.h1.offset)
			offset += uint64(n)
		} else {
			// Take p1 from the list
			h.p1 = patternList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			n := binary.PutUvarint(numBuf, h.p1.offset)
			offset += uint64(n)
			i++
		}
		tieBreaker++
		heap.Push(&codeHeap, h)
		huffs = append(huffs, h)
	}
	root := &PatternHuff{}
	if len(patternList) > 0 {
		root = heap.Pop(&codeHeap).(*PatternHuff)
	}
	var cf *os.File
	var err error
	if cf, err = os.Create(segmentFileName); err != nil {
		return err
	}
	cw := bufio.NewWriterSize(cf, etl.BufIOSize)
	// First, output dictionary
	binary.BigEndian.PutUint64(numBuf, offset) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Secondly, output directory root
	binary.BigEndian.PutUint64(numBuf, root.offset)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Thirdly, output pattern cutoff offset
	binary.BigEndian.PutUint64(numBuf, patternCutoff)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Write all the pattens
	for _, p := range patternList {
		n := binary.PutUvarint(numBuf, uint64(len(p.w)))
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err = cw.Write(p.w); err != nil {
			return err
		}
	}
	// Write all the huffman nodes
	for _, h := range huffs {
		var n int
		if h.h0 != nil {
			n = binary.PutUvarint(numBuf, h.h0.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p0.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if h.h1 != nil {
			n = binary.PutUvarint(numBuf, h.h1.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p1.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
	}
	log.Info("Dictionary", "size", offset, "pattern cutoff", patternCutoff)

	var positionList PositionList
	pos2code := make(map[uint64]*Position)
	for pos, uses := range posMap {
		p := &Position{pos: pos, uses: uses, code: 0, codeBits: 0, offset: 0}
		positionList = append(positionList, p)
		pos2code[pos] = p
	}
	sort.Sort(&positionList)
	// Calculate offsets of the dictionary positions and total size
	offset = 0
	for _, p := range positionList {
		p.offset = offset
		n := binary.PutUvarint(numBuf, p.pos)
		offset += uint64(n)
	}
	positionCutoff := offset // All offsets below this will be considered positions
	i = 0
	log.Info("Positional dictionary", "size", positionList.Len())
	// Build Huffman tree for codes
	var posHeap PositionHeap
	heap.Init(&posHeap)
	tieBreaker = uint64(0)
	var posHuffs []*PositionHuff // To be used to output dictionary
	for posHeap.Len()+(positionList.Len()-i) > 1 {
		// New node
		h := &PositionHuff{
			tieBreaker: tieBreaker,
			offset:     offset,
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&posHeap).(*PositionHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
			n := binary.PutUvarint(numBuf, h.h0.offset)
			offset += uint64(n)
		} else {
			// Take p0 from the list
			h.p0 = positionList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			n := binary.PutUvarint(numBuf, h.p0.offset)
			offset += uint64(n)
			i++
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&posHeap).(*PositionHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
			n := binary.PutUvarint(numBuf, h.h1.offset)
			offset += uint64(n)
		} else {
			// Take p1 from the list
			h.p1 = positionList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			n := binary.PutUvarint(numBuf, h.p1.offset)
			offset += uint64(n)
			i++
		}
		tieBreaker++
		heap.Push(&posHeap, h)
		posHuffs = append(posHuffs, h)
	}
	posRoot := heap.Pop(&posHeap).(*PositionHuff)
	// First, output dictionary
	binary.BigEndian.PutUint64(numBuf, offset) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Secondly, output directory root
	binary.BigEndian.PutUint64(numBuf, posRoot.offset)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Thirdly, output pattern cutoff offset
	binary.BigEndian.PutUint64(numBuf, positionCutoff)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Write all the positions
	for _, p := range positionList {
		n := binary.PutUvarint(numBuf, p.pos)
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
	}
	// Write all the huffman nodes
	for _, h := range posHuffs {
		var n int
		if h.h0 != nil {
			n = binary.PutUvarint(numBuf, h.h0.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p0.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if h.h1 != nil {
			n = binary.PutUvarint(numBuf, h.h1.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p1.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
	}
	log.Info("Positional dictionary", "size", offset, "position cutoff", positionCutoff)
	df, err := os.Create("huffman_codes.txt")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(df, etl.BufIOSize)
	for _, p := range positionList {
		fmt.Fprintf(w, "%d %x %d uses %d\n", p.codeBits, p.code, p.pos, p.uses)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	df.Close()

	aggregator := etl.NewCollector(CompressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	for _, collector := range collectors {
		if err = collector.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return aggregator.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		collector.Close()
	}

	wc := 0
	var hc HuffmanCoder
	hc.w = cw
	if err = aggregator.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Re-encode it
		r := bytes.NewReader(v)
		var l uint64
		var e error
		if l, err = binary.ReadUvarint(r); e != nil {
			return e
		}
		posCode := pos2code[l+1]
		if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
			return e
		}
		if l > 0 {
			var pNum uint64 // Number of patterns
			if pNum, e = binary.ReadUvarint(r); e != nil {
				return e
			}
			// Now reading patterns one by one
			var lastPos uint64
			var lastUncovered int
			var uncoveredCount int
			for i := 0; i < int(pNum); i++ {
				var pos uint64 // Starting position for pattern
				if pos, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				posCode = pos2code[pos-lastPos+1]
				lastPos = pos
				if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
					return e
				}
				var code uint64 // Code of the pattern
				if code, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				patternCode := code2pattern[code]
				if int(pos) > lastUncovered {
					uncoveredCount += int(pos) - lastUncovered
				}
				lastUncovered = int(pos) + len(patternCode.w)
				if e = hc.encode(patternCode.code, patternCode.codeBits); e != nil {
					return e
				}
			}
			if int(l) > lastUncovered {
				uncoveredCount += int(l) - lastUncovered
			}
			// Terminating position and flush
			posCode = pos2code[0]
			if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
				return e
			}
			if e = hc.flush(); e != nil {
				return e
			}
			// Copy uncovered characters
			if uncoveredCount > 0 {
				if _, e = io.CopyN(cw, r, int64(uncoveredCount)); e != nil {
					return e
				}
			}
		}
		wc++
		if wc%10_000_000 == 0 {
			log.Info("Compressed", "millions", wc/1_000_000)
		}
		return nil
	}, etl.TransformArgs{}); err != nil {
		return err
	}
	aggregator.Close()
	if err = cw.Flush(); err != nil {
		return err
	}
	if err = cf.Close(); err != nil {
		return err
	}
	return nil
}
func recsplitWholeChain(chaindata string) error {
	blocksPerFile := uint64(*blockTotal) //uint64(500_000)
	lastChunk := func(tx kv.Tx, blocksPerFile uint64) (uint64, error) {
		c, err := tx.Cursor(kv.BlockBody)
		if err != nil {
			return 0, err
		}
		k, _, err := c.Last()
		if err != nil {
			return 0, err
		}
		last := binary.BigEndian.Uint64(k)
		// TODO: enable next condition (disabled for tests)
		//if last > params.FullImmutabilityThreshold {
		//	last -= params.FullImmutabilityThreshold
		//} else {
		//	last = 0
		//}
		last = last - last%blocksPerFile
		return last, nil
	}

	var last uint64

	database := mdbx.MustOpen(chaindata)
	defer database.Close()
	chainConfig := tool.ChainConfigFromDB(database)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)
	_ = chainID
	if err := database.View(context.Background(), func(tx kv.Tx) (err error) {
		last, err = lastChunk(tx, blocksPerFile)
		return err
	}); err != nil {
		return err
	}
	database.Close()
	dataDir := path.Dir(chaindata)
	snapshotDir := path.Join(dataDir, "snapshots")
	_ = os.MkdirAll(snapshotDir, fs.ModePerm)

	log.Info("Last body number", "last", last)
	for i := uint64(*block); i < last; i += blocksPerFile {
		fileName := snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Transactions)
		segmentFile := path.Join(snapshotDir, fileName) + ".seg"
		log.Info("Creating", "file", fileName+".seg")
		db := mdbx.MustOpen(chaindata)
		if err := snapshotsync.DumpTxs(db, "", i, int(blocksPerFile)); err != nil {
			panic(err)
		}
		db.Close()
		if err := compress1(chaindata, fileName, segmentFile); err != nil {
			panic(err)
		}
		if err := snapshotsync.TransactionsIdx(*chainID, segmentFile); err != nil {
			panic(err)
		}
		_ = os.Remove(fileName + ".dat")

		fileName = snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Headers)
		segmentFile = path.Join(snapshotDir, fileName) + ".seg"
		log.Info("Creating", "file", fileName+".seg")
		db = mdbx.MustOpen(chaindata)
		if err := snapshotsync.DumpHeaders(db, "", i, int(blocksPerFile)); err != nil {
			panic(err)
		}
		db.Close()
		if err := compress1(chaindata, fileName, segmentFile); err != nil {
			panic(err)
		}

		if err := snapshotsync.HeadersIdx(segmentFile); err != nil {
			panic(err)
		}
		_ = os.Remove(fileName + ".dat")

		fileName = snapshotsync.FileName(i, i+blocksPerFile, snapshotsync.Bodies)
		segmentFile = path.Join(snapshotDir, fileName) + ".seg"
		log.Info("Creating", "file", fileName+".seg")
		db = mdbx.MustOpen(chaindata)
		if err := snapshotsync.DumpBodies(db, "", i, int(blocksPerFile)); err != nil {
			panic(err)
		}
		db.Close()
		if err := compress1(chaindata, fileName, segmentFile); err != nil {
			panic(err)
		}
		if err := snapshotsync.BodiesIdx(segmentFile); err != nil {
			panic(err)
		}
		_ = os.Remove(fileName + ".dat")

		//nolint
		break // TODO: remove me - useful for tests
	}
	return nil
}

func decompress(name string) error {
	d, err := compress.NewDecompressor(name + ".seg")
	if err != nil {
		return err
	}
	defer d.Close()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	var df *os.File
	if df, err = os.Create(name + ".decompressed.dat"); err != nil {
		return err
	}
	dw := bufio.NewWriterSize(df, etl.BufIOSize)
	var word = make([]byte, 0, 256)
	numBuf := make([]byte, binary.MaxVarintLen64)
	var decodeTime time.Duration
	g := d.MakeGetter()
	start := time.Now()
	wc := 0
	for g.HasNext() {
		word, _ = g.Next(word[:0])
		decodeTime += time.Since(start)
		n := binary.PutUvarint(numBuf, uint64(len(word)))
		if _, e := dw.Write(numBuf[:n]); e != nil {
			return e
		}
		if len(word) > 0 {
			if _, e := dw.Write(word); e != nil {
				return e
			}
		}
		wc++
		select {
		default:
		case <-logEvery.C:
			log.Info("Decompressed", "millions", wc/1_000_000)
		}
		start = time.Now()
	}
	log.Info("Average decoding time", "per word", time.Duration(int64(decodeTime)/int64(wc)))
	if err = dw.Flush(); err != nil {
		return err
	}
	if err = df.Close(); err != nil {
		return err
	}
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
		// This is a mapping of CodeHash => Byte code
		if err := tx.ForEach(kv.Code, nil, func(k, v []byte) error {
			if len(v) > 0 && v[0] == 0xef {
				fmt.Printf("Found code with hash %x: %x\n", k, v)
				hashes[common.BytesToHash(k)] = common.CopyBytes(v)
			}
			return nil
		}); err != nil {
			return err
		}
		// This is a mapping of contractAddress + incarnation => CodeHash
		if err := tx.ForEach(kv.PlainContractCode, nil, func(k, v []byte) error {
			hash := common.BytesToHash(v)
			if code, ok := hashes[hash]; ok {
				fmt.Printf("address: %x: %x\n", k[:20], code)
			}
			return nil
		}); err != nil {
			return err
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

func extractHeaders(chaindata string, block uint64, blockTotal uint64) error {
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
	for k, v, err := c.Seek(blockEncoded); k != nil && blockTotal > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])
		blockHash := common.BytesToHash(k[8:])
		var header types.Header
		if err = rlp.DecodeBytes(v, &header); err != nil {
			return fmt.Errorf("decoding header from %x: %w", v, err)
		}
		fmt.Printf("Header %d %x: stateRoot %x, parentHash %x, diff %d\n", blockNumber, blockHash, header.Root, header.ParentHash, header.Difficulty)
		blockTotal--
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
				return fmt.Errorf("decoding header from %x: %w", v, err)
			}
			if header.Number.Uint64() == 0 {
				continue
			}
			var parentK [40]byte
			binary.BigEndian.PutUint64(parentK[:], header.Number.Uint64()-1)
			copy(parentK[8:], header.ParentHash[:])
			var parentTdRec []byte
			if parentTdRec, err = tx.GetOne(kv.HeaderTD, parentK[:]); err != nil {
				return fmt.Errorf("reading parentTd Rec for %d: %w", header.Number.Uint64(), err)
			}
			var parentTd big.Int
			if err = rlp.DecodeBytes(parentTdRec, &parentTd); err != nil {
				return fmt.Errorf("decoding parent Td record for block %d, from %x: %w", header.Number.Uint64(), parentTdRec, err)
			}
			var td big.Int
			td.Add(&parentTd, header.Difficulty)
			var newHv []byte
			if newHv, err = rlp.EncodeToBytes(&td); err != nil {
				return fmt.Errorf("encoding td record for block %d: %w", header.Number.Uint64(), err)
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
			return fmt.Errorf("decoding header from %x: %w", v, err)
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
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
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

	chainConfig := tool.ChainConfig(tx)
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
				return fmt.Errorf("encode block receipts for block %d: %w", blockNum, err)
			}
			if err = tx.Put(kv.Receipts, key[:], buf.Bytes()); err != nil {
				return fmt.Errorf("writing receipts for block %d: %w", blockNum, err)
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
	systemcontracts.UpgradeBuildInSystemContract(chainConfig, header.Number, ibs)
	rules := chainConfig.Rules(block.NumberU64())
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, txnWriter, header, tx, usedGas, vmConfig, contractHasTEVM)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
		//fmt.Printf("%d, cumulative gas: %d\n", i, receipt.CumulativeGasUsed)
	}

	if !vmConfig.ReadOnly {
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		if _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, nil, nil, nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(rules, blockWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
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
	cc := tool.ChainConfig(tx)
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
	debug.RaiseFdLimit()
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
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Error("Failure in running pprof server", "err", err)
		}
	}()

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
		err = extractHeaders(*chaindata, uint64(*block), uint64(*blockTotal))

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
		err = mphf(*chaindata, *block)

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
	case "dumpState":
		err = dumpState(*chaindata, int(*block), *name)
	case "compress":
		err = compress1(*chaindata, *name, *name)
	case "recsplitWholeChain":
		err = recsplitWholeChain(*chaindata)
	case "decompress":
		err = decompress(*name)
	case "genstate":
		err = genstate()
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
