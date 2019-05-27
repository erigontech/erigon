package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"io"
	"io/ioutil"
	"log"
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

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"github.com/wcharczuk/go-chart/util"
)

var emptyCodeHash = crypto.Keccak256(nil)

var action = flag.String("action", "", "action to execute")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write mem profile `file`")
var reset = flag.Int("reset", -1, "reset to given block number")
var rewind = flag.Int("rewind", 1, "rewind to given number of blocks")
var block = flag.Int("block", 1, "specifies a block number for operation")
var account = flag.String("account", "0x", "specifies account to investigate")
var genLag = flag.Int("genlag", 4096, "how many blocks to accumulate block proofs over for generator")
var consLag = flag.Int("conslag", 256, "how many blocks to accumulate block proofs over for consumer")

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func decodeTimestamp(suffix []byte) (uint64, []byte) {
	bytecount := int(suffix[0] >> 5)
	timestamp := uint64(suffix[0] & 0x1f)
	for i := 1; i < bytecount; i++ {
		timestamp = (timestamp << 8) | uint64(suffix[i])
	}
	return timestamp, suffix[bytecount:]
}

// Implements sort.Interface
type TimeSorterInt struct {
	length     int
	timestamps []uint64
	values     []int
}

func NewTimeSorterInt(length int) TimeSorterInt {
	return TimeSorterInt{
		length:     length,
		timestamps: make([]uint64, length),
		values:     make([]int, length),
	}
}

func (tsi TimeSorterInt) Len() int {
	return tsi.length
}

func (tsi TimeSorterInt) Less(i, j int) bool {
	return tsi.timestamps[i] < tsi.timestamps[j]
}

func (tsi TimeSorterInt) Swap(i, j int) {
	tsi.timestamps[i], tsi.timestamps[j] = tsi.timestamps[j], tsi.timestamps[i]
	tsi.values[i], tsi.values[j] = tsi.values[j], tsi.values[i]
}

type IntSorterAddr struct {
	length int
	ints   []int
	values []common.Address
}

func NewIntSorterAddr(length int) IntSorterAddr {
	return IntSorterAddr{
		length: length,
		ints:   make([]int, length),
		values: make([]common.Address, length),
	}
}

func (isa IntSorterAddr) Len() int {
	return isa.length
}

func (isa IntSorterAddr) Less(i, j int) bool {
	return isa.ints[i] > isa.ints[j]
}

func (isa IntSorterAddr) Swap(i, j int) {
	isa.ints[i], isa.ints[j] = isa.ints[j], isa.ints[i]
	isa.values[i], isa.values[j] = isa.values[j], isa.values[i]
}

func stateGrowth1() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth-10/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	creatorsFile, err := os.Open("creators.csv")
	check(err)
	defer creatorsFile.Close()
	creatorsReader := csv.NewReader(bufio.NewReader(creatorsFile))
	creators := make(map[common.Address]common.Address)
	for records, _ := creatorsReader.Read(); records != nil; records, _ = creatorsReader.Read() {
		creators[common.HexToAddress(records[0])] = common.HexToAddress(records[1])
	}
	var count int
	var maxTimestamp uint64
	// For each address hash, when was it last accounted
	lastTimestamps := make(map[common.Address]uint64)
	// For each timestamp, how many accounts were created in the state
	creationsByBlock := make(map[uint64]int)
	creatorsByBlock := make(map[common.Address]map[uint64]int)
	var addrHash common.Hash
	var address common.Address
	// Go through the history of account first
	err = db.View(func(tx *bolt.Tx) error {
		pre := tx.Bucket(dbutils.PreimagePrefix)
		if pre == nil {
			return nil
		}
		b := tx.Bucket(dbutils.AccountsHistoryBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// First 32 bytes is the hash of the address, then timestamp encoding
			copy(addrHash[:], k[:32])
			// Figure out the addrees via preimage
			addr, _ := pre.Get(addrHash[:])
			copy(address[:], addr)
			creator := creators[address]
			timestamp, _ := decodeTimestamp(k[32:])
			if timestamp+1 > maxTimestamp {
				maxTimestamp = timestamp + 1
			}
			if len(v) == 0 {
				cr, ok := creatorsByBlock[creator]
				if !ok {
					cr = make(map[uint64]int)
					creatorsByBlock[creator] = cr
				}
				creationsByBlock[timestamp]++
				cr[timestamp]++
				if lt, ok := lastTimestamps[address]; ok {
					creationsByBlock[lt]--
					cr[lt]--
				}
			}
			lastTimestamps[address] = timestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d account records\n", count)
			}
		}
		return nil
	})
	check(err)
	// Go through the current state
	err = db.View(func(tx *bolt.Tx) error {
		pre := tx.Bucket(dbutils.PreimagePrefix)
		if pre == nil {
			return nil
		}
		b := tx.Bucket(dbutils.AccountsBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			// First 32 bytes is the hash of the address
			copy(addrHash[:], k[:32])
			// Figure out the addrees via preimage
			addr, _ := pre.Get(addrHash[:])
			copy(address[:], addr)
			lastTimestamps[address] = maxTimestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d account records\n", count)
			}
		}
		return nil
	})
	check(err)
	for address, lt := range lastTimestamps {
		if lt < maxTimestamp {
			creationsByBlock[lt]--
			creator := creators[address]
			cr, ok := creatorsByBlock[creator]
			if !ok {
				cr = make(map[uint64]int)
				creatorsByBlock[creator] = cr
			}
			cr[lt]--
		}
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Account history records: %d\n", count)
	fmt.Printf("Creating dataset...\n")
	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(creationsByBlock))
	idx := 0
	for timestamp, count := range creationsByBlock {
		tsi.timestamps[idx] = timestamp
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...")
	f, err := os.Create("accounts_growth.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < tsi.length; i++ {
		cumulative += tsi.values[i]
		fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
	}
	cisa := NewIntSorterAddr(len(creatorsByBlock))
	idx = 0
	for creator, cr := range creatorsByBlock {
		cumulative := 0
		for _, count := range cr {
			cumulative += count
		}
		cisa.ints[idx] = cumulative
		cisa.values[idx] = creator
		idx++
	}
	sort.Sort(cisa)
	// Top 16 account creators
	for i := 0; i < 20 && i < cisa.length; i++ {
		creator := cisa.values[i]
		tsi := NewTimeSorterInt(len(creatorsByBlock[creator]))
		idx := 0
		for timestamp, count := range creatorsByBlock[creator] {
			tsi.timestamps[idx] = timestamp
			tsi.values[idx] = count
			idx++
		}
		sort.Sort(tsi)
		fmt.Printf("Writing dataset for creator %x...\n", creator[:])
		f, err := os.Create(fmt.Sprintf("acc_creator_%x.csv", creator[:]))
		check(err)
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		cumulative := 0
		for i := 0; i < tsi.length; i++ {
			cumulative += tsi.values[i]
			fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
		}
	}
}

func stateGrowth2() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth-10/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	creatorsFile, err := os.Open("creators.csv")
	check(err)
	defer creatorsFile.Close()
	creatorsReader := csv.NewReader(bufio.NewReader(creatorsFile))
	creators := make(map[common.Address]common.Address)
	for records, _ := creatorsReader.Read(); records != nil; records, _ = creatorsReader.Read() {
		creators[common.HexToAddress(records[0])] = common.HexToAddress(records[1])
	}
	var count int
	var maxTimestamp uint64
	// For each address hash, when was it last accounted
	lastTimestamps := make(map[common.Address]map[common.Hash]uint64)
	// For each timestamp, how many storage items were created
	creationsByBlock := make(map[common.Address]map[uint64]int)
	// For each timestamp, how many accounts or storage items were created by the creator
	creatorsByBlock := make(map[common.Address]map[uint64]int)
	var address common.Address
	var hash common.Hash
	// Go through the history of account first
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageHistoryBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// First 20 bytes is the address
			copy(address[:], k[:20])
			copy(hash[:], k[20:52])
			timestamp, _ := decodeTimestamp(k[52:])
			creator := creators[address]
			if timestamp+1 > maxTimestamp {
				maxTimestamp = timestamp + 1
			}
			if len(v) == 0 {
				c, ok := creationsByBlock[address]
				if !ok {
					c = make(map[uint64]int)
					creationsByBlock[address] = c
				}
				c[timestamp]++
				cr, ok := creatorsByBlock[creator]
				if !ok {
					cr = make(map[uint64]int)
					creatorsByBlock[creator] = cr
				}
				cr[timestamp]++
				l, ok := lastTimestamps[address]
				if !ok {
					l = make(map[common.Hash]uint64)
					lastTimestamps[address] = l
				}
				if lt, ok := l[hash]; ok {
					c[lt]--
					cr[lt]--
				}
			}
			l, ok := lastTimestamps[address]
			if !ok {
				l = make(map[common.Hash]uint64)
				lastTimestamps[address] = l
			}
			l[hash] = timestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records\n", count)
			}
		}
		return nil
	})
	check(err)
	// Go through the current state
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(address[:], k[:20])
			copy(hash[:], k[20:52])
			l, ok := lastTimestamps[address]
			if !ok {
				l = make(map[common.Hash]uint64)
				lastTimestamps[address] = l
			}
			l[hash] = maxTimestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records\n", count)
			}
		}
		return nil
	})
	check(err)
	for address, l := range lastTimestamps {
		for _, lt := range l {
			if lt < maxTimestamp {
				creationsByBlock[address][lt]--
				creator := creators[address]
				cr, ok := creatorsByBlock[creator]
				if !ok {
					cr = make(map[uint64]int)
					creatorsByBlock[creator] = cr
				}
				cr[lt]--
			}
		}
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Storage history records: %d\n", count)
	fmt.Printf("Creating dataset...\n")
	totalCreationsByBlock := make(map[uint64]int)
	isa := NewIntSorterAddr(len(creationsByBlock))
	idx := 0
	for addr, c := range creationsByBlock {
		cumulative := 0
		for timestamp, count := range c {
			totalCreationsByBlock[timestamp] += count
			cumulative += count
		}
		isa.ints[idx] = cumulative
		isa.values[idx] = addr
		idx++
	}
	sort.Sort(isa)
	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(totalCreationsByBlock))
	idx = 0
	for timestamp, count := range totalCreationsByBlock {
		tsi.timestamps[idx] = timestamp
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("storage_growth.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < tsi.length; i++ {
		cumulative += tsi.values[i]
		fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
	}
	// Top 16 contracts
	for i := 0; i < 16 && i < isa.length; i++ {
		addr := isa.values[i]
		tsi := NewTimeSorterInt(len(creationsByBlock[addr]))
		idx := 0
		for timestamp, count := range creationsByBlock[addr] {
			tsi.timestamps[idx] = timestamp
			tsi.values[idx] = count
			idx++
		}
		sort.Sort(tsi)
		fmt.Printf("Writing dataset for contract %x...\n", addr[:])
		f, err := os.Create(fmt.Sprintf("growth_%x.csv", addr[:]))
		check(err)
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		cumulative := 0
		for i := 0; i < tsi.length; i++ {
			cumulative += tsi.values[i]
			fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
		}
	}
	cisa := NewIntSorterAddr(len(creatorsByBlock))
	idx = 0
	for creator, cr := range creatorsByBlock {
		cumulative := 0
		for _, count := range cr {
			cumulative += count
		}
		cisa.ints[idx] = cumulative
		cisa.values[idx] = creator
		idx++
	}
	sort.Sort(cisa)
	// Top 16 creators
	for i := 0; i < 16 && i < cisa.length; i++ {
		creator := cisa.values[i]
		tsi := NewTimeSorterInt(len(creatorsByBlock[creator]))
		idx := 0
		for timestamp, count := range creatorsByBlock[creator] {
			tsi.timestamps[idx] = timestamp
			tsi.values[idx] = count
			idx++
		}
		sort.Sort(tsi)
		fmt.Printf("Writing dataset for creator %x...\n", creator[:])
		f, err := os.Create(fmt.Sprintf("creator_%x.csv", creator[:]))
		check(err)
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		cumulative := 0
		for i := 0; i < tsi.length; i++ {
			cumulative += tsi.values[i]
			fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
		}
	}
}

func parseFloat64(str string) float64 {
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func readData(filename string) (blocks []float64, items []float64, err error) {
	err = util.File.ReadByLines(filename, func(line string) error {
		parts := strings.Split(line, ",")
		blocks = append(blocks, parseFloat64(strings.Trim(parts[0], " "))/1000000.0)
		items = append(items, parseFloat64(strings.Trim(parts[1], " "))/1000000.0)
		return nil
	})
	return
}

func blockMillions() []chart.GridLine {
	return []chart.GridLine{
		{Value: 1.0},
		{Value: 2.0},
		{Value: 3.0},
		{Value: 4.0},
		{Value: 5.0},
		{Value: 6.0},
		{Value: 7.0},
	}
}

func accountMillions() []chart.GridLine {
	return []chart.GridLine{
		{Value: 5.0},
		{Value: 10.0},
		{Value: 15.0},
		{Value: 20.0},
		{Value: 25.0},
		{Value: 30.0},
		{Value: 35.0},
		{Value: 40.0},
		{Value: 45.0},
	}
}

func stateGrowthChart1() {
	blocks, accounts, err := readData("accounts_growth.csv")
	check(err)
	mainSeries := &chart.ContinuousSeries{
		Name: "Number of accounts (EOA and contracts)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: blocks,
		YValues: accounts,
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
			Name:      "Accounts",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: accountMillions(),
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			mainSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("accounts_growth.png", buffer.Bytes(), 0644)
	check(err)
}

func storageMillions() []chart.GridLine {
	return []chart.GridLine{
		{Value: 20.0},
		{Value: 40.0},
		{Value: 60.0},
		{Value: 80.0},
		{Value: 100.0},
		{Value: 120.0},
		{Value: 140.0},
	}
}

func stateGrowthChart2() {
	blocks, accounts, err := readData("storage_growth.csv")
	check(err)
	mainSeries := &chart.ContinuousSeries{
		Name: "Number of contract storage items",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorGreen,
			FillColor:   chart.ColorGreen.WithAlpha(100),
		},
		XValues: blocks,
		YValues: accounts,
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
			Name:      "Storage items",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: storageMillions(),
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			mainSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("storage_growth.png", buffer.Bytes(), 0644)
	check(err)
}

func stateGrowthChart3() {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		panic(err)
	}
	colors := []drawing.Color{
		chart.ColorRed,
		chart.ColorOrange,
		chart.ColorYellow,
		chart.ColorGreen,
		chart.ColorBlue,
		{R: 255, G: 0, B: 255, A: 255},
		chart.ColorBlack,
		{R: 165, G: 42, B: 42, A: 255},
	}
	seriesList := []chart.Series{}
	colorIdx := 0
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "growth_") && strings.HasSuffix(f.Name(), ".csv") {
			blocks, items, err := readData(f.Name())
			check(err)
			seriesList = append(seriesList, &chart.ContinuousSeries{
				Name: f.Name()[len("growth_") : len(f.Name())-len(".csv")],
				Style: chart.Style{
					StrokeWidth: float64(1 + 2*(colorIdx/len(colors))),
					StrokeColor: colors[colorIdx%len(colors)],
					Show:        true,
				},
				XValues: blocks,
				YValues: items,
			})
			colorIdx++
		}
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
			Name:      "Storage items",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: storageMillions(),
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
			GridLines: blockMillions(),
		},
		Series: seriesList,
	}

	graph1.Elements = []chart.Renderable{chart.LegendLeft(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("top_16_contracts.png", buffer.Bytes(), 0644)
	check(err)
}

func stateGrowthChart4() {
	addrFile, err := os.Open("addresses.csv")
	check(err)
	defer addrFile.Close()
	addrReader := csv.NewReader(bufio.NewReader(addrFile))
	names := make(map[string]string)
	for records, _ := addrReader.Read(); records != nil; records, _ = addrReader.Read() {
		names[records[0]] = records[1]
	}
	files, err := ioutil.ReadDir("./")
	if err != nil {
		panic(err)
	}
	colors := []drawing.Color{
		chart.ColorRed,
		chart.ColorOrange,
		chart.ColorYellow,
		chart.ColorGreen,
		chart.ColorBlue,
		{R: 255, G: 0, B: 255, A: 255},
		chart.ColorBlack,
		{R: 165, G: 42, B: 42, A: 255},
	}
	seriesList := []chart.Series{}
	colorIdx := 0
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "creator_") && strings.HasSuffix(f.Name(), ".csv") {
			blocks, items, err := readData(f.Name())
			check(err)
			addr := f.Name()[len("creator_") : len(f.Name())-len(".csv")]
			if name, ok := names[addr]; ok {
				addr = name
			}
			seriesList = append(seriesList, &chart.ContinuousSeries{
				Name: addr,
				Style: chart.Style{
					StrokeWidth: float64(1 + 2*(colorIdx/len(colors))),
					StrokeColor: colors[colorIdx%len(colors)],
					Show:        true,
				},
				XValues: blocks,
				YValues: items,
			})
			colorIdx++
		}
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
			Name:      "Storage items",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: storageMillions(),
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
			GridLines: blockMillions(),
		},
		Series: seriesList,
	}

	graph1.Elements = []chart.Renderable{chart.LegendLeft(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("top_16_creators.png", buffer.Bytes(), 0644)
	check(err)
}

func stateGrowthChart5() {
	addrFile, err := os.Open("addresses.csv")
	check(err)
	defer addrFile.Close()
	addrReader := csv.NewReader(bufio.NewReader(addrFile))
	names := make(map[string]string)
	for records, _ := addrReader.Read(); records != nil; records, _ = addrReader.Read() {
		names[records[0]] = records[1]
	}
	files, err := ioutil.ReadDir("./")
	if err != nil {
		panic(err)
	}
	colors := []drawing.Color{
		chart.ColorRed,
		chart.ColorOrange,
		chart.ColorYellow,
		chart.ColorGreen,
		chart.ColorBlue,
		{R: 255, G: 0, B: 255, A: 255},
		chart.ColorBlack,
		{R: 165, G: 42, B: 42, A: 255},
	}
	seriesList := []chart.Series{}
	colorIdx := 0
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "acc_creator_") && strings.HasSuffix(f.Name(), ".csv") {
			blocks, items, err := readData(f.Name())
			check(err)
			addr := f.Name()[len("acc_creator_") : len(f.Name())-len(".csv")]
			if name, ok := names[addr]; ok {
				addr = name
			}
			seriesList = append(seriesList, &chart.ContinuousSeries{
				Name: addr,
				Style: chart.Style{
					StrokeWidth: float64(1 + 2*(colorIdx/len(colors))),
					StrokeColor: colors[colorIdx%len(colors)],
					Show:        true,
				},
				XValues: blocks,
				YValues: items,
			})
			colorIdx++
		}
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
			Name:      "Accounts created",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: storageMillions(),
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
			GridLines: blockMillions(),
		},
		Series: seriesList,
	}

	graph1.Elements = []chart.Renderable{chart.LegendLeft(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("top_2_acc_creators.png", buffer.Bytes(), 0644)
	check(err)
}

type CreationTracer struct {
	w io.Writer
}

func NewCreationTracer(w io.Writer) CreationTracer {
	return CreationTracer{w: w}
}

func (ct CreationTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (ct CreationTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct CreationTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct CreationTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct CreationTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	_, err := fmt.Fprintf(ct.w, "%x,%x\n", creation, creator)
	return err
}
func (ct CreationTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct CreationTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

func makeCreators() {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb41/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	f, err := os.OpenFile("/Volumes/tb41/turbo-geth/creators.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	ct := NewCreationTracer(w)
	chainConfig := params.MainnetChainConfig
	vmConfig := vm.Config{Tracer: ct, Debug: true}
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil)
	check(err)
	blockNum := uint64(*block)
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewDbState(ethDb, block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Next time specify -block %d\n", blockNum)
}

func storageUsage() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth-10/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	/*
		creatorsFile, err := os.Open("creators.csv")
		check(err)
		defer creatorsFile.Close()
		creatorsReader := csv.NewReader(bufio.NewReader(creatorsFile))
		creators := make(map[common.Address]common.Address)
		for records, _ := creatorsReader.Read(); records != nil; records, _ = creatorsReader.Read() {
			creators[common.HexToAddress(records[0])] = common.HexToAddress(records[1])
		}
	*/
	addrFile, err := os.Open("addresses.csv")
	check(err)
	defer addrFile.Close()
	addrReader := csv.NewReader(bufio.NewReader(addrFile))
	names := make(map[common.Address]string)
	for records, _ := addrReader.Read(); records != nil; records, _ = addrReader.Read() {
		names[common.HexToAddress(records[0])] = records[1]
	}
	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	deleted := make(map[common.Address]bool) // Deleted contracts
	numDeleted := 0
	//itemsByCreator := make(map[common.Address]int)
	count := 0
	var leafSize uint64
	err = db.View(func(tx *bolt.Tx) error {
		a := tx.Bucket(dbutils.AccountsBucket)
		b := tx.Bucket(dbutils.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			copy(addr[:], k[:20])
			del, ok := deleted[addr]
			if !ok {
				vv, _ := a.Get(crypto.Keccak256(addr[:]))
				del = vv == nil
				deleted[addr] = del
				if del {
					numDeleted++
				}
			}
			if del {
				continue
			}
			itemsByAddress[addr]++
			//itemsByCreator[creators[addr]]++
			leafSize += uint64(len(v))
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records, deleted contracts: %d\n", count, numDeleted)
			}
		}
		return nil
	})
	check(err)
	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Average leaf size: %d/%d\n", leafSize, count)
	iba := NewIntSorterAddr(len(itemsByAddress))
	idx := 0
	total := 0
	for address, items := range itemsByAddress {
		total += items
		iba.ints[idx] = items
		iba.values[idx] = address
		idx++
	}
	sort.Sort(iba)
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("/Volumes/tb4/turbo-geth/items_by_address.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < iba.length; i++ {
		cumulative += iba.ints[i]
		if name, ok := names[iba.values[i]]; ok {
			fmt.Fprintf(w, "%d,%x,%s,%d,%.3f\n", i+1, iba.values[i], name, iba.ints[i], 100.0*float64(cumulative)/float64(total))
		} else {
			fmt.Fprintf(w, "%d,%x,,%d,%.3f\n", i+1, iba.values[i], iba.ints[i], 100.0*float64(cumulative)/float64(total))
		}
	}
	fmt.Printf("Total storage items: %d\n", cumulative)
	/*
		ciba := NewIntSorterAddr(len(itemsByCreator))
		idx = 0
		for creator, items := range itemsByCreator {
			ciba.ints[idx] = items
			ciba.values[idx] = creator
			idx++
		}
		sort.Sort(ciba)
		fmt.Printf("Writing dataset...\n")
		cf, err := os.Create("items_by_creator.csv")
		check(err)
		defer cf.Close()
		cw := bufio.NewWriter(cf)
		defer cw.Flush()
		cumulative = 0
		for i := 0; i < ciba.length; i++ {
			cumulative += ciba.ints[i]
			fmt.Fprintf(cw, "%d,%x,%d,%.3f\n", i, ciba.values[i], ciba.ints[i], float64(cumulative)/float64(total))
		}
	*/
}

func tokenUsage() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	tokensFile, err := os.Open("tokens.csv")
	check(err)
	defer tokensFile.Close()
	tokensReader := csv.NewReader(bufio.NewReader(tokensFile))
	tokens := make(map[common.Address]struct{})
	for records, _ := tokensReader.Read(); records != nil; records, _ = tokensReader.Read() {
		tokens[common.HexToAddress(records[0])] = struct{}{}
	}
	addrFile, err := os.Open("addresses.csv")
	check(err)
	defer addrFile.Close()
	addrReader := csv.NewReader(bufio.NewReader(addrFile))
	names := make(map[common.Address]string)
	for records, _ := addrReader.Read(); records != nil; records, _ = addrReader.Read() {
		names[common.HexToAddress(records[0])] = records[1]
	}
	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	//itemsByCreator := make(map[common.Address]int)
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(addr[:], k[:20])
			if _, ok := tokens[addr]; ok {
				itemsByAddress[addr]++
				count++
				if count%100000 == 0 {
					fmt.Printf("Processed %d storage records\n", count)
				}
			}
		}
		return nil
	})
	check(err)
	fmt.Printf("Processing took %s\n", time.Since(startTime))
	iba := NewIntSorterAddr(len(itemsByAddress))
	idx := 0
	total := 0
	for address, items := range itemsByAddress {
		total += items
		iba.ints[idx] = items
		iba.values[idx] = address
		idx++
	}
	sort.Sort(iba)
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("items_by_token.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < iba.length; i++ {
		cumulative += iba.ints[i]
		if name, ok := names[iba.values[i]]; ok {
			fmt.Fprintf(w, "%d,%s,%d,%.3f\n", i+1, name, iba.ints[i], 100.0*float64(cumulative)/float64(total))
		} else {
			fmt.Fprintf(w, "%d,%x,%d,%.3f\n", i+1, iba.values[i], iba.ints[i], 100.0*float64(cumulative)/float64(total))
		}
	}
	fmt.Printf("Total storage items: %d\n", cumulative)
}

func nonTokenUsage() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	tokensFile, err := os.Open("tokens.csv")
	check(err)
	defer tokensFile.Close()
	tokensReader := csv.NewReader(bufio.NewReader(tokensFile))
	tokens := make(map[common.Address]struct{})
	for records, _ := tokensReader.Read(); records != nil; records, _ = tokensReader.Read() {
		tokens[common.HexToAddress(records[0])] = struct{}{}
	}
	addrFile, err := os.Open("addresses.csv")
	check(err)
	defer addrFile.Close()
	addrReader := csv.NewReader(bufio.NewReader(addrFile))
	names := make(map[common.Address]string)
	for records, _ := addrReader.Read(); records != nil; records, _ = addrReader.Read() {
		names[common.HexToAddress(records[0])] = records[1]
	}
	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	//itemsByCreator := make(map[common.Address]int)
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(addr[:], k[:20])
			if _, ok := tokens[addr]; !ok {
				itemsByAddress[addr]++
				count++
				if count%100000 == 0 {
					fmt.Printf("Processed %d storage records\n", count)
				}
			}
		}
		return nil
	})
	check(err)
	fmt.Printf("Processing took %s\n", time.Since(startTime))
	iba := NewIntSorterAddr(len(itemsByAddress))
	idx := 0
	total := 0
	for address, items := range itemsByAddress {
		total += items
		iba.ints[idx] = items
		iba.values[idx] = address
		idx++
	}
	sort.Sort(iba)
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("items_by_nontoken.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < iba.length; i++ {
		cumulative += iba.ints[i]
		if name, ok := names[iba.values[i]]; ok {
			fmt.Fprintf(w, "%d,%s,%d,%.3f\n", i+1, name, iba.ints[i], 100.0*float64(cumulative)/float64(total))
		} else {
			fmt.Fprintf(w, "%d,%x,%d,%.3f\n", i+1, iba.values[i], iba.ints[i], 100.0*float64(cumulative)/float64(total))
		}
	}
	fmt.Printf("Total storage items: %d\n", cumulative)
}

func oldStorage() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	histKey := make([]byte, 32+len(ethdb.EndSuffix))
	copy(histKey[32:], ethdb.EndSuffix)
	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(addr[:], k[:20])
			itemsByAddress[addr]++
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records\n", count)
			}
		}
		return nil
	})
	check(err)
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.AccountsHistoryBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for addr := range itemsByAddress {
			addrHash := crypto.Keccak256(addr[:])
			copy(histKey[:], addrHash)
			c.Seek(histKey)
			k, _ := c.Prev()
			if bytes.HasPrefix(k, addrHash[:]) {
				timestamp, _ := decodeTimestamp(k[32:])
				if timestamp > 4530000 {
					delete(itemsByAddress, addr)
				}
			}
		}
		return nil
	})
	check(err)
	fmt.Printf("Processing took %s\n", time.Since(startTime))
	iba := NewIntSorterAddr(len(itemsByAddress))
	idx := 0
	total := 0
	for address, items := range itemsByAddress {
		total += items
		iba.ints[idx] = items
		iba.values[idx] = address
		idx++
	}
	sort.Sort(iba)
	fmt.Printf("Writing dataset (total %d)...\n", total)
	f, err := os.Create("items_by_address.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < iba.length; i++ {
		cumulative += iba.ints[i]
		fmt.Fprintf(w, "%d,%x,%d,%.3f\n", i, iba.values[i], iba.ints[i], float64(cumulative)/float64(total))
	}
}

func dustEOA() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	count := 0
	eoas := 0
	maxBalance := big.NewInt(1000000000000000000)
	// Go through the current state
	thresholdMap := make(map[uint64]int)
	var a accounts.Account
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err1 := a.DecodeForStorage(v); err1 != nil {
				return err1
			}
			count++
			if !a.IsEmptyCodeHash() {
				// Only processing EOA
				continue
			}
			eoas++
			if a.Balance.Cmp(maxBalance) >= 0 {
				continue
			}
			thresholdMap[a.Balance.Uint64()]++
			if count%100000 == 0 {
				fmt.Printf("Processed %d account records\n", count)
			}
		}
		return nil
	})
	check(err)
	fmt.Printf("Total accounts: %d, EOAs: %d\n", count, eoas)
	tsi := NewTimeSorterInt(len(thresholdMap))
	idx := 0
	for t, count := range thresholdMap {
		tsi.timestamps[idx] = t
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...")
	f, err := os.Create("dust_eoa.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < tsi.length; i++ {
		cumulative += tsi.values[i]
		fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
	}
	fmt.Printf("Processing took %s\n", time.Since(startTime))
}

func dustChartEOA() {
	dust_eoaFile, err := os.Open("dust_eoa.csv")
	check(err)
	defer dust_eoaFile.Close()
	dust_eoaReader := csv.NewReader(bufio.NewReader(dust_eoaFile))
	var thresholds, counts []float64
	for records, _ := dust_eoaReader.Read(); records != nil; records, _ = dust_eoaReader.Read() {
		thresholds = append(thresholds, parseFloat64(records[0]))
		counts = append(counts, parseFloat64(records[1][1:]))
	}
	//thresholds = thresholds[1:]
	//counts = counts[1:]
	countSeries := &chart.ContinuousSeries{
		Name: "EOA accounts",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: thresholds,
		YValues: counts,
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
			{Value: 1e11, Label: "1e11"},
			{Value: 1e12, Label: "1e12"},
			{Value: 1e13, Label: "1e13"},
			{Value: 1e14, Label: "1e14"},
			{Value: 1e15, Label: "1e15"},
			{Value: 1e16, Label: "1e16"},
			{Value: 1e17, Label: "1e17"},
			{Value: 1e18, Label: "1e18"},
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
			Name:      "EOA Accounts",
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
			countSeries,
		},
	}
	graph3.Elements = []chart.Renderable{chart.LegendThin(&graph3)}
	buffer := bytes.NewBuffer([]byte{})
	err = graph3.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("dust_eoa.png", buffer.Bytes(), 0644)
	check(err)
}

func makeSha3Preimages() {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	f, err := bolt.Open("/Volumes/tb4/turbo-geth/sha3preimages", 0600, &bolt.Options{})
	check(err)
	defer f.Close()
	bucket := []byte("sha3")
	chainConfig := params.MainnetChainConfig
	vmConfig := vm.Config{EnablePreimageRecording: true}
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil)
	check(err)
	blockNum := uint64(*block)
	interrupt := false
	tx, err := f.Begin(true)
	if err != nil {
		panic(err)
	}
	b, err := tx.CreateBucketIfNotExists(bucket, false)
	if err != nil {
		panic(err)
	}
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewDbState(ethDb, block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		pi := statedb.Preimages()
		for hash, preimage := range pi {
			var found bool
			v, _ := b.Get(hash[:])
			if v != nil {
				found = true
			}
			if !found {
				if err := b.Put(hash[:], preimage); err != nil {
					panic(err)
				}
			}
		}
		blockNum++
		if blockNum%100 == 0 {
			fmt.Printf("Processed %d blocks\n", blockNum)
			if err := tx.Commit(); err != nil {
				panic(err)
			}
			tx, err = f.Begin(true)
			if err != nil {
				panic(err)
			}
			b, err = tx.CreateBucketIfNotExists(bucket, false)
			if err != nil {
				panic(err)
			}
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	fmt.Printf("Next time specify -block %d\n", blockNum)
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	//stateGrowth1()
	//stateGrowthChart1()
	//stateGrowth2()
	//stateGrowthChart2()
	//stateGrowthChart3()
	//makeCreators()
	//stateGrowthChart4()
	//stateGrowthChart5()
	//storageUsage()
	//oldStorage()
	//dustEOA()
	//dustChartEOA()
	//makeSha3Preimages()
	//makeTokens()
	//tokenUsage()
	//nonTokenUsage()
	//makeTokenBalances()
	//tokenBalances()
	//makeTokenAllowances()
	//countDepths()
	//countStorageDepths()
	//storageReadWrites()
	//accountsReadWrites()
	//speculativeExecution()
	//nakedSstoreChart()
	//nakedSloadChart()
	//nakedAccountChart()
	//specExecChart1()
	if *action == "stateless" {
		stateless(*genLag, *consLag)
	}
	if *action == "stateless_chart" {
		stateless_chart_key_values("/Users/alexeyakhunov/mygit/go-ethereum/st_1/stateless.csv", []int{21, 20, 19, 18}, "breakdown.png", 2800000, 1)
	}
	//stateless_chart_key_values("stateless1.csv", []int{17}, "total.png", 1, 0)
	//stateless_chart_key_values("stateless1_256.csv", []int{17}, "total256.png", 1, 0)
	//stateless_chart_key_values([]int{17}, "total_2675000.png", 2675000, 0)
	//stateless_chart_key_values("stateless1.csv", []int{12, 15, 16}, "breakdown.png", 1, 1)
	//stateless_chart_key_values("stateless1_256.csv", []int{12, 15, 16}, "breakdown256.png", 1, 1)
	//stateless_chart_key_values([]int{12, 15, 16}, "breakdown_2675000.png", 2675000, 1)
	//stateless_chart_key_values("stateless1.csv", []int{13,14}, "key_vals.png", 1, 4)
	//stateless_chart_key_values("stateless1_256.csv", []int{13,14}, "key_vals256.png", 1, 4)
	//stateless_chart_key_values("stateless1.csv", []int{10,11}, "c_key_vals.png", 1, 4)
	//stateless_chart_key_values("stateless1_256.csv", []int{10,11}, "c_key_vals256.png", 1, 4)
	//stateless_chart_key_values("stateless1.csv", []int{6, 7}, "mask_hash.png", 1, 4)
	//stateless_chart_key_values("stateless1_256.csv", []int{6, 7}, "mask_hash256.png", 1, 4)
	//stateless_chart_key_values("stateless1.csv", []int{1, 2}, "c_mask_hash.png", 1, 4)
	//stateless_chart_key_values("stateless1_256.csv", []int{1, 2}, "c_mask_hash256.png", 1, 4)
	//stateless_chart_key_values([]int{12}, "codes_28m.png", 2800000)
	if *action == "stateSnapshot" {
		if err := stateSnapshot(); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
	//estimate()
	//verify_snapshot()
	//feemarket()
	//transaction_stats()
	//naked_storage_vs_blockproof()
	//visual()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create mem profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
