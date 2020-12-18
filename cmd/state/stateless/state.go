package stateless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/cbor"
	"github.com/ledgerwatch/turbo-geth/ethdb/typedcursor"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"github.com/wcharczuk/go-chart/util"
)

var emptyCodeHash = crypto.Keccak256(nil)

const (
	ReportsProgressBucket = "reports_progress"
	MainHashesBucket      = "gl_main_hashes"
)

const (
	PrintMemStatsEvery = 1_000_000
	PrintProgressEvery = 100_000
	CommitEvery        = 100_000
	MaxIterationsPerTx = 10_000_000
	CursorBatchSize    = uint(10_000)
)

func check(e error) {
	if e != nil {
		panic(e)
	}
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

func commit(k []byte, tx ethdb.Tx, data interface{}) {
	//defer func(t time.Time) { fmt.Println("Commit:", time.Since(t)) }(time.Now())
	var buf bytes.Buffer

	encoder := cbor.Encoder(&buf)
	defer cbor.Return(encoder)

	encoder.MustEncode(data)
	if err := tx.Cursor(ReportsProgressBucket).Put(k, buf.Bytes()); err != nil {
		panic(err)
	}
}

func restore(k []byte, tx ethdb.Tx, data interface{}) {
	_, v, err := tx.Cursor(ReportsProgressBucket).SeekExact(k)
	if err != nil {
		panic(err)
	}
	if v == nil {
		return
	}

	decoder := cbor.Decoder(bytes.NewReader(v))
	decoder.MustDecode(data)
}

type StateGrowth1Reporter struct {
	StartedWhenBlockNumber uint64
	MaxTimestamp           uint64
	HistoryKey             []byte
	AccountKey             []byte
	// For each timestamp, how many accounts were created in the state
	CreationsByBlock map[uint64]int
	remoteDB         ethdb.KV `codec:"-"`
}

func NewStateGrowth1Reporter(ctx context.Context, remoteDB ethdb.KV, _ ethdb.KV) *StateGrowth1Reporter {

	rep := &StateGrowth1Reporter{
		remoteDB:         remoteDB,
		HistoryKey:       []byte{},
		AccountKey:       []byte{},
		MaxTimestamp:     0,
		CreationsByBlock: make(map[uint64]int),
	}

	return rep
}

func (r *StateGrowth1Reporter) interrupt(ctx context.Context, i int, startTime time.Time) (breakTx bool) {
	if i%PrintProgressEvery == 0 {
		fmt.Printf("Processed %dK, %s\n", i/1000, time.Since(startTime))
	}
	if i%PrintMemStatsEvery == 0 {
		debug.PrintMemStats(true)
	}
	if i%MaxIterationsPerTx == 0 {
		return true
	}
	return false
}

func (r *StateGrowth1Reporter) StateGrowth1(ctx context.Context) {
	tx, err2 := r.remoteDB.Begin(ctx, nil, ethdb.RO)
	if err2 != nil {
		panic(err2)
	}
	defer tx.Rollback()
	startTime := time.Now()

	var i int

	// Go through the history of account first
	if r.StartedWhenBlockNumber == 0 {
		var err error
		r.StartedWhenBlockNumber, _, err = stages.GetStageProgress(ethdb.NewObjectDatabase(r.remoteDB), stages.Execution)
		if err != nil {
			panic(err)
		}
	}

	var lastAddress []byte
	_ = lastAddress
	var lastTimestamp uint64
	cs := tx.Cursor(dbutils.PlainStateBucket).Prefetch(CursorBatchSize)
	sk, _, serr := cs.First()
	if serr != nil {
		panic(serr)
	}
	c := tx.Cursor(dbutils.AccountsHistoryBucket).Prefetch(CursorBatchSize)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			panic(err)
		}
		address := k[:common.AddressLength]
		hi := roaring.New()
		if _, err1 := hi.FromBuffer(v); err1 != nil {
			panic(err1)
		}
		blockNums := hi.ToArray()
		// TODO: there is no more index for creations
		//if created[0] {
		//	if bytes.Equal(address, lastAddress) {
		//		r.CreationsByBlock[lastTimestamp]--
		//	}
		//}
		//for i, timestamp := range blockNums {
		//	if created[i] {
		//		r.CreationsByBlock[timestamp]++
		//		if i > 0 {
		//			r.CreationsByBlock[blockNums[i-1]]--
		//		}
		//	}
		//}
		if binary.BigEndian.Uint64(k[common.AddressLength:]) == 0xffffffffffffffff {
			for ; sk != nil && bytes.Compare(sk, address) < 0; sk, _, serr = cs.Next() {
				if serr != nil {
					panic(serr)
				}
			}
			if !bytes.Equal(sk, address) {
				r.CreationsByBlock[uint64(blockNums[len(blockNums)-1])]--
			}
		}
		i++
		if i%100000 == 0 {
			fmt.Printf("Processed %d account history records\n", i)
		}
		lastAddress = address
		lastTimestamp = uint64(blockNums[len(blockNums)-1])

		if lastTimestamp+1 > r.MaxTimestamp {
			r.MaxTimestamp = lastTimestamp + 1
		}
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Account history records: %d\n", i)
	fmt.Printf("Creating dataset...\n")
	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(r.CreationsByBlock))
	idx := 0
	for timestamp, count := range r.CreationsByBlock {
		tsi.timestamps[idx] = timestamp
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("accounts_growth.csv")
	check(err)
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < tsi.length; i++ {
		cumulative += tsi.values[i]
		fmt.Fprintf(w, "%d, %d, %d\n", tsi.timestamps[i], tsi.values[i], cumulative)
	}
	_ = lastAddress
	_ = lastTimestamp
}

type StateGrowth2Reporter struct {
	StartedWhenBlockNumber uint64
	MaxTimestamp           uint64
	HistoryKey             []byte
	StorageKey             []byte

	CreationsByBlock map[uint64]int

	remoteDB ethdb.KV `codec:"-"`
}

func NewStateGrowth2Reporter(ctx context.Context, remoteDB ethdb.KV, _ ethdb.KV) *StateGrowth2Reporter {
	rep := &StateGrowth2Reporter{
		remoteDB:         remoteDB,
		HistoryKey:       []byte{},
		StorageKey:       []byte{},
		MaxTimestamp:     0,
		CreationsByBlock: make(map[uint64]int),
	}
	return rep
}

func (r *StateGrowth2Reporter) interrupt(ctx context.Context, i int, startTime time.Time) (breakTx bool) {
	if i%PrintProgressEvery == 0 {
		fmt.Printf("Processed %dK, %s\n", i/1000, time.Since(startTime))
	}
	if i%PrintMemStatsEvery == 0 {
		debug.PrintMemStats(true)
	}
	if i%MaxIterationsPerTx == 0 {
		return true
	}
	return false
}

func (r *StateGrowth2Reporter) StateGrowth2(ctx context.Context) {
	tx, err2 := r.remoteDB.Begin(context.Background(), nil, ethdb.RO)
	if err2 != nil {
		panic(err2)
	}
	defer tx.Rollback()
	startTime := time.Now()

	var i int
	var lastAddress []byte
	var lastLocation []byte
	_ = lastAddress
	_ = lastLocation
	var lastTimestamp uint64
	cs := tx.Cursor(dbutils.PlainStateBucket).Prefetch(CursorBatchSize)
	sk, _, serr := cs.First()
	if serr != nil {
		panic(serr)
	}
	c := tx.Cursor(dbutils.StorageHistoryBucket).Prefetch(CursorBatchSize)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			panic(err)
		}
		address := k[:common.AddressLength]
		location := k[common.AddressLength : common.AddressLength+common.HashLength]
		hi := roaring.New()
		if _, err1 := hi.FromBuffer(v); err1 != nil {
			panic(err1)
		}
		blockNums := hi.ToArray()
		// TODO: there is no more index for creations
		//if created[0] {
		//	if bytes.Equal(address, lastAddress) || bytes.Equal(location, lastLocation) {
		//		r.CreationsByBlock[lastTimestamp]--
		//	}
		//}
		//for i, timestamp := range blockNums {
		//	if created[i] {
		//		r.CreationsByBlock[timestamp]++
		//		if i > 0 {
		//			r.CreationsByBlock[blockNums[i-1]]--
		//		}
		//	}
		//}
		if binary.BigEndian.Uint64(k[common.AddressLength+common.HashLength:]) == 0xffffffffffffffff {
			var aCmp, lCmp int
			for ; sk != nil; sk, _, serr = cs.Next() {
				if serr != nil {
					panic(serr)
				}
				sAddress := sk[:common.AddressLength]
				var sLocation []byte
				if len(sk) >= common.AddressLength+common.IncarnationLength {
					sLocation = sk[common.AddressLength+common.IncarnationLength:]
				}
				aCmp = bytes.Compare(sAddress, address)
				lCmp = bytes.Compare(sLocation, location)
				if aCmp > 0 || (aCmp == 0 && lCmp >= 0) {
					break
				}
			}
			if aCmp != 0 || lCmp != 0 {
				r.CreationsByBlock[uint64(blockNums[len(blockNums)-1])]--
			}
		}
		i++
		if i%100000 == 0 {
			fmt.Printf("Processed %d storage history records\n", i)
		}
		lastAddress = address
		lastLocation = location
		lastTimestamp = uint64(blockNums[len(blockNums)-1])
		if lastTimestamp+1 > r.MaxTimestamp {
			r.MaxTimestamp = lastTimestamp + 1
		}
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Storage history records: %d\n", i)
	fmt.Printf("Creating dataset...\n")

	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(r.CreationsByBlock))
	idx := 0
	for timestamp, count := range r.CreationsByBlock {
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
		fmt.Fprintf(w, "%d, %d, %d\n", tsi.timestamps[i], tsi.values[i], cumulative)
	}

	debug.PrintMemStats(true)
	_ = lastAddress
	_ = lastLocation
}

type GasLimitReporter struct {
	remoteDB               ethdb.KV `codec:"-"`
	StartedWhenBlockNumber uint64
	HeaderPrefixKey1       []byte
	HeaderPrefixKey2       []byte

	// map[common.Hash]uint64 - key is addrHash + hash
	mainHashes *typedcursor.Uint64 // For each address hash, when was it last accounted

	commit   func(ctx context.Context)
	rollback func(ctx context.Context)
}

func NewGasLimitReporter(ctx context.Context, remoteDB ethdb.KV, localDB ethdb.KV) *GasLimitReporter {
	var ProgressKey = []byte("gas_limit")

	var err error
	var localTx ethdb.Tx

	if localTx, err = localDB.Begin(ctx, nil, ethdb.RW); err != nil {
		panic(err)
	}

	rep := &GasLimitReporter{
		remoteDB:         remoteDB,
		HeaderPrefixKey1: []byte{},
		HeaderPrefixKey2: []byte{},
		mainHashes:       typedcursor.NewUint64(localTx.Cursor(MainHashesBucket)),
	}
	rep.commit = func(ctx context.Context) {
		commit(ProgressKey, localTx, rep)
		if err = localTx.Commit(ctx); err != nil {
			panic(err)
		}
		if localTx, err = localDB.Begin(ctx, nil, ethdb.RW); err != nil {
			panic(err)
		}

		rep.mainHashes = typedcursor.NewUint64(localTx.Cursor(MainHashesBucket))
	}

	rep.rollback = func(ctx context.Context) {
		localTx.Rollback()
	}

	restore(ProgressKey, localTx, rep)
	return rep
}

func (r *GasLimitReporter) interrupt(ctx context.Context, i int, startTime time.Time) (breakTx bool) {
	if i%PrintProgressEvery == 0 {
		fmt.Printf("Processed %dK, %s\n", i/1000, time.Since(startTime))
	}
	if i%PrintMemStatsEvery == 0 {
		debug.PrintMemStats(true)
	}
	if i%CommitEvery == 0 {
		r.commit(ctx)
	}
	if i%MaxIterationsPerTx == 0 {
		return true
	}
	return false
}
func (r *GasLimitReporter) GasLimits(ctx context.Context) {
	defer r.rollback(ctx)

	f, ferr := os.Create("gas_limits.csv")
	check(ferr)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	//var blockNum uint64 = 5346726
	var blockNum uint64 = 0

	if err := r.remoteDB.View(ctx, func(tx ethdb.Tx) error {

		c := tx.Cursor(dbutils.HeaderPrefix).Prefetch(CursorBatchSize)
		if err := ethdb.ForEach(c, func(k, v []byte) (bool, error) {
			if len(k) != 40 {
				return true, nil
			}
			header := new(types.Header)
			if err := rlp.Decode(bytes.NewReader(v), header); err != nil {
				return false, err
			}
			fmt.Fprintf(w, "%d, %d\n", header.Number.Uint64(), header.GasLimit)
			blockNum++
			return true, nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}

	fmt.Printf("Finish processing %d blocks\n", blockNum)
	debug.PrintMemStats(true)
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
		blocks = append(blocks, parseFloat64(strings.Trim(parts[0], " ")))
		items = append(items, parseFloat64(strings.Trim(parts[1], " ")))
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
		{Value: 8.0},
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

func startFrom(axis, data []float64, start float64) ([]float64, []float64) {
	// Find position where axis[i] >= start
	for i := 0; i < len(axis); i++ {
		if axis[i] >= start {
			return axis[i:], data[i:]
		}
	}
	return []float64{}, []float64{}
}

func movingAverage(axis, data []float64, window float64) []float64 {
	var windowSum float64
	var windowStart int
	movingAvgs := make([]float64, len(data))
	for j := 0; j < len(data); j++ {
		windowSum += data[j]
		for axis[j]-axis[windowStart] > window {
			windowSum -= data[windowStart]
			windowStart++
		}
		if axis[j]-axis[windowStart] > 0 {
			movingAvgs[j] = windowSum / (axis[j] - axis[windowStart])
		}
	}
	return movingAvgs
}

func StateGrowthChart1(start int, window int) {
	blocks, accounts, err := readData("accounts_growth.csv")
	check(err)
	accounts = movingAverage(blocks, accounts, float64(window))
	blocks, accounts = startFrom(blocks, accounts, float64(start))
	gBlocks, gaslimits, err := readData("gas_limits.csv")
	check(err)
	gaslimits = movingAverage(gBlocks, gaslimits, float64(window))
	gBlocks, gaslimits = startFrom(gBlocks, gaslimits, float64(start))
	mainSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Number of accounts created per block (EOA and contracts), moving average over %d blocks", window),
		Style: chart.Style{
			Show:        true,
			StrokeWidth: 1,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: blocks,
		YValues: accounts,
	}
	gasLimitSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Block gas limit, moving average over %d blocks", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: gBlocks,
		YValues: gaslimits,
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
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
		},
		YAxisSecondary: chart.YAxis{
			Name:      "Block gas limit",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fm", v.(float64)/1000000.0)
			},
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64)/1000000.0)
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: blockMillions(),
		},
		Series: []chart.Series{
			mainSeries,
			gasLimitSeries,
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

func StateGrowthChart2(start, window int) {
	blocks, accounts, err := readData("storage_growth.csv")
	check(err)
	accounts = movingAverage(blocks, accounts, float64(window))
	blocks, accounts = startFrom(blocks, accounts, float64(start))
	gBlocks, gaslimits, err := readData("gas_limits.csv")
	check(err)
	gaslimits = movingAverage(gBlocks, gaslimits, float64(window))
	gBlocks, gaslimits = startFrom(gBlocks, gaslimits, float64(start))
	mainSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Storage items created per block, moving average over %d blocks", window),
		Style: chart.Style{
			Show:        true,
			StrokeWidth: 1,
			StrokeColor: chart.ColorGreen,
			FillColor:   chart.ColorGreen.WithAlpha(100),
		},
		XValues: blocks,
		YValues: accounts,
	}
	gasLimitSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Block gas limit, moving average over %d blocks", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: gBlocks,
		YValues: gaslimits,
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
			Name:      "Storage items created",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
		},
		YAxisSecondary: chart.YAxis{
			Name:      "Block gas limit",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fm", v.(float64)/1000000.0)
			},
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64)/1000000.0)
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: blockMillions(),
		},
		Series: []chart.Series{
			mainSeries,
			gasLimitSeries,
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

func storageUsage() {
	startTime := time.Now()
	//db := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	db := ethdb.MustOpen("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	//db := ethdb.MustOpen("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
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
	addrFile, openErr := os.Open("addresses.csv")
	check(openErr)
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
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.CurrentStateBucket)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) == 32 {
				continue
			}
			copy(addr[:], k[:20])
			del, ok := deleted[addr]
			if !ok {
				vv, err := tx.GetOne(dbutils.CurrentStateBucket, crypto.Keccak256(addr[:]))
				if err != nil {
					return err
				}
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
				fmt.Printf("Processed %dK storage records, deleted contracts: %d\n", count/1000, numDeleted)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
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
	//remoteDB := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	db := ethdb.MustOpen("/Volumes/tb4/turbo-geth/geth/chaindata")
	//remoteDB := ethdb.MustOpen("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	defer db.Close()
	tokensFile, errOpen := os.Open("tokens.csv")
	check(errOpen)
	defer tokensFile.Close()
	tokensReader := csv.NewReader(bufio.NewReader(tokensFile))
	tokens := make(map[common.Address]struct{})
	for records, _ := tokensReader.Read(); records != nil; records, _ = tokensReader.Read() {
		tokens[common.HexToAddress(records[0])] = struct{}{}
	}
	addrFile, errOpen := os.Open("addresses.csv")
	check(errOpen)
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
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.CurrentStateBucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) == 32 {
				continue
			}
			copy(addr[:], k[:20])
			if _, ok := tokens[addr]; ok {
				itemsByAddress[addr]++
				count++
				if count%100000 == 0 {
					fmt.Printf("Processed %dK storage records\n", count/1000)
				}
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
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
	//db := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	db := ethdb.MustOpen("/Volumes/tb4/turbo-geth/geth/chaindata")
	//db := ethdb.MustOpen("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	defer db.Close()
	tokensFile, errOpen := os.Open("tokens.csv")
	check(errOpen)
	defer tokensFile.Close()
	tokensReader := csv.NewReader(bufio.NewReader(tokensFile))
	tokens := make(map[common.Address]struct{})
	for records, _ := tokensReader.Read(); records != nil; records, _ = tokensReader.Read() {
		tokens[common.HexToAddress(records[0])] = struct{}{}
	}
	addrFile, errOpen := os.Open("addresses.csv")
	check(errOpen)
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
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.CurrentStateBucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) == 32 {
				continue
			}
			copy(addr[:], k[:20])
			if _, ok := tokens[addr]; !ok {
				itemsByAddress[addr]++
				count++
				if count%100000 == 0 {
					fmt.Printf("Processed %dK storage records\n", count/1000)
				}
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
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
	db := ethdb.MustOpen("/Volumes/tb4/turbo-geth/geth/chaindata").KV()
	defer db.Close()
	histKey := make([]byte, common.HashLength+len(ethdb.EndSuffix))
	copy(histKey[common.HashLength:], ethdb.EndSuffix)
	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	count := 0
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.CurrentStateBucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) == 32 {
				continue
			}
			copy(addr[:], k[:20])
			itemsByAddress[addr]++
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %dK storage records\n", count/1000)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.AccountsHistoryBucket)
		for addr := range itemsByAddress {
			addrHash := crypto.Keccak256(addr[:])
			copy(histKey[:], addrHash)
			_, _, _ = c.Seek(histKey)
			k, _, err := c.Prev()
			if err != nil {
				return err
			}
			if bytes.HasPrefix(k, addrHash[:]) {
				timestamp, _ := dbutils.DecodeTimestamp(k[32:])
				if timestamp > 4530000 {
					delete(itemsByAddress, addr)
				}
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Printf("Processing took %s\n", time.Since(startTime))
	iba := NewIntSorterAddr(len(itemsByAddress))
	idx := 0
	total := 0
	for addrHash, items := range itemsByAddress {
		total += items
		iba.ints[idx] = items
		iba.values[idx] = addrHash
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
	//db := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	db := ethdb.MustOpen("/Volumes/tb4/turbo-geth/geth/chaindata")
	//db := ethdb.MustOpen("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	defer db.Close()
	count := 0
	eoas := 0
	maxBalance := uint256.NewInt().SetUint64(1000000000000000000)
	// Go through the current state
	thresholdMap := make(map[uint64]int)
	var a accounts.Account
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.CurrentStateBucket)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) != 32 {
				continue
			}
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
				fmt.Printf("Processed %dK account records\n", count/1000)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Printf("Total accounts: %d, EOAs: %d\n", count, eoas)
	tsi := NewTimeSorterInt(len(thresholdMap))
	idx := 0
	for t, count := range thresholdMap {
		tsi.timestamps[idx] = t
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...\n")
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

//nolint:deadcode,unused,golint,stylecheck
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
