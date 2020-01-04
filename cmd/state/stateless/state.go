package stateless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/typedtree"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ugorji/go/codec"

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

const PrintProgressEvery = 1 * 1000 * 1000
const PrintMemStatsEvery = 10 * 1000 * 1000
const SaveSnapshotEvery = 1 * 1000 * 1000

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

type Reporter struct {
	db *remote.DB
}

func NewReporter(ctx context.Context, remoteDbAddress string) (*Reporter, error) {
	dial := func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error) {
		dialer := net.Dialer{}
		conn, err := dialer.DialContext(ctx, "tcp", remoteDbAddress)
		return conn, conn, conn, err
	}

	db, err := remote.NewDB(ctx, dial)
	if err != nil {
		return nil, err
	}

	return &Reporter{db: db}, nil
}

// Generate name off the file for snapshot
// Each day has it's own partition
// It means that you can only continue execution of report from last snapshot.Save() checkpoint - read buckets forward from last key
// But not re-read bucket
func file(prefix string, version int) string {
	return path.Join(dir(), fmt.Sprintf("%s_%s_v%d.cbor", prefix, time.Now().Format("2006-01-28"), version))
}

func dir() string {
	dir := path.Join(os.TempDir(), "turbo_geth_reports")
	if err := os.MkdirAll(dir, 0770); err != nil {
		panic(err)
	}
	return dir
}

func restore(file string, snapshot interface{}) {
	t := time.Now()
	defer func() {
		fmt.Println("Restore: " + time.Since(t).String())
	}()
	f, err := os.OpenFile(file, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var handle codec.CborHandle
	handle.ReaderBufferSize = 1024 * 1024
	if err := codec.NewDecoder(f, &handle).Decode(snapshot); err != nil {
		if err != io.EOF {
			panic(err)
		}
	}
}

func save(file string, snapshot interface{}) {
	t := time.Now()
	defer func() {
		fmt.Println("Save: " + time.Since(t).String())
	}()

	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var handle codec.CborHandle
	handle.WriterBufferSize = 1024 * 1024
	codec.NewEncoder(f, &handle).MustEncode(snapshot)
}

type StateGrowth1Snapshot struct {
	version          string
	MaxTimestamp     uint64
	HistoryKey       []byte
	AccountKey       []byte
	LastTimestamps   *typedtree.RadixUint64 // For each address hash, when was it last accounted
	CreationsByBlock map[uint64]int         // For each timestamp, how many accounts were created in the state
}

var StateGrowth1SnapshotFile = file("StateGrowth1", 1)

func (s *StateGrowth1Snapshot) Restore() {
	restore(StateGrowth1SnapshotFile, s)
}

func (s *StateGrowth1Snapshot) Save() {
	save(StateGrowth1SnapshotFile, s)
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("HeapInuse: %vMb, Alloc: %vMb, TotalAlloc: %vMb, Sys: %vMb, NumGC: %v, PauseNs: %d\n", bToMb(m.HeapInuse), bToMb(m.Alloc), bToMb(m.TotalAlloc), bToMb(m.Sys), m.NumGC, m.PauseNs[(m.NumGC+255)%256])
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func (r *Reporter) StateGrowth1(ctx context.Context) {
	startTime := time.Now()
	const MaxIteration = 1 * 1000 * 1000

	var count int
	var addrHash common.Hash

	s := &StateGrowth1Snapshot{
		version:          "5",
		HistoryKey:       []byte{},
		AccountKey:       []byte{},
		MaxTimestamp:     0,
		LastTimestamps:   typedtree.NewRadixUint64(), // For each address hash, when was it last accounted
		CreationsByBlock: make(map[uint64]int),       // For each timestamp, how many accounts were created in the state
	}

	s.Restore()
	fmt.Println("Len:", s.LastTimestamps.Len())

	var vIsEmpty bool
	var err error
	// Go through the history of account first
	if err := r.db.View(ctx, func(tx *remote.Tx) error {
		b, err := tx.Bucket(dbutils.AccountsHistoryBucket)
		if err != nil {
			return err
		}

		if b == nil {
			return nil
		}
		c, err := b.BatchCursor(10000)
		if err != nil {
			return err
		}

		skipFirst := len(s.HistoryKey) > 0 // snapshot stores last analyzed key
		for s.HistoryKey, vIsEmpty, err = c.SeekKey(s.HistoryKey); s.HistoryKey != nil || err != nil; s.HistoryKey, vIsEmpty, err = c.NextKey() {
			if err != nil {
				return err
			}
			if skipFirst {
				skipFirst = false
				continue
			}

			copy(addrHash[:], s.HistoryKey[:32])
			addr := string(addrHash.Bytes())
			timestamp, _ := dbutils.DecodeTimestamp(s.HistoryKey[32:])
			if timestamp+1 > s.MaxTimestamp {
				s.MaxTimestamp = timestamp + 1
			}
			if vIsEmpty {
				s.CreationsByBlock[timestamp]++
				if lt, ok := s.LastTimestamps.Get(addr); ok {
					s.CreationsByBlock[lt]--
				}
			}

			s.LastTimestamps.Set(addr, timestamp)
			count++
			if count%PrintProgressEvery == 0 {
				fmt.Printf("Processed %d account records. %s\n", count, time.Since(startTime))
			}
			if count%PrintMemStatsEvery == 0 {
				PrintMemUsage()
			}
			if count%SaveSnapshotEvery == 0 {
				s.Save()
			}

		}
		return nil
	}); err != nil {
		check(err)
	}

	s.Save()
	/*
		var b bytes.Buffer
		zw, err := zlib.NewWriterLevel(&b, zlib.BestSpeed)
		if err != nil {
			panic(err)
		}

		all := make([]byte, 10)
		s.LastTimestamps.Walk(func(k string, _ uint64) bool {
			all = append(all, k...)

			_, err = zw.Write([]byte(fmt.Sprintf("%x", k)))
			if err != nil {
				panic(err)
			}
			return false
		})
		err = zw.Close()
		if err != nil {
			panic(err)
		}
		fmt.Println("Compress: ", len(b.Bytes())/1024)
		fmt.Println("No Compress: ", len(all)/1024)
	*/

	// Go through the current state
	if err := r.db.View(ctx, func(tx *remote.Tx) error {
		pre, err := tx.Bucket(dbutils.PreimagePrefix)
		if err != nil {
			return err
		}

		if pre == nil {
			return nil
		}
		b, err := tx.Bucket(dbutils.AccountsBucket)
		if err != nil {
			return err
		}

		if b == nil {
			return nil
		}
		c, err := b.BatchCursor(10000)
		if err != nil {
			return err
		}

		skipFirst := len(s.AccountKey) > 0 // snapshot stores last analyzed key
		for s.AccountKey, _, err = c.SeekKey(s.AccountKey); s.AccountKey != nil || err != nil; s.AccountKey, _, err = c.NextKey() {
			if err != nil {
				return err
			}
			if skipFirst {
				skipFirst = false
				continue
			}
			// First 32 bytes is the hash of the address
			copy(addrHash[:], s.AccountKey[:32])
			s.LastTimestamps.Set(string(addrHash.Bytes()), s.MaxTimestamp)
			count++
			if count%PrintProgressEvery == 0 {
				fmt.Printf("Processed %d account records. %s\n", count, time.Since(startTime))
			}
			if count%PrintMemStatsEvery == 0 {
				PrintMemUsage()
			}
			if count%SaveSnapshotEvery == 0 {
				s.Save()
			}
		}
		return nil
	}); err != nil {
		check(err)
	}

	s.LastTimestamps.Walk(func(_ string, lt uint64) bool {
		if lt < s.MaxTimestamp {
			s.CreationsByBlock[lt]--
		}
		return false
	})
	//for _, lt := range s.LastTimestamps {
	//	if lt < s.MaxTimestamp {
	//		s.CreationsByBlock[lt]--
	//	}
	//}

	s.Save()

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Account history records: %d\n", count)
	fmt.Printf("Creating dataset...\n")
	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(s.CreationsByBlock))
	idx := 0
	for timestamp, count := range s.CreationsByBlock {
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
		fmt.Fprintf(w, "%d, %d, %d\n", tsi.timestamps[i], tsi.values[i], cumulative)
	}
}

func (r *Reporter) StateGrowth2(ctx context.Context) {
	startTime := time.Now()
	var count int
	var maxTimestamp uint64

	// For each address hash, when was it last accounted
	lastTimestamps := typedtree.NewRadixMapHash2Uint64() // values are map[common.Hash]uint64

	// For each timestamp, how many storage items were created
	creationsByBlock := typedtree.NewRadixMapUint642Int() // values are map[uint64]int

	var addrHash common.Hash
	var hash common.Hash
	// Go through the history of account first
	if err := r.db.View(ctx, func(tx *remote.Tx) error {
		b, err := tx.Bucket(dbutils.StorageHistoryBucket)
		if err != nil {
			return err
		}

		if b == nil {
			return nil
		}
		c, err := b.BatchCursor(10000)
		if err != nil {
			return err
		}

		for k, vIsEmpty, err := c.FirstKey(); k != nil || err != nil; k, vIsEmpty, err = c.NextKey() {
			if err != nil {
				return err
			}
			// First 20 bytes is the address
			copy(addrHash[:], k[:32])
			addr := string(addrHash.Bytes())
			copy(hash[:], k[40:72])
			timestamp, _ := dbutils.DecodeTimestamp(k[72:])
			if timestamp+1 > maxTimestamp {
				maxTimestamp = timestamp + 1
			}
			if vIsEmpty {
				c, ok := creationsByBlock.Get(addr)
				if !ok {
					c = make(map[uint64]int)
					creationsByBlock.Set(addr, c)
				}
				c[timestamp]++
				l, ok := lastTimestamps.Get(addr)
				if !ok {
					l = make(map[common.Hash]uint64)
					lastTimestamps.Set(addr, l)
				}
				if lt, ok := l[hash]; ok {
					c[lt]--
				}
			}
			l, ok := lastTimestamps.Get(addr)
			if !ok {
				l = make(map[common.Hash]uint64)
				lastTimestamps.Set(addr, l)
			}
			l[hash] = timestamp

			count++
			if count%PrintProgressEvery == 0 {
				fmt.Printf("Processed %d storage records, %s\n", count, time.Since(startTime))
			}
			if count%PrintMemStatsEvery == 0 {
				PrintMemUsage()
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Printf("lt: %d\n", lastTimestamps.Len())

	// Go through the current state
	if err := r.db.View(ctx, func(tx *remote.Tx) error {
		b, err := tx.Bucket(dbutils.StorageBucket)
		if err != nil {
			return err
		}

		if b == nil {
			return nil
		}
		c, err := b.BatchCursor(10000)
		if err != nil {
			return err
		}

		for k, _, err := c.FirstKey(); k != nil || err != nil; k, _, err = c.NextKey() {
			if err != nil {
				return err
			}
			copy(addrHash[:], k[:32])
			addr := string(addrHash.Bytes())
			copy(hash[:], k[40:72])
			l, ok := lastTimestamps.Get(addr)
			if !ok {
				l = make(map[common.Hash]uint64)
				lastTimestamps.Set(addr, l)
			}
			l[hash] = maxTimestamp
			count++
			if count%PrintProgressEvery == 0 {
				fmt.Printf("Processed %d storage records, %s\n", count, time.Since(startTime))
			}
			if count%PrintMemStatsEvery == 0 {
				PrintMemUsage()
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}

	lastTimestamps.Walk(func(address string, l map[common.Hash]uint64) bool {
		for _, lt := range l {
			if lt < maxTimestamp {
				v, _ := creationsByBlock.Get(address)
				v[lt]--
				creationsByBlock.Set(address, v)
			}
		}
		return false
	})
	//for address, l := range lastTimestamps {
	//	for _, lt := range l {
	//		if lt < maxTimestamp {
	//			creationsByBlock[address][lt]--
	//		}
	//	}
	//}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Storage history records: %d\n", count)
	fmt.Printf("Creating dataset...\n")
	totalCreationsByBlock := make(map[uint64]int)
	creationsByBlock.Walk(func(_ string, c map[uint64]int) bool {
		cumulative := 0
		for timestamp, count := range c {
			totalCreationsByBlock[timestamp] += count
			cumulative += count
		}
		return false
	})

	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(totalCreationsByBlock))
	idx := 0
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
		fmt.Fprintf(w, "%d, %d, %d\n", tsi.timestamps[i], tsi.values[i], cumulative)
	}

	PrintMemUsage()
}

func (r *Reporter) GasLimits(ctx context.Context) {
	startTime := time.Now()

	f, ferr := os.Create("gas_limits.csv")
	check(ferr)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	//var blockNum uint64 = 5346726
	var blockNum uint64 = 0

	mainHashes := typedtree.NewRadixUint64()

	err := r.db.View(ctx, func(tx *remote.Tx) error {
		b, err := tx.Bucket(dbutils.HeaderPrefix)
		if err != nil {
			return err
		}

		if b == nil {
			return nil
		}
		c, err := b.BatchCursor(10000)
		if err != nil {
			return err
		}

		fmt.Println("Preloading block numbers...")

		i := 0
		for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}

			i++
			// skip bucket keys not useful for analysis
			if !dbutils.IsHeaderHashKey(k) {
				continue
			}

			mainHashes.Set(string(v), 0)

			if i%PrintProgressEvery == 0 {
				fmt.Printf("Scanned %d keys, %s\n", i, time.Since(startTime))
			}
			if i%PrintMemStatsEvery == 0 {
				PrintMemUsage()
			}
		}

		fmt.Println("Preloaded: ", mainHashes.Len())
		PrintMemUsage()

		for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if !dbutils.IsHeaderKey(k) {
				continue
			}

			if _, ok := mainHashes.Get(string(k[common.BlockNumberLength:])); !ok {
				continue
			}

			header := new(types.Header)
			if decodeErr := rlp.Decode(bytes.NewReader(v), header); decodeErr != nil {
				log.Error("Invalid block header RLP", "blockNum", blockNum, "err", decodeErr)
				return nil
			}

			fmt.Fprintf(w, "%d, %d\n", blockNum, header.GasLimit)
			if blockNum%PrintProgressEvery == 0 {
				fmt.Printf("Processed %d blocks, %s\n", blockNum, time.Since(startTime))
			}
			if blockNum%PrintMemStatsEvery == 0 {
				PrintMemUsage()
			}
			blockNum++
		}
		return nil
	})
	check(err)

	fmt.Printf("Finish processing %d blocks\n", blockNum)
	PrintMemUsage()
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

//nolint:deadcode,unused
func makeCreators(blockNum uint64) {
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
	histKey := make([]byte, common.HashLength+len(ethdb.EndSuffix))
	copy(histKey[common.HashLength:], ethdb.EndSuffix)
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
				timestamp, _ := dbutils.DecodeTimestamp(k[32:])
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

//nolint:deadcode,unused
func makeSha3Preimages(blockNum uint64) {
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
