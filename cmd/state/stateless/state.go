package stateless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/bolt"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"github.com/wcharczuk/go-chart/util"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotechain"
	"github.com/ledgerwatch/turbo-geth/ethdb/typedbucket"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var emptyCodeHash = crypto.Keccak256(nil)

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

var ReportsProgressBucket = []byte("reports_progress")

func commit(k []byte, tx *bolt.Tx, data interface{}) {
	//defer func(t time.Time) { fmt.Println("Commit:", time.Since(t)) }(time.Now())
	var buf bytes.Buffer

	encoder := codecpool.Encoder(&buf)
	defer codecpool.Return(encoder)

	encoder.MustEncode(data)
	if err := tx.Bucket(ReportsProgressBucket).Put(k, buf.Bytes()); err != nil {
		panic(err)
	}
}

func restore(k []byte, tx *bolt.Tx, data interface{}) {
	//defer func(t time.Time) { fmt.Println("Restore:", time.Since(t)) }(time.Now())
	reportsProgress, err := tx.CreateBucketIfNotExists(ReportsProgressBucket, false)
	if err != nil {
		panic(err)
	}
	v, _ := reportsProgress.Get(k)
	if v == nil {
		return
	}

	decoder := codecpool.Decoder(bytes.NewReader(v))
	decoder.MustDecode(data)
}

type StateGrowth1Reporter struct {
	StartedWhenBlockNumber uint64
	MaxTimestamp           uint64
	HistoryKey             []byte
	AccountKey             []byte
	// For each timestamp, how many accounts were created in the state
	CreationsByBlock map[uint64]int

	// map[addrHash]uint64
	lastTimestamps *typedbucket.Uint64 `codec:"-"`
	commit         func(ctx context.Context)
	rollback       func(ctx context.Context)
	remoteDB       ethdb.KV `codec:"-"`
}

func NewStateGrowth1Reporter(ctx context.Context, remoteDB ethdb.KV, localDB *bolt.DB) *StateGrowth1Reporter {
	var LastTimestampsBucket = []byte("sg1_accounts_last_timestamps")
	var ProgressKey = []byte("state_growth_1")

	var err error
	var localTx *bolt.Tx
	var lastTimestamps *bolt.Bucket

	if localTx, err = localDB.Begin(true); err != nil {
		panic(err)
	}

	if lastTimestamps, err = localTx.CreateBucketIfNotExists(LastTimestampsBucket, false); err != nil {
		panic(err)
	}

	rep := &StateGrowth1Reporter{
		remoteDB:         remoteDB,
		HistoryKey:       []byte{},
		AccountKey:       []byte{},
		MaxTimestamp:     0,
		lastTimestamps:   typedbucket.NewUint64(lastTimestamps),
		CreationsByBlock: make(map[uint64]int),
	}
	rep.commit = func(ctx context.Context) {
		commit(ProgressKey, localTx, rep)
		if err = localTx.Commit(); err != nil {
			panic(err)
		}
		localTx, err = localDB.Begin(true)
		if err != nil {
			panic(err)
		}

		rep.lastTimestamps = typedbucket.NewUint64(localTx.Bucket(LastTimestampsBucket))
	}
	rep.rollback = func(ctx context.Context) {
		if err := localTx.Rollback(); err != nil {
			panic(err)
		}
	}

	restore(ProgressKey, localTx, rep)
	return rep
}

func (r *StateGrowth1Reporter) interrupt(ctx context.Context, i int, startTime time.Time) (breakTx bool) {
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

func (r *StateGrowth1Reporter) StateGrowth1(ctx context.Context) {
	defer r.rollback(ctx)
	startTime := time.Now()

	var i int
	var processingDone bool

beginTx:
	// Go through the history of account first
	if err := r.remoteDB.View(ctx, func(tx ethdb.Tx) error {
		var err error
		if r.StartedWhenBlockNumber == 0 {
			r.StartedWhenBlockNumber, err = remotechain.ReadLastBlockNumber(tx)
			if err != nil {
				return err
			}
		}

		c := tx.Bucket(dbutils.AccountsHistoryBucket).Cursor().Prefetch(CursorBatchSize).NoValues()

		for k, vSize, err := c.Seek(r.HistoryKey); k != nil; k, vSize, err = c.Next() {
			if err != nil {
				return err
			}
			vIsEmpty := vSize == 0
			i++
			r.HistoryKey = k
			if r.interrupt(ctx, i, startTime) {
				return nil
			}

			var addrHash common.Hash

			copy(addrHash[:], k[:32]) // First 32 bytes is the hash of the address, then timestamp encoding
			timestamp, _ := dbutils.DecodeTimestamp(k[32:])

			if timestamp > r.StartedWhenBlockNumber { // skip what happened after analysis started
				continue
			}

			if timestamp+1 > r.MaxTimestamp {
				r.MaxTimestamp = timestamp + 1
			}
			if vIsEmpty {
				r.CreationsByBlock[timestamp]++
				if lt, ok := r.lastTimestamps.Get(addrHash.Bytes()); ok {
					r.CreationsByBlock[lt]--
				}
			}

			if err := r.lastTimestamps.Put(addrHash.Bytes(), timestamp); err != nil {
				return err
			}
		}
		processingDone = true
		return nil
	}); err != nil {
		check(err)
	}

	if !processingDone {
		goto beginTx
	}

	processingDone = false
	i = 0

beginTx2:
	// Go through the current state
	if err := r.remoteDB.View(ctx, func(tx ethdb.Tx) error {
		pre := tx.Bucket(dbutils.PreimagePrefix)
		if pre == nil {
			return nil
		}
		c := tx.Bucket(dbutils.CurrentStateBucket).Cursor().Prefetch(CursorBatchSize).NoValues()
		for k, _, err := c.Seek(r.AccountKey); k != nil; k, _, err = c.Next() {
			if len(k) != 32 {
				continue
			}
			if err != nil {
				return err
			}
			i++
			r.AccountKey = k
			if r.interrupt(ctx, i, startTime) {
				return nil
			}

			var addrHash common.Hash

			copy(addrHash[:], k[:32]) // First 32 bytes is the hash of the address
			if err := r.lastTimestamps.Put(addrHash.Bytes(), r.MaxTimestamp); err != nil {
				return err
			}
		}
		processingDone = true
		return nil
	}); err != nil {
		check(err)
	}

	if !processingDone {
		goto beginTx2
	}

	if err := r.lastTimestamps.ForEach(func(_ []byte, lt uint64) error {
		if lt < r.MaxTimestamp {
			r.CreationsByBlock[lt]--
		}
		return nil
	}); err != nil {
		panic(err)
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
}

type StateGrowth2Reporter struct {
	StartedWhenBlockNumber uint64
	MaxTimestamp           uint64
	HistoryKey             []byte
	StorageKey             []byte

	// map[common.Hash]map[common.Hash]uint64 - key is addrHash + hash
	// For each address hash, when was it last accounted
	lastTimestamps *typedbucket.Uint64 `codec:"-"`
	// map[common.Hash]map[uint64]int - key is addrHash + hash
	// For each address hash, when was it last accounted
	creationsByBlock *typedbucket.Int `codec:"-"`

	remoteDB ethdb.KV `codec:"-"`
	commit   func(ctx context.Context)
	rollback func(ctx context.Context)
}

func NewStateGrowth2Reporter(ctx context.Context, remoteDB ethdb.KV, localDB *bolt.DB) *StateGrowth2Reporter {
	var LastTimestampsBucket = []byte("sg2_accounts_last_timestamps")
	var CreationsByBlockBucket = []byte("sg2_creations_by_block")
	var ProgressKey = []byte("state_growth_2")

	var err error
	var localTx *bolt.Tx
	var lastTimestamps *bolt.Bucket
	var creationsByBlock *bolt.Bucket

	if localTx, err = localDB.Begin(true); err != nil {
		panic(err)
	}

	if lastTimestamps, err = localTx.CreateBucketIfNotExists(LastTimestampsBucket, false); err != nil {
		panic(err)
	}

	if creationsByBlock, err = localTx.CreateBucketIfNotExists(CreationsByBlockBucket, false); err != nil {
		panic(err)
	}

	rep := &StateGrowth2Reporter{
		remoteDB:         remoteDB,
		HistoryKey:       []byte{},
		StorageKey:       []byte{},
		MaxTimestamp:     0,
		lastTimestamps:   typedbucket.NewUint64(lastTimestamps),
		creationsByBlock: typedbucket.NewInt(creationsByBlock),
	}
	rep.commit = func(ctx context.Context) {
		commit(ProgressKey, localTx, rep)
		if err = localTx.Commit(); err != nil {
			panic(err)
		}
		if localTx, err = localDB.Begin(true); err != nil {
			panic(err)
		}

		rep.lastTimestamps = typedbucket.NewUint64(localTx.Bucket(LastTimestampsBucket))
		rep.creationsByBlock = typedbucket.NewInt(localTx.Bucket(CreationsByBlockBucket))
	}
	rep.rollback = func(ctx context.Context) {
		if err := localTx.Rollback(); err != nil {
			panic(err)
		}
	}

	restore(ProgressKey, localTx, rep)
	return rep
}

func (r *StateGrowth2Reporter) interrupt(ctx context.Context, i int, startTime time.Time) (breakTx bool) {
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

func (r *StateGrowth2Reporter) StateGrowth2(ctx context.Context) {
	defer r.rollback(ctx)
	startTime := time.Now()
	var i int

	var processingDone bool

beginTx:
	// Go through the history of account first
	if err := r.remoteDB.View(ctx, func(tx ethdb.Tx) error {
		var err error
		if r.StartedWhenBlockNumber == 0 {
			r.StartedWhenBlockNumber, err = remotechain.ReadLastBlockNumber(tx)
			if err != nil {
				return err
			}
		}

		c := tx.Bucket(dbutils.StorageHistoryBucket).Cursor().Prefetch(CursorBatchSize).NoValues()
		for k, vSize, err := c.Seek(r.HistoryKey); k != nil; k, vSize, err = c.Next() {
			if err != nil {
				return err
			}
			vIsEmpty := vSize == 0
			r.HistoryKey = k
			i++
			if r.interrupt(ctx, i, startTime) {
				return nil
			}

			var addrHash, hash common.Hash
			copy(addrHash[:], k[:32]) // First 20 bytes is the address
			copy(hash[:], k[40:72])
			timestamp, _ := dbutils.DecodeTimestamp(k[72:])

			if timestamp > r.StartedWhenBlockNumber { // only count what happened before analysis started
				continue
			}

			addr2HashKey := append(addrHash.Bytes(), hash.Bytes()...)

			if timestamp+1 > r.MaxTimestamp {
				r.MaxTimestamp = timestamp + 1
			}
			if vIsEmpty {
				addr2TsKey := append(addrHash.Bytes(), dbutils.EncodeTimestamp(timestamp)...)
				if err := r.creationsByBlock.Increment(addr2TsKey); err != nil {
					return err
				}

				if err := r.lastTimestamps.DecrementIfExist(addr2HashKey); err != nil {
					return err
				}
			}
			if err := r.lastTimestamps.Put(addr2HashKey, timestamp); err != nil {
				return err
			}
		}
		processingDone = true
		return nil
	}); err != nil {
		panic(err)
	}

	if !processingDone {
		goto beginTx
	}

	processingDone = false
	i = 0

beginTx2:
	// Go through the current state
	if err := r.remoteDB.View(ctx, func(tx ethdb.Tx) error {
		c := tx.Bucket(dbutils.CurrentStateBucket).Cursor().Prefetch(CursorBatchSize).NoValues()
		for k, _, err := c.Seek(r.StorageKey); k != nil; k, _, err = c.Next() {
			if len(k) == 32 {
				continue
			}
			if err != nil {
				return err
			}
			i++
			r.StorageKey = k
			if r.interrupt(ctx, i, startTime) {
				return nil
			}

			var addrHash, hash common.Hash
			copy(addrHash[:], k[:32])
			copy(hash[:], k[40:72])
			localKey := append(addrHash.Bytes(), hash.Bytes()...)
			if err := r.lastTimestamps.Put(localKey, r.MaxTimestamp); err != nil {
				return err
			}
		}
		processingDone = true
		return nil
	}); err != nil {
		panic(err)
	}

	if !processingDone {
		goto beginTx2
	}

	if err := r.lastTimestamps.ForEach(func(k []byte, lt uint64) error {
		address := common.BytesToHash(k[:32])
		if lt < r.MaxTimestamp {
			addr2BlockKey := append(address.Bytes(), dbutils.EncodeTimestamp(lt)...)
			if h, ok := r.creationsByBlock.Get(addr2BlockKey); ok {
				h--
				if err := r.creationsByBlock.Put(addr2BlockKey, h); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Storage history records: %d\n", i)
	fmt.Printf("Creating dataset...\n")
	totalCreationsByBlock := make(map[uint64]int)
	if err := r.creationsByBlock.ForEach(func(k []byte, count int) error {
		timestamp, _ := dbutils.DecodeTimestamp(k[32:])
		totalCreationsByBlock[timestamp] += count
		return nil
	}); err != nil {
		panic(err)
	}

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

	debug.PrintMemStats(true)
}

type GasLimitReporter struct {
	remoteDB               ethdb.KV `codec:"-"`
	StartedWhenBlockNumber uint64
	HeaderPrefixKey1       []byte
	HeaderPrefixKey2       []byte

	// map[common.Hash]uint64 - key is addrHash + hash
	mainHashes *typedbucket.Uint64 // For each address hash, when was it last accounted

	commit   func(ctx context.Context)
	rollback func(ctx context.Context)
}

func NewGasLimitReporter(ctx context.Context, remoteDB ethdb.KV, localDB *bolt.DB) *GasLimitReporter {
	var MainHashesBucket = []byte("gl_main_hashes")
	var ProgressKey = []byte("gas_limit")

	var err error
	var localTx *bolt.Tx
	var mainHashes *bolt.Bucket

	if localTx, err = localDB.Begin(true); err != nil {
		panic(err)
	}
	if mainHashes, err = localTx.CreateBucketIfNotExists(MainHashesBucket, false); err != nil {
		panic(err)
	}

	rep := &GasLimitReporter{
		remoteDB:         remoteDB,
		HeaderPrefixKey1: []byte{},
		HeaderPrefixKey2: []byte{},
		mainHashes:       typedbucket.NewUint64(mainHashes),
	}
	rep.commit = func(ctx context.Context) {
		commit(ProgressKey, localTx, rep)
		if err = localTx.Commit(); err != nil {
			panic(err)
		}
		if localTx, err = localDB.Begin(true); err != nil {
			panic(err)
		}

		rep.mainHashes = typedbucket.NewUint64(localTx.Bucket(MainHashesBucket))
	}

	rep.rollback = func(ctx context.Context) {
		if err := localTx.Rollback(); err != nil {
			panic(err)
		}
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
	startTime := time.Now()

	f, ferr := os.Create("gas_limits.csv")
	check(ferr)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	//var blockNum uint64 = 5346726
	var blockNum uint64 = 0
	i := 0
	var processingDone bool

beginTx:
	if err := r.remoteDB.View(ctx, func(tx ethdb.Tx) error {
		var err error
		if r.StartedWhenBlockNumber == 0 {
			r.StartedWhenBlockNumber, err = remotechain.ReadLastBlockNumber(tx)
			if err != nil {
				return err
			}
		}

		fmt.Println("Preloading block numbers...")

		c := tx.Bucket(dbutils.HeaderPrefix).Cursor().Prefetch(CursorBatchSize)
		for k, v, err := c.Seek(r.HeaderPrefixKey1); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			i++
			r.HeaderPrefixKey1 = k
			if r.interrupt(ctx, i, startTime) {
				return nil
			}

			timestamp := binary.BigEndian.Uint64(k[:common.BlockNumberLength])
			if timestamp > r.StartedWhenBlockNumber { // skip everything what happened after analysis started
				break
			}

			// skip bucket keys not useful for analysis
			if !dbutils.IsHeaderHashKey(k) {
				continue
			}

			mainHash := make([]byte, len(v))
			copy(mainHash[:], v[:])
			if err := r.mainHashes.Put(mainHash, 0); err != nil {
				return err
			}
		}

		fmt.Println("Preloaded: ", r.mainHashes.Stats().KeyN)
		i = 0
		for k, v, err := c.Seek(r.HeaderPrefixKey2); k != nil; k, v, err = c.Next() {
			if err != nil {
				return fmt.Errorf("loop break: %w", err)
			}
			i++
			r.HeaderPrefixKey2 = k
			if r.interrupt(ctx, i, startTime) {
				return nil
			}

			timestamp := binary.BigEndian.Uint64(k[:common.BlockNumberLength])
			if timestamp > r.StartedWhenBlockNumber { // skip everything what happened after analysis started
				continue
			}

			if !dbutils.IsHeaderKey(k) {
				continue
			}

			if _, ok := r.mainHashes.Get(k[common.BlockNumberLength:]); !ok {
				continue
			}

			header := new(types.Header)
			if decodeErr := rlp.Decode(bytes.NewReader(v), header); decodeErr != nil {
				log.Error("Invalid block header RLP", "blockNum", blockNum, "err", decodeErr)
				return nil
			}

			fmt.Fprintf(w, "%d, %d\n", blockNum, header.GasLimit)
			blockNum++
		}
		processingDone = true
		return nil
	}); err != nil {
		panic(err)
	}

	if !processingDone {
		goto beginTx
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
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil, nil)
	check(err)
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewDbState(ethDb.KV(), block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Processed %dK blocks\n", blockNum/1000)
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
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(k) == 32 {
				continue
			}
			copy(addr[:], k[:20])
			del, ok := deleted[addr]
			if !ok {
				vv, _ := b.Get(crypto.Keccak256(addr[:]))
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
	//remoteDB, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//remoteDB, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
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
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
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
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
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
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
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
	maxBalance := uint256.NewInt().SetUint64(1000000000000000000)
	// Go through the current state
	thresholdMap := make(map[uint64]int)
	var a accounts.Account
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
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
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil, nil)
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
		dbstate := state.NewDbState(ethDb.KV(), block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, err = core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
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
			fmt.Printf("Processed %dK blocks\n", blockNum/1000)
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
