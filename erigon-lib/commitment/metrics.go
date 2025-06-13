package commitment

import (
	"encoding/csv"
	"fmt"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/length"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

/*
ERIGON_COMMITMENT_TRACE - file path prefix to write commitment metrics
*/
func init() {
	metricsFile = dbg.EnvString("ERIGON_COMMITMENT_TRACE", "")
	collectCommitmentMetrics = metricsFile != ""
}

var (
	metricsFile              string
	collectCommitmentMetrics bool
)

type CsvMetrics interface {
	Headers() []string
	Values() [][]string
}

type Metrics struct {
	Accounts        *AccountMetrics
	updates         atomic.Uint64
	addressKeys     atomic.Uint64
	storageKeys     atomic.Uint64
	loadBranch      atomic.Uint64
	loadAccount     atomic.Uint64
	loadStorage     atomic.Uint64
	updateBranch    atomic.Uint64
	loadDepths      [10]uint64
	unfolds         atomic.Uint64
	spentUnfolding  time.Duration
	spentFolding    time.Duration
	spentProcessing time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		Accounts: NewAccounts(),
	}
}

func (m *Metrics) WriteToCSV() {
	if collectCommitmentMetrics {
		if err := writeMetricsToCSV(m, metricsFile+"_process.csv"); err != nil {
			panic(err)
		}
		if err := writeMetricsToCSV(m.Accounts, metricsFile+"_accounts.csv"); err != nil {
			panic(err)
		}
	}
}

func (m *Metrics) Headers() []string {
	return []string{
		"updates",
		"address plainKey",
		"account plainKeys",
		"PatriciaContext.Branch()",
		"PatriciaContext.Account()",
		"PatriciaContext.Storage()",
		"PatriciaContext.PutBranch()",
		"L0 - Load Account/Storage",
		"L1 - Load Account/Storage",
		"L2 - Load Account/Storage",
		"L3 - Load Account/Storage",
		"L4 - Load Account/Storage",
		"unfold/fold calls",
		"total unfolding time (ms)",
		"total folding time (ms)",
		"total processing time (ms)",
	}
}

func (m *Metrics) Values() [][]string {
	return [][]string{
		{
			strconv.FormatUint(m.updates.Load(), 10),
			strconv.FormatUint(m.addressKeys.Load(), 10),
			strconv.FormatUint(m.storageKeys.Load(), 10),
			strconv.FormatUint(m.loadBranch.Load(), 10),
			strconv.FormatUint(m.loadAccount.Load(), 10),
			strconv.FormatUint(m.loadStorage.Load(), 10),
			strconv.FormatUint(m.updateBranch.Load(), 10),
			strconv.FormatUint(m.loadDepths[0], 10) + "/" + strconv.FormatUint(m.loadDepths[1], 10),
			strconv.FormatUint(m.loadDepths[2], 10) + "/" + strconv.FormatUint(m.loadDepths[3], 10),
			strconv.FormatUint(m.loadDepths[4], 10) + "/" + strconv.FormatUint(m.loadDepths[5], 10),
			strconv.FormatUint(m.loadDepths[6], 10) + "/" + strconv.FormatUint(m.loadDepths[7], 10),
			strconv.FormatUint(m.loadDepths[8], 10) + "/" + strconv.FormatUint(m.loadDepths[9], 10),
			strconv.FormatUint(m.unfolds.Load(), 10),
			strconv.FormatInt(m.spentUnfolding.Milliseconds(), 10),
			strconv.FormatInt(m.spentFolding.Milliseconds(), 10),
			strconv.FormatInt(m.spentProcessing.Milliseconds(), 10),
		},
	}
}

func (m *Metrics) Reset() {
	if !collectCommitmentMetrics {
		return
	}

	m.Accounts.Reset()
	m.updates.Store(0)
	m.addressKeys.Store(0)
	m.storageKeys.Store(0)
	m.loadBranch.Store(0)
	m.loadAccount.Store(0)
	m.loadStorage.Store(0)
	m.updateBranch.Store(0)
	m.unfolds.Store(0)
	m.spentUnfolding = 0
	m.spentFolding = 0
	m.spentProcessing = 0
}

func (m *Metrics) CollectFileDepthStats(endTxNumStats map[uint64]skipStat) {
	if !collectCommitmentMetrics {
		return
	}
	ends := make([]uint64, 0, len(endTxNumStats))
	for k := range endTxNumStats {
		ends = append(ends, k)
	}
	// sort by file endTxNum
	sort.Slice(ends, func(i, j int) bool { return ends[i] > ends[j] })
	for i := 0; i < 5 && i < len(ends); i++ {
		// get stats for specific file depth
		v := endTxNumStats[ends[i]]
		// write level i file stats - account and storage loads
		m.loadDepths[i*2], m.loadDepths[i*2+1] = v.accLoaded, v.storLoaded
	}
}

func (m *Metrics) Updates(plainKey []byte) {
	if !collectCommitmentMetrics {
		return
	}
	if len(plainKey) == length.Addr {
		m.addressKeys.Add(1)
	} else {
		m.storageKeys.Add(1)

		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.StorageUpates++
		})
	}
}

func (m *Metrics) AccountLoad(plainKey []byte) {
	if collectCommitmentMetrics {
		m.loadAccount.Add(1)
		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.LoadAccount++
		})
	}
}

func (m *Metrics) StorageLoad(plainKey []byte) {
	if collectCommitmentMetrics {
		m.loadStorage.Add(1)
		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.LoadStorage++
		})
	}
}

func (m *Metrics) BranchLoad(plainKey []byte) {
	if collectCommitmentMetrics {
		m.loadBranch.Add(1)
		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.LoadBranch++
		})
	}
}

func (m *Metrics) StartUnfolding(plainKey []byte) func() {
	if collectCommitmentMetrics {
		start := time.Now()
		m.unfolds.Add(1)
		return func() {
			d := time.Since(start)
			m.spentUnfolding += d
			m.Accounts.collect(plainKey, func(mx *AccountStats) {
				mx.SpentUnfolding += d
			})
		}
	}
	return func() {}
}

func (m *Metrics) StartFolding(plainKey []byte) func() {
	if collectCommitmentMetrics {
		start := time.Now()
		return func() {
			d := time.Since(start)
			m.spentFolding += d
			m.Accounts.collect(plainKey, func(mx *AccountStats) {
				mx.SpentFolding += d
			})
		}
	}
	return func() {}
}

func (m *Metrics) TotalProcessingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		m.spentProcessing += time.Since(t)
	}
}

func NewAccounts() *AccountMetrics {
	return &AccountMetrics{
		AccountStats: make(map[string]*AccountStats),
	}
}

type AccountStats struct {
	StorageUpates  uint64
	LoadBranch     uint64
	LoadAccount    uint64
	LoadStorage    uint64
	Unfolds        uint64
	Folds          uint64
	SpentUnfolding time.Duration
	SpentFolding   time.Duration
}

type AccountMetrics struct {
	//m sync.Mutex
	// will be separate value for each key in parallel processing
	AccountStats map[string]*AccountStats
}

func (am *AccountMetrics) collect(plainKey []byte, fn func(mx *AccountStats)) {
	//am.m.Lock()
	//defer am.m.Unlock()

	var addr string
	if len(plainKey) > 0 {
		addr = toStringZeroCopy(plainKey[:min(length.Addr, len(plainKey))])
	}
	as, ok := am.AccountStats[addr]
	if !ok {
		as = &AccountStats{}
	}
	fn(as)
	am.AccountStats[addr] = as
}

func (am *AccountMetrics) Headers() []string {
	return []string{
		"account",
		"storage updates",
		"loading branch",
		"loading account",
		"loading storage",
		"total unfolds",
		"total unfolding time (μs)",
		"total folds",
		"total folding time (μs)",
	}
}

func (am *AccountMetrics) Values() [][]string {
	//am.m.Lock()
	//defer am.m.Unlock()

	values := make([][]string, len(am.AccountStats)+1, len(am.AccountStats)+1) // + 1 to add one empty line between "process" calls
	vi := 1
	for addr, stat := range am.AccountStats {
		values[vi] = []string{
			fmt.Sprintf("%x", addr),
			strconv.FormatUint(stat.StorageUpates, 10),
			strconv.FormatUint(stat.LoadBranch, 10),
			strconv.FormatUint(stat.LoadAccount, 10),
			strconv.FormatUint(stat.LoadStorage, 10),
			strconv.FormatUint(stat.Unfolds, 10),
			strconv.Itoa(int(stat.SpentUnfolding.Microseconds())),
			strconv.FormatUint(stat.Folds, 10),
			strconv.Itoa(int(stat.SpentFolding.Microseconds())),
		}
		vi++
	}
	return values
}

func (am *AccountMetrics) Reset() {
	//am.m.Lock()
	//defer am.m.Unlock()
	am.AccountStats = make(map[string]*AccountStats)
}

func writeMetricsToCSV(metrics CsvMetrics, filePath string) error {
	if !collectCommitmentMetrics {
		return nil
	}
	// Open the file in append mode or create if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a new writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Optionally write header if file is empty
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if err := writer.Write(metrics.Headers()); err != nil {
			return err
		}
	}
	// Write the actual data
	for _, value := range metrics.Values() {
		if err := writer.Write(value); err != nil {
			return err
		}
	}
	return nil
}
