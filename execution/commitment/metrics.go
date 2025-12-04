package commitment

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
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
	batchStart      time.Time // used to tell which metrics belong to the same processed batch
	// metric config related
	metricsFilePrefix        string
	collectCommitmentMetrics bool
	writeCommitmentMetrics   bool
}

type MetricValues struct {
	mu              *sync.RWMutex
	Accounts        map[string]*AccountStats
	Updates         uint64
	AddressKeys     uint64
	StorageKeys     uint64
	LoadBranch      uint64
	LoadAccount     uint64
	LoadStorage     uint64
	UpdateBranch    uint64
	LoadDepths      [10]uint64
	Unfolds         uint64
	SpentUnfolding  time.Duration
	SpentFolding    time.Duration
	SpentProcessing time.Duration
}

func (m MetricValues) RLock() {
	if m.mu != nil {
		m.mu.RLock()
	}
}

func (m MetricValues) RUnlock() {
	if m.mu != nil {
		m.mu.RUnlock()
	}
}

func NewMetrics() *Metrics {
	metrics := &Metrics{
		Accounts:                 NewAccounts(),
		collectCommitmentMetrics: true,
	}
	csvFilePathPrefix := dbg.EnvString("ERIGON_COMMITMENT_CSV_METRICS_FILE_PATH_PREFIX", "")
	if csvFilePathPrefix != "" {
		metrics.EnableCsvMetrics(csvFilePathPrefix)
	}
	return metrics
}

func (m *Metrics) EnableCsvMetrics(filePathPrefix string) {
	m.metricsFilePrefix = filePathPrefix
	m.writeCommitmentMetrics = true
	m.collectCommitmentMetrics = true
}

func (m *Metrics) AsValues() MetricValues {
	return MetricValues{
		mu:              &m.Accounts.m,
		Accounts:        m.Accounts.AccountStats,
		Updates:         m.updates.Load(),
		AddressKeys:     m.addressKeys.Load(),
		StorageKeys:     m.storageKeys.Load(),
		LoadBranch:      m.loadBranch.Load(),
		LoadAccount:     m.loadAccount.Load(),
		LoadStorage:     m.loadStorage.Load(),
		UpdateBranch:    m.updateBranch.Load(),
		LoadDepths:      m.loadDepths,
		Unfolds:         m.unfolds.Load(),
		SpentUnfolding:  m.spentUnfolding,
		SpentFolding:    m.spentFolding,
		SpentProcessing: m.spentProcessing,
	}
}

func (m *Metrics) WriteToCSV() {
	if !m.writeCommitmentMetrics {
		return
	}
	if err := writeMetricsToCSV(m, m.metricsFilePrefix+"_process.csv"); err != nil {
		panic(err)
	}
	if err := writeMetricsToCSV(m.Accounts, m.metricsFilePrefix+"_accounts.csv"); err != nil {
		panic(err)
	}
}

func (m *Metrics) logMetrics() []any {
	return []any{
		"akeys", common.PrettyCounter(m.addressKeys.Load()), "skeys", common.PrettyCounter(m.storageKeys.Load()),
		"rdb", common.PrettyCounter(m.loadBranch.Load()), "rda", common.PrettyCounter(m.loadAccount.Load()),
		"rds", common.PrettyCounter(m.loadStorage.Load()), "wrb", common.PrettyCounter(m.updateBranch.Load()),
		"fld", common.PrettyCounter(m.unfolds.Load()), "pdur", common.Round(m.spentProcessing, 0).String(),
		"fdur", common.Round(m.spentFolding, 0).String(), "ufdur", common.Round(m.spentUnfolding, 0),
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

func UnmarshallMetricsCsv(filePath string) ([]*Metrics, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metrics) Reset() {
	if !m.collectCommitmentMetrics {
		return
	}
	m.batchStart = time.Now()
	m.Accounts.Reset(m.batchStart)
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
	if !m.writeCommitmentMetrics {
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
	if !m.collectCommitmentMetrics {
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
	if m.collectCommitmentMetrics {
		m.loadAccount.Add(1)
		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.LoadAccount++
		})
	}
}

func (m *Metrics) StorageLoad(plainKey []byte) {
	if m.collectCommitmentMetrics {
		m.loadStorage.Add(1)
		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.LoadStorage++
		})
	}
}

func (m *Metrics) BranchLoad(plainKey []byte) {
	if m.collectCommitmentMetrics {
		m.loadBranch.Add(1)
		m.Accounts.collect(plainKey, func(mx *AccountStats) {
			mx.LoadBranch++
		})
	}
}

func (m *Metrics) StartUnfolding(plainKey []byte) func() {
	if m.collectCommitmentMetrics {
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
	if m.collectCommitmentMetrics {
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
	if m.collectCommitmentMetrics {
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
	m sync.RWMutex
	// will be separate value for each key in parallel processing
	AccountStats map[string]*AccountStats
	BatchStart   time.Time
}

func (am *AccountMetrics) collect(plainKey []byte, fn func(mx *AccountStats)) {
	var addr string
	if len(plainKey) > 0 {
		addr = toStringZeroCopy(plainKey[:min(length.Addr, len(plainKey))])
	}
	am.m.Lock()
	defer am.m.Unlock()
	as, ok := am.AccountStats[addr]
	if !ok {
		as = &AccountStats{}
		am.AccountStats[addr] = as
	}
	fn(as)
}

var accountMetricsHeaders = []string{
	"batchStart",
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

func (am *AccountMetrics) Headers() []string {
	return accountMetricsHeaders
}

func (am *AccountMetrics) Values() [][]string {
	am.m.Lock()
	defer am.m.Unlock()
	values := make([][]string, len(am.AccountStats)+1) // + 1 to add one empty line between "process" calls
	vi := 1
	for addr, stat := range am.AccountStats {
		values[vi] = []string{
			fmt.Sprintf("%d", am.BatchStart.Unix()),
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

func AccountMetricsUnmarshallCsv(filePath string) ([]*AccountMetrics, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error("failed to close commitment account metrics csv file", "err", err)
		}
	}()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	var metrics []*AccountMetrics
	var current *AccountMetrics
	for i, row := range records {
		if i == 0 {
			if len(row) != len(accountMetricsHeaders) {
				return nil, fmt.Errorf("headers len don't match: got=%d, wanted=%d", len(row), len(accountMetricsHeaders))
			}
			for j, header := range row {
				if header != accountMetricsHeaders[j] {
					return nil, fmt.Errorf("headers sequence doesn't match: got=%s, wanted=%s, idx=%d", header, accountMetricsHeaders[j], j)
				}
			}
		} else {
			var col int
			batchStart, err := strconv.ParseInt(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			batchStartTime := time.Unix(batchStart, 0)
			if current == nil || current.BatchStart != batchStartTime {
				current = &AccountMetrics{BatchStart: batchStartTime}
				metrics = append(metrics, current)
			}
			col++
			addr := row[col]
			if _, ok := current.AccountStats[addr]; ok {
				return nil, fmt.Errorf("duplicate account address in metrics batch: addr=%s, batchStart=%d", addr, batchStart)
			}
			accStats := &AccountStats{}
			current.AccountStats[addr] = accStats
			col++
			accStats.StorageUpates, err = strconv.ParseUint(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			col++
			accStats.LoadBranch, err = strconv.ParseUint(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			col++
			accStats.LoadAccount, err = strconv.ParseUint(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			col++
			accStats.LoadStorage, err = strconv.ParseUint(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			col++
			accStats.Unfolds, err = strconv.ParseUint(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			col++
			spentUnfolding, err := strconv.ParseInt(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			accStats.SpentUnfolding = time.Duration(spentUnfolding) * time.Microsecond
			col++
			accStats.Folds, err = strconv.ParseUint(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			col++
			spentFolding, err := strconv.ParseInt(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
			accStats.SpentFolding = time.Duration(spentFolding) * time.Microsecond
			if col != len(accountMetricsHeaders) {
				return nil, fmt.Errorf("haven't processed correct number of columns in metrics row: got=%d, wanted=%d", col, len(accountMetricsHeaders))
			}
		}
	}
	return metrics, nil
}

func (am *AccountMetrics) Reset(batchStart time.Time) {
	am.m.Lock()
	defer am.m.Unlock()
	am.AccountStats = make(map[string]*AccountStats)
	am.BatchStart = batchStart
}

func writeMetricsToCSV(metrics CsvMetrics, filePath string) (err error) {
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
