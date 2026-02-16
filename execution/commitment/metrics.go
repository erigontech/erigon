package commitment

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
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
	Branches        *BranchMetrics
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
	// metric config related
	metricsFilePrefix        string
	collectCommitmentMetrics bool
	writeCommitmentMetrics   bool
}

type MetricValues struct {
	mu              *sync.RWMutex
	Accounts        map[string]*AccountStats
	Branches        map[string]*BranchStats
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
		Branches:                 NewBranches(),
		collectCommitmentMetrics: dbg.KVReadLevelledMetrics,
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
	m.Accounts.writeCommitmentMetrics = true
	m.Branches.writeCommitmentMetrics = true
}

func (m *Metrics) AsValues() MetricValues {
	return MetricValues{
		mu:              &m.Accounts.m,
		Accounts:        m.Accounts.AccountStats,
		Branches:        m.Branches.BranchStats,
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
	if err := writeMetricsToCSV(m.Branches, m.metricsFilePrefix+"_branches.csv"); err != nil {
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

func metricsHeaders() []string {
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

func (m *Metrics) Headers() []string {
	return metricsHeaders()
}

func (m *Metrics) Values() [][]string {
	vals := [][]string{
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
	if have, want := len(vals[0]), len(m.Headers()); have != want {
		panic(fmt.Errorf("invalid number of values in metrics row: have=%d, want=%d", have, want))
	}
	return vals
}

func UnmarshallMetricsCsv(filePath string) ([]*Metrics, error) {
	return unmarshallCsvMetrics(filePath, metricsHeaders(), func(records [][]string) ([]*Metrics, error) {
		var metrics []*Metrics
		for i, row := range records {
			current := &Metrics{}
			var col int
			current.updates.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.addressKeys.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.storageKeys.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.loadBranch.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.loadAccount.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.loadStorage.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.updateBranch.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			for k := range 5 {
				depthsPair := row[col]
				depthsSplit := strings.Split(depthsPair, "/")
				if len(depthsSplit) != 2 {
					return nil, fmt.Errorf("invalid depths pair: %s", depthsPair)
				}
				current.loadDepths[k*2] = mustParseUintCsvCell(depthsSplit, 0, filePath)
				current.loadDepths[k*2+1] = mustParseUintCsvCell(depthsSplit, 1, filePath)
				col++
			}
			current.unfolds.Store(mustParseUintCsvCell(row, col, filePath))
			col++
			current.spentUnfolding = mustParseMillisecondsCsvCell(row, col, filePath)
			col++
			current.spentFolding = mustParseMillisecondsCsvCell(row, col, filePath)
			col++
			current.spentProcessing = mustParseMillisecondsCsvCell(row, col, filePath)
			if cols := col + 1; cols != len(row) {
				return nil, fmt.Errorf("invalid number of columns processed: row=%d, have=%d, want=%d, file=%s", i, cols, len(row), filePath)
			}
			metrics = append(metrics, current)
		}
		return metrics, nil
	})
}

func (m *Metrics) Reset() {
	if !m.collectCommitmentMetrics {
		return
	}

	m.Accounts.Reset()
	m.Branches.Reset()
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
		m.Branches.collect(plainKey, func(mx *BranchStats) {
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
	// metric config related
	writeCommitmentMetrics bool
}

func (am *AccountMetrics) collect(plainKey []byte, fn func(mx *AccountStats)) {
	if !am.writeCommitmentMetrics {
		return
	}
	var addr string
	if len(plainKey) > 0 {
		addr = string(plainKey[:min(length.Addr, len(plainKey))])
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

func (am *AccountMetrics) Headers() []string {
	return accountMetricsHeaders()
}

func accountMetricsHeaders() []string {
	return []string{
		"account",
		"storage updates",
		"loading account",
		"loading storage",
		"total unfolds",
		"total unfolding time (μs)",
		"total folds",
		"total folding time (μs)",
	}
}

func (am *AccountMetrics) Values() [][]string {
	am.m.Lock()
	defer am.m.Unlock()
	values := make([][]string, len(am.AccountStats)+1) // + 1 to add one empty line between "process" calls
	headersLen := len(am.Headers())
	var vi uint64
	for addr, stat := range am.AccountStats {
		values[vi] = []string{
			fmt.Sprintf("%x", addr),
			strconv.FormatUint(stat.StorageUpates, 10),
			strconv.FormatUint(stat.LoadAccount, 10),
			strconv.FormatUint(stat.LoadStorage, 10),
			strconv.FormatUint(stat.Unfolds, 10),
			strconv.Itoa(int(stat.SpentUnfolding.Microseconds())),
			strconv.FormatUint(stat.Folds, 10),
			strconv.Itoa(int(stat.SpentFolding.Microseconds())),
		}
		if len(values[vi]) != headersLen {
			panic(fmt.Errorf("invalid number of values in account metrics row: have=%d, want=%d", len(values[vi]), headersLen))
		}
		vi++
	}
	values[vi] = make([]string, headersLen)
	return values
}

func UnmarshallAccountMetricsCsv(filePath string) ([]*AccountMetrics, error) {
	return unmarshallCsvMetrics(filePath, accountMetricsHeaders(), func(records [][]string) ([]*AccountMetrics, error) {
		var metrics []*AccountMetrics
		current := &AccountMetrics{AccountStats: make(map[string]*AccountStats)}
		for i, row := range records {
			if isRowEmpty(row) {
				metrics = append(metrics, current)
				current = &AccountMetrics{AccountStats: make(map[string]*AccountStats)}
				continue
			}
			var col int
			addr := row[col]
			if _, ok := current.AccountStats[addr]; ok {
				return nil, fmt.Errorf("duplicate account address in metrics batch: addr=%s, batchIdx=%d, file=%s", addr, i, filePath)
			}
			accStats := &AccountStats{}
			current.AccountStats[addr] = accStats
			col++
			accStats.StorageUpates = mustParseUintCsvCell(row, col, filePath)
			col++
			accStats.LoadAccount = mustParseUintCsvCell(row, col, filePath)
			col++
			accStats.LoadStorage = mustParseUintCsvCell(row, col, filePath)
			col++
			accStats.Unfolds = mustParseUintCsvCell(row, col, filePath)
			col++
			accStats.SpentUnfolding = mustParseMicrosecondsCsvCell(row, col, filePath)
			col++
			accStats.Folds = mustParseUintCsvCell(row, col, filePath)
			col++
			accStats.SpentFolding = mustParseMicrosecondsCsvCell(row, col, filePath)
			if cols := col + 1; cols != len(row) {
				return nil, fmt.Errorf("invalid number of columns processed: row=%d, have=%d, want=%d, file=%s", i, cols, len(row), filePath)
			}
		}
		return metrics, nil
	})
}

func (am *AccountMetrics) Reset() {
	if !am.writeCommitmentMetrics {
		return
	}
	am.m.Lock()
	defer am.m.Unlock()
	am.AccountStats = make(map[string]*AccountStats)
}

type BranchStats struct {
	LoadBranch uint64
}

func NewBranches() *BranchMetrics {
	return &BranchMetrics{BranchStats: make(map[string]*BranchStats)}
}

type BranchMetrics struct {
	m sync.RWMutex
	// will be separate value for each key in parallel processing
	BranchStats map[string]*BranchStats
	// metric config related
	writeCommitmentMetrics bool
}

func (bm *BranchMetrics) Headers() []string {
	return branchMetricsHeaders()
}

func branchMetricsHeaders() []string {
	return []string{
		"branchKey",
		"loads",
	}
}

func (bm *BranchMetrics) Values() [][]string {
	bm.m.RLock()
	defer bm.m.RUnlock()
	values := make([][]string, len(bm.BranchStats)+1) // + 1 to add one empty line between "process" calls
	headersLen := len(bm.Headers())
	var vi uint64
	for branchKey, stat := range bm.BranchStats {
		values[vi] = []string{
			fmt.Sprintf("%x", branchKey),
			strconv.FormatUint(stat.LoadBranch, 10),
		}
		if len(values[vi]) != headersLen {
			panic(fmt.Errorf("invalid number of values in branch metrics metrics row: have=%d, want=%d", len(values[vi]), headersLen))
		}
		vi++
	}
	values[vi] = make([]string, headersLen)
	return values
}

func (bm *BranchMetrics) collect(plainKey []byte, fn func(mx *BranchStats)) {
	if !bm.writeCommitmentMetrics {
		return
	}
	if len(plainKey) == 0 {
		return
	}
	addr := string(plainKey)
	bm.m.Lock()
	defer bm.m.Unlock()
	bs, ok := bm.BranchStats[addr]
	if !ok {
		bs = &BranchStats{}
		bm.BranchStats[addr] = bs
	}
	fn(bs)
}

func (bm *BranchMetrics) Reset() {
	if !bm.writeCommitmentMetrics {
		return
	}
	bm.m.Lock()
	defer bm.m.Unlock()
	bm.BranchStats = make(map[string]*BranchStats)
}

func UnmarshallBranchMetricsCsv(filePath string) ([]*BranchMetrics, error) {
	return unmarshallCsvMetrics(filePath, branchMetricsHeaders(), func(records [][]string) ([]*BranchMetrics, error) {
		var metrics []*BranchMetrics
		current := &BranchMetrics{BranchStats: make(map[string]*BranchStats)}
		for i, row := range records {
			if isRowEmpty(row) {
				metrics = append(metrics, current)
				current = &BranchMetrics{BranchStats: make(map[string]*BranchStats)}
				continue
			}
			var col int
			branchKey := row[col]
			if _, ok := current.BranchStats[branchKey]; ok {
				return nil, fmt.Errorf("duplicate branch key in metrics batch: branchKey=%s, batchIdx=%d, file=%s", branchKey, i, filePath)
			}
			stats := &BranchStats{}
			current.BranchStats[branchKey] = stats
			col++
			stats.LoadBranch = mustParseUintCsvCell(row, col, filePath)
			if cols := col + 1; cols != len(row) {
				return nil, fmt.Errorf("invalid number of columns processed: row=%d, have=%d, want=%d, file=%s", i, cols, len(row), filePath)
			}
		}
		return metrics, nil
	})
}

func writeMetricsToCSV(metrics CsvMetrics, filePath string) (err error) {
	// Open the file in append mode or create if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Error("failed to close metrics file while writing", "err", err, "filePath", filePath)
		}
	}()

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

func UnmarshallMetricValuesCsv(filePathPrefix string) ([]MetricValues, error) {
	metrics, err := UnmarshallMetricsCsv(filePathPrefix + "_process.csv")
	if err != nil {
		return nil, err
	}
	accMetrics, err := UnmarshallAccountMetricsCsv(filePathPrefix + "_accounts.csv")
	if err != nil {
		return nil, err
	}
	if len(metrics) != len(accMetrics) {
		return nil, fmt.Errorf("different number of batch metrics: metrics=%d, accMetrics=%d", len(metrics), len(accMetrics))
	}
	branchMetrics, err := UnmarshallBranchMetricsCsv(filePathPrefix + "_branches.csv")
	if err != nil {
		return nil, err
	}
	if len(metrics) != len(branchMetrics) {
		return nil, fmt.Errorf("different number of batch metrics: metrics=%d, branchMetrics=%d", len(metrics), len(branchMetrics))
	}
	metricValues := make([]MetricValues, len(metrics))
	for i, m := range metrics {
		m.Accounts = accMetrics[i]
		m.Branches = branchMetrics[i]
		metricValues[i] = m.AsValues()
	}
	return metricValues, nil
}

func unmarshallCsvMetrics[T any](filePath string, headers []string, extractor func([][]string) ([]T, error)) ([]T, error) {
	records, err := readMetricsFromCSV(filePath)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("no metrics found in file: %s", filePath)
	}
	err = validateMetricsHeader(records[0], headers)
	if err != nil {
		return nil, err
	}
	return extractor(records[1:])
}

func readMetricsFromCSV(filePath string) ([][]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Error("failed to close metrics file while reading", "err", err, "filePath", filePath)
		}
	}()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("could not read metrics from file %s: %w", filePath, err)
	}
	return records, nil
}

func validateMetricsHeader(have []string, want []string) error {
	if len(have) != len(want) {
		return fmt.Errorf("invalid number of headers: have=%d, want=%d", len(have), len(want))
	}
	for i := range have {
		if have[i] != want[i] {
			return fmt.Errorf("invalid header at idx=%d: have=%s, want=%s", i, have[i], want[i])
		}
	}
	return nil
}

func mustParseUintCsvCell(row []string, col int, filePath string) uint64 {
	n, err := parseUintCsvCell(row, col, filePath)
	if err != nil {
		panic(err)
	}
	return n
}

func parseUintCsvCell(row []string, col int, filePath string) (uint64, error) {
	c := row[col]
	n, err := strconv.ParseUint(c, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse uint csv cell: cell=%s, col=%d, file=%s, row=%v", c, col, filePath, row)
	}
	return n, nil
}

func mustParseIntCsvCell(row []string, col int, filePath string) int64 {
	n, err := parseIntCsvCell(row, col, filePath)
	if err != nil {
		panic(err)
	}
	return n
}

func parseIntCsvCell(row []string, col int, filePath string) (int64, error) {
	c := row[col]
	n, err := strconv.ParseInt(c, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse int csv cell: cell=%s, col=%d, file=%s, row=%v", c, col, filePath, row)
	}
	return n, nil
}

func mustParseMillisecondsCsvCell(row []string, col int, filePath string) time.Duration {
	return time.Duration(mustParseIntCsvCell(row, col, filePath)) * time.Millisecond
}

func mustParseMicrosecondsCsvCell(row []string, col int, filePath string) time.Duration {
	return time.Duration(mustParseIntCsvCell(row, col, filePath)) * time.Microsecond
}

func isRowEmpty(row []string) bool {
	for _, c := range row {
		if c != "" {
			return false
		}
	}
	return true
}
