package commitment

import (
	"encoding/csv"
	"encoding/hex"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

/*
ERIGON_COMMITMENT_TRACE - file path prefix to write commitment metrics
*/
func init() {
	metricsFile = os.Getenv("ERIGON_COMMITMENT_TRACE")
	collectCommitmentMetrics = metricsFile != ""
}

var (
	metricsFile              string
	collectCommitmentMetrics bool
)

type CsvMetrics interface {
	Reset()
	Headers() []string
	Values() [][]string
}

type Metrics struct {
	Accounts            *AccountMetrics
	updates             atomic.Uint64
	addressKeys         atomic.Uint64
	storageKeys         atomic.Uint64
	loadBranch          atomic.Uint64
	loadAccount         atomic.Uint64
	loadStorage         atomic.Uint64
	updateBranch        atomic.Uint64
	loadDepths          [10]uint64
	unfolds             atomic.Uint64
	totalUnfoldingTime  time.Duration
	totalFoldingTime    time.Duration
	totalProcessingTime time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		Accounts: NewAccounts(),
	}
}

func (metrics *Metrics) Headers() []string {
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

func (metrics *Metrics) Values() [][]string {
	return [][]string{
		[]string{
			strconv.FormatUint(metrics.updates.Load(), 10),
			strconv.FormatUint(metrics.addressKeys.Load(), 10),
			strconv.FormatUint(metrics.storageKeys.Load(), 10),
			strconv.FormatUint(metrics.loadBranch.Load(), 10),
			strconv.FormatUint(metrics.loadAccount.Load(), 10),
			strconv.FormatUint(metrics.loadStorage.Load(), 10),
			strconv.FormatUint(metrics.updateBranch.Load(), 10),
			strconv.FormatUint(metrics.loadDepths[0], 10) + "/" + strconv.FormatUint(metrics.loadDepths[1], 10),
			strconv.FormatUint(metrics.loadDepths[2], 10) + "/" + strconv.FormatUint(metrics.loadDepths[3], 10),
			strconv.FormatUint(metrics.loadDepths[4], 10) + "/" + strconv.FormatUint(metrics.loadDepths[5], 10),
			strconv.FormatUint(metrics.loadDepths[6], 10) + "/" + strconv.FormatUint(metrics.loadDepths[7], 10),
			strconv.FormatUint(metrics.loadDepths[8], 10) + "/" + strconv.FormatUint(metrics.loadDepths[9], 10),
			strconv.FormatUint(metrics.unfolds.Load(), 10),
			strconv.Itoa(int(metrics.totalUnfoldingTime.Milliseconds())),
			strconv.Itoa(int(metrics.totalFoldingTime.Milliseconds())),
			strconv.Itoa(int(metrics.totalProcessingTime.Milliseconds())),
		},
	}
}

func (metrics *Metrics) Reset() {
	metrics.Accounts.Reset()
	metrics.updates.Store(0)
	metrics.addressKeys.Store(0)
	metrics.storageKeys.Store(0)
	metrics.loadBranch.Store(0)
	metrics.loadAccount.Store(0)
	metrics.loadStorage.Store(0)
	metrics.updateBranch.Store(0)
	metrics.unfolds.Store(0)
	metrics.totalUnfoldingTime = 0
	metrics.totalFoldingTime = 0
	metrics.totalProcessingTime = 0
}

func (metrics *Metrics) Now() time.Time {
	if collectCommitmentMetrics {
		return time.Now()
	}
	return time.Time{}
}

func (metrics *Metrics) CollectFileDepthStats(m map[uint64]skipStat) {
	if collectCommitmentMetrics {
		ends := make([]uint64, 0, len(m))
		for k := range m {
			ends = append(ends, k)
		}
		sort.Slice(ends, func(i, j int) bool { return ends[i] > ends[j] })
		for i := 0; i < 5 && i < len(ends); i++ {
			// get stats for specific file depth
			v := m[ends[i]]
			// write level i file stats - account and storage loads
			metrics.loadDepths[i*2], metrics.loadDepths[i*2+1] = v.accLoaded, v.storLoaded
		}
	}
}

func (metrics *Metrics) Account(plainKey []byte) {
	if collectCommitmentMetrics {
		metrics.loadAccount.Add(1)
		metrics.Accounts.LoadAccountInc(plainKey)
	}
}

func (metrics *Metrics) Storage(plainKey []byte) {
	if collectCommitmentMetrics {
		metrics.loadStorage.Add(1)
		metrics.Accounts.LoadStorageInc(plainKey)
	}
}

func (metrics *Metrics) Branch(plainKey []byte) {
	if collectCommitmentMetrics {
		metrics.loadBranch.Add(1)
		metrics.Accounts.LoadBranchInc(plainKey)
	}
}

func (metrics *Metrics) StartUnfolding(plainKey []byte) func() {
	if collectCommitmentMetrics {
		startUnfold := metrics.Now()
		metrics.Accounts.UnfoldsInc(metrics.Accounts.currentPlainKey)
		metrics.unfolds.Add(1)
		return func() {
			metrics.TotalUnfoldingTimeInc(startUnfold)
			metrics.Accounts.TotalUnfoldingTimeInc(metrics.Accounts.currentPlainKey, startUnfold)
		}
	}
	return func() {}
}

func (metrics *Metrics) StartFolding(plainKey []byte) func() {
	if collectCommitmentMetrics {
		startUnfold := metrics.Now()
		metrics.Accounts.FoldsInc(plainKey)
		return func() {
			metrics.TotalFoldingTimeInc(startUnfold)
		}
	}
	return func() {}
}

func (metrics *Metrics) TotalUnfoldingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		metrics.totalUnfoldingTime += time.Since(t)
	}
}

func (metrics *Metrics) TotalProcessingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		metrics.totalProcessingTime += time.Since(t)
	}
}

func (metrics *Metrics) TotalFoldingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		metrics.Accounts.TotalFoldingTimeInc(metrics.Accounts.currentPlainKey, t)
		metrics.totalFoldingTime += time.Since(t)
	}
}

func NewAccounts() *AccountMetrics {
	return &AccountMetrics{
		AccountStats: make(map[string]*AccountStats),
	}
}

type AccountStats struct {
	AccountUpdates     uint64
	StorageUpates      uint64
	LoadBranch         uint64
	LoadAccount        uint64
	LoadStorage        uint64
	Unfolds            uint64
	TotalUnfoldingTime time.Duration
	Folds              uint64
	TotalFoldingTime   time.Duration
}

type AccountMetrics struct {
	m sync.Mutex
	// will be separate value for each key in parallel processing
	currentPlainKey []byte
	AccountStats    map[string]*AccountStats
}

func (processAccount *AccountMetrics) Headers() []string {
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

func (processAccount *AccountMetrics) Values() [][]string {
	processAccount.m.Lock()
	defer processAccount.m.Unlock()
	// + 1 to add one empty line between "process" calls
	values := make([][]string, len(processAccount.AccountStats)+1, len(processAccount.AccountStats)+1)
	ind := 1
	for i, account := range processAccount.AccountStats {
		values[ind] = []string{
			i,
			strconv.FormatUint(account.StorageUpates, 10),
			strconv.FormatUint(account.LoadBranch, 10),
			strconv.FormatUint(account.LoadAccount, 10),
			strconv.FormatUint(account.LoadStorage, 10),
			strconv.FormatUint(account.Unfolds, 10),
			strconv.Itoa(int(account.TotalUnfoldingTime.Microseconds())),
			strconv.FormatUint(account.Folds, 10),
			strconv.Itoa(int(account.TotalFoldingTime.Microseconds())),
		}
		ind++
	}
	return values
}

func (processAccount *AccountMetrics) Reset() {
	processAccount.m.Lock()
	defer processAccount.m.Unlock()
	processAccount.AccountStats = make(map[string]*AccountStats)
}

func (processAccount *AccountMetrics) KeyRegister(plainKey []byte) {
	if !collectCommitmentMetrics {
		return
	}
	processAccount.UpdatesInc(plainKey)
	processAccount.currentPlainKey = plainKey
	processAccount.UpdatesStorageInc(plainKey)
}

func (processAccount *AccountMetrics) UpdatesInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		if len(plainKey) == 20 {
			processAccount.AccountStats[account].AccountUpdates++
		}
	}
}

func (processAccount *AccountMetrics) UpdatesStorageInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		if len(plainKey) > 20 {
			processAccount.AccountStats[account].StorageUpates++
		}
	}
}

func (processAccount *AccountMetrics) LoadBranchInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].LoadBranch++
	}
}

func (processAccount *AccountMetrics) LoadAccountInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].LoadAccount++
	}
}

func (processAccount *AccountMetrics) LoadStorageInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].LoadStorage++
	}
}

func (processAccount *AccountMetrics) UnfoldsInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].Unfolds++
	}
}

func (processAccount *AccountMetrics) FoldsInc(plainKey []byte) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].Folds++
	}
}

func (processAccount *AccountMetrics) TotalUnfoldingTimeInc(plainKey []byte, t time.Time) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].TotalUnfoldingTime += time.Since(t)
	}
}

func (processAccount *AccountMetrics) TotalFoldingTimeInc(plainKey []byte, t time.Time) {
	if collectCommitmentMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:20])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].TotalFoldingTime += time.Since(t)
	}
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
