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
	Updates             atomic.Uint64
	AddressKeys         atomic.Uint64
	StorageKeys         atomic.Uint64
	LoadBranch          atomic.Uint64
	LoadAccount         atomic.Uint64
	LoadStorage         atomic.Uint64
	UpdateBranch        atomic.Uint64
	LoadDepths          [10]uint64
	Unfolds             atomic.Uint64
	TotalUnfoldingTime  time.Duration
	TotalFoldingTime    time.Duration
	TotalProcessingTime time.Duration
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
			strconv.FormatUint(metrics.Updates.Load(), 10),
			strconv.FormatUint(metrics.AddressKeys.Load(), 10),
			strconv.FormatUint(metrics.StorageKeys.Load(), 10),
			strconv.FormatUint(metrics.LoadBranch.Load(), 10),
			strconv.FormatUint(metrics.LoadAccount.Load(), 10),
			strconv.FormatUint(metrics.LoadStorage.Load(), 10),
			strconv.FormatUint(metrics.UpdateBranch.Load(), 10),
			strconv.FormatUint(metrics.LoadDepths[0], 10) + "/" + strconv.FormatUint(metrics.LoadDepths[1], 10),
			strconv.FormatUint(metrics.LoadDepths[2], 10) + "/" + strconv.FormatUint(metrics.LoadDepths[3], 10),
			strconv.FormatUint(metrics.LoadDepths[4], 10) + "/" + strconv.FormatUint(metrics.LoadDepths[5], 10),
			strconv.FormatUint(metrics.LoadDepths[6], 10) + "/" + strconv.FormatUint(metrics.LoadDepths[7], 10),
			strconv.FormatUint(metrics.LoadDepths[8], 10) + "/" + strconv.FormatUint(metrics.LoadDepths[9], 10),
			strconv.FormatUint(metrics.Unfolds.Load(), 10),
			strconv.Itoa(int(metrics.TotalUnfoldingTime.Milliseconds())),
			strconv.Itoa(int(metrics.TotalFoldingTime.Milliseconds())),
			strconv.Itoa(int(metrics.TotalProcessingTime.Milliseconds())),
		},
	}
}

func (metrics *Metrics) Reset() {
	metrics.Accounts.Reset()
	metrics.Updates.Store(0)
	metrics.AddressKeys.Store(0)
	metrics.StorageKeys.Store(0)
	metrics.LoadBranch.Store(0)
	metrics.LoadAccount.Store(0)
	metrics.LoadStorage.Store(0)
	metrics.UpdateBranch.Store(0)
	metrics.Unfolds.Store(0)
	metrics.TotalUnfoldingTime = 0
	metrics.TotalFoldingTime = 0
	metrics.TotalProcessingTime = 0
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
			metrics.LoadDepths[i*2], metrics.LoadDepths[i*2+1] = v.accLoaded, v.storLoaded
		}
	}
}

func (metrics *Metrics) Account(plainKey []byte) {
	if collectCommitmentMetrics {
		metrics.LoadAccount.Add(1)
		metrics.Accounts.LoadAccountInc(plainKey)
	}
}

func (metrics *Metrics) Storage(plainKey []byte) {
	if collectCommitmentMetrics {
		metrics.LoadStorage.Add(1)
		metrics.Accounts.LoadStorageInc(plainKey)
	}
}

func (metrics *Metrics) Branch(plainKey []byte) {
	if collectCommitmentMetrics {
		metrics.LoadBranch.Add(1)
		metrics.Accounts.LoadBranchInc(plainKey)
	}
}

func (metrics *Metrics) StartUnfolding(plainKey []byte) func() {
	if collectCommitmentMetrics {
		startUnfold := metrics.Now()
		metrics.Accounts.UnfoldsInc(metrics.Accounts.currentPlainKey)
		metrics.Unfolds.Add(1)
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
		metrics.TotalUnfoldingTime += time.Since(t)
	}
}

func (metrics *Metrics) TotalProcessingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		metrics.TotalProcessingTime += time.Since(t)
	}
}

func (metrics *Metrics) TotalFoldingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		metrics.Accounts.TotalFoldingTimeInc(metrics.Accounts.currentPlainKey, t)
		metrics.TotalFoldingTime += time.Since(t)
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
