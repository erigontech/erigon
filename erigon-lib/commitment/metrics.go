package commitment

import (
	"encoding/csv"
	"encoding/hex"
	"os"
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
		"total unfolding time (ms)",
		"total folds",
		"total folding time (ms)",
	}
}

func (processAccount *AccountMetrics) Values() [][]string {
	processAccount.m.Lock()
	defer processAccount.m.Unlock()
	values := make([][]string, len(processAccount.AccountStats), len(processAccount.AccountStats))
	var ind int
	for i, account := range processAccount.AccountStats {
		values[ind] = []string{
			i,
			strconv.FormatUint(account.StorageUpates, 10),
			strconv.FormatUint(account.LoadBranch, 10),
			strconv.FormatUint(account.LoadAccount, 10),
			strconv.FormatUint(account.LoadStorage, 10),
			strconv.FormatUint(account.Unfolds, 10),
			strconv.Itoa(int(account.TotalUnfoldingTime.Milliseconds())),
			strconv.FormatUint(account.Folds, 10),
			strconv.Itoa(int(account.TotalFoldingTime.Milliseconds())),
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
