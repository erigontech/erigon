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

func init() {
	commitmentMetricsFile = os.Getenv("ERIGON_COMMITMENT_TRACE")
	accountMetricsFile = os.Getenv("ERIGON_ACCOUNT_COMMITMENT_TRACE")
	collectCommitmentMetrics = os.Getenv("ERIGON_COMMITMENT_TRACE") != ""
	collectAccountMetrics = os.Getenv("ERIGON_ACCOUNT_COMMITMENT_TRACE") != ""
}

var (
	commitmentMetricsFile    string
	accountMetricsFile       string
	collectCommitmentMetrics bool
	collectAccountMetrics    bool
)

type Metrics interface {
	Reset()
	Headers() []string
	Values() [][]string
}

type ProcessCommitment struct {
	Updates             atomic.Uint64
	AddressKeys         atomic.Uint64
	StorageKeys         atomic.Uint64
	LoadBranch          atomic.Uint64
	LoadAccount         atomic.Uint64
	LoadStorage         atomic.Uint64
	UpdateBranch        atomic.Uint64
	Unfolds             atomic.Uint64
	TotalUnfoldingTime  time.Duration
	Folds               atomic.Uint64
	TotalFoldingTime    time.Duration
	TotalProcessingTime time.Duration
}

func (processCommitment *ProcessCommitment) Now() time.Time {
	if collectCommitmentMetrics {
		return time.Now()
	}
	return time.Time{}
}

func (ProcessCommitment *ProcessCommitment) TotalUnfoldingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		ProcessCommitment.TotalUnfoldingTime += time.Since(t)
	}
}

func (ProcessCommitment *ProcessCommitment) TotalProcessingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		ProcessCommitment.TotalProcessingTime += time.Since(t)
	}
}

func (ProcessCommitment *ProcessCommitment) TotalFoldingTimeInc(t time.Time) {
	if collectCommitmentMetrics {
		ProcessCommitment.TotalFoldingTime += time.Since(t)
	}
}

func (processCommitment *ProcessCommitment) Reset() {
	processCommitment.Updates.Store(0)
	processCommitment.AddressKeys.Store(0)
	processCommitment.StorageKeys.Store(0)
	processCommitment.LoadBranch.Store(0)
	processCommitment.LoadAccount.Store(0)
	processCommitment.LoadStorage.Store(0)
	processCommitment.UpdateBranch.Store(0)
	processCommitment.Unfolds.Store(0)
	processCommitment.TotalUnfoldingTime = 0
	processCommitment.Folds.Store(0)
	processCommitment.TotalFoldingTime = 0
	processCommitment.TotalProcessingTime = 0
}

func (processCommitment *ProcessCommitment) Headers() []string {
	return []string{
		"updates",
		"address keys",
		"storage keys",
		"loading branch",
		"loading account",
		"loading storage",
		"updating branch",
		"total unfolds",
		"total unfolding time (ms)",
		"total folds",
		"total folding time (ms)",
		"total processing time (ms)",
	}
}

func (processCommitment *ProcessCommitment) Values() [][]string {
	return [][]string{
		[]string{
			strconv.FormatUint(processCommitment.Updates.Load(), 10),
			strconv.FormatUint(processCommitment.AddressKeys.Load(), 10),
			strconv.FormatUint(processCommitment.StorageKeys.Load(), 10),
			strconv.FormatUint(processCommitment.LoadBranch.Load(), 10),
			strconv.FormatUint(processCommitment.LoadAccount.Load(), 10),
			strconv.FormatUint(processCommitment.LoadStorage.Load(), 10),
			strconv.FormatUint(processCommitment.UpdateBranch.Load(), 10),
			strconv.FormatUint(processCommitment.Unfolds.Load(), 10),
			strconv.Itoa(int(processCommitment.TotalUnfoldingTime.Milliseconds())),
			strconv.FormatUint(processCommitment.Folds.Load(), 10),
			strconv.Itoa(int(processCommitment.TotalFoldingTime.Milliseconds())),
			strconv.Itoa(int(processCommitment.TotalProcessingTime.Milliseconds())),
		},
	}
}

type AccountStats struct {
	Updates            uint64
	StorageKeys        uint64
	LoadBranch         uint64
	LoadAccount        uint64
	LoadStorage        uint64
	Unfolds            uint64
	TotalUnfoldingTime time.Duration
	Folds              uint64
	TotalFoldingTime   time.Duration
}
type ProcessAcount struct {
	m            sync.Mutex
	AccountStats map[string]*AccountStats
}

func (processAccount *ProcessAcount) Reset() {
	processAccount.m.Lock()
	defer processAccount.m.Unlock()
	processAccount.AccountStats = make(map[string]*AccountStats)
}

func (processAccount *ProcessAcount) Headers() []string {
	return []string{
		"account",
		"updates",
		"storage keys",
		"loading branch",
		"loading account",
		"loading storage",
		"total unfolds",
		"total unfolding time (ms)",
		"total folds",
		"total folding time (ms)",
	}
}

func (processAccount *ProcessAcount) UpdatesInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].Updates++
	}
}

func (processAccount *ProcessAcount) UpdatesStorageInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		if len(plainKey) > 40 {
			processAccount.AccountStats[account].StorageKeys++
		}
	}
}

func (processAccount *ProcessAcount) LoadBranchInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].LoadBranch++
	}
}

func (processAccount *ProcessAcount) LoadAccountInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].LoadAccount++
	}
}

func (processAccount *ProcessAcount) LoadStorageInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].LoadStorage++
	}
}

func (processAccount *ProcessAcount) UnfoldsInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].Unfolds++
	}
}

func (processAccount *ProcessAcount) FoldsInc(plainKey []byte) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].Folds++
	}
}

func (processAccount *ProcessAcount) TotalUnfoldingTimeInc(plainKey []byte, t time.Time) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].TotalUnfoldingTime += time.Since(t)
	}
}

func (processAccount *ProcessAcount) TotalFoldingTimeInc(plainKey []byte, t time.Time) {
	if collectAccountMetrics && plainKey != nil {
		processAccount.m.Lock()
		defer processAccount.m.Unlock()
		account := hex.EncodeToString(plainKey[0:40])
		if _, ok := processAccount.AccountStats[account]; !ok {
			processAccount.AccountStats[account] = &AccountStats{}
		}
		processAccount.AccountStats[account].TotalFoldingTime += time.Since(t)
	}
}

func (processAccount *ProcessAcount) Values() [][]string {
	processAccount.m.Lock()
	defer processAccount.m.Unlock()
	values := make([][]string, len(processAccount.AccountStats), len(processAccount.AccountStats))
	var ind int
	for i, account := range processAccount.AccountStats {
		values[ind] = []string{
			i,
			strconv.FormatUint(account.Updates, 10),
			strconv.FormatUint(account.StorageKeys, 10),
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

func writeMetricsToCSV(metrics Metrics, filePath string) error {
	if filePath == "" {
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
