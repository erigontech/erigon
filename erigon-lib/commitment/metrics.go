package commitment

import (
	"encoding/csv"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func init() {
	metricsFile = os.Getenv("ERIGON_COMMITMENT_TRACE")
	collectMetrics = os.Getenv("ERIGON_COMMITMENT_TRACE") != ""
}

var (
	metricsFile    string
	collectMetrics bool
)

type Metrics interface {
	Reset()
	Headers() []string
	Values() []string
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
	if collectMetrics {
		return time.Now()
	}
	return time.Time{}
}

func (ProcessCommitment *ProcessCommitment) TotalUnfoldingTimeInc(t time.Time) {
	if collectMetrics {
		ProcessCommitment.TotalUnfoldingTime += time.Since(t)
	}
}

func (ProcessCommitment *ProcessCommitment) TotalProcessingTimeInc(t time.Time) {
	if collectMetrics {
		ProcessCommitment.TotalProcessingTime += time.Since(t)
	}
}

func (ProcessCommitment *ProcessCommitment) TotalFoldingTimeInc(t time.Time) {
	if collectMetrics {
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

func (processCommitment *ProcessCommitment) Values() []string {
	return []string{
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
	}
}

func writeMetricsToCSV(metrics Metrics) error {
	if !collectMetrics {
		return nil
	}
	// Open the file in append mode or create if it doesn't exist
	file, err := os.OpenFile(metricsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	return writer.Write(metrics.Values())
}
