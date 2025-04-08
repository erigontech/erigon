package commitment

import (
	"encoding/csv"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	metricsFile    string
	collectMetrics bool
)

type CommitmentMetrics struct {
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

func init() {
	metricsFile = os.Getenv("ERIGON_COMMITMENT_TRACE")
	collectMetrics = os.Getenv("ERIGON_COMMITMENT_TRACE") != ""
}

func writeMetricsToCSV(commitmentMetrics *CommitmentMetrics) error {
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
		header := []string{
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
		if err := writer.Write(header); err != nil {
			return err
		}
	}

	// Write the actual data
	record := []string{
		strconv.FormatUint(commitmentMetrics.Updates.Load(), 10),
		strconv.FormatUint(commitmentMetrics.AddressKeys.Load(), 10),
		strconv.FormatUint(commitmentMetrics.StorageKeys.Load(), 10),
		strconv.FormatUint(commitmentMetrics.LoadBranch.Load(), 10),
		strconv.FormatUint(commitmentMetrics.LoadAccount.Load(), 10),
		strconv.FormatUint(commitmentMetrics.LoadStorage.Load(), 10),
		strconv.FormatUint(commitmentMetrics.UpdateBranch.Load(), 10),
		strconv.FormatUint(commitmentMetrics.Unfolds.Load(), 10),
		strconv.Itoa(int(commitmentMetrics.TotalUnfoldingTime.Milliseconds())),
		strconv.FormatUint(commitmentMetrics.Folds.Load(), 10),
		strconv.Itoa(int(commitmentMetrics.TotalFoldingTime.Milliseconds())),
		strconv.Itoa(int(commitmentMetrics.TotalProcessingTime.Milliseconds())),
	}
	return writer.Write(record)
}
