package commitment

import (
	"encoding/csv"
	"os"
	"strconv"
)

var (
	metricsFile    string
	collectMetrics bool
)

type CommitmentMetrics struct {
	Updates             int
	AddressKeys         int
	StorageKeys         int
	LoadBranch          int
	LoadAccount         int
	LoadStorage         int
	UpdateBranch        int
	Unfolds             int
	TotalUnfoldingTime  int
	Folds               int
	TotalFoldingTime    int
	TotalProcessingTime int
}

var CurrentCommitmentMetrics CommitmentMetrics

func init() {
	metricsFile = os.Getenv("ERIGON_COMMITMENT_TRACE")
	collectMetrics = os.Getenv("ERIGON_COMMITMENT_TRACE") != ""
}

func writeMetricsToCSV() error {
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
		strconv.Itoa(CurrentCommitmentMetrics.Updates),
		strconv.Itoa(CurrentCommitmentMetrics.AddressKeys),
		strconv.Itoa(CurrentCommitmentMetrics.StorageKeys),
		strconv.Itoa(CurrentCommitmentMetrics.LoadBranch),
		strconv.Itoa(CurrentCommitmentMetrics.LoadAccount),
		strconv.Itoa(CurrentCommitmentMetrics.LoadStorage),
		strconv.Itoa(CurrentCommitmentMetrics.UpdateBranch),
		strconv.Itoa(CurrentCommitmentMetrics.Unfolds),
		strconv.Itoa(CurrentCommitmentMetrics.TotalUnfoldingTime),
		strconv.Itoa(CurrentCommitmentMetrics.Folds),
		strconv.Itoa(CurrentCommitmentMetrics.TotalFoldingTime),
		strconv.Itoa(CurrentCommitmentMetrics.TotalProcessingTime),
	}
	CurrentCommitmentMetrics = CommitmentMetrics{}
	return writer.Write(record)
}
