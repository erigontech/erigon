package stagedsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
)

// writeBALToFile writes the Block Access List to a text file for debugging/analysis
func writeBALToFile(bal types.BlockAccessList, blockNum uint64, dataDir string) {
	if dataDir == "" {
		return
	}

	balDir := filepath.Join(dataDir, "bal")
	if err := os.MkdirAll(balDir, 0755); err != nil {
		log.Warn("Failed to create BAL directory", "dir", balDir, "error", err)
		return
	}

	filename := filepath.Join(balDir, fmt.Sprintf("bal_block_%d.txt", blockNum))

	file, err := os.Create(filename)
	if err != nil {
		log.Warn("Failed to create BAL file", "blockNum", blockNum, "error", err)
		return
	}
	defer file.Close()

	// Write header information
	fmt.Fprintf(file, "Block Access List for Block %d\n", blockNum)
	fmt.Fprintf(file, "Total Accounts: %d\n\n", len(bal))
	bal.DebugPrint(file)
	fmt.Fprintf(file, "\n")
}
