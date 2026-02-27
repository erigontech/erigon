package stagedsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

func CreateBAL(blockNum uint64, txIO *state.VersionedIO, dataDir string) types.BlockAccessList {
	bal := txIO.AsBlockAccessList()
	writeBALToFile(bal, blockNum, dataDir)
	return bal
}

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

	// Write each account's changes
	for _, account := range bal {
		fmt.Fprintf(file, "Account: %s\n", account.Address.Value().Hex())

		// Storage changes
		if len(account.StorageChanges) > 0 {
			fmt.Fprintf(file, "  Storage Changes (%d):\n", len(account.StorageChanges))
			for _, slotChange := range account.StorageChanges {
				fmt.Fprintf(file, "    Slot: %s\n", slotChange.Slot.Value().Hex())
				for _, change := range slotChange.Changes {
					fmt.Fprintf(file, "      [%d] -> %s\n", change.Index, change.Value.Hex())
				}
			}
		}

		// Storage reads
		if len(account.StorageReads) > 0 {
			fmt.Fprintf(file, "  Storage Reads (%d):\n", len(account.StorageReads))
			for _, read := range account.StorageReads {
				fmt.Fprintf(file, "    %s\n", read.Value().Hex())
			}
		}

		// Balance changes
		if len(account.BalanceChanges) > 0 {
			fmt.Fprintf(file, "  Balance Changes (%d):\n", len(account.BalanceChanges))
			for _, change := range account.BalanceChanges {
				fmt.Fprintf(file, "    [%d] -> %s\n", change.Index, change.Value.String())
			}
		}

		// Nonce changes
		if len(account.NonceChanges) > 0 {
			fmt.Fprintf(file, "  Nonce Changes (%d):\n", len(account.NonceChanges))
			for _, change := range account.NonceChanges {
				fmt.Fprintf(file, "    [%d] -> %d\n", change.Index, change.Value)
			}
		}

		// Code changes
		if len(account.CodeChanges) > 0 {
			fmt.Fprintf(file, "  Code Changes (%d):\n", len(account.CodeChanges))
			for _, change := range account.CodeChanges {
				fmt.Fprintf(file, "    [%d] -> %d bytes\n", change.Index, len(change.Bytecode))
				if len(change.Bytecode) <= 64 {
					fmt.Fprintf(file, "      Bytecode: %x\n", change.Bytecode)
				} else {
					fmt.Fprintf(file, "      Bytecode: %x... (truncated)\n", change.Bytecode[:64])
				}
			}
		}

		// If no changes, indicate that
		if len(account.StorageChanges) == 0 && len(account.StorageReads) == 0 &&
			len(account.BalanceChanges) == 0 && len(account.NonceChanges) == 0 &&
			len(account.CodeChanges) == 0 {
			fmt.Fprintf(file, "  No changes (accessed only)\n")
		}

		fmt.Fprintf(file, "\n")
	}

	//log.Info("BAL written to file", "blockNum", blockNum, "filename", filename, "accounts", len(bal))
}
