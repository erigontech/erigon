package stagedsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol/rules"
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

func ProcessBAL(tx kv.TemporalRwTx, h *types.Header, vio *state.VersionedIO, amsterdam bool, experimental bool, dataDir string) error {
	if !amsterdam && !experimental {
		return nil
	}
	if h == nil {
		return nil
	}
	blockNum := h.Number.Uint64()
	blockHash := h.Hash()
	bal := CreateBAL(blockNum, vio, dataDir)
	err := bal.Validate()
	if err != nil {
		return fmt.Errorf("block %d: invalid computed block access list: %w", blockNum, err)
	}
	if err := bal.ValidateMaxItems(h.GasLimit); err != nil {
		return fmt.Errorf("block %d: %w", blockNum, err)
	}
	log.Debug("bal", "blockNum", blockNum, "hash", bal.Hash())
	if !amsterdam {
		return nil
	}
	if h.BlockAccessListHash == nil {
		return fmt.Errorf("block %d: missing block access list hash", blockNum)
	}
	headerBALHash := *h.BlockAccessListHash
	dbBALBytes, err := rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)
	if err != nil {
		return fmt.Errorf("block %d: read stored block access list: %w", blockNum, err)
	}
	// BAL data may not be stored for blocks downloaded via backward
	// block downloader (p2p sync) since it does not carry BAL sidecars.
	// Remove after eth/71 has been implemented.
	if dbBALBytes != nil {
		dbBAL, err := types.DecodeBlockAccessListBytes(dbBALBytes)
		if err != nil {
			return fmt.Errorf("block %d: read stored block access list: %w", blockNum, err)
		}
		if err = dbBAL.Validate(); err != nil {
			return fmt.Errorf("block %d: db block access list is invalid: %w", blockNum, err)
		}

		if headerBALHash != dbBAL.Hash() {
			log.Info(fmt.Sprintf("bal from block: %s", dbBAL.DebugString()))
			return fmt.Errorf("block %d: invalid block access list, hash mismatch: got %s expected %s", blockNum, dbBAL.Hash(), headerBALHash)
		}
	}
	// Always validate computed BAL against header. The BalancePath cross-check
	// in VersionMap.validateRead ensures deterministic parallel execution even
	// without a stored BAL body (HasBAL=false), so the computed BAL is accurate.
	if headerBALHash != bal.Hash() {
		if dataDir != "" {
			balDir := filepath.Join(dataDir, "bal")
			if err := os.MkdirAll(balDir, 0o755); err != nil {
				log.Warn("failed to create BAL debug directory", "dir", balDir, "err", err)
			} else {
				computedPath := filepath.Join(balDir, fmt.Sprintf("computed_bal_%d.txt", blockNum))
				if err := os.WriteFile(computedPath, []byte(bal.DebugString()), 0o644); err != nil {
					log.Warn("failed to write computed BAL debug file", "path", computedPath, "err", err)
				}
				dbBAL2, err := types.DecodeBlockAccessListBytes(dbBALBytes)
				if err != nil {
					log.Warn("failed to decode stored BAL for debug dump", "err", err)
				} else if dbBAL2 != nil {
					storedPath := filepath.Join(balDir, fmt.Sprintf("stored_bal_%d.txt", blockNum))
					if err := os.WriteFile(storedPath, []byte(dbBAL2.DebugString()), 0o644); err != nil {
						log.Warn("failed to write stored BAL debug file", "path", storedPath, "err", err)
					}
				}
			}
		}
		return fmt.Errorf("%w, block=%d: block access list mismatch: got %s expected %s", rules.ErrInvalidBlock, blockNum, bal.Hash(), headerBALHash)
	}
	return nil
}
