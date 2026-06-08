package stagedsync

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

func CreateBAL(blockNum uint64, txIO *state.VersionedIO, dataDir string, logger log.Logger) types.BlockAccessList {
	bal := txIO.AsBlockAccessList()
	if dbg.TraceBlockAccessLists {
		writeBALToFile(bal, dataDir, fmt.Sprintf("computed_bal_%d.txt", blockNum), logger)
	}
	return bal
}

// writeBALToFile dumps the Block Access List to <dataDir>/bal/<name> for debugging/analysis.
func writeBALToFile(bal types.BlockAccessList, dataDir string, name string, logger log.Logger) {
	if dataDir == "" {
		return
	}
	balDir := filepath.Join(dataDir, "bal")
	if err := os.MkdirAll(balDir, 0o755); err != nil {
		logger.Warn("failed to create BAL debug directory", "dir", balDir, "err", err)
		return
	}
	path := filepath.Join(balDir, name)
	if err := os.WriteFile(path, []byte(bal.DebugString()), 0o644); err != nil {
		logger.Warn("failed to write BAL debug file", "path", path, "err", err)
	}
}

func ProcessBAL(tx kv.TemporalRwTx, h *types.Header, vio *state.VersionedIO, isEIP7928 bool, experimental bool, dataDir string, logger log.Logger) error {
	if !isEIP7928 && !experimental {
		return nil
	}
	if h == nil {
		return nil
	}
	blockNum := h.Number.Uint64()
	blockHash := h.Hash()
	computedBlockBal := CreateBAL(blockNum, vio, dataDir, logger)
	err := computedBlockBal.Validate()
	if err != nil {
		return fmt.Errorf("block %d: invalid computed block access list: %w", blockNum, err)
	}
	if !isEIP7928 {
		return nil
	}
	// EIP-7928 size bound is only consensus-binding once Amsterdam activates.
	if err := computedBlockBal.ValidateMaxItems(h.GasLimit); err != nil {
		return fmt.Errorf("%w, block %d: %w", rules.ErrInvalidBlock, blockNum, err)
	}
	if h.BlockAccessListHash == nil {
		return fmt.Errorf("block %d: EIP-7928 active but BlockAccessListHash is nil in header", blockNum)
	}
	blockBalHash := *h.BlockAccessListHash
	blockBalBytes, err := rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)
	if err != nil {
		return fmt.Errorf("block %d: read stored block access list: %w", blockNum, err)
	}
	// A stored BAL sidecar may be absent — eth/71 backfill is best-effort and
	// never blocks stage progress — so cross-check it only when present.
	var blockBal types.BlockAccessList
	if blockBalBytes != nil {
		blockBal, err = types.DecodeBlockAccessListBytes(blockBalBytes)
		if err != nil {
			return fmt.Errorf("block %d: read stored block access list: %w", blockNum, err)
		}
		if err = blockBal.Validate(); err != nil {
			return fmt.Errorf("block %d: db block access list is invalid: %w", blockNum, err)
		}
		if err = blockBal.ValidateMaxItems(h.GasLimit); err != nil {
			return fmt.Errorf("block %d: stored block access list exceeds max items: %w", blockNum, err)
		}
		if blockBalHash != blockBal.Hash() {
			reportBALMismatch(blockNum, blockHash, blockBal, blockBalHash, computedBlockBal, dataDir, logger)
			return fmt.Errorf("block %d: invalid block access list, hash mismatch: got %s expected %s", blockNum, blockBal.Hash(), blockBalHash)
		}
	}
	// Always validate computed BAL against header. The BalancePath cross-check
	// in VersionMap.validateRead ensures deterministic parallel execution even
	// without a stored BAL body (HasBAL=false), so the computed BAL is accurate.
	computedBlockBalHash := computedBlockBal.Hash()
	if blockBalHash != computedBlockBalHash {
		reportBALMismatch(blockNum, blockHash, blockBal, blockBalHash, computedBlockBal, dataDir, logger)
		return fmt.Errorf("%w, block=%d (hash=%s): block access list mismatch: got %s expected %s",
			rules.ErrInvalidBlock, blockNum, blockHash, computedBlockBalHash, blockBalHash)
	}
	return nil
}

// reportBALMismatch logs a BAL hash mismatch and dumps the block's BAL and the
// computed one under <dataDir>/bal for offline diffing.
func reportBALMismatch(blockNum uint64, blockHash common.Hash, blockBal types.BlockAccessList, blockBalHash common.Hash, computedBlockBal types.BlockAccessList, dataDir string, logger log.Logger) {
	logger.Error("BAL mismatch", "blockNum", blockNum, "blockHash", blockHash, "blockBalHash", blockBalHash, "computedBlockBalHash", computedBlockBal.Hash())
	writeBALToFile(computedBlockBal, dataDir, fmt.Sprintf("computed_bal_%d.txt", blockNum), logger)
	if blockBal != nil {
		writeBALToFile(blockBal, dataDir, fmt.Sprintf("block_bal_%d.txt", blockNum), logger)
	}
}
