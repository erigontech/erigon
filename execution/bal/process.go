// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

// Create finishes b, which may hold a prefix of the block's txs or be nil, with the rest of txIO.
func Create(blockNum uint64, b *state.BlockAccessListBuilder, txIO *state.VersionedIO, dataDir string, logger log.Logger) types.BlockAccessList {
	if b == nil {
		b = state.NewBlockAccessListBuilder()
	}
	b.AddRemaining(txIO)
	bal := b.Build()
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

// Process derives the block's access list from the executed VersionedIO and,
// once Amsterdam is active, cross-checks it against the header commitment. It
// returns the computed BAL so callers (e.g. the pre-Amsterdam experimental
// path, where the header carries no BAL hash) can capture it. blockBalBytes is
// the stored BAL sidecar, nil when absent.
func Process(blockBalBytes []byte, h *types.Header, built *state.BlockAccessListBuilder, vio *state.VersionedIO, isEIP7928 bool, experimental bool, dataDir string, logger log.Logger) (types.BlockAccessList, error) {
	if !isEIP7928 && !experimental {
		return nil, nil
	}
	if h == nil {
		return nil, nil
	}
	blockNum := h.Number.Uint64()
	blockHash := h.Hash()
	computedBlockBal := Create(blockNum, built, vio, dataDir, logger)
	err := computedBlockBal.Validate()
	if err != nil {
		return nil, fmt.Errorf("block %d: invalid computed block access list: %w", blockNum, err)
	}
	if !isEIP7928 {
		return computedBlockBal, nil
	}
	// EIP-7928 size bound is only consensus-binding once Amsterdam activates.
	if err := computedBlockBal.ValidateMaxItems(h.GasLimit); err != nil {
		return nil, fmt.Errorf("%w, block %d: %w", rules.ErrInvalidBlock, blockNum, err)
	}
	if h.BlockAccessListHash == nil {
		return nil, fmt.Errorf("block %d: EIP-7928 active but BlockAccessListHash is nil in header", blockNum)
	}
	blockBalHash := *h.BlockAccessListHash
	// A stored BAL sidecar may be absent, so cross-check it only when present.
	// The stored bytes are canonical RLP, so their Keccak is the BAL hash.
	if blockBalBytes != nil {
		if storedHash := crypto.Keccak256Hash(blockBalBytes); storedHash != blockBalHash {
			blockBal, _ := types.DecodeBlockAccessListBytes(blockBalBytes)
			reportBALMismatch(blockNum, blockHash, blockBal, blockBalHash, computedBlockBal, dataDir, logger)
			return nil, fmt.Errorf("block %d: invalid block access list, hash mismatch: got %s expected %s", blockNum, storedHash, blockBalHash)
		}
	}
	// Always validate computed BAL against header. The BalancePath cross-check
	// in VersionMap.validateRead ensures deterministic parallel execution even
	// without a stored BAL body, so the computed BAL is accurate.
	computedBlockBalHash := computedBlockBal.Hash()
	if blockBalHash != computedBlockBalHash {
		var blockBal types.BlockAccessList
		if blockBalBytes != nil {
			blockBal, _ = types.DecodeBlockAccessListBytes(blockBalBytes)
		}
		reportBALMismatch(blockNum, blockHash, blockBal, blockBalHash, computedBlockBal, dataDir, logger)
		return nil, fmt.Errorf("%w, block=%d (hash=%s): block access list mismatch: got %s expected %s",
			rules.ErrInvalidBlock, blockNum, blockHash, computedBlockBalHash, blockBalHash)
	}
	return computedBlockBal, nil
}

// reportBALMismatch logs a BAL hash mismatch and dumps the block's BAL and the
// computed one under <dataDir>/bal for offline diffing.
func reportBALMismatch(blockNum uint64, blockHash common.Hash, blockBal types.BlockAccessList, blockBalHash common.Hash, computedBlockBal types.BlockAccessList, dataDir string, logger log.Logger) {
	logger.Error("BAL mismatch", "blockNum", blockNum, "blockHash", blockHash, "blockBalHash", blockBalHash, "computedBlockBalHash", computedBlockBal.Hash())
	if dbg.Trace {
		fmt.Printf("[BALTRACE] computed BAL block %d:\n%s\n", blockNum, computedBlockBal.DebugString())
		if blockBal != nil {
			fmt.Printf("[BALTRACE] stored/expected BAL block %d:\n%s\n", blockNum, blockBal.DebugString())
		}
	}
	writeBALToFile(computedBlockBal, dataDir, fmt.Sprintf("computed_bal_%d.txt", blockNum), logger)
	if blockBal != nil {
		writeBALToFile(blockBal, dataDir, fmt.Sprintf("block_bal_%d.txt", blockNum), logger)
	}
}
