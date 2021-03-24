// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/params"
)

// BlockValidator is responsible for validating block headers, uncles and
// processed state.
//
// BlockValidator implements Validator.
type BlockValidator struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for validating
}

// NewBlockValidator returns a new block validator which is safe for re-use
func NewBlockValidator(config *params.ChainConfig, blockchain *BlockChain, engine consensus.Engine) *BlockValidator {
	validator := &BlockValidator{
		config: config,
		engine: engine,
		bc:     blockchain,
	}
	return validator
}

// ValidateBody validates the given block's uncles and verifies the block
// header's transaction and uncle roots. The headers are assumed to be already
// validated at this point.
func (v *BlockValidator) ValidateBody(ctx context.Context, block *types.Block) error {
	// Check whether the block is linkable
	_, noHistory := params.GetNoHistoryByBlock(ctx, block.Number())
	if !noHistory && v.bc.GetBlockByHash(block.ParentHash()) == nil {
		return consensus.ErrUnknownAncestor
	}
	if err := v.engine.VerifyUncles(v.bc, block); err != nil {
		return err
	}
	if noHistory {
		return nil
	}
	if hash := types.DeriveSha(block.Transactions()); hash != block.Header().TxHash {
		return nil
	}
	if !v.bc.HasBlockAndState(block.ParentHash(), block.NumberU64()-1) {
		if !v.bc.HasBlock(block.ParentHash(), block.NumberU64()-1) {
			return consensus.ErrUnknownAncestor
		}
		return consensus.ErrPrunedAncestor
	}
	return nil
}

// ValidateReceipts validates block receipts.
func (v *BlockValidator) ValidateReceipts(block *types.Block, receipts types.Receipts) error {
	header := block.Header()
	var errorBuf strings.Builder

	// Validate the received block's bloom with the one derived from the generated receipts.
	// For valid blocks this should always validate to true.
	rbloom := types.CreateBloom(receipts)
	if rbloom != header.Bloom {
		if errorBuf.Len() > 0 {
			errorBuf.WriteString("; ")
		}
		fmt.Fprintf(&errorBuf, "invalid bloom (remote: %x  local: %x)", header.Bloom, rbloom)
	}

	// Tre receipt Trie's root (R = (Tr [[H1, R1], ... [Hn, Rn]]))
	if v.config.IsByzantium(block.Header().Number) {
		receiptSha := types.DeriveSha(receipts)
		if receiptSha != header.ReceiptHash {
			if errorBuf.Len() > 0 {
				errorBuf.WriteString("; ")
			}
			for _, r := range receipts {
				for _, l := range r.Logs {
					fmt.Printf("receipts: %s %x\n", l.Data, l.Data)
				}

			}
			fmt.Fprintf(&errorBuf, "invalid receipt root hash (remote: %x local: %x)", header.ReceiptHash, receiptSha)
		}
	}

	if errorBuf.Len() > 0 {
		return errors.New(errorBuf.String())
	}
	return nil
}

// ValidateGasAndRoot validates the amount of used gas and the state root.
func (v *BlockValidator) ValidateGasAndRoot(block *types.Block, root common.Hash, usedGas uint64, tds *state.TrieDbState) error {
	var errorBuf strings.Builder
	if block.GasUsed() != usedGas {
		fmt.Fprintf(&errorBuf, "invalid gas used (remote: %d local: %d)", block.GasUsed(), usedGas)
	}

	// Validate the state root against the received state root and throw
	// an error if they don't match.
	if block.Header().Root != root {
		if errorBuf.Len() > 0 {
			errorBuf.WriteString("; ")
		}
		fmt.Fprintf(&errorBuf, "[pre-processed] invalid merkle root (remote: %x local: %x)", block.Header().Root, root)
	}

	if errorBuf.Len() > 0 {
		return errors.New(errorBuf.String())
	}
	return nil
}

// CalcGasLimit computes the gas limit of the next block after parent. It aims
// to keep the baseline gas above the provided floor, and increase it towards the
// ceil if the blocks are full. If the ceil is exceeded, it will always decrease
// the gas allowance.
func CalcGasLimit(parentGasUsed, parentGasLimit, gasFloor, gasCeil uint64) uint64 {
	// contrib = (parentGasUsed * 3 / 2) / 1024
	contrib := (parentGasUsed + parentGasUsed/2) / params.GasLimitBoundDivisor

	// decay = parentGasLimit / 1024 -1
	decay := parentGasLimit/params.GasLimitBoundDivisor - 1

	/*
		strategy: gasLimit of block-to-mine is set based on parent's
		gasUsed value.  if parentGasUsed > parentGasLimit * (2/3) then we
		increase it, otherwise lower it (or leave it unchanged if it's right
		at that usage) the amount increased/decreased depends on how far away
		from parentGasLimit * (2/3) parentGasUsed is.
	*/
	limit := parentGasLimit - decay + contrib
	if limit < params.MinGasLimit {
		limit = params.MinGasLimit
	}
	// If we're outside our allowed gas range, we try to hone towards them
	if limit < gasFloor {
		limit = parentGasLimit + decay
		if limit > gasFloor {
			limit = gasFloor
		}
	} else if limit > gasCeil {
		limit = parentGasLimit - decay
		if limit < gasCeil {
			limit = gasCeil
		}
	}
	return limit
}
