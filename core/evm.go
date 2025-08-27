// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package core

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/merge"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/types"
	lru "github.com/hashicorp/golang-lru/v2"
)

// NewEVMBlockContext creates a new context for use in the EVM.
func NewEVMBlockContext(header *types.Header, blockHashFunc func(n uint64) (common.Hash, error),
	engine consensus.EngineReader, author *common.Address, config *chain.Config) evmtypes.BlockContext {
	// If we don't have an explicit author (i.e. not mining), extract from the header
	var beneficiary common.Address
	if author == nil {
		beneficiary, _ = engine.Author(header) // Ignore error, we're past header validation
	} else {
		beneficiary = *author
	}
	var baseFee uint256.Int
	if header.BaseFee != nil {
		overflow := baseFee.SetFromBig(header.BaseFee)
		if overflow {
			panic("header.BaseFee higher than 2^256-1")
		}
	}

	var prevRandDao *common.Hash
	if header.Difficulty != nil && header.Difficulty.Cmp(merge.ProofOfStakeDifficulty) == 0 {
		// EIP-4399. We use ProofOfStakeDifficulty (i.e. 0) as a telltale of Proof-of-Stake blocks.
		prevRandDao = new(common.Hash)
		*prevRandDao = header.MixDigest
	}

	var blobBaseFee *uint256.Int
	if header.ExcessBlobGas != nil {
		var err error
		blobBaseFee, err = misc.GetBlobGasPrice(config, *header.ExcessBlobGas, header.Time)
		if err != nil {
			panic(err)
		}
	}

	var transferFunc evmtypes.TransferFunc
	var postApplyMessageFunc evmtypes.PostApplyMessageFunc
	if engine != nil {
		transferFunc = engine.GetTransferFunc()
		postApplyMessageFunc = engine.GetPostApplyMessageFunc()
	} else {
		transferFunc = consensus.Transfer
		postApplyMessageFunc = nil
	}
	blockContext := evmtypes.BlockContext{
		CanTransfer:      CanTransfer,
		Transfer:         transferFunc,
		GetHash:          blockHashFunc,
		PostApplyMessage: postApplyMessageFunc,
		Coinbase:         beneficiary,
		BlockNumber:      header.Number.Uint64(),
		Time:             header.Time,
		BaseFee:          &baseFee,
		GasLimit:         header.GasLimit,
		PrevRanDao:       prevRandDao,
		BlobBaseFee:      blobBaseFee,
	}
	if header.Difficulty != nil {
		blockContext.Difficulty = new(big.Int).Set(header.Difficulty)
	}
	return blockContext
}

// NewEVMTxContext creates a new transaction context for a single transaction.
func NewEVMTxContext(msg Message) evmtypes.TxContext {
	return evmtypes.TxContext{
		Origin:     msg.From(),
		GasPrice:   msg.GasPrice(),
		BlobHashes: msg.BlobHashes(),
	}
}

// GetHashFn returns a GetHashFunc which retrieves header hashes by number
func GetHashFn(ref *types.Header, getHeader func(hash common.Hash, number uint64) (*types.Header, error)) func(n uint64) (common.Hash, error) {
	refNumber := ref.Number.Uint64() - 1
	refHash := ref.ParentHash
	lastKnownNumber := refNumber
	lastKnownHash := refHash

	// lru.New only returns err on -ve size
	hashLookupCache, _ := lru.New[uint64, common.Hash](8192)
	hashLookupCacheLock := sync.Mutex{}
	hashLookupCache.Add(refNumber, refHash)

	return func(n uint64) (common.Hash, error) {
		hashLookupCacheLock.Lock()
		defer hashLookupCacheLock.Unlock()

		if n == lastKnownNumber {
			//fmt.Println("GH-LN", n, refHash)
			return lastKnownHash, nil
		}

		if n == refNumber {
			//fmt.Println("GH-RF", n, refHash)
			return refHash, nil
		}

		if hash, ok := hashLookupCache.Get(n); ok {
			//fmt.Println("GH-CA", n, hash)
			return hash, nil
		}

		if n > lastKnownNumber {
			if n > refNumber {
				return common.Hash{}, fmt.Errorf("block number out of range: max=%d", refNumber)
			}
			lastKnownNumber = refNumber
			lastKnownHash = refHash
		}

		for {
			for {
				hash, ok := hashLookupCache.Get(lastKnownNumber - 1)

				if !ok {
					break
				}

				lastKnownHash = hash
				lastKnownNumber = lastKnownNumber - 1
				if n == lastKnownNumber {
					//fmt.Println("GH-CA1", lastKnownNumber, lastKnownHash)
					return lastKnownHash, nil
				}
			}

			header, err := func() (*types.Header, error) {
				hash, num := lastKnownHash, lastKnownNumber
				hashLookupCacheLock.Unlock()
				defer hashLookupCacheLock.Lock()
				return getHeader(hash, num)
			}()

			if err != nil {
				return common.Hash{}, err
			}
			if header == nil {
				break
			}
			lastKnownHash = header.ParentHash
			lastKnownNumber = header.Number.Uint64() - 1
			hashLookupCache.Add(lastKnownNumber, lastKnownHash)

			if n == lastKnownNumber {
				//fmt.Println("GH-DB", lastKnownNumber, lastKnownHash)
				return lastKnownHash, nil
			}
		}

		return common.Hash{}, nil
	}
}

// CanTransfer checks whether there are enough funds in the address' account to make a transfer.
// This does not take the necessary gas in to account to make the transfer valid.
func CanTransfer(db evmtypes.IntraBlockState, addr common.Address, amount *uint256.Int) (bool, error) {
	balance, err := db.GetBalance(addr)
	if err != nil {
		return false, err
	}
	return !balance.Lt(amount), nil
}
