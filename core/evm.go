// Copyright 2016 The go-ethereum Authors
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
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
)

// NewEVMBlockContext creates a new context for use in the EVM.
func NewEVMBlockContext(header *types.Header, blockHashFunc func(n uint64) common.Hash, engine consensus.Engine, author *common.Address) vm.BlockContext {
	// If we don't have an explicit author (i.e. not mining), extract from the header
	var beneficiary common.Address
	if author == nil {
		beneficiary, _ = engine.Author(header) // Ignore error, we're past header validation
	} else {
		beneficiary = *author
	}
	var baseFee uint256.Int
	if header.Eip1559 {
		overflow := baseFee.SetFromBig(header.BaseFee)
		if overflow {
			panic(fmt.Errorf("header.BaseFee higher than 2^256-1"))
		}
	}

	var prevRandDao *common.Hash
	if header.Difficulty.Cmp(serenity.SerenityDifficulty) == 0 {
		// EIP-4399. We use SerenityDifficulty (i.e. 0) as a telltale of Proof-of-Stake blocks.
		prevRandDao = &header.MixDigest
	}

	var transferFunc vm.TransferFunc
	if engine != nil && engine.Type() == params.BorConsensus {
		transferFunc = BorTransfer
	} else {
		transferFunc = Transfer
	}

	return vm.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    transferFunc,
		GetHash:     blockHashFunc,
		Coinbase:    beneficiary,
		BlockNumber: header.Number.Uint64(),
		Time:        header.Time,
		Difficulty:  new(big.Int).Set(header.Difficulty),
		BaseFee:     &baseFee,
		GasLimit:    header.GasLimit,
		PrevRanDao:  prevRandDao,
	}
}

// NewEVMTxContext creates a new transaction context for a single transaction.
func NewEVMTxContext(msg Message) vm.TxContext {
	return vm.TxContext{
		Origin:   msg.From(),
		GasPrice: msg.GasPrice().ToBig(),
	}
}

// GetHashFn returns a GetHashFunc which retrieves header hashes by number
func GetHashFn(ref *types.Header, getHeader func(hash common.Hash, number uint64) *types.Header) func(n uint64) common.Hash {
	// Cache will initially contain [refHash.parent],
	// Then fill up with [refHash.p, refHash.pp, refHash.ppp, ...]
	var cache []common.Hash

	return func(n uint64) common.Hash {
		// If there's no hash cache yet, make one
		if len(cache) == 0 {
			cache = append(cache, ref.ParentHash)
		}
		if idx := ref.Number.Uint64() - n - 1; idx < uint64(len(cache)) {
			return cache[idx]
		}
		// No luck in the cache, but we can start iterating from the last element we already know
		lastKnownHash := cache[len(cache)-1]
		lastKnownNumber := ref.Number.Uint64() - uint64(len(cache))

		for {
			header := getHeader(lastKnownHash, lastKnownNumber)
			if header == nil {
				break
			}
			cache = append(cache, header.ParentHash)
			lastKnownHash = header.ParentHash
			lastKnownNumber = header.Number.Uint64() - 1
			if n == lastKnownNumber {
				return lastKnownHash
			}
		}
		return common.Hash{}
	}
}

// CanTransfer checks whether there are enough funds in the address' account to make a transfer.
// This does not take the necessary gas in to account to make the transfer valid.
func CanTransfer(db vm.IntraBlockState, addr common.Address, amount *uint256.Int) bool {
	return !db.GetBalance(addr).Lt(amount)
}

// Transfer subtracts amount from sender and adds amount to recipient using the given Db
func Transfer(db vm.IntraBlockState, sender, recipient common.Address, amount *uint256.Int, bailout bool) {
	if !bailout {
		db.SubBalance(sender, amount)
	}
	db.AddBalance(recipient, amount)
}

// BorTransfer transfer in Bor
func BorTransfer(db vm.IntraBlockState, sender, recipient common.Address, amount *uint256.Int, bailout bool) {
	// get inputs before
	input1 := db.GetBalance(sender).Clone()
	input2 := db.GetBalance(recipient).Clone()

	if !bailout {
		db.SubBalance(sender, amount)
	}
	db.AddBalance(recipient, amount)

	// get outputs after
	output1 := db.GetBalance(sender).Clone()
	output2 := db.GetBalance(recipient).Clone()

	// add transfer log
	AddTransferLog(db, sender, recipient, amount, input1, input2, output1, output2)
}
