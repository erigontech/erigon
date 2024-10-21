// Copyright 2024 The Erigon Authors
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

package whitelist

import (
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/finality/rawdb"
)

type finality[T rawdb.BlockFinality[T]] struct {
	sync.RWMutex
	db       kv.RwDB
	Hash     common.Hash // Whitelisted Hash, populated by reaching out to heimdall
	Number   uint64      // Number , populated by reaching out to heimdall
	interval uint64      // Interval, until which we can allow importing
	doExist  bool
}

type finalityService interface {
	IsValidChain(currentHeader uint64, chain []*types.Header) bool
	Get() (bool, uint64, common.Hash)
	Process(block uint64, hash common.Hash)
	Purge()
}

// IsValidChain checks the validity of chain by comparing it
// against the local checkpoint entry
func (f *finality[T]) IsValidChain(currentHeader uint64, chain []*types.Header) bool {
	// Return if we've received empty chain
	if len(chain) == 0 {
		return false
	}

	res := isValidChain(currentHeader, chain, f.doExist, f.Number, f.Hash, f.interval)

	return res
}

func (f *finality[T]) Process(block uint64, hash common.Hash) {
	f.doExist = true
	f.Hash = hash
	f.Number = block

	err := rawdb.WriteLastFinality[T](f.db, block, hash)

	if err != nil {
		log.Error("Error in writing whitelist state to db", "err", err)
	}
}

// Get returns the existing whitelisted
// entries of checkpoint of the form (doExist,block number,block hash.)
func (f *finality[T]) Get() (bool, uint64, common.Hash) {
	f.RLock()
	defer f.RUnlock()

	if f.doExist {
		return f.doExist, f.Number, f.Hash
	}

	block, hash, err := rawdb.ReadFinality[T](f.db)
	if err != nil {
		return false, f.Number, f.Hash
	}

	return true, block, hash
}

// Purge purges the whitlisted checkpoint
func (f *finality[T]) Purge() {
	f.Lock()
	defer f.Unlock()

	f.doExist = false
}
