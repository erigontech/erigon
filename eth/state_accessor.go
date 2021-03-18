// Copyright 2021 The go-ethereum Authors
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

package eth

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
)

// stateAtBlock retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func (eth *Ethereum) stateAtBlock(block *types.Block, reexec uint64) (statedb *state.IntraBlockState, release func(), err error) {
	return nil, nil, nil
}

// statesInRange retrieves a batch of state databases associated with the specific
// block ranges. If no state is locally available for the given range, a number of
// blocks are attempted to be reexecuted to generate the ancestor state.
func (eth *Ethereum) statesInRange(fromBlock, toBlock *types.Block, reexec uint64) (states []*state.IntraBlockState, release func(), err error) {
	return nil, nil, nil
}

// stateAtTransaction returns the execution environment of a certain transaction.
func (eth *Ethereum) stateAtTransaction(block *types.Block, txIndex int, reexec uint64) (core.Message, vm.BlockContext, *state.IntraBlockState, func(), error) {
	return nil, vm.BlockContext{}, nil, nil, nil

}
