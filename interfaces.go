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

// Package ethereum defines types for interacting with Ethereum.
package ethereum

import (
	"errors"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// NotFound is returned by API methods if the requested item does not exist.
var NotFound = errors.New("not found")

// CallMsg contains parameters for contract calls.
type CallMsg struct {
	From             common.Address  // the sender of the 'transaction'
	To               *common.Address // the destination contract (nil for contract creation)
	Gas              uint64          // if 0, the call executes with near-infinite gas
	MaxFeePerBlobGas *uint256.Int    // EIP-4844 max_fee_per_blob_gas
	GasPrice         *uint256.Int    // wei <-> gas exchange ratio
	Value            *uint256.Int    // amount of wei sent along with the call
	Data             []byte          // input data, usually an ABI-encoded contract method invocation

	FeeCap         *uint256.Int          // EIP-1559 max_fee_per_gas
	TipCap         *uint256.Int          // EIP-1559 max_priority_fee_per_gas
	AccessList     types.AccessList      // EIP-2930 access list
	BlobHashes     []common.Hash         // EIP-4844 versioned blob hashes
	Authorizations []types.Authorization // EIP-3074 authorizations
}

// FilterQuery contains options for contract log filtering.
type FilterQuery struct {
	BlockHash *common.Hash     // used by eth_getLogs, return logs only from block with this hash
	FromBlock *big.Int         // beginning of the queried range, nil means genesis block
	ToBlock   *big.Int         // end of the range, nil means latest block
	Addresses []common.Address // restricts matches to events created by specific contracts

	// The Topic list restricts matches to particular event topics. Each event has a list
	// of topics. Topics matches a prefix of that list. An empty element slice matches any
	// topic. Non-empty elements represent an alternative that matches any of the
	// contained topics.
	//
	// Examples:
	// {} or nil          matches any topic list
	// {{A}}              matches topic A in first position
	// {{}, {B}}          matches any topic in first position AND B in second position
	// {{A}, {B}}         matches topic A in first position AND B in second position
	// {{A, B}, {C, D}}   matches topic (A OR B) in first position AND (C OR D) in second position
	Topics [][]common.Hash
}
