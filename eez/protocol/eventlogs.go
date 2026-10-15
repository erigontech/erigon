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
package protocol

import (
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/types"
)

func eventLog(address common.Address, ev *abi.Event, topics []common.Hash, values ...any) (*types.Log, error) {
	data, err := ev.Inputs.NonIndexed().Pack(values...)
	if err != nil {
		return nil, err
	}
	return &types.Log{Address: address, Topics: append([]common.Hash{ev.ID}, topics...), Data: data}, nil
}

func uint64Topic(v uint64) common.Hash { return common.BigToHash(new(big.Int).SetUint64(v)) }

// BatchPostedLog is the BatchPosted log the registry emits.
func BatchPostedLog(registry common.Address, sharedPublicInput common.Hash, rollupIDs []uint64) (*types.Log, error) {
	return eventLog(registry, &batchPostedEvent, nil, sharedPublicInput, rollupIDs)
}

// L2ExecutionPerformedLog is the L2ExecutionPerformed log the registry emits.
func L2ExecutionPerformedLog(registry common.Address, rollupID uint64, newRoot common.Hash, etherBalance *big.Int) (*types.Log, error) {
	return eventLog(registry, &l2ExecutionPerformedEvent, []common.Hash{uint64Topic(rollupID)}, newRoot, etherBalance)
}

// ExecutionConsumedLog is the ExecutionConsumed log the registry emits.
func ExecutionConsumedLog(registry common.Address, callHash common.Hash, rollupID uint64, entryQueueIndex *big.Int) (*types.Log, error) {
	return eventLog(registry, &executionConsumedEvent, []common.Hash{callHash, uint64Topic(rollupID), common.BigToHash(entryQueueIndex)})
}

// L2TxSkippedLog is the L2TxSkipped log the registry emits.
func L2TxSkippedLog(registry common.Address, entryIndex *big.Int, revertData []byte) (*types.Log, error) {
	return eventLog(registry, &l2TxSkippedEvent, []common.Hash{common.BigToHash(entryIndex)}, revertData)
}

// L2CrossChainCallExecutedLog is the CrossChainCallExecuted log EEZL2 emits.
func L2CrossChainCallExecutedLog(eezl2 common.Address, e *L2CrossChainCallExecuted) (*types.Log, error) {
	return eventLog(eezl2, &l2CrossChainCallExecutedEvent,
		[]common.Hash{e.CrossChainCallHash, common.BytesToHash(e.Proxy[:])}, e.SourceAddress, e.CallData, e.Value, e.CallGas)
}
