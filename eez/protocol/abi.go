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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/abi"
)

// Tuple components in IEEZ.sol and IEEZL2.sol order. Names match the struct
// tags in types.go; they do not enter selectors or encodings.
var (
	rollupUpdateABI = []abi.ArgumentMarshaling{
		{Name: "rollupId", Type: "uint64"},
		{Name: "etherDelta", Type: "int192"},
		{Name: "currentRoot", Type: "bytes32"},
		{Name: "newRoot", Type: "bytes32"},
	}
	expectedRootPerRollupABI = []abi.ArgumentMarshaling{
		{Name: "rollupId", Type: "uint64"},
		{Name: "root", Type: "bytes32"},
	}
	crossChainCallABI = []abi.ArgumentMarshaling{
		{Name: "revertNextNCalls", Type: "uint16"},
		{Name: "isStatic", Type: "bool"},
		{Name: "gas", Type: "uint64"},
		{Name: "sourceAddress", Type: "address"},
		{Name: "sourceRollupId", Type: "uint64"},
		{Name: "targetAddress", Type: "address"},
		{Name: "value", Type: "uint256"},
		{Name: "data", Type: "bytes"},
	}
	expectedCallABI = []abi.ArgumentMarshaling{
		{Name: "hash", Type: "bytes32"},
		{Name: "calls", Type: "tuple[]", Components: crossChainCallABI},
		{Name: "revertedOrStaticRollingHash", Type: "bytes32"},
		{Name: "success", Type: "bool"},
		{Name: "returnData", Type: "bytes"},
	}
	executionEntryABI = []abi.ArgumentMarshaling{
		{Name: "rollupUpdates", Type: "tuple[]", Components: rollupUpdateABI},
		{Name: "proxyEntryHash", Type: "bytes32"},
		{Name: "l2ToL1Calls", Type: "tuple[]", Components: crossChainCallABI},
		{Name: "expectedL1ToL2Calls", Type: "tuple[]", Components: expectedCallABI},
		{Name: "rollingHash", Type: "bytes32"},
		{Name: "destinationRollupId", Type: "uint64"},
		{Name: "success", Type: "bool"},
		{Name: "returnData", Type: "bytes"},
	}
	staticExecutionEntryABI = []abi.ArgumentMarshaling{
		{Name: "expectedRoots", Type: "tuple[]", Components: expectedRootPerRollupABI},
		{Name: "proxyEntryHash", Type: "bytes32"},
		{Name: "l2ToL1Calls", Type: "tuple[]", Components: crossChainCallABI},
		{Name: "rollingHash", Type: "bytes32"},
		{Name: "destinationRollupId", Type: "uint64"},
		{Name: "success", Type: "bool"},
		{Name: "returnData", Type: "bytes"},
	}
	rollupIDWithProofSystemsABI = []abi.ArgumentMarshaling{
		{Name: "rollupId", Type: "uint64"},
		{Name: "proofSystemIndexes", Type: "uint64[]"},
	}
	proofSystemBatchABI = []abi.ArgumentMarshaling{
		{Name: "expectedRootPerRollup", Type: "tuple[]", Components: expectedRootPerRollupABI},
		{Name: "entries", Type: "tuple[]", Components: executionEntryABI},
		{Name: "staticEntries", Type: "tuple[]", Components: staticExecutionEntryABI},
		{Name: "immediateEntryCount", Type: "uint256"},
		{Name: "immediateStaticEntryCount", Type: "uint256"},
		{Name: "proofSystems", Type: "address[]"},
		{Name: "rollupIdsWithProofSystems", Type: "tuple[]", Components: rollupIDWithProofSystemsABI},
		{Name: "blobIndices", Type: "uint256[]"},
		{Name: "callData", Type: "bytes"},
		{Name: "proofs", Type: "bytes[]"},
		{Name: "blockNumber", Type: "uint64"},
		{Name: "bindMsgSenderInPublicInput", Type: "bool"},
	}
	l2ExecutionEntryABI = []abi.ArgumentMarshaling{
		{Name: "proxyEntryHash", Type: "bytes32"},
		{Name: "incomingCalls", Type: "tuple[]", Components: crossChainCallABI},
		{Name: "expectedOutgoingCalls", Type: "tuple[]", Components: expectedCallABI},
		{Name: "rollingHash", Type: "bytes32"},
		{Name: "success", Type: "bool"},
		{Name: "returnData", Type: "bytes"},
	}
	l2StaticExecutionEntryABI = []abi.ArgumentMarshaling{
		{Name: "expectedEntryIndex", Type: "uint256"},
		{Name: "proxyEntryHash", Type: "bytes32"},
		{Name: "incomingCalls", Type: "tuple[]", Components: crossChainCallABI},
		{Name: "rollingHash", Type: "bytes32"},
		{Name: "success", Type: "bool"},
		{Name: "returnData", Type: "bytes"},
	}
)

var (
	executionEntryArgs       = abi.Arguments{{Type: mustType("tuple", executionEntryABI)}}
	staticExecutionEntryArgs = abi.Arguments{{Type: mustType("tuple", staticExecutionEntryABI)}}

	// The EEZL2 table entrypoints take the same two arguments.
	l2TableArgs = abi.Arguments{
		{Name: "entries", Type: mustType("tuple[]", l2ExecutionEntryABI)},
		{Name: "staticEntries", Type: mustType("tuple[]", l2StaticExecutionEntryABI)},
	}

	postAndVerifyBatchMethod = abi.NewMethod("postAndVerifyBatch", "postAndVerifyBatch", abi.Function, "nonpayable", false, false,
		abi.Arguments{{Name: "batch", Type: mustType("tuple", proofSystemBatchABI)}}, nil)
	loadExecutionTableMethod = abi.NewMethod("loadExecutionTable", "loadExecutionTable", abi.Function, "payable", false, true,
		l2TableArgs, nil)
	executeIncomingCrossChainCallMethod = abi.NewMethod("executeIncomingCrossChainCall", "executeIncomingCrossChainCall", abi.Function, "payable", false, true,
		l2TableArgs, abi.Arguments{{Name: "result", Type: mustType("bytes", nil)}})
)

// Selectors of the EEZ entrypoints.
var (
	PostAndVerifyBatchSelector            = [4]byte(postAndVerifyBatchMethod.ID)
	LoadExecutionTableSelector            = [4]byte(loadExecutionTableMethod.ID)
	ExecuteIncomingCrossChainCallSelector = [4]byte(executeIncomingCrossChainCallMethod.ID)
)

func mustType(t string, components []abi.ArgumentMarshaling) abi.Type {
	typ, err := abi.NewType(t, "", components)
	if err != nil {
		panic(fmt.Sprintf("eez/protocol: ABI type %s: %v", t, err))
	}
	return typ
}

// EncodeExecutionEntry returns abi.encode(e).
func EncodeExecutionEntry(e *ExecutionEntry) ([]byte, error) {
	if err := e.validate(); err != nil {
		return nil, fmt.Errorf("execution entry: %w", err)
	}
	return executionEntryArgs.Pack(*e)
}

// ExecutionEntryHash returns keccak256(abi.encode(e)), the entry's leaf in
// the batch public input.
func ExecutionEntryHash(e *ExecutionEntry) (common.Hash, error) {
	enc, err := EncodeExecutionEntry(e)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(enc), nil
}

// EncodeStaticExecutionEntry returns abi.encode(e).
func EncodeStaticExecutionEntry(e *StaticExecutionEntry) ([]byte, error) {
	if err := e.validate(); err != nil {
		return nil, fmt.Errorf("static execution entry: %w", err)
	}
	return staticExecutionEntryArgs.Pack(*e)
}

// StaticExecutionEntryHash returns keccak256(abi.encode(e)), the static
// entry's leaf in the batch public input.
func StaticExecutionEntryHash(e *StaticExecutionEntry) (common.Hash, error) {
	enc, err := EncodeStaticExecutionEntry(e)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(enc), nil
}
