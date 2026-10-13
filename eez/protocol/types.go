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

// Package protocol implements the EEZ core protocol at eez-core-protocol
// 855fe0602484750861b4c31502a1a91355d33ed6: the contract ABI types and the
// encodings and hashes a follower recomputes.
package protocol

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
)

// Field order and tags mirror IEEZ.sol and IEEZL2.sol: the ABI decoder copies
// tuples into these structs by position, and the encoder maps fields by tag.

// RollupUpdate is one rollup's state transition in an L1 execution entry.
type RollupUpdate struct {
	RollupID    uint64      `abi:"rollupId"`
	EtherDelta  *big.Int    `abi:"etherDelta"` // int192
	CurrentRoot common.Hash `abi:"currentRoot"`
	NewRoot     common.Hash `abi:"newRoot"`
}

// ExpectedRootPerRollup asserts a rollup's live state root.
type ExpectedRootPerRollup struct {
	RollupID uint64      `abi:"rollupId"`
	Root     common.Hash `abi:"root"`
}

// CrossChainCall is a call executed on the destination chain: L2ToL1Call in
// IEEZ.sol and CrossChainCall in IEEZL2.sol.
type CrossChainCall struct {
	RevertNextNCalls uint16         `abi:"revertNextNCalls"`
	IsStatic         bool           `abi:"isStatic"`
	Gas              uint64         `abi:"gas"`
	SourceAddress    common.Address `abi:"sourceAddress"`
	SourceRollupID   uint64         `abi:"sourceRollupId"`
	TargetAddress    common.Address `abi:"targetAddress"`
	Value            *big.Int       `abi:"value"` // uint256
	Data             []byte         `abi:"data"`
}

// ExpectedCall is the precomputed result of a reentrant call:
// ExpectedL1ToL2Call in IEEZ.sol and ExpectedOutgoingCrossChainCall in
// IEEZL2.sol.
type ExpectedCall struct {
	// Hash is expectedL1toL2Hash on L1 and expectedOutgoingHash on L2.
	Hash common.Hash `abi:"hash"`
	// Calls is l2ToL1Calls on L1 and incomingCalls on L2.
	Calls                       []CrossChainCall `abi:"calls"`
	RevertedOrStaticRollingHash common.Hash      `abi:"revertedOrStaticRollingHash"`
	Success                     bool             `abi:"success"`
	ReturnData                  []byte           `abi:"returnData"`
}

// ExecutionEntry is a mutable L1 execution entry.
type ExecutionEntry struct {
	RollupUpdates       []RollupUpdate   `abi:"rollupUpdates"`
	ProxyEntryHash      common.Hash      `abi:"proxyEntryHash"`
	L2ToL1Calls         []CrossChainCall `abi:"l2ToL1Calls"`
	ExpectedL1ToL2Calls []ExpectedCall   `abi:"expectedL1ToL2Calls"`
	RollingHash         common.Hash      `abi:"rollingHash"`
	DestinationRollupID uint64           `abi:"destinationRollupId"`
	Success             bool             `abi:"success"`
	ReturnData          []byte           `abi:"returnData"`
}

// StaticExecutionEntry is a read-only L1 execution entry.
type StaticExecutionEntry struct {
	ExpectedRoots       []ExpectedRootPerRollup `abi:"expectedRoots"`
	ProxyEntryHash      common.Hash             `abi:"proxyEntryHash"`
	L2ToL1Calls         []CrossChainCall        `abi:"l2ToL1Calls"`
	RollingHash         common.Hash             `abi:"rollingHash"`
	DestinationRollupID uint64                  `abi:"destinationRollupId"`
	Success             bool                    `abi:"success"`
	ReturnData          []byte                  `abi:"returnData"`
}

// RollupIDWithProofSystems is a rollup taking part in a batch and the indexes
// into ProofSystemBatch.ProofSystems it is verified against.
type RollupIDWithProofSystems struct {
	RollupID           uint64   `abi:"rollupId"`
	ProofSystemIndexes []uint64 `abi:"proofSystemIndexes"`
}

// ProofSystemBatch is the argument of postAndVerifyBatch,
// ProofSystemBatchPerVerificationEntries in IEEZ.sol.
type ProofSystemBatch struct {
	ExpectedRootPerRollup      []ExpectedRootPerRollup    `abi:"expectedRootPerRollup"`
	Entries                    []ExecutionEntry           `abi:"entries"`
	StaticEntries              []StaticExecutionEntry     `abi:"staticEntries"`
	ImmediateEntryCount        *big.Int                   `abi:"immediateEntryCount"`       // uint256
	ImmediateStaticEntryCount  *big.Int                   `abi:"immediateStaticEntryCount"` // uint256
	ProofSystems               []common.Address           `abi:"proofSystems"`
	RollupIDsWithProofSystems  []RollupIDWithProofSystems `abi:"rollupIdsWithProofSystems"`
	BlobIndices                []*big.Int                 `abi:"blobIndices"` // uint256[]
	CallData                   []byte                     `abi:"callData"`
	Proofs                     [][]byte                   `abi:"proofs"`
	BlockNumber                uint64                     `abi:"blockNumber"`
	BindMsgSenderInPublicInput bool                       `abi:"bindMsgSenderInPublicInput"`
}

// L2ExecutionEntry is a mutable entry of the EEZL2 execution table,
// ExecutionEntry in IEEZL2.sol.
type L2ExecutionEntry struct {
	ProxyEntryHash        common.Hash      `abi:"proxyEntryHash"`
	IncomingCalls         []CrossChainCall `abi:"incomingCalls"`
	ExpectedOutgoingCalls []ExpectedCall   `abi:"expectedOutgoingCalls"`
	RollingHash           common.Hash      `abi:"rollingHash"`
	Success               bool             `abi:"success"`
	ReturnData            []byte           `abi:"returnData"`
}

// L2StaticExecutionEntry is a read-only entry of the EEZL2 execution table,
// StaticExecutionEntryL2 in IEEZL2.sol.
type L2StaticExecutionEntry struct {
	ExpectedEntryIndex *big.Int         `abi:"expectedEntryIndex"` // uint256
	ProxyEntryHash     common.Hash      `abi:"proxyEntryHash"`
	IncomingCalls      []CrossChainCall `abi:"incomingCalls"`
	RollingHash        common.Hash      `abi:"rollingHash"`
	Success            bool             `abi:"success"`
	ReturnData         []byte           `abi:"returnData"`
}

var (
	// ErrNilInteger reports a nil *big.Int in a value to encode.
	ErrNilInteger = errors.New("nil integer")
	// ErrIntegerRange reports an integer outside its Solidity type's range.
	ErrIntegerRange = errors.New("integer out of range")

	minInt192 = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 191))
	maxInt192 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 191), big.NewInt(1))
)

func checkUint256(v *big.Int) error {
	switch {
	case v == nil:
		return ErrNilInteger
	case v.Sign() < 0 || v.BitLen() > 256:
		return ErrIntegerRange
	}
	return nil
}

func checkInt192(v *big.Int) error {
	switch {
	case v == nil:
		return ErrNilInteger
	case v.Cmp(minInt192) < 0 || v.Cmp(maxInt192) > 0:
		return ErrIntegerRange
	}
	return nil
}

// The validate methods reject what the ABI encoder would panic on or encode
// modulo 2^256: nil and out-of-range big integers.

func validateCalls(field string, calls []CrossChainCall) error {
	for i := range calls {
		if err := checkUint256(calls[i].Value); err != nil {
			return fmt.Errorf("%s[%d].value: %w", field, i, err)
		}
	}
	return nil
}

func validateExpectedCalls(field string, calls []ExpectedCall) error {
	for i := range calls {
		if err := validateCalls(fmt.Sprintf("%s[%d].calls", field, i), calls[i].Calls); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExecutionEntry) validate() error {
	for i := range e.RollupUpdates {
		if err := checkInt192(e.RollupUpdates[i].EtherDelta); err != nil {
			return fmt.Errorf("rollupUpdates[%d].etherDelta: %w", i, err)
		}
	}
	if err := validateCalls("l2ToL1Calls", e.L2ToL1Calls); err != nil {
		return err
	}
	return validateExpectedCalls("expectedL1ToL2Calls", e.ExpectedL1ToL2Calls)
}

func (e *StaticExecutionEntry) validate() error {
	return validateCalls("l2ToL1Calls", e.L2ToL1Calls)
}
