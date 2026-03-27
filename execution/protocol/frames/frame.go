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

// Package frames provides a generic frame execution pipeline used by both
// RIP-7560 (account abstraction) and EIP-8141 (frame transactions).
//
// A "frame" is a single sub-call executed within a transaction, with its own
// sender, target, calldata, and gas budget. Frames share EVM and GasPool state
// across the sequence.
//
// Migration phases:
//
//	Phase 4 (this package): Frame pipeline engine. Refactor RIP-7560 to use it.
//	Phase 5: Add EIP-8141 FrameTransaction type; register FrameHandler.
//	Phase 6: APPROVE opcode for VERIFY frames.
package frames

import (
	"github.com/erigontech/erigon/execution/protocol"
	evmtypes "github.com/erigontech/erigon/execution/vm/evmtypes"
)

// Message is a type alias for the protocol.Message interface so that callers
// of this package do not need to import execution/protocol directly.
type Message = protocol.Message

// FrameType identifies the role of a frame within a transaction.
type FrameType uint8

const (
	// EIP-8141 frame types (Phase 5+).
	FrameVerify  FrameType = 0 // Custom signature validation (APPROVE opcode)
	FrameSender  FrameType = 1 // Determine effective sender address
	FrameDefault FrameType = 2 // Normal EVM execution from determined sender

	// RIP-7560 frame types.
	FrameDeployer        FrameType = 10 // Deploy account contract if needed
	FrameValidation      FrameType = 11 // Validate account accepts the transaction
	FramePaymaster       FrameType = 12 // Validate paymaster accepts payment responsibility
	FrameExecution       FrameType = 13 // Execute the actual user operation
	FramePaymasterPostOp FrameType = 14 // Paymaster post-operation cleanup
)

// FrameResult captures the outcome of executing a single frame.
type FrameResult struct {
	Type       FrameType
	GasUsed    uint64
	ReturnData []byte
	Failed     bool
	Err        error
}

// FrameStep describes one step in a frame pipeline.
//
// Steps are executed in order by Pipeline.Execute. Each step's Build receives
// the results of all previously executed steps, allowing gas limits and message
// parameters to depend on actual prior usage.
type FrameStep struct {
	// Type identifies the frame role (for reporting and lookup by callers).
	Type FrameType

	// Build constructs the protocol.Message to execute.
	// Returning (nil, nil) skips the frame — use this for optional frames
	// (e.g. deployer frame when no deployer is present).
	// prior contains results of all frames executed so far in this pipeline run.
	Build func(prior []FrameResult) (Message, error)

	// Validate runs post-execution checks after a successful ApplyFrame call.
	// Returning a non-nil error aborts the pipeline.
	// May be nil when no post-execution check is needed.
	Validate func(result *evmtypes.ExecutionResult) error
}

// GasUsedByType returns the GasUsed of the first result with the given FrameType.
// Returns 0 if no such frame was executed.
func GasUsedByType(results []FrameResult, ft FrameType) uint64 {
	for _, r := range results {
		if r.Type == ft {
			return r.GasUsed
		}
	}
	return 0
}

// FailedByType returns whether the first result with the given FrameType failed.
// Returns false if no such frame was executed.
func FailedByType(results []FrameResult, ft FrameType) bool {
	for _, r := range results {
		if r.Type == ft {
			return r.Failed
		}
	}
	return false
}
