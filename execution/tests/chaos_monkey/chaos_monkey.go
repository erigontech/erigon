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

package chaos_monkey

import (
	"fmt"
	"sync"

	"math/rand/v2"

	"github.com/erigontech/erigon/execution/protocol/rules"
)

const (
	consensusFailureRate = 300
)

func ThrowRandomConsensusError(IsInitialCycle bool, txIndex int, badBlockHalt bool, txTaskErr error) error {
	if !IsInitialCycle && rand.Int()%consensusFailureRate == 0 && txIndex == 0 && !badBlockHalt {
		return fmt.Errorf("monkey in the datacenter: %w: %v", rules.ErrInvalidBlock, txTaskErr)
	}
	return nil
}

// armedError is a test-armed fault: throw returns nil unless a test armed it.
type armedError struct {
	mu  sync.Mutex
	err error
}

func (a *armedError) arm(err error) (disarm func()) {
	a.mu.Lock()
	a.err = err
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.err = nil
		a.mu.Unlock()
	}
}

func (a *armedError) throw() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

var (
	preExecErr armedError
	workerErr  armedError
)

// ArmPreExecutionError makes ThrowPreExecutionError return err (while chaos is
// enabled) until the returned disarm func runs. Test-only; production never arms it.
func ArmPreExecutionError(err error) (disarm func()) {
	return preExecErr.arm(err)
}

// ThrowPreExecutionError reproduces a failure that hits executeBlocks before it
// dispatches any block (snapshot step misalignment, a missing block, a BAL decode
// error). Returns nil unless a test armed it via ArmPreExecutionError.
func ThrowPreExecutionError() error {
	return preExecErr.throw()
}

// ArmWorkerError makes ThrowWorkerError return err until the returned disarm
// func runs. Test-only; production never arms it.
func ArmWorkerError(err error) (disarm func()) {
	return workerErr.arm(err)
}

// ThrowWorkerError reproduces an OCC worker goroutine dying outside a tx task
// (a panic in the worker loop). Returns nil unless a test armed it via
// ArmWorkerError.
func ThrowWorkerError() error {
	return workerErr.throw()
}
