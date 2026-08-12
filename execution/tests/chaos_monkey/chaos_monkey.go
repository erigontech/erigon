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

func ThrowRandomConsensusError(isInitialCycle bool, txIndex int, badBlockHalt bool, txTaskErr error) error {
	if !isInitialCycle && rand.Int()%consensusFailureRate == 0 && txIndex == 0 && !badBlockHalt {
		return fmt.Errorf("monkey in the datacenter: %w: %v", rules.ErrInvalidBlock, txTaskErr)
	}
	return nil
}

// armedError stores a process-local fault until its disarm function runs.
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
	preExecErr     armedError
	workerErr      armedError
	taskFault      armedError
	execLoopFault  armedError
	applyLoopFault armedError
)

// ArmPreExecutionError makes ThrowPreExecutionError return err until disarm runs.
func ArmPreExecutionError(err error) (disarm func()) {
	return preExecErr.arm(err)
}

// ThrowPreExecutionError returns the armed pre-dispatch failure, if any.
func ThrowPreExecutionError() error {
	return preExecErr.throw()
}

// ArmWorkerError makes ThrowWorkerError return err until disarm runs.
func ArmWorkerError(err error) (disarm func()) {
	return workerErr.arm(err)
}

// ThrowWorkerError returns the armed worker failure, if any.
func ThrowWorkerError() error {
	return workerErr.throw()
}

// ArmExecLoopPanic makes ExecLoopPanic panic with err until disarm runs.
func ArmExecLoopPanic(err error) (disarm func()) {
	return execLoopFault.arm(err)
}

// ExecLoopPanic panics with the armed fault, if any.
func ExecLoopPanic() {
	if err := execLoopFault.throw(); err != nil {
		panic(err)
	}
}

// ArmTaskPanic makes TaskPanic panic with err until disarm runs.
func ArmTaskPanic(err error) (disarm func()) {
	return taskFault.arm(err)
}

// TaskPanic panics with the armed fault, if any.
func TaskPanic() {
	if err := taskFault.throw(); err != nil {
		panic(err)
	}
}

// ArmApplyLoopPanic makes ApplyLoopPanic panic with err until disarm runs.
func ArmApplyLoopPanic(err error) (disarm func()) {
	return applyLoopFault.arm(err)
}

// ApplyLoopPanic panics with the armed fault, if any.
func ApplyLoopPanic() {
	if err := applyLoopFault.throw(); err != nil {
		panic(err)
	}
}
