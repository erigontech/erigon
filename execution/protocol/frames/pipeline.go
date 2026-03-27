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

package frames

import (
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/vm"
)

// Pipeline executes a sequence of FrameSteps that share a single EVM instance
// and GasPool. Each step builds its message lazily — it can inspect the results
// of prior steps to calculate gas limits or skip conditionally.
type Pipeline struct {
	evm     *vm.EVM
	gasPool *protocol.GasPool
}

// New creates a Pipeline backed by the given EVM and gas pool.
func New(evm *vm.EVM, gasPool *protocol.GasPool) *Pipeline {
	return &Pipeline{evm: evm, gasPool: gasPool}
}

// Execute runs each step in sequence and returns per-frame results.
//
// For each step:
//  1. Build(prior) is called; if it returns (nil, nil), the step is skipped.
//  2. ApplyFrame executes the returned message against the shared EVM/GasPool.
//  3. If the step has a Validate hook, it is called with the execution result.
//     A non-nil Validate error stops the pipeline.
//
// Errors from Build or Validate abort the pipeline immediately. The results
// slice contains all frames executed up to (and including) the failing frame.
func (p *Pipeline) Execute(steps []FrameStep) ([]FrameResult, error) {
	results := make([]FrameResult, 0, len(steps))
	for _, step := range steps {
		msg, err := step.Build(results)
		if err != nil {
			return results, err
		}
		if msg == nil {
			continue
		}

		applyRes, err := protocol.ApplyFrame(p.evm, msg, p.gasPool)
		fr := FrameResult{
			Type:       step.Type,
			GasUsed:    applyRes.ReceiptGasUsed,
			ReturnData: applyRes.ReturnData,
			Failed:     applyRes.Failed(),
			Err:        applyRes.Err,
		}
		results = append(results, fr)

		if err != nil {
			return results, err
		}
		if step.Validate != nil {
			if err := step.Validate(applyRes); err != nil {
				return results, err
			}
		}
	}
	return results, nil
}
