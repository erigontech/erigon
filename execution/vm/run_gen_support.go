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

//go:generate go run -tags gendispatch ./gen

package vm

import (
	"fmt"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
)

// genDispatchDisabled is a kill switch for the generated dispatch loop,
// forcing every frame through the generic jump-table interpreter.
var genDispatchDisabled = dbg.EnvBool("EVM_NO_GEN_DISPATCH", false)

// runGeneratedFastFn/runGeneratedTracedFn are set by the generated file's
// init. Delegating through nil-checked vars lets the package build before the
// generator has run.
var (
	runGeneratedFastFn   func(evm *EVM, contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error)
	runGeneratedTracedFn func(evm *EVM, contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) ([]byte, mdgas.MdGas, mdgas.MdGasUsage, error)
)

// genTables holds the canonical tables the generated loop's baked literals
// were verified against. Populated in init rather than referenced from Run:
// a table's construction chain reaches Run through the CALL and CREATE op
// funcs, so a static reference from Run back to a table var would be an
// initialization cycle. ExtraEips copies are absent by construction.
var genTables map[*JumpTable]struct{}

func init() {
	genTables = map[*JumpTable]struct{}{
		&frontierInstructionSet:         {},
		&homesteadInstructionSet:        {},
		&tangerineWhistleInstructionSet: {},
		&spuriousDragonInstructionSet:   {},
		&byzantiumInstructionSet:        {},
		&constantinopleInstructionSet:   {},
		&istanbulInstructionSet:         {},
		&berlinInstructionSet:           {},
		&londonInstructionSet:           {},
		&shanghaiInstructionSet:         {},
		&napoliInstructionSet:           {},
		&cancunInstructionSet:           {},
		&pragueInstructionSet:           {},
		&bhilaiInstructionSet:           {},
		&osakaInstructionSet:            {},
		&amsterdamInstructionSet:        {},
	}
}

// traceInstruction prints the dbg.TraceInstructions per-op line.
func traceInstruction(evm *EVM, operation *operation, op OpCode, pc uint64, callGas, cost uint64, callContext *CallContext) {
	var opstr string
	if operation.string != nil {
		opstr = operation.string(pc, callContext)
	} else {
		opstr = op.String()
	}
	fmt.Printf("%d (%d.%d) %5d %5d %s\n", evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), pc, traceGas(op, callGas, cost), opstr)
}

// traceDynamicGasPrint prints the dbg.TraceDynamicGas line.
func traceDynamicGasPrint(evm *EVM, op OpCode, callGas, cost uint64) {
	fmt.Printf("%d (%d.%d) Dynamic Gas: %d (%s)\n", evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), traceGas(op, callGas, cost), op)
}
