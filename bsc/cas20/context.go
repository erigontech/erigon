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

package cas20

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// PrecompileContext is the execution environment of one CAS20 call.
type PrecompileContext struct {
	evm *vm.EVM

	StateDB StateDB

	// Self is the callee address and the storage root of its state.
	Self common.Address

	// Caller is msg.sender for this frame.
	Caller common.Address

	// ReadOnly is true inside a STATICCALL or any read-only ancestor frame.
	ReadOnly bool

	// DirectCall is false for CALLCODE and DELEGATECALL, which would make Self
	// something other than the genuine callee.
	DirectCall bool

	// Value is the wei attached to the frame; nil means zero.
	Value *uint256.Int

	gas *mdgas.MdGas

	// adminRenounced is per-frame, not stored: outside the bootstrap window the zero
	// admin count freezes role mutation on its own (BEP-702 3.4).
	adminRenounced bool

	// frame is shared with every context spawned in this EVM frame, so a child's
	// execution failure is not lost. Lazily allocated.
	frame *frameAccounting
}

type frameAccounting struct {
	outOfGas bool

	// writeProtected outranks outOfGas at the exit: the frame had no business
	// writing at all, whatever it could afford.
	writeProtected bool
}

func newPrecompileContext(env *vm.PrecompileEnv) *PrecompileContext {
	return &PrecompileContext{
		evm:        env.EVM,
		StateDB:    newIBSState(env.EVM.IntraBlockState()),
		Self:       env.Self.Value(),
		Caller:     env.Caller.Value(),
		ReadOnly:   env.ReadOnly,
		DirectCall: env.DirectCall,
		Value:      &env.Value,
		gas:        env.Gas,
	}
}

func (ctx *PrecompileContext) tracer() *tracing.Hooks {
	if ctx.evm == nil {
		return nil
	}
	return ctx.evm.Config().Tracer
}

// UseGas exhausts the budget when the charge cannot be covered, as the EVM does.
func (ctx *PrecompileContext) UseGas(cost uint64) bool {
	prior := *ctx.gas
	if ctx.gas.Execution < cost {
		ctx.gas.Execution = 0
		return false
	}
	ctx.gas.Execution -= cost
	if t := ctx.tracer(); t.HasGasChangeHook() {
		t.EmitGasChange(prior, *ctx.gas, tracing.GasChangeCallPrecompiledContract)
	}
	return true
}

// ReadOnly is carried across: dropping it would let initCalls write during a STATICCALL.
func (ctx *PrecompileContext) spawnBootstrap(self, caller common.Address) *PrecompileContext {
	return &PrecompileContext{
		evm:        ctx.evm,
		StateDB:    ctx.StateDB,
		Self:       self,
		Caller:     caller,
		DirectCall: true,
		ReadOnly:   ctx.ReadOnly,
		gas:        ctx.gas,
		frame:      ctx.frameGas(),
	}
}

func (ctx *PrecompileContext) frameGas() *frameAccounting {
	if ctx.frame == nil {
		ctx.frame = new(frameAccounting)
	}
	return ctx.frame
}

func (ctx *PrecompileContext) markOutOfGas() { ctx.frameGas().outOfGas = true }

// chargeGas is where every CAS20 charge arrives. False means stop before the
// operation the charge pays for, as the interpreter checks gas before an opcode.
func (ctx *PrecompileContext) chargeGas(cost uint64) bool {
	if ctx.OutOfGas() {
		return false
	}
	if !ctx.UseGas(cost) {
		ctx.markOutOfGas()
		return false
	}
	return true
}

func (ctx *PrecompileContext) OutOfGas() bool { return ctx.frame != nil && ctx.frame.outOfGas }

func (ctx *PrecompileContext) gasLeft() uint64 { return ctx.gas.Execution }

func (ctx *PrecompileContext) BlockTime() uint64 { return ctx.evm.Context.Time }

func (ctx *PrecompileContext) ChainID() *uint256.Int {
	return new(uint256.Int).Set(ctx.evm.ChainConfig().ChainID)
}

// The handlers each check ReadOnly first; this is what makes a missing check fail
// closed instead of writing inside a STATICCALL.
func (ctx *PrecompileContext) markWriteProtected() { ctx.frameGas().writeProtected = true }

func (ctx *PrecompileContext) writeProtectionViolated() bool {
	return ctx.frame != nil && ctx.frame.writeProtected
}

// AddLog's result must be honoured: an Approval log fits inside the 2,300 gas the
// EIP-2200 sentry leaves behind, so a refused write could still emit its event.
func (ctx *PrecompileContext) AddLog(topics []common.Hash, data []byte) bool {
	if ctx.ReadOnly {
		ctx.markWriteProtected()
		return false
	}
	if !ctx.chargeLog(len(topics), len(data)) {
		return false
	}
	ctx.StateDB.AddLog(&types.Log{
		Address: ctx.Self,
		Topics:  topics,
		Data:    data,
	})
	return true
}
