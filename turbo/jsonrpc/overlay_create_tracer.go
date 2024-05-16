package jsonrpc

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

type OverlayCreateTracer struct {
	contractAddress libcommon.Address
	isCapturing     bool
	code            []byte
	gasCap          uint64
	err             error
	resultCode      []byte
	evm             *vm.EVM
}

func (ct *OverlayCreateTracer) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: nil,
			OnEnter:   ct.OnEnter,
		},
	}
}

// Top call frame
func (ct *OverlayCreateTracer) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.evm = env
}
func (ct *OverlayCreateTracer) CaptureEnd(output []byte, usedGas uint64, err error) {}

// Rest of the frames
func (ct *OverlayCreateTracer) OnEnter(depth int, typ byte, from libcommon.Address, to libcommon.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.isCapturing {
		return
	}

	if vm.OpCode(typ) == vm.CREATE && to == ct.contractAddress {
		ct.isCapturing = true
		_, _, _, err := ct.evm.OverlayCreate(vm.AccountRef(from), vm.NewCodeAndHash(ct.code), ct.gasCap, value, to, vm.OpCode(typ), true /* incrementNonce */)
		if err != nil {
			ct.err = err
		} else {
			ct.resultCode = ct.evm.IntraBlockState().GetCode(ct.contractAddress)
		}
	}
}
