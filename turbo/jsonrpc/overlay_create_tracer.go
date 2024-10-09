package jsonrpc

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/vm"
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

// Transaction level
func (ct *OverlayCreateTracer) CaptureTxStart(gasLimit uint64) {}
func (ct *OverlayCreateTracer) CaptureTxEnd(restGas uint64)    {}

// Top call frame
func (ct *OverlayCreateTracer) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ct.evm = env
}
func (ct *OverlayCreateTracer) CaptureEnd(output []byte, usedGas uint64, err error) {}

// Rest of the frames
func (ct *OverlayCreateTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.isCapturing {
		return
	}

	if create && to == ct.contractAddress {
		ct.isCapturing = true
		_, _, _, err := ct.evm.OverlayCreate(vm.AccountRef(from), vm.NewCodeAndHash(ct.code), ct.gasCap, value, to, typ, true /* incrementNonce */)
		if err != nil {
			ct.err = err
		} else {
			ct.resultCode = ct.evm.IntraBlockState().GetCode(ct.contractAddress)
		}
	}
}
func (ct *OverlayCreateTracer) CaptureExit(output []byte, usedGas uint64, err error) {}

// Opcode level
func (ct *OverlayCreateTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *OverlayCreateTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
