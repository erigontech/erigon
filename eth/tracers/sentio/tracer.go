package sentio

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracer)
}

type sentioTracer struct {
	internalCall bool
}

func (t *sentioTracer) CaptureTxStart(gasLimit uint64) {

}
func (t *sentioTracer) CaptureTxEnd(restGas uint64) {

}

// Top call frame
func (t *sentioTracer) CaptureStart(env vm.VMInterface, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {

}
func (t *sentioTracer) CaptureEnd(output []byte, usedGas uint64, err error) {

}

// Rest of the frames
func (t *sentioTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {

}
func (t *sentioTracer) CaptureExit(output []byte, usedGas uint64, err error) {

}

// Opcode level
func (t *sentioTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {

}
func (t *sentioTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {

}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracer) CapturePreimage(pc uint64, hash common.Hash, preimage []byte) {

}

func (t *sentioTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

func (t *sentioTracer) Stop(err error) {

}

func newSentioTracer(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	if name != "sentio" {
		return nil, errors.New("no tracer found")
	}

	// TODO add configures
	return &sentioTracer{
		internalCall: true,
	}, nil
}
