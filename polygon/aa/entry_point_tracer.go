package aa

import (
	"errors"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
)

type EntryPointTracer struct {
	OnEnterSuper tracing.EnterHook

	Input []byte
	From  libcommon.Address
	Error error
}

func (epc *EntryPointTracer) OnEnter(depth int, typ byte, from libcommon.Address, to libcommon.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if epc.OnEnterSuper != nil {
		epc.OnEnterSuper(depth, typ, from, to, precompile, input, gas, value, code)
	}

	isRip7560EntryPoint := to.Cmp(types.AA_ENTRY_POINT) == 0
	if !isRip7560EntryPoint {
		return
	}

	if epc.Input != nil {
		epc.Error = errors.New("illegal repeated call to the EntryPoint callback")
		return
	}

	epc.Input = make([]byte, len(input))
	copy(epc.Input, input)
	epc.From = from
}

func (epc *EntryPointTracer) Reset() {
	epc.Input = nil
	epc.From = libcommon.Address{}
	epc.Error = nil
}
