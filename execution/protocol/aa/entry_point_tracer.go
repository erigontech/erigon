package aa

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type EntryPointTracer struct {
	OnEnterSuper tracing.EnterHook

	Input []byte
	From  accounts.Address
	Error error
}

func (epc *EntryPointTracer) OnEnter(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
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

func (epc *EntryPointTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: epc.OnEnter,
	}
}

func (epc *EntryPointTracer) Reset() {
	epc.Input = nil
	epc.From = accounts.Address{}
	epc.Error = nil
}
