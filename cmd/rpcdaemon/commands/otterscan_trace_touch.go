package commands

import (
	"bytes"
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
)

type TouchTracer struct {
	DefaultTracer
	searchAddr common.Address
	Found      bool
}

func NewTouchTracer(searchAddr common.Address) *TouchTracer {
	return &TouchTracer{
		searchAddr: searchAddr,
	}
}

func (t *TouchTracer) CaptureStart(env *vm.EVM, depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) {
	if !t.Found && (bytes.Equal(t.searchAddr.Bytes(), from.Bytes()) || bytes.Equal(t.searchAddr.Bytes(), to.Bytes())) {
		t.Found = true
	}
}
