package commands

import (
	"context"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
)

type CreateTracer struct {
	DefaultTracer
	ctx     context.Context
	target  libcommon.Address
	found   bool
	Creator libcommon.Address
	Tx      types.Transaction
}

func NewCreateTracer(ctx context.Context, target libcommon.Address) *CreateTracer {
	return &CreateTracer{
		ctx:    ctx,
		target: target,
		found:  false,
	}
}

func (t *CreateTracer) SetTransaction(tx types.Transaction) {
	t.Tx = tx
}

func (t *CreateTracer) Found() bool {
	return t.found
}

func (t *CreateTracer) captureStartOrEnter(from, to libcommon.Address, create bool) {
	if t.found {
		return
	}
	if !create {
		return
	}
	if to != t.target {
		return
	}

	t.found = true
	t.Creator = from
}

func (t *CreateTracer) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.captureStartOrEnter(from, to, create)
}

func (t *CreateTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.captureStartOrEnter(from, to, create)
}
