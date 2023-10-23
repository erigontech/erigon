package jsonrpc

import (
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

// Helper implementation of vm.Tracer; since the interface is big and most
// custom tracers implement just a few of the methods, this is a base struct
// to avoid lots of empty boilerplate code
type DefaultTracer struct {
}

func (a *DefaultTracer) CaptureTxStart(env *vm.EVM, tx types.Transaction) {}

func (a *DefaultTracer) CaptureTxEnd(receipt *types.Receipt, err error) {}

func (a *DefaultTracer) CaptureStart(from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (t *DefaultTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (t *DefaultTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (t *DefaultTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *DefaultTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}

func (t *DefaultTracer) OnBlockStart(b *types.Block, td *big.Int, finalized, safe *types.Header) {
}

func (t *DefaultTracer) OnBlockEnd(err error) {
}

func (t *DefaultTracer) OnGenesisBlock(b *types.Block, alloc types.GenesisAlloc) {
}

func (t *DefaultTracer) CaptureKeccakPreimage(hash libcommon.Hash, data []byte) {}

func (t *DefaultTracer) OnGasChange(old, new uint64, reason vm.GasChangeReason) {}

func (t *DefaultTracer) OnBalanceChange(addr libcommon.Address, prev, new *uint256.Int, reason evmtypes.BalanceChangeReason) {
}

func (t *DefaultTracer) OnNonceChange(addr libcommon.Address, prev, new uint64) {}

func (t *DefaultTracer) OnCodeChange(addr libcommon.Address, prevCodeHash libcommon.Hash, prev []byte, codeHash libcommon.Hash, code []byte) {
}

func (t *DefaultTracer) OnStorageChange(addr libcommon.Address, k *libcommon.Hash, prev, new uint256.Int) {
}

func (t *DefaultTracer) OnLog(log *types.Log) {}

func (t *DefaultTracer) OnNewAccount(addr libcommon.Address) {}

func (t *DefaultTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}
