package exec3

import (
	"encoding/json"
	"math/big"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

type CallTracer struct {
	froms map[libcommon.Address]struct{}
	tos   map[libcommon.Address]struct{}
}

func NewCallTracer() *CallTracer {
	return &CallTracer{}
}
func (ct *CallTracer) Reset() {
	ct.froms, ct.tos = nil, nil
}
func (ct *CallTracer) Froms() map[libcommon.Address]struct{} { return ct.froms }
func (ct *CallTracer) Tos() map[libcommon.Address]struct{}   { return ct.tos }

func (ct *CallTracer) CaptureTxStart(env *vm.EVM, tx types.Transaction) {}

func (ct *CallTracer) CaptureTxEnd(receipt *types.Receipt, err error) {}

func (ct *CallTracer) CaptureStart(from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.froms == nil {
		ct.froms = map[libcommon.Address]struct{}{}
		ct.tos = map[libcommon.Address]struct{}{}
	}
	ct.froms[from], ct.tos[to] = struct{}{}, struct{}{}
}
func (ct *CallTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if ct.froms == nil {
		ct.froms = map[libcommon.Address]struct{}{}
		ct.tos = map[libcommon.Address]struct{}{}
	}
	ct.froms[from], ct.tos[to] = struct{}{}, struct{}{}
}
func (ct *CallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}
func (ct *CallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (ct *CallTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}
func (ct *CallTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}

func (ct *CallTracer) OnBlockStart(b *types.Block, td *big.Int, finalized, safe *types.Header) {
}

func (ct *CallTracer) OnBlockEnd(err error) {
}

func (ct *CallTracer) OnGenesisBlock(b *types.Block, alloc types.GenesisAlloc) {
}

func (ct *CallTracer) CaptureKeccakPreimage(hash libcommon.Hash, data []byte) {}

func (ct *CallTracer) OnGasChange(old, new uint64, reason vm.GasChangeReason) {}

func (ct *CallTracer) OnBalanceChange(a libcommon.Address, prev, new *uint256.Int, reason evmtypes.BalanceChangeReason) {
}

func (ct *CallTracer) OnNonceChange(a libcommon.Address, prev, new uint64) {}

func (ct *CallTracer) OnCodeChange(a libcommon.Address, prevCodeHash libcommon.Hash, prev []byte, codeHash libcommon.Hash, code []byte) {
}

func (ct *CallTracer) OnStorageChange(a libcommon.Address, k *libcommon.Hash, prev, new uint256.Int) {
}

func (ct *CallTracer) OnLog(log *types.Log) {}

func (ct *CallTracer) OnNewAccount(a libcommon.Address) {}

// GetResult returns an empty json object.
func (ct *CallTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

// Stop terminates execution of the tracer at the first opportune moment.
func (ct *CallTracer) Stop(err error) {
}
