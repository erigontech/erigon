package tracer

import (
	"encoding/json"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func NewBorStateSyncTxnTracer(
	tracer vm.EVMLogger,
	stateSyncEventsCount int,
	stateReceiverContractAddress libcommon.Address,
) tracers.Tracer {
	return &borStateSyncTxnTracer{
		EVMLogger:                    tracer,
		stateSyncEventsCount:         stateSyncEventsCount,
		stateReceiverContractAddress: stateReceiverContractAddress,
	}
}

// borStateSyncTxnTracer is a special tracer which is used only for tracing bor state sync transactions. Bor state sync
// transactions are synthetic transactions that are used to bridge assets from L1 (root chain) to L2 (child chain).
// At end of each sprint bor executes the state sync events (0, 1 or many) coming from Heimdall by calling the
// StateReceiverContract with event.Data as input call data.
//
// The borStateSyncTxnTracer wraps any other tracer that the users have requested to use for tracing and tricks them
// to think that they are running in the same transaction as sub-calls. This is needed since when bor executes the
// state sync events at end of each sprint these are synthetically executed as if they were sub-calls of the
// state sync events bor transaction.
type borStateSyncTxnTracer struct {
	vm.EVMLogger
	captureStartCalledOnce       bool
	stateSyncEventsCount         int
	stateReceiverContractAddress libcommon.Address
}

func (bsstt *borStateSyncTxnTracer) CaptureTxStart(evm *vm.EVM, tx types.Transaction) {
	bsstt.EVMLogger.CaptureTxStart(evm, tx)
}

func (bsstt *borStateSyncTxnTracer) CaptureTxEnd(receipt *types.Receipt, err error) {
	bsstt.EVMLogger.CaptureTxEnd(receipt, err)
}

func (bsstt *borStateSyncTxnTracer) CaptureStart(
	from libcommon.Address,
	to libcommon.Address,
	precompile bool,
	create bool,
	input []byte,
	gas uint64,
	value *uint256.Int,
	code []byte,
) {
	if !bsstt.captureStartCalledOnce {
		// first event execution started
		// perform a CaptureStart for the synthetic state sync transaction
		from := state.SystemAddress
		to := bsstt.stateReceiverContractAddress
		bsstt.EVMLogger.CaptureStart(from, to, false, false, nil, 0, uint256.NewInt(0), nil)
		bsstt.captureStartCalledOnce = true
	}

	// trick the tracer to think it is a CaptureEnter
	bsstt.EVMLogger.CaptureEnter(vm.CALL, from, to, precompile, create, input, gas, value, code)
}

func (bsstt *borStateSyncTxnTracer) CaptureEnd(output []byte, usedGas uint64, err error, reverted bool) {
	if bsstt.stateSyncEventsCount == 0 {
		// guard against unexpected use
		panic("unexpected extra call to borStateSyncTxnTracer.CaptureEnd")
	}

	// finished executing 1 event
	bsstt.stateSyncEventsCount--

	// trick tracer to think it is a CaptureExit
	bsstt.EVMLogger.CaptureExit(output, usedGas, err, reverted)

	if bsstt.stateSyncEventsCount == 0 {
		// reached last event
		// perform a CaptureEnd for the synthetic state sync transaction
		bsstt.EVMLogger.CaptureEnd(nil, 0, nil, reverted)
	}
}

func (bsstt *borStateSyncTxnTracer) CaptureState(
	pc uint64,
	op vm.OpCode,
	gas uint64,
	cost uint64,
	scope *vm.ScopeContext,
	rData []byte,
	depth int,
	err error,
) {
	// trick tracer to think it is 1 level deeper
	bsstt.EVMLogger.CaptureState(pc, op, gas, cost, scope, rData, depth+1, err)
}

func (bsstt *borStateSyncTxnTracer) CaptureFault(
	pc uint64,
	op vm.OpCode,
	gas uint64,
	cost uint64,
	scope *vm.ScopeContext,
	depth int,
	err error,
) {
	// trick tracer to think it is 1 level deeper
	bsstt.EVMLogger.CaptureFault(pc, op, gas, cost, scope, depth+1, err)
}

func (bsstt *borStateSyncTxnTracer) GetResult() (json.RawMessage, error) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		return tracer.GetResult()
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.GetResult called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) Stop(err error) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.Stop(err)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.Stop called on a wrapped tracer which does not support it")
	}
}

// CaptureKeccakPreimage is called during the KECCAK256 opcode.
func (bsstt *borStateSyncTxnTracer) CaptureKeccakPreimage(hash libcommon.Hash, data []byte) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.CaptureKeccakPreimage(hash, data)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.CaptureKeccakPreimage called on a wrapped tracer which does not support it")
	}
}

// OnGasChange is called when gas is either consumed or refunded.
func (bsstt *borStateSyncTxnTracer) OnGasChange(old, new uint64, reason vm.GasChangeReason) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnGasChange(old, new, reason)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnGasChange called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnBlockStart(b *types.Block, td *big.Int, finalized, safe *types.Header, chainConfig *chain.Config) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnBlockStart(b, td, finalized, safe, chainConfig)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnBlockStart called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnBlockEnd(err error) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnBlockEnd(err)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnBlockEnd called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnGenesisBlock(b *types.Block, alloc types.GenesisAlloc) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnGenesisBlock(b, alloc)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnGenesisBlock called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnBeaconBlockRootStart(root libcommon.Hash) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnBeaconBlockRootStart(root)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnBeaconBlockRootStart called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnBeaconBlockRootEnd() {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnBeaconBlockRootEnd()
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnBeaconBlockRootEnd called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnBalanceChange(a libcommon.Address, prev, new *uint256.Int, reason evmtypes.BalanceChangeReason) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnBalanceChange(a, prev, new, reason)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnBalanceChange called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnNonceChange(a libcommon.Address, prev, new uint64) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnNonceChange(a, prev, new)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnNonceChange called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnCodeChange(a libcommon.Address, prevCodeHash libcommon.Hash, prev []byte, codeHash libcommon.Hash, code []byte) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnCodeChange(a, prevCodeHash, prev, codeHash, code)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnCodeChange called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnStorageChange(a libcommon.Address, k *libcommon.Hash, prev, new uint256.Int) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnStorageChange(a, k, prev, new)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnStorageChange called on a wrapped tracer which does not support it")
	}
}

func (bsstt *borStateSyncTxnTracer) OnLog(log *types.Log) {
	if tracer, ok := bsstt.EVMLogger.(tracers.Tracer); ok {
		tracer.OnLog(log)
	} else {
		panic("unexpected usage - borStateSyncTxnTracer.OnLog called on a wrapped tracer which does not support it")
	}
}
