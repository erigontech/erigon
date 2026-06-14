package decodedstate

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/holiman/uint256"
)

// NewTracingHooks builds tracing.Hooks that drive a decoded state Collector.
// If existingHooks is non-nil, the returned hooks chain to them.
func NewTracingHooks(c Collector, existingHooks *tracing.Hooks) *tracing.Hooks {
	h := &decodedHooks{collector: c}
	hooks := &tracing.Hooks{
		OnKeccak256:     h.onKeccak256,
		OnStorageChange: h.onStorageChange,
		OnEnter:         h.onEnter,
		OnExit:          h.onExit,
	}
	if existingHooks != nil {
		// Merge: wrap callbacks so both fire
		if existingHooks.OnKeccak256 != nil {
			orig := existingHooks.OnKeccak256
			hooks.OnKeccak256 = func(addr accounts.Address, input []byte, output common.Hash) {
				h.onKeccak256(addr, input, output)
				orig(addr, input, output)
			}
		}
		if existingHooks.OnStorageChange != nil {
			orig := existingHooks.OnStorageChange
			hooks.OnStorageChange = func(addr accounts.Address, slot accounts.StorageKey, prev, new uint256.Int) {
				h.onStorageChange(addr, slot, prev, new)
				orig(addr, slot, prev, new)
			}
		}
		if existingHooks.OnEnter != nil {
			orig := existingHooks.OnEnter
			hooks.OnEnter = func(depth int, typ byte, from, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
				h.onEnter(depth, typ, from, to, precompile, input, gas, value, code)
				orig(depth, typ, from, to, precompile, input, gas, value, code)
			}
		}
		if existingHooks.OnExit != nil {
			orig := existingHooks.OnExit
			hooks.OnExit = func(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
				h.onExit(depth, output, gasUsed, err, reverted)
				orig(depth, output, gasUsed, err, reverted)
			}
		}
		// Copy through hooks we don't wrap
		if existingHooks.OnOpcode != nil {
			hooks.OnOpcode = existingHooks.OnOpcode
		}
		if hooks.OnTxStart == nil {
			hooks.OnTxStart = existingHooks.OnTxStart
		}
		if hooks.OnTxEnd == nil {
			hooks.OnTxEnd = existingHooks.OnTxEnd
		}
		if hooks.OnFault == nil {
			hooks.OnFault = existingHooks.OnFault
		}
		if hooks.OnGasChange == nil {
			hooks.OnGasChange = existingHooks.OnGasChange
		}
		if hooks.OnBlockchainInit == nil {
			hooks.OnBlockchainInit = existingHooks.OnBlockchainInit
		}
		if hooks.OnBlockStart == nil {
			hooks.OnBlockStart = existingHooks.OnBlockStart
		}
		if hooks.OnBlockEnd == nil {
			hooks.OnBlockEnd = existingHooks.OnBlockEnd
		}
		if hooks.OnGenesisBlock == nil {
			hooks.OnGenesisBlock = existingHooks.OnGenesisBlock
		}
		if hooks.OnSystemCallStart == nil {
			hooks.OnSystemCallStart = existingHooks.OnSystemCallStart
		}
		if hooks.OnSystemCallEnd == nil {
			hooks.OnSystemCallEnd = existingHooks.OnSystemCallEnd
		}
		if hooks.OnBalanceChange == nil {
			hooks.OnBalanceChange = existingHooks.OnBalanceChange
		}
		if hooks.OnNonceChange == nil {
			hooks.OnNonceChange = existingHooks.OnNonceChange
		}
		if hooks.OnCodeChange == nil {
			hooks.OnCodeChange = existingHooks.OnCodeChange
		}
		if hooks.OnLog == nil {
			hooks.OnLog = existingHooks.OnLog
		}
		if hooks.Flush == nil {
			hooks.Flush = existingHooks.Flush
		}
	}
	return hooks
}

type decodedHooks struct {
	collector Collector
}

func (h *decodedHooks) onKeccak256(addr accounts.Address, input []byte, output common.Hash) {
	h.collector.OnKeccak256(addr.Value(), input, output)
}

func (h *decodedHooks) onStorageChange(addr accounts.Address, slot accounts.StorageKey, prev, new uint256.Int) {
	h.collector.OnSStore(addr.Value(), slot.Value(), new)
}

func (h *decodedHooks) onEnter(depth int, typ byte, from, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	if precompile {
		return
	}
	storageAddr := to.Value()
	codeAddr := to.Value()
	isDelegateCall := typ == byte(vm.DELEGATECALL) || typ == byte(vm.CALLCODE)
	if isDelegateCall {
		storageAddr = from.Value()
	}
	h.collector.OnEnterCall(storageAddr, codeAddr, isDelegateCall)
}

func (h *decodedHooks) onExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	h.collector.OnExitCall(reverted)
}
