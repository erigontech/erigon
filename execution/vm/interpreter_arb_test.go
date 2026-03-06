package vm

import (
	"math/big"
	"testing"

	arbchaintypes "github.com/erigontech/erigon/arb/chain/types"
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	arbtypes "github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type recordingHook struct {
	DefaultTxProcessor
	pushCalls   []*Contract
	popCalls    int
	wasmCalls   int
	wasmRet     []byte
	wasmErr     error
	wasmGasUsed uint64
}

func (h *recordingHook) PushContract(contract *Contract) {
	h.pushCalls = append(h.pushCalls, contract)
}

func (h *recordingHook) PopContract() {
	h.popCalls++
}

func (h *recordingHook) ExecuteWASM(scope *CallContext, input []byte, evm *EVM) ([]byte, error) {
	h.wasmCalls++
	if h.wasmGasUsed > 0 {
		scope.Contract.UseGas(h.wasmGasUsed, nil, 0)
	}
	return h.wasmRet, h.wasmErr
}

func (h *recordingHook) IsArbitrum() bool { return true }

func (h *recordingHook) SetMessage(*arbtypes.Message, evmtypes.IntraBlockState) {}

func (h *recordingHook) FillReceiptInfo(*arbtypes.Receipt) {}

func (h *recordingHook) MsgIsNonMutating() bool { return false }

func (h *recordingHook) StartTxHook() (bool, multigas.MultiGas, error, []byte) {
	return false, multigas.ZeroGas(), nil, nil
}

func (h *recordingHook) ScheduledTxes() arbtypes.Transactions { return nil }

func (h *recordingHook) EndTxHook(gasRemaining uint64, evmSuccess bool) {}

func (h *recordingHook) GasChargingHook(gasRemaining *uint64, intrinsicGas uint64) (accounts.Address, multigas.MultiGas, error) {
	return accounts.InternAddress(common.Address{}), multigas.ZeroGas(), nil
}

func (h *recordingHook) ForceRefundGas() uint64 { return 0 }

func (h *recordingHook) NonrefundableGas() uint64 { return 0 }

func (h *recordingHook) DropTip() bool { return false }

func (h *recordingHook) IsCalldataPricingIncreaseEnabled() bool { return true }

func (h *recordingHook) GasPriceOp(evm *EVM) *uint256.Int {
	gp := evm.GasPrice
	return &gp
}

func (h *recordingHook) L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error) {
	return blockCtx.BlockNumber, nil
}

func (h *recordingHook) L1BlockHash(blockCtx evmtypes.BlockContext, l1BlockNumber uint64) (common.Hash, error) {
	return blockCtx.GetHash(l1BlockNumber)
}

func TestRun_PushPopContract_SkippedForEmptyCode(t *testing.T) {
	evm := newTestEVM()
	hook := &recordingHook{}
	evm.ProcessingHook = hook

	contract := Contract{
		Code: nil,
	}
	ret, gas, _, err := evm.Run(contract, 10000, nil, false)
	require.NoError(t, err)
	require.Nil(t, ret)
	require.Equal(t, uint64(10000), gas)
	require.Equal(t, 0, len(hook.pushCalls), "no PushContract for empty code")
	require.Equal(t, 0, hook.popCalls, "no PopContract for empty code")
}

func TestRun_PushPopContract_CalledForSTOP(t *testing.T) {
	evm := newTestEVM()
	hook := &recordingHook{}
	evm.ProcessingHook = hook

	contract := Contract{
		Code: []byte{byte(STOP)},
	}
	_, _, _, err := evm.Run(contract, 10000, nil, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(hook.pushCalls))
	require.Equal(t, 1, hook.popCalls)
}

func newStylusEVM() *EVM {
	cfg := &chain.Config{
		ChainID:             big.NewInt(42161),
		ArbitrumChainParams: arbchaintypes.ArbitrumChainParams{EnableArbOS: true},
	}
	blockCtx := evmtypes.BlockContext{
		BlockNumber:  100,
		Time:         1000,
		ArbOSVersion: 31,
		Coinbase:     accounts.InternAddress(common.HexToAddress("0xdead")),
		GasLimit:     30_000_000,
		GetHash: func(n uint64) (common.Hash, error) {
			return common.Hash{byte(n)}, nil
		},
	}
	txCtx := evmtypes.TxContext{
		GasPrice: *uint256.NewInt(1),
	}
	return NewEVM(blockCtx, txCtx, nil, cfg, Config{})
}

func TestRun_StylusDispatch_CallsExecuteWASM(t *testing.T) {
	evm := newStylusEVM()

	if !evm.chainRules.IsStylus {
		t.Fatal("expected IsStylus to be true for ArbOSVersion 31")
	}

	expectedRet := []byte{0x01, 0x02, 0x03}
	hook := &recordingHook{wasmRet: expectedRet, wasmGasUsed: 1000}
	evm.ProcessingHook = hook

	stylusCode := state.NewStylusPrefix(0x00)
	stylusCode = append(stylusCode, 0xDE, 0xAD)
	contract := Contract{
		Code: stylusCode,
	}
	startGas := uint64(50000)
	ret, remainingGas, _, err := evm.Run(contract, startGas, []byte{0xAA}, false)
	require.NoError(t, err)
	require.Equal(t, expectedRet, ret)
	require.Equal(t, 1, hook.wasmCalls)
	require.Equal(t, 1, len(hook.pushCalls))
	require.Equal(t, 1, hook.popCalls)
	require.Equal(t, startGas-hook.wasmGasUsed, remainingGas, "remaining gas should reflect WASM consumption")
}

func TestRun_NonStylus_DoesNotCallExecuteWASM(t *testing.T) {
	evm := newTestEVM()
	hook := &recordingHook{}
	evm.ProcessingHook = hook

	contract := Contract{
		Code: []byte{byte(STOP)},
	}
	_, _, _, err := evm.Run(contract, 10000, nil, false)
	require.NoError(t, err)
	require.Equal(t, 0, hook.wasmCalls)
}

func newArbitrumEVM() *EVM {
	cfg := &chain.Config{
		ChainID:             big.NewInt(42161),
		ArbitrumChainParams: arbchaintypes.ArbitrumChainParams{EnableArbOS: true},
	}
	blockCtx := evmtypes.BlockContext{
		BlockNumber:  100,
		Time:         1000,
		ArbOSVersion: 31,
		Coinbase:     accounts.InternAddress(common.HexToAddress("0xdead")),
		GasLimit:     30_000_000,
		GetHash: func(n uint64) (common.Hash, error) {
			return common.Hash{byte(n)}, nil
		},
	}
	txCtx := evmtypes.TxContext{
		GasPrice: *uint256.NewInt(1),
	}
	return NewEVM(blockCtx, txCtx, nil, cfg, Config{})
}

func TestRun_MultiGasAccumulation_ConstantGas(t *testing.T) {
	evm := newArbitrumEVM()
	require.True(t, evm.chainRules.IsArbitrum)

	hook := &recordingHook{}
	evm.ProcessingHook = hook

	// PUSH1 0x01, PUSH1 0x02, ADD, STOP
	// PUSH1 has constant gas = 3 (GasFastestStep), ADD has constant gas = 3
	code := []byte{
		byte(PUSH1), 0x01,
		byte(PUSH1), 0x02,
		byte(ADD),
		byte(STOP),
	}
	contract := Contract{Code: code}
	_, gasLeft, mg, err := evm.Run(contract, 100000, nil, false)
	require.NoError(t, err)

	gasUsed := 100000 - gasLeft
	require.Greater(t, gasUsed, uint64(0))
	require.Equal(t, gasUsed, mg.SingleGas())
	require.Equal(t, gasUsed, mg.Get(multigas.ResourceKindComputation))
}

func TestRun_MultiGasAccumulation_NonArbitrum(t *testing.T) {
	evm := newTestEVM()
	require.False(t, evm.chainRules.IsArbitrum)

	code := []byte{
		byte(PUSH1), 0x01,
		byte(PUSH1), 0x02,
		byte(ADD),
		byte(STOP),
	}
	contract := Contract{Code: code}
	_, _, mg, err := evm.Run(contract, 100000, nil, false)
	require.NoError(t, err)

	require.True(t, mg.IsZero())
}
