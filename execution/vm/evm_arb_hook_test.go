package vm

import (
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	arbtypes "github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func newTestEVM() *EVM {
	blockCtx := evmtypes.BlockContext{
		BlockNumber: 100,
		Time:        1000,
		Coinbase:    accounts.InternAddress(common.HexToAddress("0xdead")),
		GasLimit:    30_000_000,
		GetHash: func(n uint64) (common.Hash, error) {
			return common.Hash{byte(n)}, nil
		},
	}
	txCtx := evmtypes.TxContext{
		GasPrice: *uint256.NewInt(1),
	}
	cfg := &chain.Config{
		ChainID: big.NewInt(1),
	}
	return NewEVM(blockCtx, txCtx, nil, cfg, Config{})
}

func TestDefaultTxProcessor_IsArbitrum(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	require.False(t, p.IsArbitrum())
}

func TestDefaultTxProcessor_StartTxHook(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	bail, mg, err, data := p.StartTxHook()
	require.False(t, bail)
	require.True(t, mg.IsZero())
	require.NoError(t, err)
	require.Nil(t, data)
}

func TestDefaultTxProcessor_GasChargingHook(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	gas := uint64(1000)
	coinbase, mg, err := p.GasChargingHook(&gas, 21000)
	require.NoError(t, err)
	require.Equal(t, evm.Context.Coinbase, coinbase)
	require.True(t, mg.IsZero())
}

func TestDefaultTxProcessor_ForceRefundGas(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	require.Equal(t, uint64(0), p.ForceRefundGas())
}

func TestDefaultTxProcessor_NonrefundableGas(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	require.Equal(t, uint64(0), p.NonrefundableGas())
}

func TestDefaultTxProcessor_DropTip(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	require.False(t, p.DropTip())
}

func TestDefaultTxProcessor_ScheduledTxes(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	txes := p.ScheduledTxes()
	require.Nil(t, txes)
}

func TestDefaultTxProcessor_L1BlockNumber(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	num, err := p.L1BlockNumber(evm.Context)
	require.NoError(t, err)
	require.Equal(t, evm.Context.BlockNumber, num)
}

func TestDefaultTxProcessor_L1BlockHash(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	hash, err := p.L1BlockHash(evm.Context, 5)
	require.NoError(t, err)
	expected := common.Hash{5}
	require.Equal(t, expected, hash)
}

func TestDefaultTxProcessor_GasPriceOp(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	price := p.GasPriceOp(evm)
	require.Equal(t, &evm.GasPrice, price)
}

func TestDefaultTxProcessor_MsgIsNonMutating(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	require.False(t, p.MsgIsNonMutating())
}

func TestDefaultTxProcessor_IsCalldataPricingIncreaseEnabled(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	require.True(t, p.IsCalldataPricingIncreaseEnabled())
}

func TestDefaultTxProcessor_FillReceiptInfo(t *testing.T) {
	evm := newTestEVM()
	p := DefaultTxProcessor{evm: evm}
	receipt := &arbtypes.Receipt{}
	p.FillReceiptInfo(receipt)
}

func TestNewEVM_SetsDefaultProcessingHook(t *testing.T) {
	evm := newTestEVM()
	require.NotNil(t, evm.ProcessingHook)
	require.False(t, evm.ProcessingHook.IsArbitrum())
}
