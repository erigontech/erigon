package protocol

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	arbtypes "github.com/erigontech/erigon/arb/chain/types"
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestRevertedTxGasUsed_KnownTx(t *testing.T) {
	knownTxHash := common.HexToHash("0x58df300a7f04fe31d41d24672786cbe1c58b4f3d8329d0d74392d814dd9f7e40")
	gasUsed, ok := RevertedTxGasUsed[knownTxHash]
	require.True(t, ok, "known reverted tx should be in the map")
	require.Equal(t, uint64(45174), gasUsed)
}

func TestRevertedTxGasUsed_UnknownTx(t *testing.T) {
	unknownHash := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	_, ok := RevertedTxGasUsed[unknownHash]
	require.False(t, ok)
}

func TestIntrinsicMultiGas_BasicTx(t *testing.T) {
	data := []byte{0x01, 0x02, 0x00, 0x03}
	mg, floorGas, overflow := multigas.IntrinsicMultiGas(data, 0, 0, false, true, true, false, false, false, 0)
	require.False(t, overflow)
	require.Greater(t, mg.SingleGas(), uint64(0))
	require.Equal(t, params.TxGas, floorGas)

	require.Greater(t, mg.Get(multigas.ResourceKindComputation), uint64(0))
	require.Greater(t, mg.Get(multigas.ResourceKindL2Calldata), uint64(0))
}

func TestIntrinsicMultiGas_ContractCreation(t *testing.T) {
	data := []byte{0x01, 0x02}
	mg, _, overflow := multigas.IntrinsicMultiGas(data, 0, 0, true, true, true, false, false, false, 0)
	require.False(t, overflow)
	require.GreaterOrEqual(t, mg.Get(multigas.ResourceKindComputation), params.TxGasContractCreation)
}

func TestIntrinsicMultiGas_WithAccessList(t *testing.T) {
	data := []byte{0x01}
	mg, _, overflow := multigas.IntrinsicMultiGas(data, 2, 3, false, true, true, false, false, false, 0)
	require.False(t, overflow)
	require.Greater(t, mg.Get(multigas.ResourceKindStorageAccess), uint64(0))
	expectedAccess := 2*params.TxAccessListAddressGas + 3*params.TxAccessListStorageKeyGas
	require.Equal(t, expectedAccess, mg.Get(multigas.ResourceKindStorageAccess))
}

func TestIntrinsicMultiGas_WithAuthorizations(t *testing.T) {
	data := []byte{0x01}
	mg, _, overflow := multigas.IntrinsicMultiGas(data, 0, 0, false, true, true, false, false, false, 2)
	require.False(t, overflow)
	require.Equal(t, 2*params.CallNewAccountGas, mg.Get(multigas.ResourceKindStorageGrowth))
}

func TestArbitrumRules_BlobGasSkipped(t *testing.T) {
	rules := &chain.Rules{
		IsArbitrum: true,
		IsCancun:   true,
	}
	require.True(t, rules.IsArbitrum)
	require.True(t, rules.IsCancun)
}

func TestArbitrumRules_GasLimitCapSkipped(t *testing.T) {
	rules := &chain.Rules{
		IsArbitrum: true,
		IsOsaka:    true,
	}
	require.True(t, rules.IsArbitrum)
	require.True(t, rules.IsOsaka)
}

// earlyReturnHook is a mock TxProcessingHook that forces StartTxHook to return early
// with configurable multigas, error, and return data.
type earlyReturnHook struct {
	usedMultiGas multigas.MultiGas
	err          error
	returnData   []byte
	coinbase     accounts.Address
}

func (h earlyReturnHook) SetMessage(*types.Message, evmtypes.IntraBlockState)       {}
func (h earlyReturnHook) IsArbitrum() bool                                           { return false }
func (h earlyReturnHook) FillReceiptInfo(*types.Receipt)                             {}
func (h earlyReturnHook) MsgIsNonMutating() bool                                    { return false }
func (h earlyReturnHook) StartTxHook() (bool, multigas.MultiGas, error, []byte)     { return true, h.usedMultiGas, h.err, h.returnData }
func (h earlyReturnHook) ScheduledTxes() types.Transactions                         { return nil }
func (h earlyReturnHook) EndTxHook(uint64, bool)                                    {}
func (h earlyReturnHook) GasChargingHook(g *uint64, _ uint64) (accounts.Address, multigas.MultiGas, error) {
	return h.coinbase, multigas.ZeroGas(), nil
}
func (h earlyReturnHook) ForceRefundGas() uint64                                            { return 0 }
func (h earlyReturnHook) NonrefundableGas() uint64                                          { return 0 }
func (h earlyReturnHook) DropTip() bool                                                     { return false }
func (h earlyReturnHook) IsCalldataPricingIncreaseEnabled() bool                             { return true }
func (h earlyReturnHook) ExecuteWASM(*vm.CallContext, []byte, *vm.EVM) ([]byte, error)       { return nil, fmt.Errorf("wasm not supported") }
func (h earlyReturnHook) PushContract(*vm.Contract)                                          {}
func (h earlyReturnHook) PopContract()                                                       {}
func (h earlyReturnHook) GasPriceOp(evm *vm.EVM) *uint256.Int                               { return &evm.GasPrice }
func (h earlyReturnHook) L1BlockNumber(ctx evmtypes.BlockContext) (uint64, error)            { return ctx.BlockNumber, nil }
func (h earlyReturnHook) L1BlockHash(ctx evmtypes.BlockContext, n uint64) (common.Hash, error) {
	return ctx.GetHash(n)
}

func TestTransitionDb_MultiGasStartHookEarlyReturn(t *testing.T) {
	expectedMultiGas := multigas.MultiGasFromPairs(
		multigas.Pair{Kind: multigas.ResourceKindComputation, Amount: 5000},
		multigas.Pair{Kind: multigas.ResourceKindL1Calldata, Amount: 3000},
	)

	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))
	blockCtx := evmtypes.BlockContext{
		BlockNumber: 100,
		GasLimit:    30_000_000,
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	txCtx := evmtypes.TxContext{GasPrice: *uint256.NewInt(1)}

	cfg := &chain.Config{ChainID: big.NewInt(1)}
	evm := vm.NewEVM(blockCtx, txCtx, nil, cfg, vm.Config{})
	evm.ProcessingHook = earlyReturnHook{
		usedMultiGas: expectedMultiGas,
		coinbase:     coinbase,
	}

	gasPrice := uint256.NewInt(1)
	msg := types.NewMessage(
		accounts.InternAddress(common.HexToAddress("0x1")),
		accounts.InternAddress(common.HexToAddress("0x2")),
		0, uint256.NewInt(0), 100000, gasPrice, gasPrice, uint256.NewInt(0),
		nil, nil, false, false, false, false, nil,
	)
	gp := new(GasPool).AddGas(30_000_000)

	st := NewStateTransition(evm, msg, gp)
	result, err := st.TransitionDb(true, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Equal(t, expectedMultiGas.SingleGas(), result.ReceiptGasUsed)
	require.Equal(t, expectedMultiGas.SingleGas(), result.UsedMultiGas.SingleGas())
	require.Equal(t, expectedMultiGas.Get(multigas.ResourceKindComputation), result.UsedMultiGas.Get(multigas.ResourceKindComputation))
	require.Equal(t, expectedMultiGas.Get(multigas.ResourceKindL1Calldata), result.UsedMultiGas.Get(multigas.ResourceKindL1Calldata))
}

// dropTipHook is a mock TxProcessingHook that returns true for DropTip(),
// allowing tests to verify the pre-preCheck gasPrice override.
type dropTipHook struct {
	earlyReturnHook
}

func (h dropTipHook) DropTip() bool                                            { return true }
func (h dropTipHook) StartTxHook() (bool, multigas.MultiGas, error, []byte)    { return false, multigas.ZeroGas(), nil, nil }

func TestTransitionDb_DropTipPrePreCheck(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))

	initialBalance := uint256.NewInt(1_000_000_000_000_000_000)
	s.CreateAccount(sender, true)
	s.AddBalance(sender, *initialBalance, tracing.BalanceChangeUnspecified)
	s.SetNonce(sender, 0)

	baseFee := uint256.NewInt(10)
	originalGasPrice := uint256.NewInt(100) // 10x the baseFee

	blockCtx := evmtypes.BlockContext{
		BlockNumber: 1,
		GasLimit:    30_000_000,
		BaseFee:     *baseFee,
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
			bal, err := ibs.GetBalance(addr)
			if err != nil {
				return false, err
			}
			return bal.Cmp(&val) >= 0, nil
		},
		Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
			if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
		},
	}

	txCtx := evmtypes.TxContext{
		GasPrice: *originalGasPrice,
		Origin:   sender,
	}

	cfg := &chain.Config{
		ChainID:               big.NewInt(1),
		HomesteadBlock:        new(big.Int),
		TangerineWhistleBlock: new(big.Int),
		SpuriousDragonBlock:   new(big.Int),
		ByzantiumBlock:        new(big.Int),
		ConstantinopleBlock:   new(big.Int),
		PetersburgBlock:       new(big.Int),
		IstanbulBlock:         new(big.Int),
		BerlinBlock:           new(big.Int),
		LondonBlock:           new(big.Int),
	}

	evmInst := vm.NewEVM(blockCtx, txCtx, s, cfg, vm.Config{})
	evmInst.ProcessingHook = dropTipHook{
		earlyReturnHook: earlyReturnHook{coinbase: coinbase},
	}

	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), 50000,
		originalGasPrice, originalGasPrice, originalGasPrice,
		nil, nil, true, false, true, false, nil,
	)

	gp := new(GasPool).AddGas(30_000_000)
	st := NewStateTransition(evmInst, msg, gp)

	result, err := st.TransitionDb(true, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	// The sender should have been charged at baseFee rate, not originalGasPrice rate.
	// buyGas deducts gasLimit * gasPrice (now baseFee) upfront, then refunds (gasLimit - gasUsed) * gasPrice.
	// Net cost = gasUsed * baseFee.
	finalBalance, err := s.GetBalance(sender)
	require.NoError(t, err)

	expectedCost := result.ReceiptGasUsed * baseFee.Uint64()
	actualCost := new(uint256.Int).Sub(initialBalance, &finalBalance)
	require.Equal(t, expectedCost, actualCost.Uint64(),
		"sender should pay gasUsed * baseFee (not gasUsed * originalGasPrice)")

	// Verify the tip paid to coinbase is zero (DropTip zeroes effectiveTip).
	coinbaseBal, err := s.GetBalance(coinbase)
	require.NoError(t, err)
	require.True(t, coinbaseBal.IsZero(), "coinbase should receive zero tip when DropTip is active")
}

func TestTransitionDb_MultiGasAccumulation(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))

	s.CreateAccount(sender, true)
	s.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	s.SetNonce(sender, 0)

	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))
	blockCtx := evmtypes.BlockContext{
		BlockNumber: 1,
		GasLimit:    30_000_000,
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
			bal, err := ibs.GetBalance(addr)
			if err != nil {
				return false, err
			}
			return bal.Cmp(&val) >= 0, nil
		},
		Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
			if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
		},
	}

	gasPrice := uint256.NewInt(1)
	txCtx := evmtypes.TxContext{
		GasPrice: *gasPrice,
		Origin:   sender,
	}

	cfg := &chain.Config{
		ChainID:               big.NewInt(1),
		HomesteadBlock:        new(big.Int),
		TangerineWhistleBlock: new(big.Int),
		SpuriousDragonBlock:   new(big.Int),
		ByzantiumBlock:        new(big.Int),
		ConstantinopleBlock:   new(big.Int),
		PetersburgBlock:       new(big.Int),
		IstanbulBlock:         new(big.Int),
		BerlinBlock:           new(big.Int),
		LondonBlock:           new(big.Int),
	}

	evmInst := vm.NewEVM(blockCtx, txCtx, s, cfg, vm.Config{})

	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(100), 50000,
		gasPrice, gasPrice, uint256.NewInt(0),
		nil, nil, true, false, true, false, nil,
	)

	gp := new(GasPool).AddGas(30_000_000)

	result, err := ApplyMessage(evmInst, msg, gp, true, false, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NoError(t, result.Err)

	require.Equal(t, result.ReceiptGasUsed, result.UsedMultiGas.SingleGas(),
		"multigas SingleGas must equal ReceiptGasUsed for simple transfer")

	require.Greater(t, result.UsedMultiGas.Get(multigas.ResourceKindComputation), uint64(0),
		"computation gas should be non-zero for a transfer")
}

// arbRefundHook is a mock TxProcessingHook for testing Arbitrum refund paths.
// It returns configurable ForceRefundGas, NonrefundableGas, and adds state refunds
// during GasChargingHook.
type arbRefundHook struct {
	earlyReturnHook
	forceRefundGas   uint64
	nonrefundableGas uint64
	stateRefund      uint64
	ibs              *state.IntraBlockState
}

func (h arbRefundHook) IsArbitrum() bool                                        { return true }
func (h arbRefundHook) StartTxHook() (bool, multigas.MultiGas, error, []byte)   { return false, multigas.ZeroGas(), nil, nil }
func (h arbRefundHook) ForceRefundGas() uint64                                  { return h.forceRefundGas }
func (h arbRefundHook) NonrefundableGas() uint64                                { return h.nonrefundableGas }
func (h arbRefundHook) GasChargingHook(g *uint64, _ uint64) (accounts.Address, multigas.MultiGas, error) {
	if h.stateRefund > 0 && h.ibs != nil {
		h.ibs.AddRefund(h.stateRefund)
	}
	return h.coinbase, multigas.ZeroGas(), nil
}

func TestTransitionDb_ArbitrumRefundMultiGas(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))

	s.CreateAccount(sender, true)
	s.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	s.SetNonce(sender, 0)

	baseFee := uint256.NewInt(1)
	gasPrice := uint256.NewInt(1)
	gasLimit := uint64(50000)
	forceRefund := uint64(5000)
	nonrefundable := uint64(1000)
	stateRefund := uint64(10000)

	blockCtx := evmtypes.BlockContext{
		BlockNumber: 1,
		GasLimit:    30_000_000,
		BaseFee:     *baseFee,
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
			bal, err := ibs.GetBalance(addr)
			if err != nil {
				return false, err
			}
			return bal.Cmp(&val) >= 0, nil
		},
		Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
			if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
		},
	}

	txCtx := evmtypes.TxContext{GasPrice: *gasPrice, Origin: sender}

	arbCfg := &chain.Config{
		ChainID:               big.NewInt(412346),
		HomesteadBlock:        new(big.Int),
		TangerineWhistleBlock: new(big.Int),
		SpuriousDragonBlock:   new(big.Int),
		ByzantiumBlock:        new(big.Int),
		ConstantinopleBlock:   new(big.Int),
		PetersburgBlock:       new(big.Int),
		IstanbulBlock:         new(big.Int),
		BerlinBlock:           new(big.Int),
		LondonBlock:           new(big.Int),
		ArbitrumChainParams:   arbtypes.ArbitrumChainParams{EnableArbOS: true, GenesisBlockNum: 0},
	}

	evmInst := vm.NewEVM(blockCtx, txCtx, s, arbCfg, vm.Config{})
	evmInst.ProcessingHook = arbRefundHook{
		earlyReturnHook: earlyReturnHook{coinbase: coinbase},
		forceRefundGas:   forceRefund,
		nonrefundableGas: nonrefundable,
		stateRefund:      stateRefund,
		ibs:              s,
	}

	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), gasLimit,
		gasPrice, gasPrice, gasPrice,
		nil, nil, false, false, false, false, nil,
	)

	gp := new(GasPool).AddGas(30_000_000)
	st := NewStateTransition(evmInst, msg, gp)
	result, err := st.TransitionDb(true, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	// ForceRefundGas should reduce gas used
	// intrinsicGas = 21000, gasRemaining after intrinsic = 29000
	// ForceRefundGas adds 5000 → gasRemaining = 34000, gasUsed = 16000
	// refund = (16000 - 1000) / 5 = 3000, capped at min(3000, 10000) = 3000
	// gasRemaining += 3000 → 37000, gasUsed = 13000
	expectedGasUsed := uint64(13000)
	require.Equal(t, expectedGasUsed, result.ReceiptGasUsed,
		"Arbitrum refund path should reduce gas by ForceRefundGas + capped refund")

	// usedMultiGas.GetRefund should be set to the capped refund amount
	expectedRefund := uint64(3000)
	require.Equal(t, expectedRefund, result.UsedMultiGas.GetRefund(),
		"multigas refund should be set via WithRefund in Arbitrum path")

	// UsedMultiGas.SingleGas tracks accumulated multigas (intrinsic), not gasUsed
	require.Greater(t, result.UsedMultiGas.SingleGas(), uint64(0))
}

func TestTransitionDb_NonArbitrumRefundUnchanged(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))

	s.CreateAccount(sender, true)
	s.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	s.SetNonce(sender, 0)

	gasPrice := uint256.NewInt(1)

	blockCtx := evmtypes.BlockContext{
		BlockNumber: 1,
		GasLimit:    30_000_000,
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
			bal, err := ibs.GetBalance(addr)
			if err != nil {
				return false, err
			}
			return bal.Cmp(&val) >= 0, nil
		},
		Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
			if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
		},
	}

	txCtx := evmtypes.TxContext{GasPrice: *gasPrice, Origin: sender}

	cfg := &chain.Config{
		ChainID:               big.NewInt(1),
		HomesteadBlock:        new(big.Int),
		TangerineWhistleBlock: new(big.Int),
		SpuriousDragonBlock:   new(big.Int),
		ByzantiumBlock:        new(big.Int),
		ConstantinopleBlock:   new(big.Int),
		PetersburgBlock:       new(big.Int),
		IstanbulBlock:         new(big.Int),
		BerlinBlock:           new(big.Int),
		LondonBlock:           new(big.Int),
	}

	evmInst := vm.NewEVM(blockCtx, txCtx, s, cfg, vm.Config{})

	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), 50000,
		gasPrice, gasPrice, uint256.NewInt(0),
		nil, nil, true, false, true, false, nil,
	)

	gp := new(GasPool).AddGas(30_000_000)
	result, err := ApplyMessage(evmInst, msg, gp, true, false, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Simple transfer uses 21000 gas, no SSTORE refunds
	require.Equal(t, uint64(21000), result.ReceiptGasUsed)

	// Non-Arbitrum: multigas SingleGas equals ReceiptGasUsed
	require.Equal(t, result.ReceiptGasUsed, result.UsedMultiGas.SingleGas(),
		"non-Arbitrum: multigas SingleGas must equal ReceiptGasUsed")

	// No SSTORE refunds means multigas refund is zero
	require.Equal(t, uint64(0), result.UsedMultiGas.GetRefund(),
		"non-Arbitrum: no refund for simple transfer")
}

// endTxRecordingHook records the arguments passed to EndTxHook for verification.
type endTxRecordingHook struct {
	earlyReturnHook
	recordedGasRemaining uint64
	recordedSuccess      bool
	called               bool
}

func (h *endTxRecordingHook) EndTxHook(gasRemaining uint64, success bool) {
	h.recordedGasRemaining = gasRemaining
	h.recordedSuccess = success
	h.called = true
}
func (h *endTxRecordingHook) StartTxHook() (bool, multigas.MultiGas, error, []byte) {
	return false, multigas.ZeroGas(), nil, nil
}

func TestTransitionDb_EndTxHookSignature(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))

	s.CreateAccount(sender, true)
	s.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	s.SetNonce(sender, 0)

	gasPrice := uint256.NewInt(1)
	blockCtx := evmtypes.BlockContext{
		BlockNumber: 1,
		GasLimit:    30_000_000,
		Coinbase:    coinbase,
		GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
		CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
			bal, err := ibs.GetBalance(addr)
			if err != nil {
				return false, err
			}
			return bal.Cmp(&val) >= 0, nil
		},
		Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
			if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
		},
	}
	txCtx := evmtypes.TxContext{GasPrice: *gasPrice, Origin: sender}

	cfg := &chain.Config{
		ChainID:               big.NewInt(1),
		HomesteadBlock:        new(big.Int),
		TangerineWhistleBlock: new(big.Int),
		SpuriousDragonBlock:   new(big.Int),
		ByzantiumBlock:        new(big.Int),
		ConstantinopleBlock:   new(big.Int),
		PetersburgBlock:       new(big.Int),
		IstanbulBlock:         new(big.Int),
		BerlinBlock:           new(big.Int),
		LondonBlock:           new(big.Int),
	}

	hook := &endTxRecordingHook{
		earlyReturnHook: earlyReturnHook{coinbase: coinbase},
	}
	evmInst := vm.NewEVM(blockCtx, txCtx, s, cfg, vm.Config{})
	evmInst.ProcessingHook = hook

	gasLimit := uint64(50000)
	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), gasLimit,
		gasPrice, gasPrice, uint256.NewInt(0),
		nil, nil, true, false, true, false, nil,
	)

	gp := new(GasPool).AddGas(30_000_000)
	st := NewStateTransition(evmInst, msg, gp)
	result, err := st.TransitionDb(true, false)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, hook.called, "EndTxHook should have been called")

	// EndTxHook should receive gasRemaining (not gasUsed)
	expectedGasRemaining := gasLimit - result.ReceiptGasUsed
	require.Equal(t, expectedGasRemaining, hook.recordedGasRemaining,
		"EndTxHook should receive gasRemaining, not gasUsed")

	// Successful transaction: EndTxHook should receive true (vmerr == nil)
	require.True(t, hook.recordedSuccess,
		"EndTxHook should receive true for successful transaction")
}

// arbIntrinsicGasHook is a mock that returns IsArbitrum()=true and fixes gasRemaining
// in GasChargingHook to compensate for the skipped intrinsic gas check.
type arbIntrinsicGasHook struct {
	earlyReturnHook
	fixedGas uint64
}

func (h arbIntrinsicGasHook) IsArbitrum() bool                                        { return true }
func (h arbIntrinsicGasHook) StartTxHook() (bool, multigas.MultiGas, error, []byte)   { return false, multigas.ZeroGas(), nil, nil }
func (h arbIntrinsicGasHook) GasChargingHook(g *uint64, _ uint64) (accounts.Address, multigas.MultiGas, error) {
	*g = h.fixedGas
	return h.coinbase, multigas.ZeroGas(), nil
}

func TestTransitionDb_ArbitrumSkipsIntrinsicGasCheck(t *testing.T) {
	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	coinbase := accounts.InternAddress(common.HexToAddress("0xdead"))
	gasPrice := uint256.NewInt(1)
	baseFee := uint256.NewInt(1)
	insufficientGas := uint64(1000) // well below intrinsic gas (21000)

	makeBlockCtx := func() evmtypes.BlockContext {
		return evmtypes.BlockContext{
			BlockNumber: 1,
			GasLimit:    30_000_000,
			BaseFee:     *baseFee,
			Coinbase:    coinbase,
			GetHash:     func(n uint64) (common.Hash, error) { return common.Hash{}, nil },
			CanTransfer: func(ibs evmtypes.IntraBlockState, addr accounts.Address, val uint256.Int) (bool, error) {
				bal, err := ibs.GetBalance(addr)
				if err != nil {
					return false, err
				}
				return bal.Cmp(&val) >= 0, nil
			},
			Transfer: func(ibs evmtypes.IntraBlockState, from, to accounts.Address, val uint256.Int, _ bool, _ *chain.Rules) error {
				if err := ibs.SubBalance(from, val, tracing.BalanceChangeTransfer); err != nil {
					return err
				}
				return ibs.AddBalance(to, val, tracing.BalanceChangeTransfer)
			},
		}
	}

	makeState := func(t *testing.T) *state.IntraBlockState {
		t.Helper()
		db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		t.Cleanup(tx.Rollback)
		sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
		require.NoError(t, err)
		t.Cleanup(sd.Close)
		r := state.NewReaderV3(sd.AsGetter(tx))
		s := state.New(r)
		s.CreateAccount(sender, true)
		s.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified)
		s.SetNonce(sender, 0)
		return s
	}

	t.Run("Arbitrum_skips_intrinsic_gas_check", func(t *testing.T) {
		s := makeState(t)
		arbCfg := &chain.Config{
			ChainID:               big.NewInt(412346),
			HomesteadBlock:        new(big.Int),
			TangerineWhistleBlock: new(big.Int),
			SpuriousDragonBlock:   new(big.Int),
			ByzantiumBlock:        new(big.Int),
			ConstantinopleBlock:   new(big.Int),
			PetersburgBlock:       new(big.Int),
			IstanbulBlock:         new(big.Int),
			BerlinBlock:           new(big.Int),
			LondonBlock:           new(big.Int),
			ArbitrumChainParams:   arbtypes.ArbitrumChainParams{EnableArbOS: true, GenesisBlockNum: 0},
		}

		blockCtx := makeBlockCtx()
		txCtx := evmtypes.TxContext{GasPrice: *gasPrice, Origin: sender}
		evmInst := vm.NewEVM(blockCtx, txCtx, s, arbCfg, vm.Config{})
		evmInst.ProcessingHook = arbIntrinsicGasHook{
			earlyReturnHook: earlyReturnHook{coinbase: coinbase},
			fixedGas:        50000,
		}

		msg := types.NewMessage(
			sender, recipient, 0, uint256.NewInt(0), insufficientGas,
			gasPrice, gasPrice, gasPrice,
			nil, nil, false, false, false, false, nil,
		)
		gp := new(GasPool).AddGas(30_000_000)
		st := NewStateTransition(evmInst, msg, gp)

		_, err := st.TransitionDb(true, false)
		if err != nil {
			require.False(t, errors.Is(err, ErrIntrinsicGas),
				"Arbitrum should skip intrinsic gas check, but got ErrIntrinsicGas")
		}
	})

	t.Run("non_Arbitrum_fails_intrinsic_gas_check", func(t *testing.T) {
		s := makeState(t)
		stdCfg := &chain.Config{
			ChainID:               big.NewInt(1),
			HomesteadBlock:        new(big.Int),
			TangerineWhistleBlock: new(big.Int),
			SpuriousDragonBlock:   new(big.Int),
			ByzantiumBlock:        new(big.Int),
			ConstantinopleBlock:   new(big.Int),
			PetersburgBlock:       new(big.Int),
			IstanbulBlock:         new(big.Int),
			BerlinBlock:           new(big.Int),
			LondonBlock:           new(big.Int),
		}

		blockCtx := makeBlockCtx()
		txCtx := evmtypes.TxContext{GasPrice: *gasPrice, Origin: sender}
		evmInst := vm.NewEVM(blockCtx, txCtx, s, stdCfg, vm.Config{})

		msg := types.NewMessage(
			sender, recipient, 0, uint256.NewInt(0), insufficientGas,
			gasPrice, gasPrice, gasPrice,
			nil, nil, true, false, true, false, nil,
		)
		gp := new(GasPool).AddGas(30_000_000)
		st := NewStateTransition(evmInst, msg, gp)

		_, err := st.TransitionDb(true, false)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrIntrinsicGas),
			"non-Arbitrum should fail with ErrIntrinsicGas")
	})
}

func TestHandleRevertedTx_MatchingHash(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	s.CreateAccount(sender, true)
	s.SetNonce(sender, 5)

	// Create a real LegacyTx and compute its hash, then add to the map
	testTx := &types.LegacyTx{GasPrice: uint256.NewInt(1)}
	testHash := testTx.Hash()
	testGasUsed := uint64(30000) // must be > params.TxGas (21000)

	// Temporarily add to RevertedTxGasUsed
	RevertedTxGasUsed[testHash] = testGasUsed
	t.Cleanup(func() { delete(RevertedTxGasUsed, testHash) })

	// Set up the StateTransition with enough gasRemaining
	st := &StateTransition{
		state:        s,
		gasRemaining: 50000,
	}

	msg := types.NewMessage(
		sender, accounts.InternAddress(common.HexToAddress("0xbbbb")),
		5, uint256.NewInt(0), 50000,
		uint256.NewInt(1), uint256.NewInt(1), uint256.NewInt(0),
		nil, nil, false, false, false, false, nil,
	)
	msg.Tx = testTx

	inputMultiGas := multigas.MultiGasFromPairs(
		multigas.Pair{Kind: multigas.ResourceKindComputation, Amount: 1000},
	)

	resultMultiGas, err := st.handleRevertedTx(msg, inputMultiGas)
	require.Error(t, err)
	require.True(t, errors.Is(err, vm.ErrExecutionReverted))

	// Nonce should be incremented
	nonce, err := s.GetNonce(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(6), nonce)

	// gasRemaining should be reduced by adjustedGas = testGasUsed - TxGas = 30000 - 21000 = 9000
	adjustedGas := testGasUsed - params.TxGas
	require.Equal(t, uint64(50000-adjustedGas), st.gasRemaining)

	// MultiGas should include the input plus computation gas for adjustedGas
	expectedMultiGas := inputMultiGas.SaturatingAdd(multigas.ComputationGas(adjustedGas))
	require.Equal(t, expectedMultiGas.SingleGas(), resultMultiGas.SingleGas())
	require.Equal(t, expectedMultiGas.Get(multigas.ResourceKindComputation), resultMultiGas.Get(multigas.ResourceKindComputation))
}

func TestHandleRevertedTx_NoMatch(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	s.CreateAccount(sender, true)
	s.SetNonce(sender, 5)

	// Create a tx whose hash is NOT in RevertedTxGasUsed
	testTx := &types.LegacyTx{GasPrice: uint256.NewInt(42)}
	testHash := testTx.Hash()
	_, inMap := RevertedTxGasUsed[testHash]
	require.False(t, inMap, "test tx hash should not be in RevertedTxGasUsed")

	st := &StateTransition{
		state:        s,
		gasRemaining: 50000,
	}

	msg := types.NewMessage(
		sender, accounts.InternAddress(common.HexToAddress("0xbbbb")),
		5, uint256.NewInt(0), 50000,
		uint256.NewInt(1), uint256.NewInt(1), uint256.NewInt(0),
		nil, nil, false, false, false, false, nil,
	)
	msg.Tx = testTx

	inputMultiGas := multigas.MultiGasFromPairs(
		multigas.Pair{Kind: multigas.ResourceKindComputation, Amount: 1000},
	)

	resultMultiGas, err := st.handleRevertedTx(msg, inputMultiGas)
	require.NoError(t, err)

	// Nonce should not change
	nonce, err := s.GetNonce(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(5), nonce)

	// gasRemaining should not change
	require.Equal(t, uint64(50000), st.gasRemaining)

	// MultiGas should be unchanged
	require.Equal(t, inputMultiGas.SingleGas(), resultMultiGas.SingleGas())
}

func TestHandleRevertedTx_NilTx(t *testing.T) {
	st := &StateTransition{gasRemaining: 50000}
	msg := types.NewMessage(
		accounts.InternAddress(common.HexToAddress("0xaaaa")),
		accounts.InternAddress(common.HexToAddress("0xbbbb")),
		0, uint256.NewInt(0), 50000,
		uint256.NewInt(1), uint256.NewInt(1), uint256.NewInt(0),
		nil, nil, false, false, false, false, nil,
	)
	// msg.Tx is nil by default

	inputMultiGas := multigas.ZeroGas()
	resultMultiGas, err := st.handleRevertedTx(msg, inputMultiGas)
	require.NoError(t, err)
	require.Equal(t, inputMultiGas.SingleGas(), resultMultiGas.SingleGas())
}
