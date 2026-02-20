package vm

import (
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type TxProcessingHook interface {
	SetMessage(msg *types.Message, ibs evmtypes.IntraBlockState)
	IsArbitrum() bool
	FillReceiptInfo(receipt *types.Receipt)
	MsgIsNonMutating() bool

	StartTxHook() (bool, multigas.MultiGas, error, []byte)
	ScheduledTxes() types.Transactions
	EndTxHook(totalGasUsed uint64, evmSuccess bool)
	GasChargingHook(gasRemaining *uint64, intrinsicGas uint64) (accounts.Address, multigas.MultiGas, error)
	ForceRefundGas() uint64
	NonrefundableGas() uint64
	DropTip() bool
	IsCalldataPricingIncreaseEnabled() bool

	ExecuteWASM(scope *CallContext, input []byte, evm *EVM) ([]byte, error)
	PushContract(contract *Contract)
	PopContract()

	GasPriceOp(evm *EVM) *uint256.Int
	L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error)
	L1BlockHash(blockCtx evmtypes.BlockContext, l1BlocKNumber uint64) (common.Hash, error)
}

type DefaultTxProcessor struct {
	evm *EVM
}

func (p DefaultTxProcessor) IsArbitrum() bool { return false }

func (p DefaultTxProcessor) SetMessage(*types.Message, evmtypes.IntraBlockState) {}

func (p DefaultTxProcessor) StartTxHook() (bool, multigas.MultiGas, error, []byte) {
	return false, multigas.ZeroGas(), nil, nil
}

func (p DefaultTxProcessor) GasChargingHook(gasRemaining *uint64, intrinsicGas uint64) (accounts.Address, multigas.MultiGas, error) {
	return p.evm.Context.Coinbase, multigas.ZeroGas(), nil
}

func (p DefaultTxProcessor) PushContract(contract *Contract) {}

func (p DefaultTxProcessor) PopContract() {}

func (p DefaultTxProcessor) ForceRefundGas() uint64 { return 0 }

func (p DefaultTxProcessor) NonrefundableGas() uint64 { return 0 }

func (p DefaultTxProcessor) DropTip() bool { return false }

func (p DefaultTxProcessor) EndTxHook(totalGasUsed uint64, evmSuccess bool) {}

func (p DefaultTxProcessor) ScheduledTxes() types.Transactions {
	return types.Transactions{}
}

func (p DefaultTxProcessor) L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error) {
	return blockCtx.BlockNumber, nil
}

func (p DefaultTxProcessor) L1BlockHash(blockCtx evmtypes.BlockContext, l1BlocKNumber uint64) (common.Hash, error) {
	return blockCtx.GetHash(l1BlocKNumber)
}

func (p DefaultTxProcessor) GasPriceOp(evm *EVM) *uint256.Int {
	gp := evm.GasPrice
	return &gp
}

func (p DefaultTxProcessor) FillReceiptInfo(*types.Receipt) {}

func (p DefaultTxProcessor) MsgIsNonMutating() bool {
	return false
}

func (p DefaultTxProcessor) ExecuteWASM(scope *CallContext, input []byte, evm *EVM) ([]byte, error) {
	log.Crit("tried to execute WASM with default processing hook")
	return nil, nil
}

func (d DefaultTxProcessor) IsCalldataPricingIncreaseEnabled() bool {
	return true
}
