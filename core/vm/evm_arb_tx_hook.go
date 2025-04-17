package vm

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

// Depth returns the current depth
func (evm *EVM) Depth() int {
	return evm.interpreter.Depth()
}

func (evm *EVM) IncrementDepth() {
	evm.interpreter.(*EVMInterpreter).IncrementDepth()
}

func (evm *EVM) DecrementDepth() {
	evm.interpreter.(*EVMInterpreter).DecrementDepth()
}

type TxProcessingHook interface {
	IsArbitrum() bool                           // returns true if that is arbos.TxProcessor
	StartTxHook() (bool, uint64, error, []byte) // return 4-tuple rather than *struct to avoid an import cycle
	GasChargingHook(gasRemaining *uint64) (common.Address, error)
	PushContract(contract *Contract)
	PopContract()
	ForceRefundGas() uint64

	SetMessage(msg *types.Message, ibs evmtypes.IntraBlockState)
	NonrefundableGas() uint64
	DropTip() bool
	EndTxHook(totalGasUsed uint64, evmSuccess bool)
	ScheduledTxes() types.Transactions
	L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error)
	L1BlockHash(blockCtx evmtypes.BlockContext, l1BlocKNumber uint64) (common.Hash, error)
	GasPriceOp(evm *EVM) *big.Int
	FillReceiptInfo(receipt *types.Receipt)
	MsgIsNonMutating() bool
	ExecuteWASM(scope *ScopeContext, input []byte, interpreter *EVMInterpreter) ([]byte, error)
}

type DefaultTxProcessor struct {
	evm *EVM
}

func (p DefaultTxProcessor) IsArbitrum() bool { return false }

func (p DefaultTxProcessor) SetMessage(*types.Message, evmtypes.IntraBlockState) {}

func (p DefaultTxProcessor) StartTxHook() (bool, uint64, error, []byte) {
	return false, 0, nil, nil
}

func (p DefaultTxProcessor) GasChargingHook(gasRemaining *uint64) (common.Address, error) {
	return p.evm.Context.Coinbase, nil
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
	return blockCtx.GetHash(l1BlocKNumber), nil
}

func (p DefaultTxProcessor) GasPriceOp(evm *EVM) *big.Int {
	return evm.GasPrice.ToBig()
}

func (p DefaultTxProcessor) FillReceiptInfo(*types.Receipt) {}

func (p DefaultTxProcessor) MsgIsNonMutating() bool {
	return false
}

func (p DefaultTxProcessor) ExecuteWASM(scope *ScopeContext, input []byte, interpreter *EVMInterpreter) ([]byte, error) {
	log.Crit("tried to execute WASM with default processing hook")
	return nil, nil
}
