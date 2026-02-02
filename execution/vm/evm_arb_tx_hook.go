package vm

import (
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
)

func (in *EVMInterpreter) EVM() *EVM {
	return in.VM.evm
}

func (in *EVMInterpreter) ReadOnly() bool {
	return in.VM.readOnly
}

func (in *EVMInterpreter) SetReturnData(data []byte) {
	in.returnData = data
}

func (in *EVMInterpreter) GetReturnData() []byte {
	return in.returnData
}

func (in *EVMInterpreter) Config() *Config {
	c := in.evm.Config()
	return &c
}

type TxProcessingHook interface {
	// helpers
	SetMessage(msg *types.Message, ibs evmtypes.IntraBlockState)
	IsArbitrum() bool // returns true if that is arbos.TxProcessor
	FillReceiptInfo(receipt *types.Receipt)
	MsgIsNonMutating() bool

	// used within STF
	StartTxHook() (bool, multigas.MultiGas, error, []byte) // return 4-tuple rather than *struct to avoid an import cycle
	ScheduledTxes() types.Transactions
	EndTxHook(totalGasUsed uint64, evmSuccess bool)
	GasChargingHook(gasRemaining *uint64, intrinsicGas uint64) (accounts.Address, multigas.MultiGas, error)
	ForceRefundGas() uint64
	NonrefundableGas() uint64
	DropTip() bool
	IsCalldataPricingIncreaseEnabled() bool // arbos 40/pectra

	// used within evm run
	ExecuteWASM(ctx *CallContext, input []byte, interpreter *EVMInterpreter) ([]byte, error)
	PushContract(contract *Contract)
	PopContract()

	// vm ops
	GasPriceOp(evm *EVM) *uint256.Int
	L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error)
	L1BlockHash(blockCtx evmtypes.BlockContext, l1BlocKNumber uint64) (common.Hash, error)
}

type DefaultTxProcessor struct {
	evm *EVM
}

func (p DefaultTxProcessor) IsArbitrum() bool { return false }

func (p DefaultTxProcessor) SetMessage(_ *types.Message, _ evmtypes.IntraBlockState) {}

func (p DefaultTxProcessor) StartTxHook() (bool, multigas.MultiGas, error, []byte) {
	return false, multigas.ZeroGas(), nil, nil
}

func (p DefaultTxProcessor) GasChargingHook(_ *uint64, _ uint64) (accounts.Address, multigas.MultiGas, error) {
	return p.evm.Context.Coinbase, multigas.ZeroGas(), nil
}

func (p DefaultTxProcessor) PushContract(_ *Contract) {}

func (p DefaultTxProcessor) PopContract() {}

func (p DefaultTxProcessor) ForceRefundGas() uint64 { return 0 }

func (p DefaultTxProcessor) NonrefundableGas() uint64 { return 0 }

func (p DefaultTxProcessor) DropTip() bool { return false }

func (p DefaultTxProcessor) EndTxHook(_ uint64, _ bool) {}

func (p DefaultTxProcessor) ScheduledTxes() types.Transactions {
	return types.Transactions{}
}

func (p DefaultTxProcessor) L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error) {
	return blockCtx.BlockNumber, nil
}

func (p DefaultTxProcessor) L1BlockHash(blockCtx evmtypes.BlockContext, l1BlocKNumber uint64) (common.Hash, error) {
	return blockCtx.GetHash(l1BlocKNumber)
}

func (p DefaultTxProcessor) GasPriceOp(_ *EVM) uint256.Int {
	return p.evm.GasPrice
}

func (p DefaultTxProcessor) FillReceiptInfo(_ *types.Receipt) {}

func (p DefaultTxProcessor) MsgIsNonMutating() bool {
	return false
}

func (p DefaultTxProcessor) ExecuteWASM(_ *CallContext, _ []byte, _ *EVMInterpreter) ([]byte, error) {
	log.Crit("tried to execute WASM with default processing hook")
	return nil, nil
}

func (d DefaultTxProcessor) IsCalldataPricingIncreaseEnabled() bool {
	return true
}
