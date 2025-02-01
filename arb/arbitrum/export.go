package arbitrum

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
)

type TransactionArgs = ethapi.SendTxArgs

// func (args *TransactionArgs) ToTransaction() types.Transaction {
// 	return args.ToTransaction()
// }

type TransactionArgs2 = ethapi.CallArgs

func EstimateGas(
		ctx context.Context,
		b *APIBackend,
		args TransactionArgs2,
		blockNrOrHash rpc.BlockNumberOrHash,
		overrides *ethapi.StateOverrides,
		gasCap uint64,
) (hexutil.Uint64, error) {
	return ethapi.DoEstimateGas(ctx, b, args, blockNrOrHash, overrides, gasCap)
}

// todo can use following?
// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
// func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Uint64, error) {

func NewRevertReason(result *evmtypes.ExecutionResult) error {
	return ethapi.NewRevertError(result)
}
