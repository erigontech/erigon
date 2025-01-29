package arbitrum

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

type TransactionArgs = ethapi.SendTxArgs

// func (args *TransactionArgs) ToTransaction() types.Transaction {
// 	return args.ToTransaction()
// }

// type TransactionArgs = ethapi.CallArgs

func EstimateGas(
	ctx context.Context,
	b rpchelper.ApiBackend,
	args TransactionArgs,
	blockNrOrHash rpc.BlockNumberOrHash,
	overrides *ethapi.StateOverrides,
	gasCap uint64,
) (hexutil.Uint64, error) {
	return ethapi.DoEstimateGas(ctx, b, args, blockNrOrHash, overrides, gasCap)
}

func NewRevertReason(result *evmtypes.ExecutionResult) error {
	return ethapi.NewRevertError(result)
}
