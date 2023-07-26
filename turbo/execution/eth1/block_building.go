package eth1

import (
	"context"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
)

func (e *EthereumExecutionModule) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !e.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if e.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

func (e *EthereumExecutionModule) evictOldBuilders() {
	ids := common.SortedKeys(e.builders)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(e.builders)-engine_helpers.MaxBuilders; i++ {
		delete(e.builders, ids[i])
	}
}

// Missing: NewPayload, AssembleBlock
func (e *EthereumExecutionModule) AssembleBlock(ctx context.Context, req *execution.AssembleBlockRequest) (*execution.PayloadId, error) {
	param := core.BlockBuilderParameters{
		ParentHash:            gointerfaces.ConvertH256ToHash(req.ParentHash),
		Timestamp:             req.Timestamp,
		PrevRandao:            gointerfaces.ConvertH256ToHash(req.MixDigest),
		SuggestedFeeRecipient: gointerfaces.ConvertH160toAddress(req.SuggestedFeeRecipent),
		Withdrawals:           ConvertWithdrawalsFromRpc(req.Withdrawals),
	}

	if err := e.checkWithdrawalsPresence(param.Timestamp, param.Withdrawals); err != nil {
		return nil, err
	}

	// First check if we're already building a block with the requested parameters
	if reflect.DeepEqual(e.lastParameters, &param) {
		e.logger.Info("[ForkChoiceUpdated] duplicate build request")
		return &execution.PayloadId{
			Id: e.nextPayloadId,
		}, nil
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	param.PayloadId = e.nextPayloadId
	e.lastParameters = &param

	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, &param)
	e.logger.Info("[ForkChoiceUpdated] BlockBuilder added", "payload", e.nextPayloadId)

	return &execution.PayloadId{
		Id: e.nextPayloadId,
	}, nil
}

func (e *EthereumExecutionModule) GetAssembledBlock(ctx context.Context, req *execution.PayloadId) (*execution.GetAssembledBlockResponse, error) {

}
