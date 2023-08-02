package eth1_utils

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/core/types"
)

const retryTimeout = 10 * time.Millisecond

func InsertHeadersAndWait(ctx context.Context, executionModule execution.ExecutionClient, headers []*types.Header) error {
	request := &execution.InsertHeadersRequest{
		Headers: HeadersToHeadersRPC(headers),
	}
	response, err := executionModule.InsertHeaders(ctx, request)
	if err != nil {
		return err
	}
	retryInterval := time.NewTicker(retryTimeout)
	defer retryInterval.Stop()
	for response.Result == execution.ExecutionStatus_Busy {
		select {
		case <-retryInterval.C:
			response, err = executionModule.InsertHeaders(ctx, request)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return context.Canceled
		}
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("insertHeadersAndWait: invalid code recieved from execution module: %s", response.Result.String())
	}
	return nil
}

func InsertBodiesAndWait(ctx context.Context, executionModule execution.ExecutionClient, bodies []*types.RawBody, blockNumbers []uint64, blockHashes []libcommon.Hash) error {
	request := &execution.InsertBodiesRequest{
		Bodies: ConvertRawBlockBodiesToRpc(bodies, blockNumbers, blockHashes),
	}
	response, err := executionModule.InsertBodies(ctx, request)
	if err != nil {
		return err
	}
	retryInterval := time.NewTicker(retryTimeout)
	defer retryInterval.Stop()
	for response.Result == execution.ExecutionStatus_Busy {
		select {
		case <-retryInterval.C:
			response, err = executionModule.InsertBodies(ctx, request)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return context.Canceled
		}
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("InsertBodiesAndWait: invalid code recieved from execution module: %s", response.Result.String())
	}
	return nil
}

func InsertHeaderAndWait(ctx context.Context, executionModule execution.ExecutionClient, header *types.Header) error {
	return InsertHeadersAndWait(ctx, executionModule, []*types.Header{header})
}

func InsertBodyAndWait(ctx context.Context, executionModule execution.ExecutionClient, body *types.RawBody, blockNumber uint64, blockHash libcommon.Hash) error {
	return InsertBodiesAndWait(ctx, executionModule, []*types.RawBody{body}, []uint64{blockNumber}, []libcommon.Hash{blockHash})
}

func InsertHeaderAndBodyAndWait(ctx context.Context, executionModule execution.ExecutionClient, header *types.Header, body *types.RawBody) error {
	if err := InsertHeaderAndWait(ctx, executionModule, header); err != nil {
		return err
	}
	return InsertBodyAndWait(ctx, executionModule, body, header.Number.Uint64(), header.Hash())
}

func ValidateChain(ctx context.Context, executionModule execution.ExecutionClient, hash libcommon.Hash, number uint64) (execution.ExecutionStatus, libcommon.Hash, error) {
	resp, err := executionModule.ValidateChain(ctx, &execution.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(hash),
		Number: number,
	})
	if err != nil {
		return 0, libcommon.Hash{}, err
	}
	return resp.ValidationStatus, gointerfaces.ConvertH256ToHash(resp.LatestValidHash), err
}

func UpdateForkChoice(ctx context.Context, executionModule execution.ExecutionClient,
	headHash, safeHash, finalizeHash libcommon.Hash,
	timeout uint64) (execution.ExecutionStatus, libcommon.Hash, error) {
	resp, err := executionModule.UpdateForkChoice(ctx, &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(headHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(safeHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizeHash),
		Timeout:            timeout,
	})
	if err != nil {
		return 0, libcommon.Hash{}, err
	}
	return resp.Status, gointerfaces.ConvertH256ToHash(resp.LatestValidHash), err
}
