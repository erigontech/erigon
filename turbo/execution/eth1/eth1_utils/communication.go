package eth1_utils

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
	for response.Result == execution.ValidationStatus_Busy {
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
	if response.Result != execution.ValidationStatus_Success {
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
	for response.Result == execution.ValidationStatus_Busy {
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
	if response.Result != execution.ValidationStatus_Success {
		return fmt.Errorf("InsertBodiesAndWait: invalid code recieved from execution module: %s", response.Result.String())
	}
	return nil
}
