package services

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

func unverifiedGloasPayloadError(store forkchoice.ForkChoiceStorageReader, blockRoot common.Hash) error {
	status, ok := store.GetRecentExecutionPayloadStatusByRoot(blockRoot)
	if ok && status == execution_client.PayloadStatusInvalidated {
		return errors.New("execution payload is invalid")
	}
	return fmt.Errorf("%w: execution payload not verified for block %v", ErrIgnore, blockRoot)
}
