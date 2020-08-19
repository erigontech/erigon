package ethapi

// This file stores proxy-objects for `internal` package
import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
)

// This package provides copy-paste and proxy objects to "internal/ethapi" package

func NewRevertError(result *core.ExecutionResult) *RevertError {
	return &RevertError{ethapi.NewRevertError(result)}
}

type RevertError struct {
	*ethapi.RevertError
}

type CallArgs struct {
	*ethapi.CallArgs
}

type ExecutionResult struct {
	*ethapi.ExecutionResult
}

//nolint
func RPCMarshalHeader(head *types.Header) map[string]interface{} {
	return ethapi.RPCMarshalHeader(head)
}

//nolint
func RPCMarshalBlock(block *types.Block, inclTx bool, fullTx bool) (map[string]interface{}, error) {
	return ethapi.RPCMarshalBlock(block, inclTx, fullTx)
}

//nolint
type RPCTransaction struct {
	*ethapi.RPCTransaction
}
