package rpc

// This file stores proxy-objects for `internal` package
import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
)

type CallArgs struct {
	*ethapi.CallArgs
}

func NewRevertError(result *core.ExecutionResult) *RevertError {
	return &RevertError{ethapi.NewRevertError(result)}
}

type RevertError struct {
	*ethapi.RevertError
}

type ExecutionResult struct {
	*ethapi.ExecutionResult
}

type StructLogRes struct {
	ethapi.StructLogRes
}

//nolint
func FormatLogs(logs []vm.StructLog) []ethapi.StructLogRes {
	return ethapi.FormatLogs(logs)
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
