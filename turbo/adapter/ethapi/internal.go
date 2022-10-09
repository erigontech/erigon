package ethapi

// This file stores proxy-objects for `internal` package
import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/internal/ethapi"
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

// nolint
func RPCMarshalHeader(head *types.Header) map[string]interface{} {
	return ethapi.RPCMarshalHeader(head)
}

// nolint
func RPCMarshalBlock(b *types.Block, inclTx bool, fullTx bool, additional map[string]interface{}) (map[string]interface{}, error) {
	fields, err := ethapi.RPCMarshalBlock(b, inclTx, fullTx)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}

// nolint
func RPCMarshalBlockEx(b *types.Block, inclTx bool, fullTx bool, borTx types.Transaction, borTxHash common.Hash, additional map[string]interface{}) (map[string]interface{}, error) {
	fields, err := ethapi.RPCMarshalBlockEx(b, inclTx, fullTx, borTx, borTxHash)
	if err != nil {
		return nil, err
	}

	for k, v := range additional {
		fields[k] = v
	}

	return fields, err
}

// nolint
type RPCTransaction struct {
	*ethapi.RPCTransaction
}
