package rpc

// This file stores proxy-objects for `internal` package
import (
	"github.com/ledgerwatch/turbo-geth/core"
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
	*ethapi.StructLogRes
}
