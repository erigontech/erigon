package bbd

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
)

type headerReader interface {
	HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error)
}
