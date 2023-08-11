package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
)

func (api *ErigonImpl) CacheCheck() (*kvcache.CacheValidationResult, error) {
	cache := api.stateCache

	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}

	result, err := cache.ValidateCurrentRoot(ctx, tx)
	if err != nil {
		return nil, err
	}

	return result, nil
}
