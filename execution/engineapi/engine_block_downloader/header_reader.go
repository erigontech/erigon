package engine_block_downloader

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bbd"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

var _ bbd.HeaderReader = (*headerReader)(nil)

type headerReader struct {
	db          kv.RoDB
	blockReader services.HeaderReader
}

func (hr headerReader) HeaderByHash(ctx context.Context, hash common.Hash) (h *types.Header, err error) {
	err = hr.db.View(ctx, func(tx kv.Tx) error {
		h, err = hr.blockReader.HeaderByHash(ctx, tx, hash)
		return err
	})
	return h, err
}
