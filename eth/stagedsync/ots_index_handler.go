package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type ResourceAwareIndexHandler interface {
	Flush(force bool) error
	Load(ctx context.Context, tx kv.RwTx) error
	Close()
}

// An IndexHandler handle a session of addresses indexing
type IndexHandler interface {
	ResourceAwareIndexHandler
	TouchIndex(addr common.Address, idx uint64)
}
