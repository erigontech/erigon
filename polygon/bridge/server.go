package bridge

import (
	"context"

	libcommon "github.com/erigontech/erigon-lib/common"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/core/types"
)

type bridgeReader interface {
	Events(ctx context.Context, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error)
}

var BorBackendAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}
