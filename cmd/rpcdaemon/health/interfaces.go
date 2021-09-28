package health

import (
	"context"

	"github.com/ledgerwatch/erigon/common/hexutil"
)

type NetAPI interface {
	PeerCount(_ context.Context) (hexutil.Uint, error)
}
