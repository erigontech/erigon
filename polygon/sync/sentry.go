package sync

import (
	"context"

	erigonlibtypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/core/types"
)

type Sentry interface {
	PeersWithMinBlock(num uint64) []*erigonlibtypes.PeerInfo
	DownloadHeaders(ctx context.Context, start uint64, end uint64, peerID string) ([]*types.Header, error)
	Penalize(peerID string)
}
