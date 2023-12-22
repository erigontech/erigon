package sync

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/sync/peerinfo"
)

//go:generate mockgen -destination=./mock/sentry_mock.go -package=mock . Sentry
type Sentry interface {
	MaxPeers() int
	PeersWithBlockNumInfo() peerinfo.PeersWithBlockNumInfo
	DownloadHeaders(ctx context.Context, start *big.Int, end *big.Int, peerID string) ([]*types.Header, error)
	Penalize(peerID string)
}
