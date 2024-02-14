package sync

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./sentry_mock.go -package=sync . Sentry
type Sentry interface {
	MaxPeers() int
	PeersWithBlockNumInfo() PeersWithBlockNumInfo
	DownloadHeaders(ctx context.Context, start *big.Int, end *big.Int, peerID string) ([]*types.Header, error)
	Penalize(peerID string)
}
