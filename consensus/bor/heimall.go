package bor

import (
	"context"

	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
)

//go:generate mockgen -destination=../../tests/bor/mocks/IHeimdallClient.go -package=mocks . IHeimdallClient
type IHeimdallClient interface {
	Fetch(ctx context.Context, path string, query string) (*heimdall.ResponseWithHeight, error)
	FetchWithRetry(ctx context.Context, path string, query string) (*heimdall.ResponseWithHeight, error)
	FetchStateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error)
}
