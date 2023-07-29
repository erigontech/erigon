package bor

import (
	"context"

	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
)

//go:generate mockgen -destination=../../tests/bor/mocks/IHeimdallClient.go -package=mocks . IHeimdallClient
type IHeimdallClient interface {
	StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error)
	Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error)
	FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	Close()
}

type HeimdallServer interface {
	StateSyncEvents(ctx context.Context, fromID uint64, to int64, limit int) (uint64, []*clerk.EventRecordWithTime, error)
	Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error)
	FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	Close()
}
