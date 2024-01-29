package sync

import (
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

//go:generate mockgen -destination=./db_mock.go -package=sync . DB
type HeaderIO interface {
	WriteHeaders(headers []*types.Header) error
}

type CheckpointIO interface {
	HeaderIO
	heimdall.CheckpointIO
}

type MilestoneIO interface {
	HeaderIO
	heimdall.MilestoneIO
}
