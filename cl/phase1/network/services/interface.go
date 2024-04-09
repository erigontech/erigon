package services

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

type Service[T any] interface {
	ProcessMessage(ctx context.Context, msg T) error
}

type BlockService Service[*cltypes.SignedBeaconBlock]
type BlobSidecarsService Service[*cltypes.BlobSidecar]
