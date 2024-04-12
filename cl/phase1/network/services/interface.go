package services

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// Note: BlobSidecarService and BlockService are tested in spectests

type Service[T any] interface {
	ProcessMessage(ctx context.Context, subnet *uint64, msg T) error
}

type BlockService Service[*cltypes.SignedBeaconBlock]
type BlobSidecarsService Service[*cltypes.BlobSidecar]
type SyncCommitteeMessagesService Service[*cltypes.SyncCommitteeMessage]
