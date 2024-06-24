package builder

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

//go:generate mockgen -typed=true -destination=./mock_services/builder_client_mock.go -package=mock_services . BuilderClient
type BuilderClient interface {
	RegisterValidator(ctx context.Context, registers []*cltypes.ValidatorRegistration) error
	GetHeader(ctx context.Context, slot int64, parentHash common.Hash, pubKey common.Bytes48) (*ExecutionHeader, error)
	SubmitBlindedBlocks(ctx context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, error)
	GetStatus(ctx context.Context) error
}
