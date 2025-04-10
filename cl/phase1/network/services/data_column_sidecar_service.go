package services

import (
	"context"

	"github.com/erigontech/erigon/cl/cltypes"
)

type dataColumnSidecarService struct {
}

func NewDataColumnSidecarService() DataColumnSidecarService {
	return &dataColumnSidecarService{}
}

func (s *dataColumnSidecarService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.DataColumnSidecar) error {
	return nil
}
