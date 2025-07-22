// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package builder

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

//go:generate mockgen -typed=true -destination=./mock_services/builder_client_mock.go -package=mock_services . BuilderClient
type BuilderClient interface {
	RegisterValidator(ctx context.Context, registers []*cltypes.ValidatorRegistration) error
	GetHeader(ctx context.Context, slot int64, parentHash common.Hash, pubKey common.Bytes48) (*ExecutionHeader, error)
	SubmitBlindedBlocks(ctx context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *cltypes.ExecutionRequests, error)
	GetStatus(ctx context.Context) error
}
