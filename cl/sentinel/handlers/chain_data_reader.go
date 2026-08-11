// Copyright 2026 The Erigon Authors
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

package handlers

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// ChainDataReader is the chain data the req/resp handlers serve to peers:
// recent blocks, light-client objects and execution payload envelopes.
type ChainDataReader interface {
	GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool)
	GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool)
	NewestLightClientUpdate() *cltypes.LightClientUpdate
	GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool)
	HasEnvelope(blockRoot common.Hash) bool
	ReadEnvelopeFromDisk(blockRoot common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error)
}
