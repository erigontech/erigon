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

package mock_services

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// ChainDataReaderMock is a map-backed fake for handlers.ChainDataReader.
type ChainDataReaderMock struct {
	Blocks                map[common.Hash]*cltypes.SignedBeaconBlock
	LightClientBootstraps map[common.Hash]*cltypes.LightClientBootstrap
	NewestLCUpdate        *cltypes.LightClientUpdate
	LCUpdates             map[uint64]*cltypes.LightClientUpdate
	Envelopes             map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
}

func NewChainDataReaderMock() *ChainDataReaderMock {
	return &ChainDataReaderMock{
		Blocks:                make(map[common.Hash]*cltypes.SignedBeaconBlock),
		LightClientBootstraps: make(map[common.Hash]*cltypes.LightClientBootstrap),
		LCUpdates:             make(map[uint64]*cltypes.LightClientUpdate),
		Envelopes:             make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
}

func (m *ChainDataReaderMock) GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return m.Blocks[blockRoot], m.Blocks[blockRoot] != nil
}

func (m *ChainDataReaderMock) GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool) {
	return m.LightClientBootstraps[blockRoot], m.LightClientBootstraps[blockRoot] != nil
}

func (m *ChainDataReaderMock) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	return m.NewestLCUpdate
}

func (m *ChainDataReaderMock) GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool) {
	return m.LCUpdates[period], m.LCUpdates[period] != nil
}

func (m *ChainDataReaderMock) HasEnvelope(blockRoot common.Hash) bool {
	_, ok := m.Envelopes[blockRoot]
	return ok
}

func (m *ChainDataReaderMock) ReadEnvelopeFromDisk(blockRoot common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return m.Envelopes[blockRoot], nil
}
