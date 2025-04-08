// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"context"

	enginetypes "github.com/erigontech/erigon/turbo/engineapi/engine_types"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

type ShutterBlockBuildingCoordinator struct {
	mockCl         *MockCl
	dks            DecryptionKeysSender
	slotCalculator shutter.SlotCalculator
	instanceId     uint64
}

func NewShutterBlockBuildingCoordinator(
	cl *MockCl,
	dks DecryptionKeysSender,
	sc shutter.SlotCalculator,
	instanceId uint64,
) ShutterBlockBuildingCoordinator {
	return ShutterBlockBuildingCoordinator{
		mockCl:         cl,
		dks:            dks,
		slotCalculator: sc,
		instanceId:     instanceId,
	}
}

func (c ShutterBlockBuildingCoordinator) BuildBlock(
	ctx context.Context,
	ekg EonKeyGeneration,
	txnPointer *uint64,
	ips ...*shutter.IdentityPreimage,
) (*enginetypes.ExecutionPayload, error) {
	slot := c.slotCalculator.CalcCurrentSlot()
	err := c.dks.PublishDecryptionKeys(ctx, ekg, slot, *txnPointer, ips, c.instanceId)
	if err != nil {
		return nil, err
	}

	block, err := c.mockCl.BuildBlock(ctx, WithBlockBuildingSlot(slot))
	if err != nil {
		return nil, err
	}

	*txnPointer = *txnPointer + uint64(len(ips))
	return block, nil
}
