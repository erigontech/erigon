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
	"time"

	"github.com/erigontech/erigon/common"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
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
	// we send them for the next slot and wait for 1 slot duration to allow time for the keys to be received
	slot := c.slotCalculator.CalcCurrentSlot()
	nextSlot := slot + 1
	err := c.dks.PublishDecryptionKeys(ctx, ekg, nextSlot, *txnPointer, ips, c.instanceId)
	if err != nil {
		return nil, err
	}

	err = common.Sleep(ctx, time.Duration(c.slotCalculator.SecondsPerSlot())*time.Second)
	if err != nil {
		return nil, err
	}

	block, err := c.mockCl.BuildBlock(ctx, WithBlockBuildingSlot(nextSlot))
	if err != nil {
		return nil, err
	}

	*txnPointer = *txnPointer + uint64(len(ips))
	return block, nil
}
