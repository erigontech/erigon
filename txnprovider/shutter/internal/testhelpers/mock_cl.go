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
	"errors"

	"github.com/erigontech/erigon/common/log/v3"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	executiontests "github.com/erigontech/erigon/execution/tests"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

type MockCl struct {
	logger         log.Logger
	base           *executiontests.MockCl
	slotCalculator shutter.SlotCalculator
	initialised    bool
}

func NewMockCl(logger log.Logger, base *executiontests.MockCl, sc shutter.SlotCalculator) *MockCl {
	return &MockCl{logger: logger, base: base, slotCalculator: sc}
}

func (cl *MockCl) Initialise(ctx context.Context) error {
	if cl.initialised {
		return nil
	}
	cl.logger.Debug("[shutter-mock-cl] initialising with an empty block")
	// we do this to ensure that the timestamp of the payload does not overlap with the previously built block
	// by the base mock cl which does not align the timestamps to the slot boundaries
	slot := cl.slotCalculator.CalcCurrentSlot() + 2
	payloadRes, err := cl.base.BuildCanonicalBlock(
		ctx,
		executiontests.WithTimestamp(cl.slotCalculator.CalcSlotStartTimestamp(slot)),
		executiontests.WithWaitUntilTimestamp(),
	)
	if err != nil {
		return err
	}
	if len(payloadRes.ExecutionPayload.Transactions) > 0 {
		return errors.New("shutter mock cl is not initialised with an empty block, call initialise before submitting txns")
	}
	cl.initialised = true
	return nil
}

func (cl *MockCl) BuildBlock(ctx context.Context, opts ...BlockBuildingOption) (*enginetypes.ExecutionPayload, error) {
	if !cl.initialised {
		return nil, errors.New("shutter mock cl is not initialised with an empty block")
	}
	options := cl.applyBlockBuildingOptions(opts...)
	timestamp := cl.slotCalculator.CalcSlotStartTimestamp(options.slot)
	cl.logger.Debug("[shutter-mock-cl] building block", "slot", options.slot, "timestamp", timestamp)
	payloadRes, err := cl.base.BuildCanonicalBlock(
		ctx,
		executiontests.WithTimestamp(timestamp),
		executiontests.WithWaitUntilTimestamp(),
	)
	if err != nil {
		return nil, err
	}
	return payloadRes.ExecutionPayload, nil
}

func (cl *MockCl) applyBlockBuildingOptions(opts ...BlockBuildingOption) blockBuildingOptions {
	defaultOptions := blockBuildingOptions{
		slot: cl.slotCalculator.CalcCurrentSlot() + 1,
	}
	for _, opt := range opts {
		opt(&defaultOptions)
	}
	return defaultOptions
}

type BlockBuildingOption func(*blockBuildingOptions)

func WithBlockBuildingSlot(slot uint64) BlockBuildingOption {
	return func(opts *blockBuildingOptions) {
		opts.slot = slot
	}
}

type blockBuildingOptions struct {
	slot uint64
}
