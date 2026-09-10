// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

type revealTestClock struct {
	slot uint64
}

func (c revealTestClock) GetCurrentSlot() uint64 {
	return c.slot
}

func (revealTestClock) GetSlotTime(uint64) time.Time {
	return time.Time{}
}

func (revealTestClock) GenesisValidatorsRoot() common.Hash {
	return common.Hash{}
}

func TestRevealRunnerPrunesExpiredTrackingBeforeBlockLookup(t *testing.T) {
	runner := newRevealRunner(
		nil,
		revealTestClock{slot: 11},
		nil,
		nil,
		&runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)},
		nil,
		nil,
		time.Second,
		1,
	)
	runner.tracked[revealKey{beaconBlockRoot: common.Hash{1}}] = 10

	require.False(t, runner.SubmitAcceptedBlock(common.Hash{2}))
	require.Empty(t, runner.tracked)
}
