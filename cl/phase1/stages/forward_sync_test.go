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

package stages

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
)

// currentSlot can overshoot the captured chainTipSlot; slotsRemaining must clamp
// to 0 rather than underflow into a ~2^64 slot count.
func TestForwardSyncProgress_CurrentSlotPastChainTip(t *testing.T) {
	slotsRemaining, ratePerSec := forwardSyncProgress(1_000_000, 1_000_050, 999_900, 30)
	if slotsRemaining != 0 {
		t.Fatalf("slotsRemaining = %d, want 0 when current slot is past the tip", slotsRemaining)
	}
	if ratePerSec < 0 {
		t.Fatalf("ratePerSec must not be negative, got %g", ratePerSec)
	}
}

// A reorg can drop currentSlot below prevProgress; the rate must clamp to 0
// rather than underflow the slots-processed denominator.
func TestForwardSyncProgress_ReorgBelowPrevProgress(t *testing.T) {
	slotsRemaining, ratePerSec := forwardSyncProgress(1_000_000, 900_000, 950_000, 30)
	if slotsRemaining != 100_000 {
		t.Fatalf("slotsRemaining = %d, want 100000", slotsRemaining)
	}
	if ratePerSec != 0 {
		t.Fatalf("ratePerSec = %g, want 0 when current slot is below prev progress", ratePerSec)
	}
}

// Normal case: slotsRemaining is the tip gap, rate is slots processed per second.
func TestForwardSyncProgress_Normal(t *testing.T) {
	slotsRemaining, ratePerSec := forwardSyncProgress(1_000_000, 900_000, 899_700, 30)
	if slotsRemaining != 100_000 {
		t.Fatalf("slotsRemaining = %d, want 100000", slotsRemaining)
	}
	if ratePerSec != 10 { // (900000-899700)/30
		t.Fatalf("ratePerSec = %g, want 10", ratePerSec)
	}
}

func TestForwardSyncFlushesBlockCollectorAfterInsertion(t *testing.T) {
	for _, test := range []struct {
		name             string
		supportInsertion bool
		canceled         bool
		flushErr         error
		wantFlushes      int
	}{
		{name: "insertion supported", supportInsertion: true, wantFlushes: 1},
		{name: "flush failure", supportInsertion: true, flushErr: errors.New("flush failed"), wantFlushes: 1},
		{name: "canceled", supportInsertion: true, canceled: true},
		{name: "insertion unsupported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, _, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
			anchorSlot := cfg.forkChoice.AnchorSlot()
			cfg.beaconCfg.GloasForkEpoch = anchorSlot/cfg.beaconCfg.SlotsPerEpoch + 1
			cfg.beaconCfg.InitializeForkSchedule()
			engine.supportInsertion = test.supportInsertion
			collector := &gloasCollectorTest{flushErr: test.flushErr}
			cfg.blockCollector = collector
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentEpoch().Return(anchorSlot / cfg.beaconCfg.SlotsPerEpoch)
			clock.EXPECT().GetCurrentSlot().Return(anchorSlot)
			cfg.ethClock = clock

			ctx := t.Context()
			if test.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			require.NoError(t, forwardSync(ctx, log.Root(), cfg, Args{}))
			require.Equal(t, test.wantFlushes, collector.flushCalls)
		})
	}
}
