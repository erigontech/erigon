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

package commitment

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// A zero-value field means "unset", so the constructor's fallbacks must resolve
// to DefaultAdaptivePinControllerConfig — one policy, not two that drift apart
// when the defaults are tuned.
func TestNewAdaptivePinController_ZeroConfigResolvesToDefaults(t *testing.T) {
	c := NewAdaptivePinController(NewBranchCache(64), AdaptivePinControllerConfig{}, log.Root())
	if want := DefaultAdaptivePinControllerConfig(); c.cfg != want {
		t.Fatalf("zero-value config resolved to %+v, want %+v", c.cfg, want)
	}
}

// An explicitly-set field must survive the fallbacks.
func TestNewAdaptivePinController_ExplicitConfigWins(t *testing.T) {
	cfg := AdaptivePinControllerConfig{
		PromoteThresholdMisses:    7,
		MaxPromotedContracts:      3,
		DemoteCooldownBlocks:      11,
		InitialViewBudgetBytes:    1 << 20,
		ExtensionBudgetBytes:      2 << 20,
		PerContractMaxBudgetBytes: 3 << 20,
	}
	c := NewAdaptivePinController(NewBranchCache(64), cfg, log.Root())
	if c.cfg != cfg {
		t.Fatalf("explicit config was overwritten: got %+v, want %+v", c.cfg, cfg)
	}
}

func TestAdaptivePinPlanDoesNotMutateCacheBeforePublication(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	publisher := branchCache.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.MaxPromotedContracts = 1
	controller := NewAdaptivePinController(branchCache, cfg, log.Root())

	var contractHash [32]byte
	contractHash[0] = 1
	prefix := nibbles.HexToCompact(ContractNibbles(contractHash[:]))
	controller.onCacheMiss(prefix)
	reader := func(key []byte) ([]byte, uint64, bool, error) {
		if !bytes.Equal(key, prefix) {
			return nil, 0, false, nil
		}
		return []byte{0, 0, 0, 0}, 1, true, nil
	}

	plan := controller.PlanBlock(1, reader, nil, nil)
	_, _, ok := branchCache.Get(prefix)
	require.False(t, ok, "planning from an uncommitted transaction must not change BranchCache")
	plan.Abort()
	require.Empty(t, controller.states, "aborting the database transaction must restore controller state")

	plan = controller.PlanBlock(2, reader, nil, nil)
	publication := publisher.Begin()
	publication.Publish(testBranchGeneration(2), nil, false, plan)
	plan.Commit()

	_, _, ok = branchCache.View(testBranchGeneration(2)).Get(prefix)
	require.True(t, ok, "publication must apply the staged pin")
}
