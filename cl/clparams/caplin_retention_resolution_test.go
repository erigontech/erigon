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

package clparams

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveRetention(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	oldest := OldestStoredResult{
		Blocks:   StoredSlot{Slot: 100, Holds: true},
		Sidecars: StoredSlot{Slot: 200, Holds: true},
		States:   StoredSlot{Slot: 300, Holds: true},
	}
	spec := CaplinRetention{Mode: CaplinPruneModeSpec}
	archive := CaplinRetention{Mode: CaplinPruneModeArchive}
	prunedAllBlocks := CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor()}
	fixed := CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinSlotFloor(100_000), Sidecars: CaplinSlotFloor(100_000), States: CaplinDistanceFloor(0)}
	laterFixed := CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinSlotFloor(200_000), Sidecars: CaplinSlotFloor(200_000), States: CaplinDistanceFloor(0)}
	tests := []struct {
		name       string
		stored     CaplinRetention
		found      bool
		requested  CaplinRetention
		source     CaplinRetentionSource
		oldest     OldestStoredResult
		want       CaplinRetention
		wantAction CaplinRetentionAction
		wantError  string
	}{
		{name: "first default", requested: spec, source: CaplinRetentionSourceDefault, want: specRetention(t, spec, &beaconCfg), wantAction: CaplinRetentionActionStore},
		{name: "first new flags", requested: archive, source: CaplinRetentionSourceNewFlags, want: specRetention(t, archive, &beaconCfg), wantAction: CaplinRetentionActionStore},
		{name: "first legacy exact", requested: archive, source: CaplinRetentionSourceLegacyExact, want: specRetention(t, archive, &beaconCfg), wantAction: CaplinRetentionActionStore},
		{name: "first legacy inexact with data", requested: spec, source: CaplinRetentionSourceLegacyInexact, oldest: oldest, want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinSlotFloor(100), Sidecars: CaplinSlotFloor(200), States: CaplinSlotFloor(300)}, wantAction: CaplinRetentionActionStore},
		{name: "first legacy inexact empty", requested: spec, source: CaplinRetentionSourceLegacyInexact, wantError: "capcli seg regime set"},
		{name: "inherit default", stored: archive, found: true, requested: spec, source: CaplinRetentionSourceDefault, want: specRetention(t, archive, &beaconCfg), wantAction: CaplinRetentionActionKeep},
		{name: "inherit legacy inexact", stored: archive, found: true, requested: spec, source: CaplinRetentionSourceLegacyInexact, want: specRetention(t, archive, &beaconCfg), wantAction: CaplinRetentionActionKeep},
		{name: "equal floors", stored: spec, found: true, requested: spec, source: CaplinRetentionSourceNewFlags, want: specRetention(t, spec, &beaconCfg), wantAction: CaplinRetentionActionKeep},
		{name: "toward tip archive to spec", stored: archive, found: true, requested: spec, source: CaplinRetentionSourceNewFlags, want: specRetention(t, spec, &beaconCfg), wantAction: CaplinRetentionActionStore},
		{name: "toward tip pruned blocks to spec", stored: prunedAllBlocks, found: true, requested: spec, source: CaplinRetentionSourceLegacyExact, want: specRetention(t, spec, &beaconCfg), wantAction: CaplinRetentionActionStore},
		{name: "toward tip later fixed slot", stored: fixed, found: true, requested: laterFixed, source: CaplinRetentionSourceLegacyExact, want: laterFixed, wantAction: CaplinRetentionActionStore},
		{name: "away from tip spec to archive", stored: spec, found: true, requested: archive, source: CaplinRetentionSourceNewFlags, wantError: "capcli seg regime set"},
		{name: "away from tip fixed slot to all", stored: fixed, found: true, requested: archive, source: CaplinRetentionSourceLegacyExact, wantError: "capcli seg regime set"},
		{name: "invalid stored", stored: CaplinRetention{Mode: CaplinPruneMode(99)}, found: true, requested: spec, source: CaplinRetentionSourceDefault, wantError: "invalid caplin prune mode"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, action, err := ResolveRetention(test.stored, test.found, test.requested, test.source, test.oldest, 15_660_000, &beaconCfg, "erigon", "<dir>")
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				if test.name != "invalid stored" {
					require.True(t, strings.Contains(err.Error(), "capcli seg regime set"))
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			require.Equal(t, test.wantAction, action)
		})
	}
}

func TestResolveRetentionRejectsInferredNesting(t *testing.T) {
	oldest := OldestStoredResult{
		Blocks:   StoredSlot{Slot: 14_000_000, Holds: true},
		Sidecars: StoredSlot{Slot: 13_000_000, Holds: true},
	}
	_, action, err := ResolveRetention(
		CaplinRetention{}, false, CaplinRetention{Mode: CaplinPruneModeSpec}, CaplinRetentionSourceLegacyInexact,
		oldest, 15_660_000, &MainnetBeaconConfig, "erigon", "<dir>",
	)
	require.Equal(t, CaplinRetentionActionRefuse, action)
	require.ErrorContains(t, err, "sidecars reach below blocks")
}

func specRetention(t *testing.T, retention CaplinRetention, beaconCfg *BeaconChainConfig) CaplinRetention {
	t.Helper()
	normalized, err := retention.Normalize(beaconCfg)
	require.NoError(t, err)
	return normalized
}

func TestHintPreservesFixedSidecar(t *testing.T) {
	cfg := &MainnetBeaconConfig
	target, exact, err := LegacyRetention(CaplinConfig{ArchiveBlocks: true, ArchiveBlobs: true, BlobsArchiveFromSlot: 14_000_000}, cfg)
	require.NoError(t, err)
	require.True(t, exact)
	_, _, err = ResolveRetention(CaplinRetention{Mode: CaplinPruneModeSpec}, true, target, CaplinRetentionSourceLegacyExact, OldestStoredResult{}, 15_660_000, cfg, "erigon", "<dir>")
	require.ErrorContains(t, err, "capcli seg regime set")
	_, start, ok := strings.Cut(err.Error(), "; erigon ")
	require.True(t, ok)
	knobs := map[string]string{}
	for flag := range strings.FieldsSeq(start) {
		name, value, _ := strings.Cut(flag, "=")
		knobs[strings.TrimPrefix(name, "--caplin.prune.")] = value
	}
	if _, explicit := knobs["mode"]; !explicit {
		return
	}
	got, err := ParseCaplinRetention(knobs["mode"], knobs["distance"], knobs["sidecars"], knobs["states"])
	require.NoError(t, err)
	if got.Sidecars != target.Sidecars {
		t.Fatalf("restart asks for %s, want %s", got.Sidecars, target.Sidecars)
	}
}

func TestHintResetsMissingPrefix(t *testing.T) {
	first := StoredSlot{Slot: 14_000_000, Holds: true}
	_, _, err := ResolveRetention(CaplinRetention{Mode: CaplinPruneModeSpec}, true, CaplinRetention{Mode: CaplinPruneModeArchive}, CaplinRetentionSourceNewFlags, OldestStoredResult{Blocks: first, Sidecars: first, States: first}, 15_660_000, &MainnetBeaconConfig, "erigon", "<dir>")
	require.ErrorContains(t, err, "capcli seg regime set archive")
	if !strings.Contains(err.Error(), "capcli seg reset") {
		t.Fatalf("missing reset: %s", err)
	}
}
