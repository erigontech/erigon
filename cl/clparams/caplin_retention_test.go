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
	"fmt"
	"strings"
	"testing"

	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/stretchr/testify/require"
)

func TestCaplinRetentionFloors(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	retention := CaplinRetention{
		Mode:     CaplinPruneModePruned,
		Blocks:   CaplinDistanceFloor(100_000),
		Sidecars: CaplinDistanceFloor(8_192),
		States:   CaplinDistanceFloor(4_096),
	}

	floors, err := retention.Floors(&beaconCfg, 15_660_000)
	require.NoError(t, err)
	require.Equal(t, uint64(12_460_000), floors.Blocks)
	require.Equal(t, uint64(15_390_000), floors.Sidecars)
	require.Equal(t, uint64(15_527_424), floors.States)
}

func TestCaplinRetentionFloorsRejectNestingAfterRounding(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	retention := CaplinRetention{
		Mode:     CaplinPruneModePruned,
		Blocks:   CaplinDistanceFloor(100_000),
		Sidecars: CaplinDistanceFloor(200_000),
		States:   CaplinDistanceFloor(4_096),
	}

	_, err := retention.Floors(&beaconCfg, 15_660_000)
	require.EqualError(t, err, "sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars")
}

func TestCaplinRetentionFloorsValidateMinimums(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	for _, test := range []struct {
		name  string
		floor CaplinRetention
		want  string
	}{
		{
			name: "blocks below minimum",
			floor: CaplinRetention{
				Mode:     CaplinPruneModePruned,
				Blocks:   CaplinDistanceFloor(beaconCfg.MinEpochsForBlockRequests() - 1),
				Sidecars: CaplinDistanceFloor(beaconCfg.MinEpochsForBlobSidecarsRequests),
				States:   CaplinDistanceFloor(0),
			},
			want: "--caplin.prune.distance must be at least 33024 epochs",
		},
		{
			name: "sidecars below minimum",
			floor: CaplinRetention{
				Mode:     CaplinPruneModePruned,
				Blocks:   CaplinDistanceFloor(beaconCfg.MinEpochsForBlockRequests()),
				Sidecars: CaplinDistanceFloor(beaconCfg.MinEpochsForBlobSidecarsRequests - 1),
				States:   CaplinDistanceFloor(0),
			},
			want: "--caplin.prune.sidecars must be at least 4096 epochs",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.floor.Floors(&beaconCfg, 15_660_000)
			require.EqualError(t, err, test.want)
		})
	}

	retention := CaplinRetention{
		Mode:     CaplinPruneModePruned,
		Blocks:   CaplinDistanceFloor(beaconCfg.MinEpochsForBlockRequests()),
		Sidecars: CaplinDistanceFloor(beaconCfg.MinEpochsForBlobSidecarsRequests),
		States:   CaplinDistanceFloor(0),
	}
	_, err := retention.Floors(&beaconCfg, 15_660_000)
	require.NoError(t, err)
}

func TestParseCaplinRetention(t *testing.T) {
	for _, test := range []struct {
		name      string
		mode      string
		distance  string
		sidecars  string
		states    string
		want      CaplinRetention
		wantError string
	}{
		{
			name: "default",
			mode: "spec",
			want: CaplinRetention{Mode: CaplinPruneModeSpec},
		},
		{
			name:     "pruned knobs",
			mode:     "pruned",
			distance: "33024",
			sidecars: "all",
			states:   "0",
			want: CaplinRetention{
				Mode:     CaplinPruneModePruned,
				Blocks:   CaplinDistanceFloor(33_024),
				Sidecars: CaplinAllFloor(),
				States:   CaplinDistanceFloor(0),
			},
		},
		{
			name:      "zero block distance is explicit",
			mode:      "pruned",
			distance:  "0",
			wantError: "--caplin.prune.distance must be at least the chain's spec window",
		},
		{
			name:      "zero sidecar distance is explicit",
			mode:      "pruned",
			sidecars:  "0",
			wantError: "--caplin.prune.sidecars must be at least the chain's spec window",
		},
		{
			name:      "knob outside pruned",
			mode:      "archive",
			distance:  "1",
			wantError: "--caplin.prune.distance can only be used with --caplin.prune.mode=pruned",
		},
		{
			name:      "minimal is reserved",
			mode:      "minimal",
			wantError: "caplin prune mode minimal is available in a later version",
		},
		{
			name:      "tip is reserved",
			mode:      "tip",
			wantError: "caplin prune mode tip is available in a later version",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseCaplinRetention(test.mode, test.distance, test.sidecars, test.states)
			if test.wantError != "" {
				require.EqualError(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestLegacyRetentionReaders(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	blockMin := caplinBlockRetentionMinimum(&beaconCfg)
	sidecarMin := caplinSidecarRetentionMinimum(&beaconCfg)
	for _, test := range []struct {
		name  string
		cfg   CaplinConfig
		want  CaplinRetention
		exact bool
	}{
		{name: "none", want: CaplinRetention{Mode: CaplinPruneModeSpec, Blocks: CaplinDistanceFloor(blockMin), Sidecars: CaplinDistanceFloor(sidecarMin)}, exact: true},
		{
			name: "blocks archive",
			cfg:  CaplinConfig{ArchiveBlocks: true},
			want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinDistanceFloor(sidecarMin)}, exact: true,
		},
		{
			name: "states archive",
			cfg:  CaplinConfig{ArchiveBlocks: true, ArchiveStates: true},
			want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinDistanceFloor(sidecarMin), States: CaplinAllFloor()}, exact: true,
		},
		{
			name: "blobs archive",
			cfg:  CaplinConfig{ArchiveBlocks: true, ArchiveBlobs: true},
			want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinAllFloor()}, exact: true,
		},
		{
			name: "full archive",
			cfg:  CaplinConfig{ArchiveBlocks: true, ArchiveBlobs: true, ArchiveStates: true},
			want: CaplinRetention{Mode: CaplinPruneModeArchive, Blocks: CaplinAllFloor(), Sidecars: CaplinAllFloor(), States: CaplinAllFloor()}, exact: true,
		},
		{
			name: "bounded blobs",
			cfg:  CaplinConfig{ArchiveBlocks: true, ArchiveBlobs: true, BlobsArchiveFromSlot: 12345},
			want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinSlotFloor(12345)}, exact: true,
		},
		{
			name: "columns aligned",
			cfg:  CaplinConfig{ColumnKeepSlots: sidecarMin * beaconCfg.SlotsPerEpoch},
			want: CaplinRetention{Mode: CaplinPruneModeSpec, Blocks: CaplinDistanceFloor(blockMin), Sidecars: CaplinDistanceFloor(sidecarMin)}, exact: false,
		},
		{
			name: "columns below window",
			cfg:  CaplinConfig{ColumnKeepSlots: 1},
			want: CaplinRetention{Mode: CaplinPruneModeSpec, Blocks: CaplinDistanceFloor(blockMin), Sidecars: CaplinDistanceFloor(sidecarMin)}, exact: false,
		},
		{
			name: "columns unaligned",
			cfg:  CaplinConfig{ColumnKeepSlots: sidecarMin*beaconCfg.SlotsPerEpoch + 1},
			want: CaplinRetention{Mode: CaplinPruneModeSpec, Blocks: CaplinDistanceFloor(blockMin), Sidecars: CaplinDistanceFloor(sidecarMin)}, exact: false,
		},
		{
			name: "no pruning with blobs archive",
			cfg:  CaplinConfig{ArchiveBlocks: true, ArchiveBlobs: true, BlobPruningDisabled: true},
			want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinAllFloor()}, exact: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, exact, err := LegacyRetention(test.cfg, &beaconCfg)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			require.Equal(t, test.exact, exact)
		})
	}
}

func TestLegacyRetentionRejectsUnnestedNoPruning(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	_, _, err := LegacyRetention(CaplinConfig{BlobPruningDisabled: true}, &beaconCfg)
	require.EqualError(t, err, "--caplin.blobs-no-pruning requires --caplin.blocks-archive; add --caplin.blocks-archive, or use --caplin.blobs-archive")
}

func TestLegacyRetentionFlagMatrix(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	sidecarMin := max(beaconCfg.MinEpochsForBlobSidecarsRequests, beaconCfg.MinEpochsForDataColumnSidecarsRequests)
	columnValues := []uint64{0, sidecarMin * beaconCfg.SlotsPerEpoch, 1, sidecarMin*beaconCfg.SlotsPerEpoch + 1}
	for bits := range 16 {
		blocks := bits&1 != 0
		states := bits&2 != 0
		blobs := bits&4 != 0
		noPruning := bits&8 != 0
		for _, fromSlot := range []struct {
			set   bool
			value uint64
		}{{}, {set: true, value: 0}, {set: true, value: 12345}} {
			for _, columns := range columnValues {
				for _, snapgen := range []bool{false, true} {
					if fromSlot.value > 0 && snapgen {
						continue
					}
					name := fmt.Sprintf("%d/%t/%d/%t", bits, fromSlot.set, columns, snapgen)
					t.Run(name, func(t *testing.T) {
						archiveBlobs := blobs || fromSlot.set
						cfg := CaplinConfig{
							ArchiveBlocks:       blocks || states || archiveBlobs,
							ArchiveBlobs:        archiveBlobs,
							ArchiveStates:       states,
							BlobPruningDisabled: noPruning,
							BlobsArchiveFromSlot: func() uint64 {
								if fromSlot.set {
									return fromSlot.value
								}
								return 0
							}(),
							ColumnKeepSlots:           columns,
							SnapshotGenerationEnabled: snapgen,
						}
						retention, exact, err := LegacyRetention(cfg, &beaconCfg)
						if noPruning && !cfg.ArchiveBlocks {
							require.EqualError(t, err, "--caplin.blobs-no-pruning requires --caplin.blocks-archive; add --caplin.blocks-archive, or use --caplin.blobs-archive")
							return
						}
						require.NoError(t, err)
						mapped := cfg
						applyErr := ApplyRetention(retention, &beaconCfg, &mapped)
						wantExact := applyErr == nil && caplinRetentionReadersEqual(cfg, mapped, &beaconCfg)
						require.Equal(t, wantExact, exact)
					})
				}
			}
		}
	}
}

func TestApplyRetentionRoundTrip(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	blockMin := caplinBlockRetentionMinimum(&beaconCfg)
	sidecarMin := caplinSidecarRetentionMinimum(&beaconCfg)
	for _, test := range []struct {
		name string
		want CaplinRetention
	}{
		{name: "spec", want: CaplinRetention{Mode: CaplinPruneModeSpec, Blocks: CaplinDistanceFloor(blockMin), Sidecars: CaplinDistanceFloor(sidecarMin)}},
		{name: "archive", want: CaplinRetention{Mode: CaplinPruneModeArchive, Blocks: CaplinAllFloor(), Sidecars: CaplinAllFloor(), States: CaplinAllFloor()}},
		{name: "blocks", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinDistanceFloor(sidecarMin)}},
		{name: "states", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinDistanceFloor(sidecarMin), States: CaplinAllFloor()}},
		{name: "columns", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinDistanceFloor(33024), Sidecars: CaplinDistanceFloor(8192)}},
		{name: "bounded blobs", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinSlotFloor(12345)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := CaplinConfig{}
			require.NoError(t, ApplyRetention(test.want, &beaconCfg, &cfg))
			got, exact, err := LegacyRetention(cfg, &beaconCfg)
			require.NoError(t, err)
			require.True(t, exact)
			require.Equal(t, test.want, got)
		})
	}
}

func TestApplyRetentionRefusals(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	for _, test := range []struct {
		name string
		want CaplinRetention
		text string
	}{
		{name: "minimal", want: CaplinRetention{Mode: CaplinPruneModeMinimal}, text: "caplin prune mode minimal is available in a later version"},
		{name: "tip", want: CaplinRetention{Mode: CaplinPruneModeTip}, text: "caplin prune mode tip is available in a later version"},
		{name: "longer blocks", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinDistanceFloor(33025)}, text: "caplin prune distance is available in a later version"},
		{name: "sidecars below blocks", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinDistanceFloor(33024), Sidecars: CaplinAllFloor()}, text: "sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars"},
		{name: "states below blocks", want: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinDistanceFloor(33024), States: CaplinAllFloor()}, text: "states reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.states"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ApplyRetention(test.want, &beaconCfg, &CaplinConfig{})
			require.EqualError(t, err, test.text)
		})
	}
}

func TestCaplinRetentionStrings(t *testing.T) {
	require.Equal(t, "archive", CaplinPruneModeArchive.String())
	require.Equal(t, "pruned", CaplinPruneModePruned.String())
	require.Equal(t, "spec", CaplinPruneModeSpec.String())
	require.Equal(t, "minimal", CaplinPruneModeMinimal.String())
	require.Equal(t, "tip", CaplinPruneModeTip.String())
	for _, value := range []string{"archive", "pruned", "spec"} {
		mode, err := ParseCaplinPruneMode(value)
		require.NoError(t, err)
		require.Equal(t, value, mode.String())
	}
	_, err := ParseCaplinPruneMode("unknown")
	require.True(t, strings.Contains(err.Error(), "caplin.prune.mode"))
}

func TestResolveBeaconConfig(t *testing.T) {
	beaconCfg, err := ResolveBeaconConfig(uint64(chainspec.MainnetChainID), "", false)
	require.NoError(t, err)
	require.Equal(t, MainnetBeaconConfig.SlotsPerEpoch, beaconCfg.SlotsPerEpoch)

	_, err = ResolveBeaconConfig(123456, "", false)
	require.EqualError(t, err, "beacon chain config not found for network 123456")
}

func TestExactColumnRetentionBeyondSample(t *testing.T) {
	chain := &MainnetBeaconConfig
	legacy := CaplinConfig{ArchiveBlocks: true, ColumnKeepSlots: 20_000_001}
	r, exact, err := LegacyRetention(legacy, chain)
	if err != nil || !exact {
		return
	}
	var mapped CaplinConfig
	if err := ApplyRetention(r, chain, &mapped); err != nil {
		t.Fatal(err)
	}
	head := legacy.ColumnKeepSlots
	got, want := mapped.ColumnRetentionFloor(chain, head), legacy.ColumnRetentionFloor(chain, head)
	if got != want {
		t.Fatalf("exact=true at slot %d: mapped column floor %d, legacy %d", head, got, want)
	}
}

func TestParsedRetentionRoundTrip(t *testing.T) {
	for _, tc := range []struct{ mode, blocks, sidecars, states string }{
		{"archive", "", "", ""},
		{"pruned", "", "", ""},
		{"pruned", "", "8192", ""},
		{"pruned", "all", "all", "all"},
	} {
		t.Run(tc.mode+"_"+tc.blocks+"_"+tc.sidecars+"_"+tc.states, func(t *testing.T) {
			r, err := ParseCaplinRetention(tc.mode, tc.blocks, tc.sidecars, tc.states)
			if err != nil {
				t.Fatal(err)
			}
			var cfg CaplinConfig
			if err := ApplyRetention(r, &MainnetBeaconConfig, &cfg); err != nil {
				t.Fatal(err)
			}
			want, err := r.Normalize(&MainnetBeaconConfig)
			if err != nil {
				t.Fatal(err)
			}
			got, exact, err := LegacyRetention(cfg, &MainnetBeaconConfig)
			if err != nil || !exact || got != want {
				t.Fatalf("requested %s %s, recovered %s %s (exact=%t err=%v)", want.Mode, want, got.Mode, got, exact, err)
			}
		})
	}
}

func TestFixedZeroSidecarFloor(t *testing.T) {
	r := CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinSlotFloor(0)}
	var cfg CaplinConfig
	if err := ApplyRetention(r, &MainnetBeaconConfig, &cfg); err != nil {
		return
	}
	if got := cfg.BlobRetentionFloor(&MainnetBeaconConfig, 15_660_000); got != 0 {
		t.Fatalf("accepted fixed slot 0: blob retention floor %d, want 0", got)
	}
}

func TestCaplinRetentionSlotParser(t *testing.T) {
	retention, err := ParseCaplinRetentionWithSlots("pruned", "all", "slot:14000000", "0", true, true, true)
	require.NoError(t, err)
	require.Equal(t, CaplinSlotFloor(14_000_000), retention.Sidecars)
	_, err = ParseCaplinRetention("pruned", "all", "slot:14000000", "0", true, true, true)
	require.EqualError(t, err, `invalid --caplin.prune.sidecars value "slot:14000000": expected an epoch count or all`)
}

func TestCaplinRetentionLegacyFlags(t *testing.T) {
	beaconCfg := MainnetBeaconConfig
	for _, test := range []struct {
		name      string
		retention CaplinRetention
		want      []string
	}{
		{name: "blocks", retention: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor()}, want: []string{"--caplin.blocks-archive"}},
		{name: "archive", retention: CaplinRetention{Mode: CaplinPruneModeArchive}, want: []string{"--caplin.blobs-archive", "--caplin.states-archive"}},
		{name: "fixed blobs", retention: CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinSlotFloor(14_000_000)}, want: []string{"--caplin.blocks-archive", "--caplin.blobs-archive-from-slot=14000000"}},
		{name: "none", retention: CaplinRetention{Mode: CaplinPruneModeSpec}, want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, exact, err := LegacyRetentionFlags(test.retention, &beaconCfg)
			require.NoError(t, err)
			require.True(t, exact)
			require.Equal(t, test.want, got)
		})
	}
}

func TestLegacyBoundedBlobsWithArchivedStates(t *testing.T) {
	cfg := CaplinConfig{ArchiveBlocks: true, ArchiveStates: true, ArchiveBlobs: true, BlobsArchiveFromSlot: 12345}
	want := CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinAllFloor(), Sidecars: CaplinSlotFloor(12345), States: CaplinAllFloor()}
	var check CaplinConfig
	if err := ApplyRetention(want, &MainnetBeaconConfig, &check); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, cfg, check)
	got, exact, err := LegacyRetention(cfg, &MainnetBeaconConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || got != want {
		t.Fatalf("bounded blobs with archived states: mode=%s sidecars=%s exact=%t, want pruned/slot 12345/true", got.Mode, got.Sidecars, exact)
	}
}

func TestAcceptedBloatnetSpecHasFloors(t *testing.T) {
	_, c := GetConfigsByNetwork(chainspec.BloatnetNetworkID)
	r := CaplinRetention{Mode: CaplinPruneModeSpec}
	if err := ApplyRetention(r, c, &CaplinConfig{}); err != nil {
		return
	}
	if _, err := r.Floors(c, 200_000); err != nil {
		t.Fatalf("accepted spec, block window %d and blob window %d: %v", c.MinEpochsForBlockRequests(), c.MinEpochsForBlobSidecarsRequests, err)
	}
}
