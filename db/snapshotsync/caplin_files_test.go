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

package snapshotsync

import (
	"cmp"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
)

const caplinFilesHeadSlot = uint64(15_660_000)

func TestCaplinFiles(t *testing.T) {
	items := caplinManifest()
	beaconCfg := clparams.MainnetBeaconConfig
	blockMin := beaconCfg.MinEpochsForBlockRequests()
	sidecarMin := max(beaconCfg.MinEpochsForBlobSidecarsRequests, beaconCfg.MinEpochsForDataColumnSidecarsRequests)
	tests := []struct {
		name      string
		retention clparams.CaplinRetention
		want      []string
	}{
		{
			name:      "archive",
			retention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive},
			want:      manifestNames(items, isRegisteredCaplinManifestFile),
		},
		{
			name: "spec",
			retention: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModeSpec,
				Blocks:   clparams.CaplinDistanceFloor(blockMin),
				Sidecars: clparams.CaplinDistanceFloor(sidecarMin),
			},
			want: manifestNames(items, func(name string, info snaptype.FileInfo) bool {
				if info.Type != nil && info.Type.Enum() == snaptype.BeaconBlocks.Enum() {
					return info.To > 14_600_000
				}
				return isBlobManifestFile(name, info) && info.To > 15_520_000
			}),
		},
		{
			name: "pruned blocks all and sidecars distance",
			retention: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModePruned,
				Blocks:   clparams.CaplinAllFloor(),
				Sidecars: clparams.CaplinDistanceFloor(8_192),
				States:   clparams.CaplinDistanceFloor(0),
			},
			want: manifestNames(items, func(name string, info snaptype.FileInfo) bool {
				return info.Type != nil && info.Type.Enum() == snaptype.BeaconBlocks.Enum() || isBlobManifestFile(name, info) && info.To > 15_390_000
			}),
		},
		{
			name: "pruned finite state floor",
			retention: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModePruned,
				Blocks:   clparams.CaplinAllFloor(),
				Sidecars: clparams.CaplinDistanceFloor(8_192),
				States:   clparams.CaplinDistanceFloor(4_096),
			},
			want: manifestNames(items, func(name string, info snaptype.FileInfo) bool {
				if info.Type != nil && info.Type.Enum() == snaptype.BeaconBlocks.Enum() {
					return true
				}
				if isBlobManifestFile(name, info) {
					return info.To > 15_390_000
				}
				if isStateManifestFile(info) {
					return info.TypeString == snaptype.StateEvents.Name() || info.To > 15_527_424
				}
				return false
			}),
		},
		{
			name: "pruned states all",
			retention: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModePruned,
				Blocks:   clparams.CaplinAllFloor(),
				Sidecars: clparams.CaplinDistanceFloor(0),
				States:   clparams.CaplinAllFloor(),
			},
			want: manifestNames(items, func(name string, info snaptype.FileInfo) bool {
				if info.Type != nil && info.Type.Enum() == snaptype.BeaconBlocks.Enum() {
					return true
				}
				if isBlobManifestFile(name, info) {
					return info.To > 15_520_000
				}
				return isStateManifestFile(info) && info.To > 0
			}),
		},
		{name: "minimal", retention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeMinimal}, want: []string{}},
		{name: "tip", retention: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeTip}, want: []string{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := CaplinFiles(items, test.retention, &beaconCfg, caplinFilesHeadSlot)
			require.NoError(t, err)
			require.Equal(t, test.want, names(got))
		})
	}
}

func caplinManifest() snapcfg.PreverifiedItems {
	var items snapcfg.PreverifiedItems
	add := func(name string) { items = append(items, snapcfg.PreverifiedItem{Name: name}) }
	addType := func(typ snaptype.Type, from, to uint64) {
		prefix := "caplin/"
		if typ.Enum() == snaptype.BeaconBlocks.Enum() || typ.Enum() == snaptype.BlobSidecars.Enum() {
			prefix = ""
		}
		add(prefix + typ.FileName(typ.Versions().Current, from, to))
		for _, indexName := range typ.IdxFileNames(from, to) {
			add(prefix + indexName)
		}
	}
	for from := uint64(0); from < 10_000_000; from += 100_000 {
		addType(snaptype.BeaconBlocks, from, from+100_000)
	}
	for from := uint64(14_500_000); from < 15_000_000; from += 10_000 {
		addType(snaptype.BeaconBlocks, from, from+10_000)
	}
	for from := uint64(15_300_000); from < 15_380_000; from += 10_000 {
		addType(snaptype.BlobSidecars, from, from+10_000)
	}
	addType(snaptype.BlobSidecars, 15_385_000, 15_395_000)
	for from := uint64(15_400_000); from < 15_540_000; from += 10_000 {
		addType(snaptype.BlobSidecars, from, from+10_000)
	}
	addType(snaptype.BlobSidecars, 0, 10_000)
	addType(snaptype.StateRoot, 0, 10_500_000)
	addType(snaptype.EpochData, 0, 15_530_000)
	addType(snaptype.ValidatorBalance, 15_450_000, 15_500_000)
	addType(snaptype.ValidatorBalance, 15_500_000, 15_550_000)
	addType(snaptype.StateEvents, 0, 50_000)
	addType(snaptype.StateEvents, 50_000, 100_000)
	add("caplin/v1.1-000000-000100-BlockProposers.seg")
	add(snaptype2.Headers.FileName(snaptype2.Headers.Versions().Current, 0, 100_000))
	slices.SortFunc(items, func(a, b snapcfg.PreverifiedItem) int { return cmp.Compare(a.Name, b.Name) })
	return items
}

func manifestNames(items snapcfg.PreverifiedItems, keep func(string, snaptype.FileInfo) bool) []string {
	var out []string
	for _, item := range items {
		info, _, ok := snaptype.ParseFileName("", item.Name)
		if ok && keep(item.Name, info) {
			out = append(out, item.Name)
		}
	}
	slices.Sort(out)
	return out
}

func names(items snapcfg.PreverifiedItems) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.Name)
	}
	slices.Sort(out)
	return out
}

func isRegisteredCaplinManifestFile(name string, info snaptype.FileInfo) bool {
	return isBlobManifestFile(name, info) || info.Type != nil && snaptype.IsCaplinType(info.Type.Enum())
}

func isBlobManifestFile(name string, info snaptype.FileInfo) bool {
	return info.Type != nil && info.Type.Enum() == snaptype.BlobSidecars.Enum() || IsBlobSidecarFile(name) && info.TypeString == snaptype.CaplinIndexes.BlobSidecarSlot.Name
}

func isStateManifestFile(info snaptype.FileInfo) bool {
	return info.Type != nil && info.Type.Enum() >= snaptype.MinCaplinStateEnum && info.Type.Enum() < snaptype.MaxCaplinEnum
}
