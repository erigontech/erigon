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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
)

func CaplinFiles(items snapcfg.PreverifiedItems, retention clparams.CaplinRetention, beaconCfg *clparams.BeaconChainConfig, headSlot uint64) (snapcfg.PreverifiedItems, error) {
	normalized, err := retention.Normalize(beaconCfg)
	if err != nil {
		return nil, err
	}
	retention = normalized
	if retention.Mode == clparams.CaplinPruneModeMinimal || retention.Mode == clparams.CaplinPruneModeTip {
		return snapcfg.PreverifiedItems{}, nil
	}
	if retention.Mode == clparams.CaplinPruneModeArchive {
		return filterCaplinFiles(items, clparams.CaplinRetentionFloors{}, func(_ CaplinFile, _ clparams.CaplinRetentionFloors) bool { return true }), nil
	}
	floors, err := retention.Floors(beaconCfg, headSlot)
	if err != nil {
		return nil, err
	}
	keepState := retention.States.Kind != clparams.CaplinFloorDistance || retention.States.Value != 0
	return filterCaplinFiles(items, floors, func(file CaplinFile, floors clparams.CaplinRetentionFloors) bool {
		switch file.Family {
		case CaplinBlocks:
			return file.To > floors.Blocks
		case CaplinSidecars:
			return file.To > floors.Sidecars
		case CaplinStates:
			if !keepState {
				return false
			}
			return file.StateEvents || file.To > floors.States
		default:
			return false
		}
	}), nil
}

type CaplinFileFamily uint8

const (
	CaplinBlocks CaplinFileFamily = iota
	CaplinSidecars
	CaplinStates
)

type CaplinFile struct {
	Family      CaplinFileFamily
	From        uint64
	To          uint64
	StateEvents bool
}

func filterCaplinFiles(items snapcfg.PreverifiedItems, floors clparams.CaplinRetentionFloors, keep func(CaplinFile, clparams.CaplinRetentionFloors) bool) snapcfg.PreverifiedItems {
	result := make(snapcfg.PreverifiedItems, 0, len(items))
	for _, item := range items {
		info, _, ok := snaptype.ParseFileName("", item.Name)
		if !ok {
			continue
		}
		file, ok := CaplinFileInfo(item.Name, info)
		if ok && keep(file, floors) {
			result = append(result, item)
		}
	}
	return result
}

func CaplinFileInfo(name string, info snaptype.FileInfo) (CaplinFile, bool) {
	if info.Type != nil {
		switch info.Type.Enum() {
		case snaptype.BeaconBlocks.Enum():
			return CaplinFile{Family: CaplinBlocks, From: info.From, To: info.To}, true
		case snaptype.BlobSidecars.Enum():
			return CaplinFile{Family: CaplinSidecars, From: info.From, To: info.To}, true
		}
		if info.Type.Enum() >= snaptype.MinCaplinStateEnum && info.Type.Enum() < snaptype.MaxCaplinEnum {
			return CaplinFile{Family: CaplinStates, From: info.From, To: info.To, StateEvents: info.Type.Enum() == snaptype.StateEvents.Enum()}, true
		}
	}
	if IsBlobSidecarFile(name) && info.TypeString == snaptype.CaplinIndexes.BlobSidecarSlot.Name {
		return CaplinFile{Family: CaplinSidecars, From: info.From, To: info.To}, true
	}
	return CaplinFile{}, false
}
