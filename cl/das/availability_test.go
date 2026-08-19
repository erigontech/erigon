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

package das

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestIsDataAvailabilityRequired(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	lastSlotInRetentionWindow := (cfg.MinEpochsForDataColumnSidecarsRequests+1)*cfg.SlotsPerEpoch - 1
	firstSlotAfterRetentionWindow := lastSlotInRetentionWindow + 1
	tests := []struct {
		name        string
		version     clparams.StateVersion
		currentSlot uint64
		blockSlot   uint64
		want        bool
	}{
		{name: "current Fulu block", version: clparams.FuluVersion, currentSlot: 100, blockSlot: 100, want: true},
		{name: "future Fulu block", version: clparams.FuluVersion, currentSlot: 100, blockSlot: 101, want: true},
		{name: "last slot in retention window", version: clparams.FuluVersion, currentSlot: lastSlotInRetentionWindow, blockSlot: 0, want: true},
		{name: "first slot after retention window", version: clparams.FuluVersion, currentSlot: firstSlotAfterRetentionWindow, blockSlot: 0},
		{name: "Electra", version: clparams.ElectraVersion, currentSlot: 100, blockSlot: 100},
		{name: "Gloas envelope owns availability", version: clparams.GloasVersion, currentSlot: 100, blockSlot: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsDataAvailabilityRequired(&cfg, tt.currentSlot, tt.blockSlot, tt.version))
		})
	}
}
