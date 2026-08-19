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

import "github.com/erigontech/erigon/cl/clparams"

// IsDataAvailabilityRequired reports whether peers are required to serve a Fulu
// block's columns. Older Fulu blocks fall outside the protocol request window.
// This helper is Fulu-specific; other fork paths enforce their own DA rules.
func IsDataAvailabilityRequired(cfg *clparams.BeaconChainConfig, currentSlot, blockSlot uint64, version clparams.StateVersion) bool {
	if version != clparams.FuluVersion {
		return false
	}
	if cfg.SlotsPerEpoch == 0 {
		return true
	}
	currentEpoch := currentSlot / cfg.SlotsPerEpoch
	blockEpoch := blockSlot / cfg.SlotsPerEpoch
	return blockEpoch >= currentEpoch || currentEpoch-blockEpoch <= cfg.MinEpochsForDataColumnSidecarsRequests
}
