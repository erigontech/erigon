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

package jsonrpc

import (
	"github.com/erigontech/erigon/common/hexutil"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
)

// BuilderAPI reports the embedded ePBS builder state.
type BuilderAPI interface {
	Status() BuilderStatus
}

type BuilderStatus struct {
	Enabled                 bool           `json:"enabled"`
	Phase                   string         `json:"phase"`
	Reason                  string         `json:"reason,omitempty"`
	LastAttemptSlot         hexutil.Uint64 `json:"lastAttemptSlot"`
	LastBidSlot             hexutil.Uint64 `json:"lastBidSlot"`
	LastBidValueGwei        hexutil.Uint64 `json:"lastBidValueGwei"`
	LastOutcomeSlot         hexutil.Uint64 `json:"lastOutcomeSlot"`
	LastOutcome             string         `json:"lastOutcome,omitempty"`
	AvailableCollateralGwei hexutil.Uint64 `json:"availableCollateralGwei"`
}

type BuilderAPIImpl struct {
	status *executionbuilder.EmbeddedBuilderStatus
}

func NewBuilderAPI(status *executionbuilder.EmbeddedBuilderStatus) *BuilderAPIImpl {
	return &BuilderAPIImpl{status: status}
}

func (api *BuilderAPIImpl) Status() BuilderStatus {
	runtime := api.status.Snapshot()
	return BuilderStatus{
		Enabled: runtime.Enabled, Phase: runtime.Phase, Reason: runtime.Reason,
		LastAttemptSlot: hexutil.Uint64(runtime.LastAttemptSlot), LastBidSlot: hexutil.Uint64(runtime.LastBidSlot),
		LastBidValueGwei: hexutil.Uint64(runtime.LastBidValueGwei), LastOutcomeSlot: hexutil.Uint64(runtime.LastOutcomeSlot),
		LastOutcome:             runtime.LastOutcome,
		AvailableCollateralGwei: hexutil.Uint64(runtime.AvailableCollateralGwei),
	}
}
