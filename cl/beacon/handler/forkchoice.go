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

package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
)

func (a *ApiHandler) GetEthV2DebugBeaconHeads(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	if a.syncedData.Syncing() {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("beacon node is syncing"))
	}
	root, slot, statusCode, err := a.getHead()
	if err != nil {
		return nil, beaconhttp.NewEndpointError(statusCode, err)
	}
	return newBeaconResponse(
		[]interface{}{
			map[string]interface{}{
				"slot":                 strconv.FormatUint(slot, 10),
				"root":                 root,
				"execution_optimistic": false,
			},
		}), nil
}

func (a *ApiHandler) GetEthV1DebugBeaconForkChoice(w http.ResponseWriter, r *http.Request) {
	justifiedCheckpoint := a.forkchoiceStore.JustifiedCheckpoint()
	finalizedCheckpoint := a.forkchoiceStore.FinalizedCheckpoint()
	forkNodes := a.forkchoiceStore.ForkNodes()
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"justified_checkpoint": justifiedCheckpoint,
		"finalized_checkpoint": finalizedCheckpoint,
		"fork_choice_nodes":    forkNodes,
	}); err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
}
