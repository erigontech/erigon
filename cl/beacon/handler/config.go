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
	"bytes"
	"net/http"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

func (a *ApiHandler) getSpec(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	outSpec := struct {
		*clparams.BeaconChainConfig
		*clparams.NetworkConfig
		MinEpochsForBlockRequests uint64 `json:"MIN_EPOCHS_FOR_BLOCK_REQUESTS,string"`
	}{
		BeaconChainConfig:         a.beaconChainCfg,
		NetworkConfig:             a.netConfig,
		MinEpochsForBlockRequests: a.beaconChainCfg.MinEpochsForBlockRequests(),
	}

	return newBeaconResponse(outSpec), nil
}

func (a *ApiHandler) getDepositContract(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(struct {
		ChainId         uint64 `json:"chain_id,string"`
		DepositContract string `json:"address"`
	}{ChainId: a.beaconChainCfg.DepositChainID, DepositContract: a.beaconChainCfg.DepositContractAddress}), nil

}

func (a *ApiHandler) getForkSchedule(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	response := []cltypes.Fork{}
	// create first response (unordered and incomplete)
	for currentVersion, entry := range a.beaconChainCfg.ForkVersionSchedule {
		response = append(response, cltypes.Fork{
			CurrentVersion: currentVersion,
			Epoch:          entry.Epoch,
		})
	}
	// Sort the responses by epoch
	sort.Slice(response, func(i, j int) bool {
		if response[i].Epoch == response[j].Epoch {
			return bytes.Compare(response[i].CurrentVersion[:], response[j].CurrentVersion[:]) < 0
		}
		return response[i].Epoch < response[j].Epoch
	})
	var previousVersion common.Bytes4
	for i := range response {
		response[i].PreviousVersion = previousVersion
		previousVersion = response[i].CurrentVersion
	}
	return newBeaconResponse(response), nil
}
