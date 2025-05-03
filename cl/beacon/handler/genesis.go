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
	"net/http"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/utils"
)

type genesisResponse struct {
	GenesisTime          uint64        `json:"genesis_time,string"`
	GenesisValidatorRoot common.Hash   `json:"genesis_validators_root"`
	GenesisForkVersion   common.Bytes4 `json:"genesis_fork_version"`
}

func (a *ApiHandler) GetEthV1BeaconGenesis(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(&genesisResponse{
		GenesisTime:          a.ethClock.GenesisTime(),
		GenesisValidatorRoot: a.ethClock.GenesisValidatorsRoot(),
		GenesisForkVersion:   utils.Uint32ToBytes4(uint32(a.beaconChainCfg.GenesisForkVersion)),
	}), nil
}
