package handler

import (
	"net/http"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/utils"
)

type genesisResponse struct {
	GenesisTime          uint64           `json:"genesis_time,string"`
	GenesisValidatorRoot common.Hash      `json:"genesis_validators_root"`
	GenesisForkVersion   libcommon.Bytes4 `json:"genesis_fork_version"`
}

func (a *ApiHandler) GetEthV1BeaconGenesis(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(&genesisResponse{
		GenesisTime:          a.ethClock.GenesisTime(),
		GenesisValidatorRoot: a.ethClock.GenesisValidatorsRoot(),
		GenesisForkVersion:   utils.Uint32ToBytes4(uint32(a.beaconChainCfg.GenesisForkVersion)),
	}), nil
}
