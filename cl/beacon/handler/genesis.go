package handler

import (
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/fork"
)

type genesisResponse struct {
	GenesisTime          uint64           `json:"genesis_time,omitempty,string"`
	GenesisValidatorRoot common.Hash      `json:"genesis_validators_root,omitempty"`
	GenesisForkVersion   libcommon.Bytes4 `json:"genesis_fork_version,omitempty"`
}

func (a *ApiHandler) getGenesis(r *http.Request) (*beaconResponse, error) {
	if a.genesisCfg == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "Genesis Config is missing")
	}

	digest, err := fork.ComputeForkDigest(a.beaconChainCfg, a.genesisCfg)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(&genesisResponse{
		GenesisTime:          a.genesisCfg.GenesisTime,
		GenesisValidatorRoot: a.genesisCfg.GenesisValidatorRoot,
		GenesisForkVersion:   digest,
	}), nil
}
