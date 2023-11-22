package handler

import (
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/fork"
)

type genesisReponse struct {
	GenesisTime          uint64           `json:"genesis_time,omitempty"`
	GenesisValidatorRoot common.Hash      `json:"genesis_validator_root,omitempty"`
	GenesisForkVersion   libcommon.Bytes4 `json:"genesis_fork_version,omitempty"`
}

func (a *ApiHandler) getGenesis(r *http.Request) *beaconResponse {
	if a.genesisCfg == nil {
		return newApiErrorResponse(http.StatusNotFound, "Genesis Config is missing")
	}

	digest, err := fork.ComputeForkDigest(a.beaconChainCfg, a.genesisCfg)
	if err != nil {
		return newCriticalErrorResponse(err)
	}

	return newBeaconResponse(&genesisReponse{
		GenesisTime:          a.genesisCfg.GenesisTime,
		GenesisValidatorRoot: a.genesisCfg.GenesisValidatorRoot,
		GenesisForkVersion:   digest,
	})
}
