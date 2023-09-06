package handler

import (
	"errors"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/fork"
)

type genesisReponse struct {
	GenesisTime          uint64           `json:"genesis_time,omitempty"`
	GenesisValidatorRoot common.Hash      `json:"genesis_validator_root,omitempty"`
	GenesisForkVersion   libcommon.Bytes4 `json:"genesis_fork_version,omitempty"`
}

func (a *ApiHandler) getGenesis(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	if a.genesisCfg == nil {
		err = errors.New("Genesis Config is missing")
		httpStatus = http.StatusNotFound
		return
	}

	digest, err := fork.ComputeForkDigest(a.beaconChainCfg, a.genesisCfg)
	if err != nil {
		err = errors.New("Failed to compute fork digest")
		httpStatus = http.StatusInternalServerError
		return
	}

	data = &genesisReponse{
		GenesisTime:          a.genesisCfg.GenesisTime,
		GenesisValidatorRoot: a.genesisCfg.GenesisValidatorRoot,
		GenesisForkVersion:   digest,
	}
	httpStatus = http.StatusAccepted
	return
}
