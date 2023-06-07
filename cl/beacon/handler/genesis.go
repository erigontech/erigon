package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/types"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/log/v3"
)

type genesisReponse struct {
	GenesisTime          uint64       `json:"genesis_time,omitempty"`
	GenesisValidatorRoot common.Hash  `json:"genesis_validator_root,omitempty"`
	GenesisForkVersion   types.Bytes4 `json:"genesis_fork_version,omitempty"`
}

func (a *ApiHandler) getGenesis(w http.ResponseWriter, _ *http.Request) {
	if a.genesisCfg == nil {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "Genesis Config is missing")
		return
	}

	digest, err := fork.ComputeForkDigest(a.beaconChainCfg, a.genesisCfg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "Failed to compute fork digest")
		log.Error("[Beacon API] genesis handler failed", err)
		return
	}

	w.Header().Set("Content-Type", "Application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(genesisReponse{
		GenesisTime:          a.genesisCfg.GenesisTime,
		GenesisValidatorRoot: a.genesisCfg.GenesisValidatorRoot,
		GenesisForkVersion:   types.Bytes4(digest),
	})
}
