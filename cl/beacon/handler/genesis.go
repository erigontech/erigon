package handler

import (
	"errors"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/beacon/types"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/fork"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type genesisReponse struct {
	GenesisTime          uint64       `json:"genesis_time,omitempty"`
	GenesisValidatorRoot common.Hash  `json:"genesis_validator_root,omitempty"`
	GenesisForkVersion   types.Bytes4 `json:"genesis_fork_version,omitempty"`
}

func (g *genesisReponse) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, g.GenesisTime, g.GenesisValidatorRoot[:], g.GenesisForkVersion[:])
}

func (g *genesisReponse) EncodingSizeSSZ() int {
	return 44
}

func (a *ApiHandler) getGenesis(r *http.Request) (data ssz.Marshaler, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
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
		GenesisForkVersion:   types.Bytes4(digest),
	}
	httpStatus = http.StatusAccepted
	return
}
