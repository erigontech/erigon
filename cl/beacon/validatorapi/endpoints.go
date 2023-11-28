package validatorapi

import (
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/fork"
)

func (v *ValidatorApiHandler) GetEthV1ConfigSpec(r *http.Request) (*clparams.BeaconChainConfig, error) {
	if v.BeaconChainCfg == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "beacon config not found")
	}
	return v.BeaconChainCfg, nil
}

func (v *ValidatorApiHandler) GetEthV1BeaconGenesis(r *http.Request) (any, error) {
	if v.GenesisCfg == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "beacon config not found")
	}
	digest, err := fork.ComputeForkDigest(v.BeaconChainCfg, v.GenesisCfg)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err.Error())
	}
	return map[string]any{
		"genesis_time":           v.GenesisCfg.GenesisTime,
		"genesis_validator_root": v.GenesisCfg.GenesisValidatorRoot,
		"genesis_fork_version":   digest,
	}, nil
}
