package epbs

import "github.com/erigontech/erigon/cl/clparams"

func init() {
	cfg := clparams.MainnetBeaconConfig
	clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
}
