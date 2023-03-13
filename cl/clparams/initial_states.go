package clparams

import (
	_ "embed"
)

// go:embed mainnet.state.ssz
var mainnetStateSSZ []byte

func getMainnetGenesisState