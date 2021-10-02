package consensusconfig

import (
	_ "embed"
	"github.com/ledgerwatch/erigon/params"
)

//go:embed poasokol.json
var Sokol []byte

//go:embed kovan.json
var Kovan []byte

//go:embed fermion.json
var Fermion []byte

func GetConfigByChain(chainName string) []byte {
	switch chainName {
	case params.SokolChainName:
		return Sokol
	case params.KovanChainName:
		return Kovan
	case params.FermionChainName:
		return Fermion
	default:
		return Sokol
	}
}
