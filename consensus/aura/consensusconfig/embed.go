package consensusconfig

import (
	_ "embed"

	"github.com/ledgerwatch/erigon/params/networkname"
)

//go:embed poasokol.json
var Sokol []byte

//go:embed kovan.json
var Kovan []byte

func GetConfigByChain(chainName string) []byte {
	switch chainName {
	case networkname.SokolChainName:
		return Sokol
	case networkname.KovanChainName:
		return Kovan
	default:
		return Sokol
	}
}
