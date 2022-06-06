package consensusconfig

import (
	_ "embed"

	"github.com/ledgerwatch/erigon/params/networkname"
)

//go:embed poasokol.json
var Sokol []byte

func GetConfigByChain(chainName string) []byte {
	switch chainName {
	case networkname.SokolChainName:
		return Sokol
	default:
		return Sokol
	}
}
