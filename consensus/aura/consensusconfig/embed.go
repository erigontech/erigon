package consensusconfig

import (
	_ "embed"

	"github.com/ledgerwatch/erigon/params/networkname"
)

//go:embed poasokol.json
var Sokol []byte

//go:embed poagnosis.json
var Gnosis []byte

func GetConfigByChain(chainName string) []byte {
	switch chainName {
	case networkname.SokolChainName:
		return Sokol
	case networkname.GnosisChainName:
		return Gnosis
	default:
		return Sokol
	}
}
