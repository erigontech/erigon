package consensusconfig

import (
	_ "embed"

	"github.com/ledgerwatch/erigon/params/networkname"
)

//go:embed poasokol.json
var Sokol []byte

//go:embed poagnosis.json
var Gnosis []byte

//go:embed poachiado.json
var Chiado []byte

//go:embed gnosis_withdrawals_devnet_2.json
var GnosisWithdrawalsDevnet2 []byte

//go:embed test.json
var Test []byte

func GetConfigByChain(chainName string) []byte {
	switch chainName {
	case networkname.SokolChainName:
		return Sokol
	case networkname.GnosisChainName:
		return Gnosis
	case networkname.ChiadoChainName:
		return Chiado
	case networkname.GnosisWithdrawalsDevnet2Name:
		return GnosisWithdrawalsDevnet2
	default:
		return Test
	}
}
