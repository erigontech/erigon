package chain

import "github.com/ledgerwatch/erigon-lib/chain/networkname"

var chainIds = []uint64{
	195,    // xlayer-testnet
	196,    // xlayer-mainet
	1101,   // mainnet
	2440,   // cardona internal
	2442,   // cardona
	10010,  //etrog testnet
	999999, // local devnet
	123,    // estestnet
}

var chainIdToName = map[uint64]string{
	195:    networkname.XLayerTestnetChainName,
	196:    networkname.XLayerMainnetChainName,
	1101:   networkname.HermezMainnetChainName,
	2440:   networkname.HermezBaliChainName,
	2442:   networkname.HermezCardonaChainName,
	10010:  networkname.HermezEtrogChainName,
	999999: networkname.HermezLocalDevnetChainName,
	123:    networkname.HermezESTestChainName,
}

func IsZk(chainId uint64) bool {
	for _, validId := range chainIds {
		if chainId == validId {
			return true
		}
	}
	return false
}

func GetChainName(chainId uint64) string {
	return chainIdToName[chainId]
}

func IsTestnet(chainId uint64) bool {
	return chainId == 1442
}

func IsDevnet(chainId uint64) bool {
	return chainId == 1440
}

func CheckForkOrder() error {
	return nil
}

func SetDynamicChainDetails(chainId uint64, chainName string) {
	chainIdToName[chainId] = chainName
	chainIds = append(chainIds, chainId)
}
