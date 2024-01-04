package zkchainconfig

var chainIds = []uint64{
	1101, // mainnet
	1440, // devnet
	1442, // testnet
	2440, // cardona internal
	2442, // cardona
}

func IsZk(chainId uint64) bool {
	for _, validId := range chainIds {
		if chainId == validId {
			return true
		}
	}
	return false
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
