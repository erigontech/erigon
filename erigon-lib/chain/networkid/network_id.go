package networkid

const (
	MainnetChainID    = 1
	HoleskyChainID    = 17000
	SepoliaChainID    = 11155111
	DevChainName      = 1337
	AmoyChainID       = 80002
	BorMainnetChainID = 137
	BorDevnetChainID  = 1337
	GnosisChainID     = 100
	ChiadoChainID     = 10200
	TestID            = 1337
)

var All = []uint64{
	MainnetChainID,
	HoleskyChainID,
	SepoliaChainID,
	AmoyChainID,
	BorMainnetChainID,
	BorDevnetChainID,
	GnosisChainID,
	ChiadoChainID,
	TestID,
}

var NetworkNameByID = map[uint64]string{
	MainnetChainID:    "mainnet",
	HoleskyChainID:    "holesky",
	SepoliaChainID:    "sepolia",
	AmoyChainID:       "amoy",
	BorMainnetChainID: "bor-mainnet",
	GnosisChainID:     "gnosis",
	ChiadoChainID:     "chiado",
}
