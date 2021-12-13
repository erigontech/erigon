package snapshothashes

import (
	_ "embed"
	"encoding/json"

	"github.com/ledgerwatch/erigon/params/networkname"
)

//go:embed erigon-snapshots/mainnet.json
var mainnet []byte
var Mainnet = fromJson(mainnet)

//go:embed erigon-snapshots/goerli.json
var goerli []byte
var Goerli = fromJson(goerli)

type Preverified map[string]string

func fromJson(in []byte) (out Preverified) {
	if err := json.Unmarshal(in, &out); err != nil {
		panic(err)
	}
	return out
}

var (
	MainnetChainSnapshotConfig = &Config{}
	GoerliChainSnapshotConfig  = &Config{
		ExpectBlocks: 5_900_000 - 1,
		Preverified:  Goerli,
	}
)

type Config struct {
	ExpectBlocks uint64
	Preverified  Preverified
}

func KnownSnapshots(networkName string) *Config {
	switch networkName {
	case networkname.MainnetChainName:
		return MainnetChainSnapshotConfig
	case networkname.GoerliChainName:
		return GoerliChainSnapshotConfig
	default:
		return nil
	}
}
